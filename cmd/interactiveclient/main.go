// Command interactiveclient is a test application driven over MQTT. It runs
// the client's supervisor (Client.Run), so it reconnects on its own, and
// subscribes from OnConnect to two control topics under -root:
//
//	<root>/on     a non-zero number or true/on/yes switches publishing on;
//	              0, false/off/no or an empty payload switches it off. A
//	              retained value is applied on every connect.
//	<root>/reset  any (non-retained) message resets the sine wave and the
//	              step to 0, and publishes at once if publishing is on.
//
// While on, it publishes every -interval:
//
//	<root>/sin    amplitude*sin(2π·t/period), t the time since the reset
//	<root>/step   0, 1, 2, … (one more each interval)
//
// Examples:
//
//	interactiveclient
//	interactiveclient -v 5 -root lab/demo -interval 500ms -period 10s
//	mosquitto_pub -t interactiveclient/on -m 1 -r     # start, and stay on across restarts
//	mosquitto_pub -t interactiveclient/reset -m x     # back to 0
//	mosquitto_pub -t interactiveclient/on -m 0 -r     # stop
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
)

type config struct {
	conn      cli.ConnFlags
	reconnect cli.ReconnectFlags
	root      string
	subQoS    uint
	pubQoS    uint
	retain    bool
	interval  time.Duration
	period    time.Duration
	amplitude float64
	quiet     bool
}

func main() {
	os.Exit(run())
}

func run() int {
	var cfg config
	cfg.conn.Register(flag.CommandLine)
	cfg.reconnect.Register(flag.CommandLine)
	flag.StringVar(&cfg.root, "root", "interactiveclient", "topic root for on, reset, sin and step")
	flag.UintVar(&cfg.subQoS, "q", 1, "QoS for the on and reset subscriptions")
	flag.UintVar(&cfg.pubQoS, "pub-qos", 0, "QoS for sin and step")
	flag.BoolVar(&cfg.retain, "retain", false, "publish sin and step retained")
	flag.DurationVar(&cfg.interval, "interval", time.Second, "time between publications; the step grows by 1 each interval")
	flag.DurationVar(&cfg.period, "period", time.Minute, "period of the sine wave")
	flag.Float64Var(&cfg.amplitude, "amplitude", 1, "amplitude of the sine wave")
	flag.BoolVar(&cfg.quiet, "quiet", false, "do not print every publication")
	flag.Parse()

	if cfg.interval <= 0 || cfg.period <= 0 || cfg.subQoS > 2 || cfg.pubQoS > 2 || strings.ContainsAny(cfg.root, "+#") || cfg.root == "" {
		fmt.Fprintln(os.Stderr, "interactiveclient: -interval and -period must be positive, QoS 0-2, and -root a non-empty topic without wildcards")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	a := newApp(&cfg)
	if err := a.run(ctx); err != nil {
		cli.Logf("FAIL: %v", err)
		return 1
	}
	return 0
}

type app struct {
	cfg   *config
	s     *cli.Session
	on    atomic.Bool
	reset chan struct{} // a reset message arrived

	n           int // intervals since the last reset; owned by the main loop
	warnedNoNet bool
}

func newApp(cfg *config) *app {
	return &app{cfg: cfg, reset: make(chan struct{}, 1)}
}

func (a *app) topic(sub string) string { return a.cfg.root + "/" + sub }

func signal1(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (a *app) run(ctx context.Context) error {
	s, err := cli.NewSession(&a.cfg.conn, mqttclient.Handlers{
		OnConnect: a.onConnect,
		OnMessage: a.onMessage,
	}, a.cfg.reconnect.Apply)
	if err != nil {
		return err
	}
	a.s = s
	cli.Logf("control: %s, %s; output: %s, %s every %v", a.topic("on"), a.topic("reset"), a.topic("sin"), a.topic("step"), a.cfg.interval)

	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	ticker := time.NewTicker(a.cfg.interval)
	defer ticker.Stop()
	for {
		select {
		case err := <-runErr:
			return err
		case <-ticker.C:
			a.tick(ctx)
		case <-a.reset:
			a.n = 0
			cli.Logf("reset: sin and step back to 0")
			if a.on.Load() {
				a.tick(ctx)
				ticker.Reset(a.cfg.interval)
			}
		}
	}
}

// onConnect subscribes on every connection: subscriptions are not resent
// after a reconnect. A retained on value arrives right after the SUBACK.
func (a *app) onConnect(c *mqttclient.Client, _ mqttclient.ConnAck) {
	subs := []mqttclient.Subscription{
		{Topic: a.topic("on"), QoS: byte(a.cfg.subQoS)},
		{Topic: a.topic("reset"), QoS: byte(a.cfg.subQoS)},
	}
	p, err := c.Subscribe(context.Background(), subs, nil)
	if err != nil {
		cli.Logf("subscribe: %v", err)
		return
	}
	// Wait off the callback goroutine, so messages keep flowing.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), a.cfg.conn.Timeout)
		defer cancel()
		res, err := p.Wait(ctx)
		if res.ReasonCodes == nil {
			cli.Logf("SUBACK not received: %v", err)
			return
		}
		cli.Logf("SUBACK %s=%s %s=%s", subs[0].Topic, granted(res.ReasonCodes, 0), subs[1].Topic, granted(res.ReasonCodes, 1))
	}()
}

func granted(codes []byte, i int) string {
	if i >= len(codes) {
		return "missing"
	}
	if codes[i] >= 0x80 {
		return fmt.Sprintf("refused 0x%02x (%s)", codes[i], mqttclient.ReasonCodeString(codes[i]))
	}
	return fmt.Sprintf("QoS %d", codes[i])
}

func (a *app) onMessage(_ *mqttclient.Client, m *mqttclient.Message) {
	retained := ""
	if m.Retain {
		retained = " (retained)"
	}
	switch m.Topic {
	case a.topic("on"):
		on, err := parseOn(m.Payload)
		if err != nil {
			cli.Logf("%s%s: %v; ignored", m.Topic, retained, err)
			return
		}
		if a.on.Swap(on) != on || m.Retain {
			state := "off"
			if on {
				state = "on"
			}
			cli.Logf("%s=%q%s: publishing %s", m.Topic, m.Payload, retained, state)
		}
	case a.topic("reset"):
		if m.Retain {
			// A retained reset would reset on every reconnect.
			cli.Logf("%s: ignoring a retained reset message", m.Topic)
			return
		}
		signal1(a.reset)
	}
}

// parseOn reads an on payload: a number (non-zero is on), true/on/yes, or
// false/off/no/empty.
func parseOn(p []byte) (bool, error) {
	s := strings.TrimSpace(string(p))
	switch strings.ToLower(s) {
	case "true", "on", "yes":
		return true, nil
	case "false", "off", "no", "":
		return false, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return false, fmt.Errorf("payload %q is not a number or true/false", s)
	}
	return f != 0, nil
}

// tick publishes the current values if on and connected, then advances.
func (a *app) tick(ctx context.Context) {
	if !a.on.Load() {
		return
	}
	if !a.s.C.IsConnected() {
		if !a.warnedNoNet {
			cli.Logf("not connected; not publishing until reconnected")
			a.warnedNoNet = true
		}
		return
	}
	a.warnedNoNet = false

	t := time.Duration(a.n) * a.cfg.interval
	sin := a.cfg.amplitude * math.Sin(2*math.Pi*t.Seconds()/a.cfg.period.Seconds())
	sinText := strconv.FormatFloat(sin, 'f', 6, 64)
	stepText := strconv.Itoa(a.n)
	for _, out := range []struct{ topic, payload string }{{a.topic("sin"), sinText}, {a.topic("step"), stepText}} {
		pctx, cancel := context.WithTimeout(ctx, a.cfg.conn.Timeout)
		_, err := a.s.C.Publish(pctx, &mqttclient.Message{Topic: out.topic, Payload: []byte(out.payload), QoS: byte(a.cfg.pubQoS), Retain: a.cfg.retain})
		cancel()
		if err != nil {
			cli.Logf("publish %s: %v", out.topic, err)
			return
		}
	}
	if !a.cfg.quiet {
		cli.Logf("sin=%s step=%s", sinText, stepText)
	}
	a.n++
}

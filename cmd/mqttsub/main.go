// Command mqttsub is a manual test tool for subscribing with this client.
// It connects, optionally unsubscribes first (-U), subscribes to the -t
// topic filters, prints messages until -C messages have arrived, -W has
// passed or Ctrl-C, optionally unsubscribes (-unsub), then disconnects.
//
// With -config it reconnects, failing over between the servers in the
// file, and subscribes again after every reconnect.
//
// Examples:
//
//	mqttsub -t 'test/#'
//	mqttsub -v 5 -t 'test/#' -q 2 -C 10 -W 30s
//	mqttsub -v 5 -t test/a -no-local -retain-handling 2 -sub-id 7 -user k=v
//	mqttsub -id inbox -clean=false -t 'test/in/#' -q 1 -W 1s
//	mqttsub -id inbox -clean=false -U 'test/in/#' -W 1s
//	mqttsub -t test/big -verify ascii -quiet
//	mqttsub -config brokers.yaml -t 'test/#' -q 1
//	mqttsub -t test/big -verify binary -C 10 -W 1m -quiet -report -
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
)

type config struct {
	conn       cli.ConnFlags
	topics     cli.Strings
	unsubFirst cli.Strings
	qos        uint
	noLocal    bool
	rap        bool
	retainH    uint
	subID      uint
	user       cli.UserProps
	unsub      bool
	count      int64
	wait       time.Duration
	reason     uint
	quiet      bool
	verify     string
	verifySize int
	report     string
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	var cfg config
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.SetOutput(stderr)
	cfg.conn.Register(fs)
	fs.Var(&cfg.topics, "t", "topic filter to subscribe to (repeatable)")
	fs.Var(&cfg.unsubFirst, "U", "topic filter to unsubscribe from right after connecting (repeatable)")
	fs.UintVar(&cfg.qos, "q", 0, "requested QoS for every filter: 0, 1 or 2")
	fs.BoolVar(&cfg.noLocal, "no-local", false, "No Local option: do not receive this client's own messages (MQTT 5)")
	fs.BoolVar(&cfg.rap, "retain-as-published", false, "Retain As Published option (MQTT 5)")
	fs.UintVar(&cfg.retainH, "retain-handling", 0, "Retain Handling option: 0 send retained, 1 only for new subscriptions, 2 never (MQTT 5)")
	fs.UintVar(&cfg.subID, "sub-id", 0, "Subscription Identifier (MQTT 5)")
	fs.Var(&cfg.user, "user", "SUBSCRIBE user property key=value (MQTT 5, repeatable)")
	fs.BoolVar(&cfg.unsub, "unsub", false, "unsubscribe from the -t filters before disconnecting")
	fs.Int64Var(&cfg.count, "C", 0, "exit after this many messages (0: no limit)")
	fs.DurationVar(&cfg.wait, "W", 0, "exit after this long (0: until -C or Ctrl-C)")
	fs.UintVar(&cfg.reason, "reason", 0, "DISCONNECT reason code (MQTT 5 only)")
	fs.BoolVar(&cfg.quiet, "quiet", false, "print only the subscription results and the summary, not every message")
	fs.StringVar(&cfg.verify, "verify", "", "check every payload is this mqttpub -pattern ("+cli.PatternNames()+") and print its size")
	fs.IntVar(&cfg.verifySize, "verify-size", -1, "with -verify, also require payloads of exactly this many bytes")
	fs.StringVar(&cfg.report, "report", "", "write a JSON run report to this file on exit; - writes it to stdout and the log to stderr")
	_ = fs.Parse(args)
	rep := cli.NewReport("mqttsub", cfg.report, &cfg.conn, stdout, stderr)

	if err := cfg.check(); err != nil {
		fmt.Fprintln(stderr, "mqttsub:", err)
		return rep.Finish(2, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := subscribe(ctx, &cfg, rep); err != nil {
		cli.Logf("FAIL: %v", err)
		return rep.Finish(cli.ExitCode(err), err)
	}
	cli.Logf("OK")
	return rep.Finish(0, nil)
}

func (cfg *config) check() error {
	if len(cfg.topics) == 0 && len(cfg.unsubFirst) == 0 {
		return errors.New("give at least one -t (or -U) topic filter")
	}
	if cfg.unsub && len(cfg.topics) == 0 {
		return errors.New("-unsub needs -t")
	}
	v5only := cfg.noLocal || cfg.rap || cfg.retainH != 0 || cfg.subID != 0 || len(cfg.user) > 0 || cfg.reason != 0
	if v5only && cfg.conn.Version != "5" {
		return errors.New("-no-local, -retain-as-published, -retain-handling, -sub-id, -user and -reason require -v 5")
	}
	if cfg.verify != "" {
		if _, err := cli.MakePattern(cfg.verify, 0); err != nil {
			return err
		}
	} else if cfg.verifySize >= 0 {
		return errors.New("-verify-size needs -verify")
	}
	return nil
}

func (cfg *config) subscriptions() ([]mqttclient.Subscription, *mqttclient.Properties) {
	subs := make([]mqttclient.Subscription, len(cfg.topics))
	for i, t := range cfg.topics {
		subs[i] = mqttclient.Subscription{Topic: t, QoS: byte(cfg.qos), NoLocal: cfg.noLocal, RetainAsPublished: cfg.rap, RetainHandling: byte(cfg.retainH)}
	}
	if cfg.subID == 0 && len(cfg.user) == 0 {
		return subs, nil
	}
	props := &mqttclient.Properties{User: cfg.user}
	if cfg.subID != 0 {
		props.SubscriptionIdentifier = []int{int(cfg.subID)}
	}
	return subs, props
}

func subscribe(ctx context.Context, cfg *config, rep *cli.Report) error {
	var received, bad, connects atomic.Int64
	var badOffset, badMessage atomic.Int64 // of the first bad payload
	badOffset.Store(-1)
	if rep.Enabled() {
		defer func() {
			n := received.Load()
			rep.Counts.Received = n
			rep.Counts.VerifyFailed = bad.Load()
			rep.Verify = cli.ReportVerify{Pattern: cfg.verify, Size: cfg.verifySize, FirstBadOffset: int(badOffset.Load()), FirstBadMessage: badMessage.Load()}
			rep.ExitReason = fmt.Sprintf("received %d messages", n)
		}()
	}
	enough := make(chan struct{})
	s, err := cli.NewSession(&cfg.conn, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, ack mqttclient.ConnAck) {
			// Subscriptions are not resent after a reconnect, and a server
			// failed over to has none. The first subscribe is session's.
			if ack.ReasonCode == 0 && connects.Add(1) > 1 {
				resubscribe(c, cfg)
			}
		},
		OnMessage: func(_ *mqttclient.Client, m *mqttclient.Message) {
			n := received.Add(1)
			if !cfg.quiet {
				cli.Logf("OnMessage %s", cli.DescribeMessage(m))
			}
			if cfg.verify != "" {
				if err := cli.VerifyPattern(cfg.verify, m.Payload, cfg.verifySize); err != nil {
					var pe *cli.PatternError
					if bad.Add(1) == 1 && errors.As(err, &pe) {
						badOffset.Store(int64(pe.Offset))
						badMessage.Store(n)
					}
					cli.Logf("BAD message %d topic=%q bytes=%d: %v", n, m.Topic, len(m.Payload), err)
				} else {
					cli.Logf("verified message %d topic=%q bytes=%d", n, m.Topic, len(m.Payload))
				}
			}
			if n == cfg.count {
				close(enough)
			}
		},
	})
	if err != nil {
		return err
	}
	rep.SetSession(s)
	if err := s.Start(ctx); err != nil {
		return err
	}
	err = session(ctx, cfg, s, enough)
	if err != nil && !s.C.IsConnected() {
		return err
	}
	if derr := s.Disconnect(byte(cfg.reason)); err == nil {
		err = derr
	}
	cli.Logf("received %d messages", received.Load())
	if cfg.verify != "" {
		cli.Logf("verified %d, bad %d", received.Load()-bad.Load(), bad.Load())
		if err == nil && bad.Load() > 0 {
			err = fmt.Errorf("%d payloads failed verification", bad.Load())
		}
	}
	if err == nil && cfg.count > 0 && received.Load() < cfg.count {
		err = fmt.Errorf("received %d of %d messages", received.Load(), cfg.count)
	}
	return err
}

// session runs the steps between connect and disconnect.
func session(ctx context.Context, cfg *config, s *cli.Session, enough <-chan struct{}) error {
	if len(cfg.unsubFirst) > 0 {
		if err := unsubscribe(ctx, cfg, s, cfg.unsubFirst); err != nil {
			return err
		}
	}
	if len(cfg.topics) == 0 {
		return nil
	}
	subs, props := cfg.subscriptions()
	p, err := s.C.Subscribe(ctx, subs, props)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	res, err := waitFor(ctx, cfg, p)
	if res.ReasonCodes == nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	cli.Logf("SUBACK mid=%d%s", p.Mid, cli.DescribeAckProps(res.Properties))
	refused := 0
	for i, code := range res.ReasonCodes {
		topic := "?"
		if i < len(subs) {
			topic = subs[i].Topic
		}
		if code < 0x80 {
			cli.Logf("  %q: granted QoS %d", topic, code)
		} else {
			refused++
			cli.Logf("  %q: refused 0x%02x (%s)", topic, code, mqttclient.ReasonCodeString(code))
		}
	}
	if refused == len(res.ReasonCodes) {
		return errors.New("all subscriptions were refused")
	}

	if err := receive(ctx, cfg, s, enough); err != nil {
		return err
	}
	if cfg.unsub {
		return unsubscribe(ctx, cfg, s, cfg.topics)
	}
	return nil
}

// resubscribe subscribes to the -t filters again after a reconnect (only
// with -config: without it there is one connection). It runs in OnConnect,
// so it waits for the SUBACK on another goroutine to keep messages flowing.
func resubscribe(c *mqttclient.Client, cfg *config) {
	if len(cfg.topics) == 0 {
		return
	}
	subs, props := cfg.subscriptions()
	p, err := c.Subscribe(context.Background(), subs, props)
	if err != nil {
		cli.Logf("resubscribe: %v", err)
		return
	}
	go func() {
		res, err := waitFor(context.Background(), cfg, p)
		if res.ReasonCodes == nil {
			cli.Logf("resubscribe: %v", err)
			return
		}
		cli.Logf("SUBACK mid=%d (resubscribed) reason codes %v", p.Mid, res.ReasonCodes)
	}()
}

// receive waits for -C messages, -W, Ctrl-C or the connection to end.
func receive(ctx context.Context, cfg *config, s *cli.Session, enough <-chan struct{}) error {
	var timeout <-chan time.Time
	if cfg.wait > 0 {
		t := time.NewTimer(cfg.wait)
		defer t.Stop()
		timeout = t.C
	}
	switch {
	case cfg.count > 0 && cfg.wait > 0:
		cli.Logf("waiting for %d messages, at most %v", cfg.count, cfg.wait)
	case cfg.count > 0:
		cli.Logf("waiting for %d messages (Ctrl-C ends)", cfg.count)
	case cfg.wait > 0:
		cli.Logf("receiving for %v", cfg.wait)
	default:
		cli.Logf("receiving until Ctrl-C")
	}
	select {
	case <-enough:
	case <-timeout:
	case <-ctx.Done():
		cli.Logf("interrupted")
	case ev := <-s.Lost():
		return fmt.Errorf("connection lost: %w", ev.Err)
	}
	return nil
}

func unsubscribe(ctx context.Context, cfg *config, s *cli.Session, topics []string) error {
	p, err := s.C.Unsubscribe(ctx, topics, nil)
	if err != nil {
		return fmt.Errorf("unsubscribe: %w", err)
	}
	res, err := waitFor(ctx, cfg, p)
	if res.ReasonCodes == nil {
		return fmt.Errorf("unsubscribe: %w", err)
	}
	cli.Logf("UNSUBACK mid=%d%s", p.Mid, cli.DescribeAckProps(res.Properties))
	for i, code := range res.ReasonCodes {
		topic := "?"
		if i < len(topics) {
			topic = topics[i]
		}
		cli.Logf("  %q: 0x%02x (%s)", topic, code, unsubackText(code))
	}
	if err != nil {
		return fmt.Errorf("unsubscribe: %w", err)
	}
	return nil
}

func unsubackText(code byte) string {
	if code == 0 {
		return "success"
	}
	return mqttclient.ReasonCodeString(code)
}

// waitFor waits for p for -timeout. The Result is returned with a
// *ReasonCodeError too, so the caller can show every code.
func waitFor(ctx context.Context, cfg *config, p *mqttclient.Pending) (mqttclient.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.conn.Timeout)
	defer cancel()
	return p.Wait(ctx)
}

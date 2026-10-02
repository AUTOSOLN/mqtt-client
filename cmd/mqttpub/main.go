// Command mqttpub is a manual test tool for publishing with this client. It
// connects, publishes -n messages at QoS 0, 1 or 2, waits for them to
// complete, optionally stays connected to print messages the broker sends,
// then disconnects.
//
// The client cannot subscribe yet (phase 3). Messages arrive anyway when
// the client resumes a persistent session that has subscriptions, for
// example one created with mosquitto_sub -c; see README.md.
//
// Examples:
//
//	mqttpub -t test/a -m hello -q 1
//	mqttpub -v 5 -t test/a -q 2 -n 1000 -quiet
//	mqttpub -v 5 -t test/a -content-type text/plain -user k=v -response-topic test/reply
//	mqttpub -id inbox -clean=false -t test/a -q 2 -n 3 -listen 2s
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
)

type config struct {
	conn     cli.ConnFlags
	topic    string
	message  string
	size     int
	qos      uint
	retain   bool
	count    int
	interval time.Duration
	sync     bool
	listen   time.Duration
	reason   uint
	quiet    bool

	contentType   string
	utf8          bool
	responseTopic string
	correlation   string
	expiry        uint
	user          userProps
}

// userProps collects repeated -user key=value flags.
type userProps []mqttclient.UserProperty

func (u *userProps) String() string { return fmt.Sprint(*u) }

func (u *userProps) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok {
		return errors.New("want key=value")
	}
	*u = append(*u, mqttclient.UserProperty{Key: k, Val: v})
	return nil
}

func main() {
	os.Exit(run())
}

func run() int {
	var cfg config
	cfg.conn.Register(flag.CommandLine)
	flag.StringVar(&cfg.topic, "t", "mqttclient/test", "topic to publish to")
	flag.StringVar(&cfg.message, "m", "hello {n}", "payload; {n} is replaced by the message number")
	flag.IntVar(&cfg.size, "size", 0, "send a payload of this many bytes instead of -m")
	flag.UintVar(&cfg.qos, "q", 0, "QoS: 0, 1 or 2")
	flag.BoolVar(&cfg.retain, "r", false, "retain flag")
	flag.IntVar(&cfg.count, "n", 1, "number of messages (0 publishes nothing, for -listen)")
	flag.DurationVar(&cfg.interval, "interval", 0, "pause between messages")
	flag.BoolVar(&cfg.sync, "sync", false, "wait for each message to complete before the next")
	flag.DurationVar(&cfg.listen, "listen", 0, "stay connected this long after publishing and print received messages (Ctrl-C ends it)")
	flag.UintVar(&cfg.reason, "reason", 0, "DISCONNECT reason code (MQTT 5 only)")
	flag.BoolVar(&cfg.quiet, "quiet", false, "print only failures and the summary, not every message")
	flag.StringVar(&cfg.contentType, "content-type", "", "Content Type property (MQTT 5)")
	flag.BoolVar(&cfg.utf8, "utf8", false, "set Payload Format Indicator to UTF-8 (MQTT 5)")
	flag.StringVar(&cfg.responseTopic, "response-topic", "", "Response Topic property (MQTT 5)")
	flag.StringVar(&cfg.correlation, "correlation", "", "Correlation Data property (MQTT 5)")
	flag.UintVar(&cfg.expiry, "expiry", 0, "Message Expiry Interval in seconds (MQTT 5)")
	flag.Var(&cfg.user, "user", "user property key=value (MQTT 5, repeatable)")
	flag.Parse()

	props, err := cfg.properties()
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqttpub:", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := publish(ctx, &cfg, props); err != nil {
		cli.Logf("FAIL: %v", err)
		return 1
	}
	cli.Logf("OK")
	return 0
}

// properties builds the PUBLISH properties from the flags, or nil if none
// is set.
func (cfg *config) properties() (*mqttclient.Properties, error) {
	p := &mqttclient.Properties{
		ContentType:           cfg.contentType,
		ResponseTopic:         cfg.responseTopic,
		CorrelationData:       []byte(cfg.correlation),
		MessageExpiryInterval: uint32(cfg.expiry),
		User:                  cfg.user,
	}
	if cfg.utf8 {
		p.PayloadFormat = 1
	}
	if p.ContentType == "" && p.ResponseTopic == "" && len(p.CorrelationData) == 0 && p.MessageExpiryInterval == 0 && len(p.User) == 0 && p.PayloadFormat == 0 {
		return nil, nil
	}
	if cfg.conn.Version != "5" {
		return nil, errors.New("PUBLISH properties require -v 5")
	}
	return p, nil
}

func (cfg *config) payload(n int) []byte {
	if cfg.size > 0 {
		b := make([]byte, cfg.size)
		for i := range b {
			b[i] = 'a' + byte(i%26)
		}
		return b
	}
	return []byte(strings.ReplaceAll(cfg.message, "{n}", strconv.Itoa(n)))
}

// stats counts messages. completed and refused are updated by the
// publishing goroutine from Pending results; received by OnMessage.
type stats struct {
	completed int
	refused   int
	received  atomic.Int64
}

func publish(ctx context.Context, cfg *config, props *mqttclient.Properties) error {
	var st stats
	s, err := cli.NewSession(&cfg.conn, mqttclient.Handlers{
		OnPublish: func(_ *mqttclient.Client, mid uint16, reason byte, _ *mqttclient.Properties) {
			if reason >= 0x80 {
				cli.Logf("OnPublish mid=%d reason=0x%02x (%s)", mid, reason, mqttclient.ReasonCodeString(reason))
			} else if !cfg.quiet {
				cli.Logf("OnPublish mid=%d reason=0x%02x", mid, reason)
			}
		},
		OnMessage: func(_ *mqttclient.Client, m *mqttclient.Message) {
			st.received.Add(1)
			if !cfg.quiet {
				cli.Logf("OnMessage %s", describe(m))
			}
		},
	})
	if err != nil {
		return err
	}
	if cfg.reason != 0 && s.Version != mqttclient.MQTT5 {
		return errors.New("-reason requires -v 5")
	}
	if _, err := s.Connect(ctx); err != nil {
		return err
	}

	start := time.Now()
	pending, err := sendAll(ctx, cfg, s, props, &st)
	if err == nil {
		err = waitAll(cfg, s, pending, &st)
	}
	if cfg.count > 0 {
		elapsed := time.Since(start)
		cli.Logf("completed %d of %d in %v (%.0f msg/s), %d refused",
			st.completed, cfg.count, elapsed.Round(time.Microsecond), float64(st.completed)/elapsed.Seconds(), st.refused)
	}
	if err != nil {
		if s.C.IsConnected() {
			_ = s.Disconnect(0)
		}
		return err
	}
	if err := s.Hold(ctx, cfg.listen); err != nil {
		return err
	}
	if err := s.Disconnect(byte(cfg.reason)); err != nil {
		return err
	}
	if n := st.received.Load(); n > 0 || cfg.listen > 0 {
		cli.Logf("received %d messages", n)
	}
	if st.refused > 0 {
		return fmt.Errorf("%d messages refused by the server", st.refused)
	}
	return nil
}

// sendAll publishes the messages and returns the ones still in flight.
func sendAll(ctx context.Context, cfg *config, s *cli.Session, props *mqttclient.Properties, st *stats) ([]*mqttclient.Pending, error) {
	var pending []*mqttclient.Pending
	for n := 1; n <= cfg.count; n++ {
		m := &mqttclient.Message{Topic: cfg.topic, Payload: cfg.payload(n), QoS: byte(cfg.qos), Retain: cfg.retain, Properties: props}
		pctx, cancel := context.WithTimeout(ctx, cfg.conn.Timeout)
		p, err := s.C.Publish(pctx, m)
		cancel()
		if err != nil {
			return pending, fmt.Errorf("publish %d: %w", n, err)
		}
		if !cfg.quiet {
			cli.Logf("Publish mid=%d qos=%d retain=%v topic=%q bytes=%d", p.Mid, m.QoS, m.Retain, m.Topic, len(m.Payload))
		}
		if cfg.sync {
			if err := waitAll(cfg, s, []*mqttclient.Pending{p}, st); err != nil {
				return nil, err
			}
		} else {
			pending = append(pending, p)
		}
		if cfg.interval > 0 && n < cfg.count {
			select {
			case <-time.After(cfg.interval):
			case <-ctx.Done():
				cli.Logf("interrupted after %d messages", n)
				return pending, nil
			}
		}
	}
	return pending, nil
}

// waitAll waits for every pending message to complete and counts the
// results. It gives up when the connection is lost or nothing completes for
// -timeout.
func waitAll(cfg *config, s *cli.Session, pending []*mqttclient.Pending, st *stats) error {
	timer := time.NewTimer(cfg.conn.Timeout)
	defer timer.Stop()
	for i, p := range pending {
		select {
		case <-p.Done():
			st.completed++
			if _, err := p.Wait(context.Background()); err != nil {
				st.refused++
			}
			timer.Reset(cfg.conn.Timeout)
		case ev := <-s.Lost():
			return fmt.Errorf("connection lost with %d messages in flight: %w", len(pending)-i, ev.Err)
		case <-timer.C:
			return fmt.Errorf("mid %d not complete after %v (%d messages in flight)", p.Mid, cfg.conn.Timeout, len(pending)-i)
		}
	}
	return nil
}

// describe formats a received message for printing.
func describe(m *mqttclient.Message) string {
	var b strings.Builder
	fmt.Fprintf(&b, "mid=%d qos=%d retain=%v topic=%q payload=%s", m.Mid, m.QoS, m.Retain, m.Topic, showPayload(m.Payload))
	p := m.Properties
	if p == nil {
		return b.String()
	}
	if p.PayloadFormatFlag {
		fmt.Fprintf(&b, " payload_format=%d", p.PayloadFormat)
	}
	if p.MessageExpiryInterval > 0 {
		fmt.Fprintf(&b, " expiry=%d", p.MessageExpiryInterval)
	}
	if p.ContentType != "" {
		fmt.Fprintf(&b, " content_type=%q", p.ContentType)
	}
	if p.ResponseTopic != "" {
		fmt.Fprintf(&b, " response_topic=%q", p.ResponseTopic)
	}
	if len(p.CorrelationData) > 0 {
		fmt.Fprintf(&b, " correlation=%s", showPayload(p.CorrelationData))
	}
	if len(p.SubscriptionIdentifier) > 0 {
		fmt.Fprintf(&b, " subscription_ids=%v", p.SubscriptionIdentifier)
	}
	for _, u := range p.User {
		fmt.Fprintf(&b, " user[%q]=%q", u.Key, u.Val)
	}
	return b.String()
}

// showPayload prints text as a quoted string and anything else as hex,
// truncated to keep lines short.
func showPayload(b []byte) string {
	const max = 64
	more := ""
	if len(b) > max {
		b, more = b[:max], fmt.Sprintf("...(%d more bytes)", len(b)-max)
	}
	if utf8.Valid(b) {
		return strconv.Quote(string(b)) + more
	}
	return "0x" + hex.EncodeToString(b) + more
}

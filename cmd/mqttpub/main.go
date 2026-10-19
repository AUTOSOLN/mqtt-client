// Command mqttpub is a manual test tool for publishing with this client. It
// connects, publishes -n messages at QoS 0, 1 or 2, waits for them to
// complete, optionally stays connected to print messages the broker sends,
// then disconnects.
//
// It does not subscribe (mqttsub does), but messages arrive when it resumes
// a persistent session that has subscriptions; see README.md.
//
// With -config it reconnects, failing over between the servers in the
// file. QoS 1 and 2 messages wait for the next connection; QoS 0 messages
// published while disconnected are dropped and counted. -timeout must then
// allow for the reconnect: it is how long to wait without a completion.
//
// Examples:
//
//	mqttpub -t test/a -m hello -q 1
//	mqttpub -v 5 -t test/a -q 2 -n 1000 -quiet
//	mqttpub -v 5 -t test/a -content-type text/plain -user k=v -response-topic test/reply
//	mqttpub -id inbox -clean=false -t test/a -q 2 -n 3 -listen 2s
//	mqttpub -t test/big -size 65536 -size-step 65536 -n 16 -pattern ascii -sync
//	mqttpub -config brokers.yaml -t test/a -q 1 -n 100 -interval 1s -timeout 1m
//	mqttpub -t test/a -q 1 -n 100 -quiet -report -
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
)

type config struct {
	conn     cli.ConnFlags
	topic    string
	message  string
	size     int
	sizeStep int
	pattern  string
	qos      uint
	retain   bool
	count    int
	interval time.Duration
	sync     bool
	listen   time.Duration
	reason   uint
	quiet    bool
	report   string

	contentType   string
	utf8          bool
	responseTopic string
	correlation   string
	expiry        uint
	user          cli.UserProps
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	var cfg config
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.SetOutput(stderr)
	cfg.conn.Register(fs)
	fs.StringVar(&cfg.topic, "t", "mqttclient/test", "topic to publish to")
	fs.StringVar(&cfg.message, "m", "hello {n}", "payload; {n} is replaced by the message number")
	fs.IntVar(&cfg.size, "size", 0, "send a payload of this many bytes of -pattern instead of -m")
	fs.IntVar(&cfg.sizeStep, "size-step", 0, "grow -size by this many bytes for each message after the first")
	fs.StringVar(&cfg.pattern, "pattern", "alpha", "-size payload pattern: "+cli.PatternNames())
	fs.UintVar(&cfg.qos, "q", 0, "QoS: 0, 1 or 2")
	fs.BoolVar(&cfg.retain, "r", false, "retain flag")
	fs.IntVar(&cfg.count, "n", 1, "number of messages (0 publishes nothing, for -listen)")
	fs.DurationVar(&cfg.interval, "interval", 0, "pause between messages")
	fs.BoolVar(&cfg.sync, "sync", false, "wait for each message to complete before the next")
	fs.DurationVar(&cfg.listen, "listen", 0, "stay connected this long after publishing and print received messages (Ctrl-C ends it)")
	fs.UintVar(&cfg.reason, "reason", 0, "DISCONNECT reason code (MQTT 5 only)")
	fs.BoolVar(&cfg.quiet, "quiet", false, "print only failures and the summary, not every message")
	fs.StringVar(&cfg.contentType, "content-type", "", "Content Type property (MQTT 5)")
	fs.BoolVar(&cfg.utf8, "utf8", false, "set Payload Format Indicator to UTF-8 (MQTT 5)")
	fs.StringVar(&cfg.responseTopic, "response-topic", "", "Response Topic property (MQTT 5)")
	fs.StringVar(&cfg.correlation, "correlation", "", "Correlation Data property (MQTT 5)")
	fs.UintVar(&cfg.expiry, "expiry", 0, "Message Expiry Interval in seconds (MQTT 5)")
	fs.Var(&cfg.user, "user", "user property key=value (MQTT 5, repeatable)")
	fs.StringVar(&cfg.report, "report", "", "write a JSON run report to this file on exit; - writes it to stdout and the log to stderr")
	_ = fs.Parse(args)
	rep := cli.NewReport("mqttpub", cfg.report, &cfg.conn, stdout, stderr)

	props, err := cfg.properties()
	if err == nil {
		err = cfg.checkSize()
	}
	if err != nil {
		fmt.Fprintln(stderr, "mqttpub:", err)
		return rep.Finish(2, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := publish(ctx, &cfg, props, rep); err != nil {
		cli.Logf("FAIL: %v", err)
		return rep.Finish(cli.ExitCode(err), err)
	}
	cli.Logf("OK")
	return rep.Finish(0, nil)
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

func (cfg *config) checkSize() error {
	if cfg.sizeStep != 0 && cfg.size <= 0 {
		return errors.New("-size-step needs -size")
	}
	if cfg.size+(cfg.count-1)*cfg.sizeStep < 0 {
		return errors.New("-size-step shrinks the payload below zero")
	}
	_, err := cli.MakePattern(cfg.pattern, 0)
	return err
}

func (cfg *config) payload(n int) []byte {
	if cfg.size > 0 {
		b, _ := cli.MakePattern(cfg.pattern, cfg.size+(n-1)*cfg.sizeStep)
		return b
	}
	return []byte(strings.ReplaceAll(cfg.message, "{n}", strconv.Itoa(n)))
}

// stats counts messages. published, completed, refused and dropped are
// updated by the publishing goroutine; received by OnMessage; latency (with
// -report) by a goroutine per QoS 1 and 2 message.
type stats struct {
	published int
	completed int
	refused   int
	dropped   int // QoS 0 while reconnecting (-config)
	received  atomic.Int64

	latency cli.Latencies
	timing  sync.WaitGroup
}

// timeAck records the time from sent to p's acknowledgement.
func (st *stats) timeAck(p *mqttclient.Pending, sent time.Time) {
	defer st.timing.Done()
	<-p.Done()
	if _, err := p.Wait(context.Background()); err == nil {
		st.latency.Add(time.Since(sent))
	}
}

// fill puts the counts in rep. err is publish's result.
func (st *stats) fill(rep *cli.Report, cfg *config, err error) {
	if err == nil {
		st.timing.Wait() // every message is complete
		rep.LatencyMs = st.latency.Summary()
	}
	rep.Counts.Published = int64(st.published)
	rep.Counts.Acked = int64(st.completed - st.refused)
	rep.Counts.Refused = int64(st.refused)
	rep.Counts.Dropped = int64(st.dropped)
	rep.Counts.Received = st.received.Load()
	if cfg.size > 0 {
		rep.Verify.Pattern, rep.Verify.Size = cfg.pattern, cfg.size
	}
	switch {
	case st.completed == cfg.count && cfg.qos > 0:
		rep.ExitReason = fmt.Sprintf("all %d messages acknowledged", st.completed)
	case st.completed == cfg.count:
		rep.ExitReason = fmt.Sprintf("all %d messages sent", st.completed)
	default:
		rep.ExitReason = fmt.Sprintf("%d of %d messages completed", st.completed, cfg.count)
	}
}

func publish(ctx context.Context, cfg *config, props *mqttclient.Properties, rep *cli.Report) (err error) {
	var st stats
	if rep.Enabled() {
		defer func() { st.fill(rep, cfg, err) }()
	}
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
				cli.Logf("OnMessage %s", cli.DescribeMessage(m))
			}
		},
	})
	if err != nil {
		return err
	}
	rep.SetSession(s)
	if cfg.reason != 0 && s.Version != mqttclient.MQTT5 {
		return cli.Setup(errors.New("-reason requires -v 5"))
	}
	if err := s.Start(ctx); err != nil {
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
	if st.dropped > 0 {
		return fmt.Errorf("%d QoS 0 messages dropped while disconnected", st.dropped)
	}
	return nil
}

// sendAll publishes the messages and returns the ones still in flight.
func sendAll(ctx context.Context, cfg *config, s *cli.Session, props *mqttclient.Properties, st *stats) ([]*mqttclient.Pending, error) {
	var pending []*mqttclient.Pending
	for n := 1; n <= cfg.count; n++ {
		p, err := publishOne(ctx, cfg, s, props, st, n)
		switch {
		case err != nil:
			return pending, err
		case p == nil: // dropped
		case cfg.sync:
			if err := waitAll(cfg, s, []*mqttclient.Pending{p}, st); err != nil {
				return nil, err
			}
		default:
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

// publishOne publishes message n. With -config, a QoS 0 message that cannot
// be sent while reconnecting is counted as dropped and p is nil.
func publishOne(ctx context.Context, cfg *config, s *cli.Session, props *mqttclient.Properties, st *stats, n int) (*mqttclient.Pending, error) {
	m := &mqttclient.Message{Topic: cfg.topic, Payload: cfg.payload(n), QoS: byte(cfg.qos), Retain: cfg.retain, Properties: props}
	ctx, cancel := context.WithTimeout(ctx, cfg.conn.Timeout)
	defer cancel()
	p, err := s.C.Publish(ctx, m)
	if errors.Is(err, mqttclient.ErrNoConn) && s.Reconnects() {
		st.dropped++
		cli.Logf("publish %d: dropped, not connected", n)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("publish %d: %w", n, err)
	}
	st.published++
	if cfg.report != "" && m.QoS > 0 {
		st.timing.Add(1)
		go st.timeAck(p, time.Now())
	}
	if !cfg.quiet {
		cli.Logf("Publish mid=%d qos=%d retain=%v topic=%q bytes=%d", p.Mid, m.QoS, m.Retain, m.Topic, len(m.Payload))
	}
	return p, nil
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

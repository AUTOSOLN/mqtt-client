// Command sparkverify subscribes to Sparkplug B traffic and checks it with
// the sparkplug/verify package: seq continuity per edge node (gaps,
// reorders, duplicates), NBIRTH seq 0, NBIRTH/NDEATH bdSeq pairing, stale
// NDEATHs, and DATA before BIRTH. It prints each violation as it is found
// and a summary per edge node at the end.
//
// Subscribe before the edge nodes connect, or their first messages will be
// reported as DATA before BIRTH.
//
// Examples:
//
//	sparkverify -W 10m
//	sparkverify -v 5 -t 'spBv1.0/plant1/#' -q 1 -report run.json
//
// Exit status: 0 when no rule was broken, 1 when one was, 2 for a setup
// error.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
	"github.com/AUTOSOLN/mqtt-client/sparkplug/verify"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type config struct {
	conn        cli.ConnFlags
	topics      cli.Strings
	qos         uint
	wait        time.Duration
	anyBirthSeq bool
	report      string
}

func run(args []string, stdout, stderr io.Writer) int {
	var cfg config
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.SetOutput(stderr)
	cfg.conn.Register(fs)
	fs.Var(&cfg.topics, "t", "topic filter to subscribe to (repeatable; default spBv1.0/#)")
	fs.UintVar(&cfg.qos, "q", 1, "requested QoS: 0, 1 or 2")
	fs.DurationVar(&cfg.wait, "W", 0, "stop after this long (0: until Ctrl-C)")
	fs.BoolVar(&cfg.anyBirthSeq, "any-birth-seq", false, "accept an NBIRTH seq other than 0 (Sparkplug 3.0 allows it)")
	fs.StringVar(&cfg.report, "report", "", "write the verify.Report as JSON to this file on exit; - writes it to stdout")
	_ = fs.Parse(args)
	if len(cfg.topics) == 0 {
		cfg.topics = cli.Strings{"spBv1.0/#"}
	}
	if cfg.qos > 2 {
		fmt.Fprintln(stderr, "sparkverify: -q must be 0, 1 or 2")
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	v := verify.New(verify.Options{
		AnyBirthSeq: cfg.anyBirthSeq,
		OnViolation: func(vi verify.Violation) { cli.Logf("VIOLATION %s", vi) },
	})
	err := listen(ctx, &cfg, v)
	r := v.Report()
	summarize(r)
	if cfg.report != "" {
		if werr := writeReport(cfg.report, r, stdout); werr != nil && err == nil {
			err = cli.Setup(werr)
		}
	}
	switch {
	case err != nil:
		cli.Logf("FAIL: %v", err)
		return 2
	case !r.OK():
		cli.Logf("FAIL: %d violations", r.Totals.Violations)
		return 1
	}
	cli.Logf("OK")
	return 0
}

// listen subscribes and feeds every message to v until -W passes, Ctrl-C,
// or the connection is lost.
func listen(ctx context.Context, cfg *config, v *verify.Verifier) error {
	s, err := cli.NewSession(&cfg.conn, mqttclient.Handlers{
		OnMessage: func(_ *mqttclient.Client, m *mqttclient.Message) { v.Feed(m.Topic, m.Payload, time.Now()) },
	})
	if err != nil {
		return err
	}
	if err := s.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = s.Disconnect(0) }()

	subs := make([]mqttclient.Subscription, len(cfg.topics))
	for i, t := range cfg.topics {
		subs[i] = mqttclient.Subscription{Topic: t, QoS: byte(cfg.qos)}
	}
	p, err := s.C.Subscribe(ctx, subs, nil)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	res, err := p.Wait(ctx)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	if slices.ContainsFunc(res.ReasonCodes, func(c byte) bool { return c >= 0x80 }) {
		return fmt.Errorf("subscribe refused: reason codes % x", res.ReasonCodes)
	}
	cli.Logf("subscribed to %v, verifying", []string(cfg.topics))

	if cfg.wait > 0 {
		return s.Hold(ctx, cfg.wait)
	}
	select {
	case <-ctx.Done():
		return nil
	case ev := <-s.Lost():
		return fmt.Errorf("connection lost: %w", ev.Err)
	}
}

func summarize(r verify.Report) {
	ids := slices.Sorted(func(yield func(string) bool) {
		for id := range r.Edges {
			if !yield(id) {
				return
			}
		}
	})
	for _, id := range ids {
		e := r.Edges[id]
		cli.Logf("edge %s: %d messages, %d births, %d deaths, bdSeq %d online=%t: %d gaps (%d lost), %d reorders, %d duplicates, %d violations",
			id, e.Messages, e.Births, e.Deaths, e.BdSeq, e.Online, e.Gaps, e.Lost, e.Reorders, e.Duplicates, e.Violations)
	}
	cli.Logf("%s", r)
}

func writeReport(path string, r verify.Report, stdout io.Writer) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if path == "-" {
		_, err = stdout.Write(b)
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

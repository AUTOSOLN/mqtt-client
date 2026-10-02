// Command mqttconnect is a manual test tool for this client. It runs a
// chosen combination of steps against a real broker:
//
//	tcp         open a TCP connection (and TLS handshake for mqtts://), then close it
//	connect     open a connection, send CONNECT and wait for CONNACK
//	disconnect  send DISCONNECT and wait for the connection to close
//
// Steps are given as a comma-separated list with -steps. "disconnect"
// requires "connect". Without "disconnect", the process exits holding the
// connection, so the broker sees it drop without DISCONNECT and publishes
// any Will.
//
// Examples:
//
//	mqttconnect -server mqtt://localhost:1883 -steps tcp
//	mqttconnect -server mqtt://localhost:1883 -v 5 -id test1 -hold 30s -debug
//	mqttconnect -server mqtts://broker:8883 -cafile ca.pem -u user -P secret
//	mqttconnect -steps connect -hold 5s -will-topic t/will -will-payload bye
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
)

type config struct {
	conn   cli.ConnFlags
	steps  string
	hold   time.Duration
	repeat int
	reason uint
}

func main() {
	os.Exit(run())
}

func run() int {
	var cfg config
	cfg.conn.Register(flag.CommandLine)
	flag.StringVar(&cfg.steps, "steps", "connect,disconnect", "comma-separated steps: tcp | connect[,disconnect]")
	flag.DurationVar(&cfg.hold, "hold", 0, "time to stay connected after CONNACK (Ctrl-C ends it early)")
	flag.IntVar(&cfg.repeat, "repeat", 1, "run the steps this many times")
	flag.UintVar(&cfg.reason, "reason", 0, "DISCONNECT reason code (MQTT 5 only)")
	flag.Parse()

	doTCP, doConnect, doDisconnect, err := parseSteps(cfg.steps)
	if err == nil {
		_, err = cfg.conn.Options() // check the connection flags before any step
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqttconnect:", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	for i := 1; i <= cfg.repeat; i++ {
		if cfg.repeat > 1 {
			cli.Logf("--- run %d of %d", i, cfg.repeat)
		}
		var err error
		switch {
		case doTCP:
			err = tcpOnly(ctx, &cfg.conn)
		case doConnect:
			err = connect(ctx, &cfg, doDisconnect)
		}
		if err != nil {
			cli.Logf("FAIL: %v", err)
			return 1
		}
		if ctx.Err() != nil {
			break
		}
	}
	cli.Logf("OK")
	return 0
}

func parseSteps(s string) (tcp, connect, disconnect bool, err error) {
	for _, st := range strings.Split(s, ",") {
		switch strings.TrimSpace(st) {
		case "tcp":
			tcp = true
		case "connect":
			connect = true
		case "disconnect":
			disconnect = true
		default:
			return false, false, false, fmt.Errorf("unknown step %q", st)
		}
	}
	switch {
	case tcp && (connect || disconnect):
		return false, false, false, errors.New(`"tcp" runs on its own; "connect" already opens the connection`)
	case disconnect && !connect:
		return false, false, false, errors.New(`"disconnect" requires "connect"`)
	case !tcp && !connect:
		return false, false, false, errors.New("no steps given")
	}
	return tcp, connect, disconnect, nil
}

// tcpOnly dials the server without speaking MQTT. The client package does
// not expose a dial-only step, so this uses net and crypto/tls directly.
func tcpOnly(ctx context.Context, f *cli.ConnFlags) error {
	u, err := url.Parse(f.Server)
	if err != nil {
		return err
	}
	useTLS, port := false, "1883"
	switch strings.ToLower(u.Scheme) {
	case "mqtt", "tcp":
	case "mqtts", "ssl", "tls":
		useTLS, port = true, "8883"
	default:
		return fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	if p := u.Port(); p != "" {
		port = p
	}
	addr := net.JoinHostPort(u.Hostname(), port)

	ctx, cancel := context.WithTimeout(ctx, f.Timeout)
	defer cancel()
	start := time.Now()
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp dial %s: %w", addr, err)
	}
	defer nc.Close()
	cli.Logf("tcp connected %s -> %s in %v", nc.LocalAddr(), nc.RemoteAddr(), cli.Since(start))
	if !useTLS {
		return nil
	}

	tc, err := f.TLSConfig()
	if err != nil {
		return err
	}
	if tc == nil {
		tc = &tls.Config{}
	}
	if tc.ServerName == "" {
		tc.ServerName = u.Hostname()
	}
	tconn := tls.Client(nc, tc)
	if err := tconn.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("tls handshake: %w", err)
	}
	st := tconn.ConnectionState()
	cli.Logf("tls %s %s", tls.VersionName(st.Version), tls.CipherSuiteName(st.CipherSuite))
	if len(st.PeerCertificates) > 0 {
		pc := st.PeerCertificates[0]
		cli.Logf("tls server cert subject=%q issuer=%q expires=%s", pc.Subject, pc.Issuer, pc.NotAfter.Format(time.RFC3339))
	}
	return nil
}

func connect(ctx context.Context, cfg *config, disconnect bool) error {
	s, err := cli.NewSession(&cfg.conn, mqttclient.Handlers{})
	if err != nil {
		return err
	}
	if cfg.reason != 0 && s.Version != mqttclient.MQTT5 {
		return errors.New("-reason requires -v 5")
	}
	if _, err := s.Connect(ctx); err != nil {
		return err
	}
	if err := s.Hold(ctx, cfg.hold); err != nil {
		return err
	}
	if !disconnect {
		cli.Logf("exiting without DISCONNECT")
		return nil
	}
	return s.Disconnect(byte(cfg.reason))
}

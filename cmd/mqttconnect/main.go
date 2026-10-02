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
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

type config struct {
	server    string
	steps     string
	version   string
	clientID  string
	clean     bool
	keepAlive uint
	username  string
	password  string
	timeout   time.Duration
	hold      time.Duration
	repeat    int
	reason    uint
	debug     bool

	cafile     string
	certfile   string
	keyfile    string
	insecure   bool
	serverName string

	willTopic   string
	willPayload string
	willQoS     uint
	willRetain  bool
}

func main() {
	os.Exit(run())
}

func run() int {
	var cfg config
	flag.StringVar(&cfg.server, "server", "mqtt://localhost:1883", "broker URL: mqtt://, tcp://, mqtts://, ssl:// or tls://")
	flag.StringVar(&cfg.steps, "steps", "connect,disconnect", "comma-separated steps: tcp | connect[,disconnect]")
	flag.StringVar(&cfg.version, "v", "311", "MQTT protocol version: 31, 311 or 5")
	flag.StringVar(&cfg.clientID, "id", "", "client id (empty requires -clean)")
	flag.BoolVar(&cfg.clean, "clean", true, "clean session / clean start")
	flag.UintVar(&cfg.keepAlive, "keepalive", 60, "keepalive in seconds (0 or >= 5)")
	flag.StringVar(&cfg.username, "u", "", "username")
	flag.StringVar(&cfg.password, "P", "", "password")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "timeout for each step")
	flag.DurationVar(&cfg.hold, "hold", 0, "time to stay connected after CONNACK (Ctrl-C ends it early)")
	flag.IntVar(&cfg.repeat, "repeat", 1, "run the steps this many times")
	flag.UintVar(&cfg.reason, "reason", 0, "DISCONNECT reason code (MQTT 5 only)")
	flag.BoolVar(&cfg.debug, "debug", false, "log client debug output")
	flag.StringVar(&cfg.cafile, "cafile", "", "PEM file of CA certificates to trust (TLS)")
	flag.StringVar(&cfg.certfile, "cert", "", "PEM client certificate (TLS)")
	flag.StringVar(&cfg.keyfile, "key", "", "PEM client private key (TLS)")
	flag.BoolVar(&cfg.insecure, "insecure", false, "skip server certificate verification (TLS)")
	flag.StringVar(&cfg.serverName, "servername", "", "TLS server name, if different from the URL host")
	flag.StringVar(&cfg.willTopic, "will-topic", "", "Will topic (enables the Will)")
	flag.StringVar(&cfg.willPayload, "will-payload", "", "Will payload")
	flag.UintVar(&cfg.willQoS, "will-qos", 0, "Will QoS")
	flag.BoolVar(&cfg.willRetain, "will-retain", false, "Will retain flag")
	flag.Parse()

	doTCP, doConnect, doDisconnect, err := parseSteps(cfg.steps)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqttconnect:", err)
		return 2
	}

	tlsCfg, err := tlsConfig(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqttconnect:", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	for i := 1; i <= cfg.repeat; i++ {
		if cfg.repeat > 1 {
			logf("--- run %d of %d", i, cfg.repeat)
		}
		var err error
		switch {
		case doTCP:
			err = tcpOnly(ctx, cfg, tlsCfg)
		case doConnect:
			err = connect(ctx, cfg, tlsCfg, doDisconnect)
		}
		if err != nil {
			logf("FAIL: %v", err)
			return 1
		}
		if ctx.Err() != nil {
			break
		}
	}
	logf("OK")
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

func tlsConfig(cfg config) (*tls.Config, error) {
	if cfg.cafile == "" && cfg.certfile == "" && cfg.keyfile == "" && !cfg.insecure && cfg.serverName == "" {
		return nil, nil
	}
	tc := &tls.Config{InsecureSkipVerify: cfg.insecure, ServerName: cfg.serverName}
	if cfg.cafile != "" {
		pem, err := os.ReadFile(cfg.cafile)
		if err != nil {
			return nil, err
		}
		tc.RootCAs = x509.NewCertPool()
		if !tc.RootCAs.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", cfg.cafile)
		}
	}
	if cfg.certfile != "" || cfg.keyfile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.certfile, cfg.keyfile)
		if err != nil {
			return nil, err
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

// tcpOnly dials the server without speaking MQTT. The client package does
// not expose a dial-only step, so this uses net and crypto/tls directly.
func tcpOnly(ctx context.Context, cfg config, tc *tls.Config) error {
	u, err := url.Parse(cfg.server)
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

	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	start := time.Now()
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp dial %s: %w", addr, err)
	}
	defer nc.Close()
	logf("tcp connected %s -> %s in %v", nc.LocalAddr(), nc.RemoteAddr(), time.Since(start).Round(time.Microsecond))
	if !useTLS {
		return nil
	}

	if tc == nil {
		tc = &tls.Config{}
	} else {
		tc = tc.Clone()
	}
	if tc.ServerName == "" {
		tc.ServerName = u.Hostname()
	}
	tconn := tls.Client(nc, tc)
	if err := tconn.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("tls handshake: %w", err)
	}
	st := tconn.ConnectionState()
	logf("tls %s %s", tls.VersionName(st.Version), tls.CipherSuiteName(st.CipherSuite))
	if len(st.PeerCertificates) > 0 {
		pc := st.PeerCertificates[0]
		logf("tls server cert subject=%q issuer=%q expires=%s", pc.Subject, pc.Issuer, pc.NotAfter.Format(time.RFC3339))
	}
	return nil
}

func connect(ctx context.Context, cfg config, tc *tls.Config, disconnect bool) error {
	opts := mqttclient.Options{
		Server:     cfg.server,
		ClientID:   cfg.clientID,
		CleanStart: cfg.clean,
		KeepAlive:  uint16(cfg.keepAlive),
		Username:   cfg.username,
		TLSConfig:  tc,
	}
	switch cfg.version {
	case "31", "3":
		opts.ProtocolVersion = mqttclient.MQTT31
	case "311", "4":
		opts.ProtocolVersion = mqttclient.MQTT311
	case "5":
		opts.ProtocolVersion = mqttclient.MQTT5
	default:
		return fmt.Errorf("unknown protocol version %q", cfg.version)
	}
	if cfg.reason != 0 && opts.ProtocolVersion != mqttclient.MQTT5 {
		return errors.New("-reason requires -v 5")
	}
	if cfg.password != "" {
		opts.Password = []byte(cfg.password)
	}
	if cfg.willTopic != "" {
		opts.Will = &mqttclient.Message{
			Topic:   cfg.willTopic,
			Payload: []byte(cfg.willPayload),
			QoS:     byte(cfg.willQoS),
			Retain:  cfg.willRetain,
		}
	}
	if cfg.debug {
		opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	lost := make(chan mqttclient.DisconnectEvent, 1)
	h := mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, ack mqttclient.ConnAck) {
			logf("OnConnect reason=0x%02x session_present=%v", ack.ReasonCode, ack.SessionPresent)
		},
		OnDisconnect: func(c *mqttclient.Client, ev mqttclient.DisconnectEvent) {
			logf("OnDisconnect err=%v reason=0x%02x", ev.Err, ev.ReasonCode)
			if ev.Properties != nil && ev.Properties.ReasonString != "" {
				logf("  server reason string: %q", ev.Properties.ReasonString)
			}
			select {
			case lost <- ev:
			default:
			}
		},
	}

	c, err := mqttclient.New(opts, h)
	if err != nil {
		return err
	}

	cctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	start := time.Now()
	ack, err := c.Connect(cctx)
	cancel()
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	logf("connected in %v client_id=%q session_present=%v", time.Since(start).Round(time.Microsecond), c.ClientID(), ack.SessionPresent)
	if p := ack.Properties; p != nil {
		logf("  CONNACK properties:%s", connackProps(p))
	}

	if cfg.hold > 0 {
		logf("holding connection for %v", cfg.hold)
		t := time.NewTimer(cfg.hold)
		select {
		case <-t.C:
		case <-ctx.Done():
			logf("interrupted")
		case ev := <-lost:
			t.Stop()
			return fmt.Errorf("connection lost while holding: %w", ev.Err)
		}
		t.Stop()
	}

	if !disconnect {
		logf("exiting without DISCONNECT")
		return nil
	}

	dctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	if err := c.Disconnect(dctx, byte(cfg.reason), nil); err != nil {
		return fmt.Errorf("disconnect: %w", err)
	}
	logf("disconnected")

	// OnDisconnect runs on the client's dispatcher goroutine; wait for it so
	// its output is not lost when the process exits.
	select {
	case <-lost:
	case <-dctx.Done():
		logf("OnDisconnect not called within %v", cfg.timeout)
	}
	return nil
}

// connackProps lists the MQTT 5 CONNACK properties the server sent.
// Absent properties keep their spec defaults and are not shown.
func connackProps(p *mqttclient.Properties) string {
	var b strings.Builder
	if p.AssignedClientID != "" {
		fmt.Fprintf(&b, " assigned_id=%q", p.AssignedClientID)
	}
	if p.ServerKeepAliveFlag {
		fmt.Fprintf(&b, " server_keepalive=%d", p.ServerKeepAlive)
	}
	if p.ReceiveMaximum != 0 {
		fmt.Fprintf(&b, " receive_max=%d", p.ReceiveMaximum)
	}
	if p.MaximumQosFlag {
		fmt.Fprintf(&b, " max_qos=%d", p.MaximumQos)
	}
	if p.RetainAvailableFlag {
		fmt.Fprintf(&b, " retain_available=%d", p.RetainAvailable)
	}
	if p.MaximumPacketSize != 0 {
		fmt.Fprintf(&b, " max_packet=%d", p.MaximumPacketSize)
	}
	if p.TopicAliasMaximum != 0 {
		fmt.Fprintf(&b, " topic_alias_max=%d", p.TopicAliasMaximum)
	}
	if p.ReasonString != "" {
		fmt.Fprintf(&b, " reason_string=%q", p.ReasonString)
	}
	if b.Len() == 0 {
		return " (none)"
	}
	return b.String()
}

func logf(format string, args ...any) {
	fmt.Printf("%s %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}

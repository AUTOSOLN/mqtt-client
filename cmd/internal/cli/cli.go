// Package cli holds what the manual test tools under cmd share: the
// connection flags, building a client from them (or from a -config file of
// servers), connect and disconnect with logging, and output helpers.
package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

// ConnFlags are the flags that describe a connection. The yaml tags name
// the keys of a server in a -config file (see loadServers); "-" marks the
// flags that apply to the whole client rather than to one server.
type ConnFlags struct {
	Config string `yaml:"-"`

	Address       string        `yaml:"address"`
	TLS           bool          `yaml:"tls"`
	Version       string        `yaml:"version"`
	ClientID      string        `yaml:"client-id"`
	Clean         bool          `yaml:"clean"`
	KeepAlive     uint          `yaml:"keepalive"`
	SessionExpiry uint          `yaml:"session-expiry"`
	Username      string        `yaml:"username"`
	Password      string        `yaml:"password"`
	Timeout       time.Duration `yaml:"-"`
	Debug         bool          `yaml:"-"`

	CAFile     string `yaml:"cafile"`
	CertFile   string `yaml:"cert"`
	KeyFile    string `yaml:"key"`
	Insecure   bool   `yaml:"insecure"`
	ServerName string `yaml:"servername"`

	WillTopic   string `yaml:"will-topic"`
	WillPayload string `yaml:"will-payload"`
	WillQoS     uint   `yaml:"will-qos"`
	WillRetain  bool   `yaml:"will-retain"`

	// Reconnect has flags only in tools that call Reconnect.Register, but
	// a -config file may set it for any tool.
	Reconnect ReconnectFlags `yaml:",inline"`

	NoTopicCheck bool `yaml:"-"`

	fs *flag.FlagSet
}

// Register adds the connection flags to fs.
func (f *ConnFlags) Register(fs *flag.FlagSet) {
	f.fs = fs
	f.Reconnect = defaultReconnect
	fs.StringVar(&f.Config, "config", "", "YAML file of servers to connect to, reconnecting and failing over between them (replaces -address)")
	fs.StringVar(&f.Address, "address", "localhost:1883", "broker address as host:port")
	fs.BoolVar(&f.TLS, "tls", false, "connect with TLS")
	fs.StringVar(&f.Version, "v", "311", "MQTT protocol version: 31, 311 or 5")
	fs.StringVar(&f.ClientID, "id", "", "client id (empty requires -clean)")
	fs.BoolVar(&f.Clean, "clean", true, "clean session / clean start")
	fs.UintVar(&f.KeepAlive, "keepalive", 60, "keepalive in seconds (0 or >= 5)")
	fs.UintVar(&f.SessionExpiry, "session-expiry", 0, "session expiry interval in seconds (MQTT 5)")
	fs.StringVar(&f.Username, "u", "", "username")
	fs.StringVar(&f.Password, "P", "", "password")
	fs.DurationVar(&f.Timeout, "timeout", 10*time.Second, "timeout for each network step")
	fs.BoolVar(&f.Debug, "debug", false, "log client debug output")
	fs.StringVar(&f.CAFile, "cafile", "", "PEM file of CA certificates to trust (TLS)")
	fs.StringVar(&f.CertFile, "cert", "", "PEM client certificate (TLS)")
	fs.StringVar(&f.KeyFile, "key", "", "PEM client private key (TLS)")
	fs.BoolVar(&f.Insecure, "insecure", false, "skip server certificate verification (TLS)")
	fs.StringVar(&f.ServerName, "servername", "", "TLS server name, if different from the -address host")
	fs.StringVar(&f.WillTopic, "will-topic", "", "Will topic (enables the Will)")
	fs.StringVar(&f.WillPayload, "will-payload", "", "Will payload")
	fs.UintVar(&f.WillQoS, "will-qos", 0, "Will QoS")
	fs.BoolVar(&f.WillRetain, "will-retain", false, "Will retain flag")
	fs.BoolVar(&f.NoTopicCheck, "no-topic-check", false, "send topics and topic filters unchecked, even if invalid (for testing a broker)")
}

// ProtocolVersion parses -v.
func (f *ConnFlags) ProtocolVersion() (byte, error) {
	switch f.Version {
	case "31", "3":
		return mqttclient.MQTT31, nil
	case "311", "4":
		return mqttclient.MQTT311, nil
	case "5":
		return mqttclient.MQTT5, nil
	}
	return 0, fmt.Errorf("unknown protocol version %q", f.Version)
}

// TLSConfig builds the TLS configuration from the flags, or returns nil
// when no TLS flag is set.
func (f *ConnFlags) TLSConfig() (*tls.Config, error) {
	if f.CAFile == "" && f.CertFile == "" && f.KeyFile == "" && !f.Insecure && f.ServerName == "" {
		return nil, nil
	}
	tc := &tls.Config{InsecureSkipVerify: f.Insecure, ServerName: f.ServerName}
	if f.CAFile != "" {
		pem, err := os.ReadFile(f.CAFile)
		if err != nil {
			return nil, err
		}
		tc.RootCAs = x509.NewCertPool()
		if !tc.RootCAs.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", f.CAFile)
		}
	}
	if f.CertFile != "" || f.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(f.CertFile, f.KeyFile)
		if err != nil {
			return nil, err
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

// Options builds client options from the flags: one server from -address,
// or the servers listed in the -config file.
func (f *ConnFlags) Options() (mqttclient.Options, error) {
	opts := mqttclient.Options{NoTopicCheck: f.NoTopicCheck}
	if f.Config != "" {
		servers, err := f.loadServers()
		if err != nil {
			return mqttclient.Options{}, err
		}
		opts.Servers = servers
	} else {
		srv, err := f.Server()
		if err != nil {
			return mqttclient.Options{}, err
		}
		opts.Servers = []mqttclient.Server{srv}
	}
	if f.Debug {
		opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return opts, nil
}

// Server builds one server's settings from the flags.
func (f *ConnFlags) Server() (mqttclient.Server, error) {
	version, err := f.ProtocolVersion()
	if err != nil {
		return mqttclient.Server{}, err
	}
	tc, err := f.TLSConfig()
	if err != nil {
		return mqttclient.Server{}, err
	}
	srv := mqttclient.Server{
		Address:              f.Address,
		TLS:                  f.TLS,
		ProtocolVersion:      version,
		ClientID:             f.ClientID,
		CleanStart:           f.Clean,
		KeepAlive:            uint16(f.KeepAlive),
		Username:             f.Username,
		TLSConfig:            tc,
		ReconnectDelay:       f.Reconnect.Delay,
		ReconnectDelayMax:    f.Reconnect.DelayMax,
		ReconnectExponential: f.Reconnect.Exponential,
		ConnectTimeout:       f.Reconnect.ConnectTimeout,
	}
	if f.Password != "" {
		srv.Password = []byte(f.Password)
	}
	if f.SessionExpiry > 0 {
		if version != mqttclient.MQTT5 {
			return mqttclient.Server{}, errors.New("-session-expiry requires -v 5")
		}
		srv.ConnectProperties = &mqttclient.Properties{SessionExpiryInterval: uint32(f.SessionExpiry), SessionExpiryIntervalFlag: true}
	}
	if f.WillTopic != "" {
		srv.Will = &mqttclient.Message{
			Topic:   f.WillTopic,
			Payload: []byte(f.WillPayload),
			QoS:     byte(f.WillQoS),
			Retain:  f.WillRetain,
		}
	}
	return srv, nil
}

// ReconnectFlags are Run's reconnect settings. Only tools that always run
// Session.Run register them as flags; with -config, any tool reconnects and
// takes them from the file.
type ReconnectFlags struct {
	Delay          time.Duration `yaml:"reconnect-delay"`
	DelayMax       time.Duration `yaml:"reconnect-delay-max"`
	Exponential    bool          `yaml:"reconnect-exponential"`
	ConnectTimeout time.Duration `yaml:"connect-timeout"`
}

var defaultReconnect = ReconnectFlags{Delay: time.Second, DelayMax: 30 * time.Second, ConnectTimeout: 30 * time.Second}

// Register adds the reconnect flags to fs.
func (r *ReconnectFlags) Register(fs *flag.FlagSet) {
	d := defaultReconnect
	fs.DurationVar(&r.Delay, "reconnect-delay", d.Delay, "wait before reconnecting")
	fs.DurationVar(&r.DelayMax, "reconnect-delay-max", d.DelayMax, "longest wait before reconnecting")
	fs.BoolVar(&r.Exponential, "reconnect-exponential", d.Exponential, "grow the wait quadratically instead of linearly")
	fs.DurationVar(&r.ConnectTimeout, "connect-timeout", d.ConnectTimeout, "limit for each connection attempt")
}

// Session is a client whose OnConnect, OnDisconnect and OnConnectError are
// logged.
//
// With -config it reconnects by itself: Start runs the client's supervisor
// (Client.Run), which reconnects and fails over between the servers, and
// Lost reports only that Run gave up. Without -config, Start connects once
// and Lost reports the end of that connection.
type Session struct {
	C          *mqttclient.Client
	Version    byte // of the first server
	reconnects bool
	timeout    time.Duration
	lost       chan mqttclient.DisconnectEvent
	closed     chan mqttclient.DisconnectEvent // every OnDisconnect, for Disconnect
	up         chan struct{}                   // closed on the first accepted connection
	upOnce     sync.Once
	stopRun    context.CancelFunc // ends Start's Run
	runDone    chan struct{}      // closed when Start's Run returns
}

// NewSession creates a client from the flags. h supplies the other
// handlers; its OnConnect, OnDisconnect and OnConnectError, if set, run
// after the logging.
func NewSession(f *ConnFlags, h mqttclient.Handlers) (*Session, error) {
	opts, err := f.Options()
	if err != nil {
		return nil, err
	}
	s := &Session{
		Version:    opts.Servers[0].ProtocolVersion,
		reconnects: f.Config != "",
		timeout:    f.Timeout,
		lost:       make(chan mqttclient.DisconnectEvent, 1),
		closed:     make(chan mqttclient.DisconnectEvent, 1),
		up:         make(chan struct{}),
	}
	onPreConnect, onConnect, onDisconnect, onConnectError := h.OnPreConnect, h.OnConnect, h.OnDisconnect, h.OnConnectError
	if s.reconnects {
		h.OnPreConnect = func(c *mqttclient.Client) {
			i := c.ServerIndex()
			Logf("connecting to server %d of %d: %s", i+1, len(opts.Servers), opts.Servers[i].Address)
			if onPreConnect != nil {
				onPreConnect(c)
			}
		}
	}
	h.OnConnectError = func(c *mqttclient.Client, err error) {
		Logf("OnConnectError %v", err)
		if onConnectError != nil {
			onConnectError(c, err)
		}
	}
	h.OnConnect = func(c *mqttclient.Client, ack mqttclient.ConnAck) {
		Logf("OnConnect reason=0x%02x session_present=%v", ack.ReasonCode, ack.SessionPresent)
		if s.reconnects && ack.ReasonCode == 0 {
			// Connect is not called, so log what it would.
			Logf("  connected to %s client_id=%q", opts.Servers[c.ServerIndex()].Address, c.ClientID())
			if p := ack.Properties; p != nil {
				Logf("  CONNACK properties:%s", ConnackProps(p))
			}
			s.upOnce.Do(func() { close(s.up) })
		}
		if onConnect != nil {
			onConnect(c, ack)
		}
	}
	h.OnDisconnect = func(c *mqttclient.Client, ev mqttclient.DisconnectEvent) {
		Logf("OnDisconnect err=%v reason=0x%02x", ev.Err, ev.ReasonCode)
		if ev.Properties != nil && ev.Properties.ReasonString != "" {
			Logf("  server reason string: %q", ev.Properties.ReasonString)
		}
		if onDisconnect != nil {
			onDisconnect(c, ev)
		}
		send(s.closed, ev)
		if !s.reconnects {
			send(s.lost, ev)
		}
	}
	s.C, err = mqttclient.New(opts, h)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// send delivers ev if ch has room. Only the first undelivered event matters.
func send(ch chan mqttclient.DisconnectEvent, ev mqttclient.DisconnectEvent) {
	select {
	case ch <- ev:
	default:
	}
}

// Reconnects reports whether the session reconnects by itself (-config).
func (s *Session) Reconnects() bool { return s.reconnects }

// Lost receives an event when the session is over without Disconnect: the
// connection ended or, with -config, Run gave up on every server.
func (s *Session) Lost() <-chan mqttclient.DisconnectEvent { return s.lost }

// Start connects. Without -config it is Connect. With -config it starts
// Run, which keeps reconnecting until Disconnect, and returns once the
// first connection is accepted.
func (s *Session) Start(ctx context.Context) error {
	if !s.reconnects {
		_, err := s.Connect(ctx)
		return err
	}
	// Run gets its own context so that Ctrl-C (ctx) ends only this wait:
	// once connected, the tool decides when to Disconnect, as without
	// -config.
	runCtx, stopRun := context.WithCancel(context.Background())
	s.stopRun = stopRun
	s.runDone = make(chan struct{})
	go func() {
		defer close(s.runDone)
		if err := s.C.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			Logf("giving up: %v", err)
			send(s.lost, mqttclient.DisconnectEvent{Err: err})
		}
	}()
	select {
	case <-s.up:
		return nil
	case ev := <-s.lost:
		stopRun()
		return fmt.Errorf("connect: %w", ev.Err)
	case <-ctx.Done():
		stopRun()
		<-s.runDone
		return fmt.Errorf("connect: %w", ctx.Err())
	}
}

// Connect connects once and logs the result.
func (s *Session) Connect(ctx context.Context) (mqttclient.ConnAck, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	start := time.Now()
	ack, err := s.C.Connect(ctx)
	if err != nil {
		return ack, fmt.Errorf("connect: %w", err)
	}
	Logf("connected in %v client_id=%q session_present=%v", Since(start), s.C.ClientID(), ack.SessionPresent)
	if p := ack.Properties; p != nil {
		Logf("  CONNACK properties:%s", ConnackProps(p))
	}
	return ack, nil
}

// Run runs the client's supervisor (Client.Run) until ctx ends, Disconnect
// is called or a permanent error. Ending ctx is not an error.
func (s *Session) Run(ctx context.Context) error {
	err := s.C.Run(ctx)
	if errors.Is(err, context.Canceled) {
		Logf("stopped")
		return nil
	}
	return err
}

// Hold stays connected for d, until ctx ends (Ctrl-C), or until the
// session is lost (see Lost), which is an error.
func (s *Session) Hold(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	Logf("holding connection for %v", d)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
		Logf("interrupted")
	case ev := <-s.lost:
		return fmt.Errorf("connection lost: %w", ev.Err)
	}
	return nil
}

// Disconnect sends DISCONNECT with reason (with -config, it also stops Run)
// and waits for OnDisconnect, which runs on the client's dispatcher
// goroutine, so that its output is not lost when the process exits.
func (s *Session) Disconnect(reason byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	select {
	case <-s.closed: // an earlier connection's, with -config
	default:
	}
	connected := s.C.IsConnected()
	if err := s.C.Disconnect(ctx, reason, nil); err != nil {
		return fmt.Errorf("disconnect: %w", err)
	}
	if connected {
		Logf("disconnected")
		select {
		case <-s.closed:
		case <-ctx.Done():
			Logf("OnDisconnect not called within %v", s.timeout)
		}
	} else {
		Logf("stopped reconnecting")
	}
	if s.runDone != nil {
		s.stopRun()
		<-s.runDone
	}
	return nil
}

// ConnackProps lists the MQTT 5 CONNACK properties the server sent.
// Absent properties keep their spec defaults and are not shown.
func ConnackProps(p *mqttclient.Properties) string {
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

// Logf prints a timestamped line to stdout.
func Logf(format string, args ...any) {
	fmt.Printf("%s %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}

// Since is the time since start, rounded for printing.
func Since(start time.Time) time.Duration {
	return time.Since(start).Round(time.Microsecond)
}

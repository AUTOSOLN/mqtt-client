// Package cli holds what the manual test tools under cmd share: the
// connection flags, building a client from them, connect and disconnect
// with logging, and output helpers.
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
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

// ConnFlags are the flags that describe a connection.
type ConnFlags struct {
	Server        string
	Version       string
	ClientID      string
	Clean         bool
	KeepAlive     uint
	SessionExpiry uint
	Username      string
	Password      string
	Timeout       time.Duration
	Debug         bool

	CAFile     string
	CertFile   string
	KeyFile    string
	Insecure   bool
	ServerName string

	WillTopic   string
	WillPayload string
	WillQoS     uint
	WillRetain  bool
}

// Register adds the connection flags to fs.
func (f *ConnFlags) Register(fs *flag.FlagSet) {
	fs.StringVar(&f.Server, "server", "mqtt://localhost:1883", "broker URL: mqtt://, tcp://, mqtts://, ssl:// or tls://")
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
	fs.StringVar(&f.ServerName, "servername", "", "TLS server name, if different from the URL host")
	fs.StringVar(&f.WillTopic, "will-topic", "", "Will topic (enables the Will)")
	fs.StringVar(&f.WillPayload, "will-payload", "", "Will payload")
	fs.UintVar(&f.WillQoS, "will-qos", 0, "Will QoS")
	fs.BoolVar(&f.WillRetain, "will-retain", false, "Will retain flag")
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

// Options builds client options from the flags.
func (f *ConnFlags) Options() (mqttclient.Options, error) {
	version, err := f.ProtocolVersion()
	if err != nil {
		return mqttclient.Options{}, err
	}
	tc, err := f.TLSConfig()
	if err != nil {
		return mqttclient.Options{}, err
	}
	opts := mqttclient.Options{
		Server:          f.Server,
		ProtocolVersion: version,
		ClientID:        f.ClientID,
		CleanStart:      f.Clean,
		KeepAlive:       uint16(f.KeepAlive),
		Username:        f.Username,
		TLSConfig:       tc,
	}
	if f.Password != "" {
		opts.Password = []byte(f.Password)
	}
	if f.SessionExpiry > 0 {
		if version != mqttclient.MQTT5 {
			return opts, errors.New("-session-expiry requires -v 5")
		}
		opts.ConnectProperties = &mqttclient.Properties{SessionExpiryInterval: uint32(f.SessionExpiry), SessionExpiryIntervalFlag: true}
	}
	if f.WillTopic != "" {
		opts.Will = &mqttclient.Message{
			Topic:   f.WillTopic,
			Payload: []byte(f.WillPayload),
			QoS:     byte(f.WillQoS),
			Retain:  f.WillRetain,
		}
	}
	if f.Debug {
		opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return opts, nil
}

// ReconnectFlags are the flags for Run's reconnect behaviour. Only tools
// that use Session.Run register them.
type ReconnectFlags struct {
	Delay          time.Duration
	DelayMax       time.Duration
	Exponential    bool
	ConnectTimeout time.Duration
}

// Register adds the reconnect flags to fs.
func (r *ReconnectFlags) Register(fs *flag.FlagSet) {
	fs.DurationVar(&r.Delay, "reconnect-delay", time.Second, "wait before reconnecting")
	fs.DurationVar(&r.DelayMax, "reconnect-delay-max", 30*time.Second, "longest wait before reconnecting")
	fs.BoolVar(&r.Exponential, "reconnect-exponential", false, "grow the wait quadratically instead of linearly")
	fs.DurationVar(&r.ConnectTimeout, "connect-timeout", 30*time.Second, "limit for each connection attempt")
}

// Apply sets the reconnect options.
func (r *ReconnectFlags) Apply(o *mqttclient.Options) {
	o.ReconnectDelay = r.Delay
	o.ReconnectDelayMax = r.DelayMax
	o.ReconnectExponential = r.Exponential
	o.ConnectTimeout = r.ConnectTimeout
}

// Session is a client whose OnConnect, OnDisconnect and OnConnectError are
// logged.
type Session struct {
	C       *mqttclient.Client
	Version byte
	timeout time.Duration
	lost    chan mqttclient.DisconnectEvent
}

// NewSession creates a client from the flags. h supplies the other
// handlers; its OnConnect, OnDisconnect and OnConnectError, if set, run
// after the logging. Each of more adjusts the options.
func NewSession(f *ConnFlags, h mqttclient.Handlers, more ...func(*mqttclient.Options)) (*Session, error) {
	opts, err := f.Options()
	if err != nil {
		return nil, err
	}
	for _, m := range more {
		m(&opts)
	}
	s := &Session{Version: opts.ProtocolVersion, timeout: f.Timeout, lost: make(chan mqttclient.DisconnectEvent, 1)}
	onConnect, onDisconnect, onConnectError := h.OnConnect, h.OnDisconnect, h.OnConnectError
	h.OnConnectError = func(c *mqttclient.Client, err error) {
		Logf("OnConnectError %v", err)
		if onConnectError != nil {
			onConnectError(c, err)
		}
	}
	h.OnConnect = func(c *mqttclient.Client, ack mqttclient.ConnAck) {
		Logf("OnConnect reason=0x%02x session_present=%v", ack.ReasonCode, ack.SessionPresent)
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
		select {
		case s.lost <- ev:
		default:
		}
	}
	s.C, err = mqttclient.New(opts, h)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Lost receives the event when the connection ends.
func (s *Session) Lost() <-chan mqttclient.DisconnectEvent { return s.lost }

// Connect connects and logs the result.
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
// connection is lost, which is an error.
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

// Disconnect sends DISCONNECT with reason and waits for OnDisconnect, which
// runs on the client's dispatcher goroutine, so that its output is not lost
// when the process exits.
func (s *Session) Disconnect(reason byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	if err := s.C.Disconnect(ctx, reason, nil); err != nil {
		return fmt.Errorf("disconnect: %w", err)
	}
	Logf("disconnected")
	select {
	case <-s.lost:
	case <-ctx.Done():
		Logf("OnDisconnect not called within %v", s.timeout)
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

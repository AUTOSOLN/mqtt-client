package mqttclient

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log/slog"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// Properties holds MQTT 5 properties. Several values only take effect when
// their matching flag is also set (SessionExpiryIntervalFlag,
// RequestProblemInfoFlag, PayloadFormatFlag, ...).
type Properties = packets.Properties

// UserProperty is an MQTT 5 user property key/value pair.
type UserProperty = packets.UserProperty

// Protocol versions accepted in Options.ProtocolVersion.
const (
	MQTT31  byte = 3
	MQTT311 byte = 4
	MQTT5   byte = 5
)

// Defaults match libmosquitto (mosquitto_reinitialise).
const (
	DefaultReceiveMaximum uint16 = 20
	DefaultMaxInflight    uint16 = 20
)

// Message is an application message: a PUBLISH payload, or a Will.
type Message struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties *Properties // MQTT 5 only

	// Mid is the packet identifier of a received QoS 1 or 2 message. It is
	// ignored by Publish and SetWill.
	Mid uint16

	// Dup is the DUP flag of a received message: the server marked it as a
	// possible redelivery. For QoS 2 it is the flag of the last PUBLISH
	// received before PUBREL. It is ignored by Publish and SetWill, which
	// set DUP themselves on a resend.
	Dup bool
}

// Options configures a Client. At least one server is required.
type Options struct {
	// Servers are the brokers the client may connect to, each with its own
	// connection settings. Connect and Run try them in order, starting from
	// the current server (see Client.ServerIndex), and move on to the next
	// one, cycling back to the first, when an attempt fails. The session
	// (queued and in-flight QoS 1 and 2 messages) belongs to the client, so
	// it carries over from one server to the next.
	Servers []Server

	// Logger receives debug output. Nil discards it.
	Logger *slog.Logger

	// NoTopicCheck turns off the topic name and topic filter checks (empty
	// topic, UTF-8 rules, wildcard placement) for PUBLISH, SUBSCRIBE,
	// UNSUBSCRIBE, the Will and received PUBLISH, so that a malformed topic
	// goes to the server as given. Only the 65535-byte limit remains. It is
	// meant for testing how a server handles invalid topics.
	NoTopicCheck bool
}

// Server is one broker in Options.Servers and the settings used to connect
// to it. Address is the only required field.
type Server struct {
	// Address is the broker as host:port, where host is an IP address or a
	// hostname (an IPv6 address in brackets, "[::1]:1883"). The port is
	// required.
	Address string

	// TLS connects with TLS rather than plain TCP. It is never inferred from
	// the port.
	TLS bool

	// ProtocolVersion is MQTT31, MQTT311 or MQTT5. Zero means MQTT311, the
	// libmosquitto default.
	ProtocolVersion byte

	// ClientID may be empty only when CleanStart is set (mosquitto_new). For
	// MQTT 3.1, which requires a client id, an empty id is replaced with a
	// random "mosq-..." id as libmosquitto does.
	ClientID string

	// CleanStart is the clean session (3.x) or clean start (5) flag.
	CleanStart bool

	// KeepAlive in seconds; 0 disables keepalive, otherwise it must be at
	// least 5 (mosquitto__connect_init).
	KeepAlive uint16

	// Username and Password are sent when non-empty. MQTT 3.x does not allow
	// a password without a username.
	Username string
	Password []byte

	// Will, if set, is registered with the server in CONNECT.
	Will *Message

	// ConnectProperties are extra MQTT 5 CONNECT properties.
	ConnectProperties *Properties

	// ReceiveMaximum is the number of inbound QoS 1/2 messages the client
	// accepts concurrently, sent in MQTT 5 CONNECT unless ConnectProperties
	// already sets it. Zero means DefaultReceiveMaximum.
	ReceiveMaximum uint16

	// MaxInflight bounds outbound QoS 1/2 messages awaiting acknowledgement
	// (mosquitto_max_inflight_messages_set). When an MQTT 5 server sends
	// Receive Maximum in CONNACK, that applies instead for the connection.
	// Messages beyond the limit are queued. Zero means DefaultMaxInflight.
	MaxInflight uint16

	// ReconnectDelay, ReconnectDelayMax and ReconnectExponential set how
	// long Run waits before connecting to this server again, or to it next
	// after another server (mosquitto_reconnect_delay_set). The n-th
	// consecutive wait before this server (n from 0) is ReconnectDelay*(n+1),
	// or ReconnectDelay*(n+1)*(n+1) with ReconnectExponential, capped at
	// ReconnectDelayMax. A successful CONNACK from this server resets its n.
	// Zero values mean 1s and ReconnectDelay, libmosquitto's defaults: a
	// fixed 1s.
	ReconnectDelay       time.Duration
	ReconnectDelayMax    time.Duration
	ReconnectExponential bool

	// ConnectTimeout bounds each Connect or Run connection attempt to this
	// server, from dialling to CONNACK. Zero means 30s.
	ConnectTimeout time.Duration

	// TLSConfig is used when TLS is set, and setting it without TLS is an
	// error. If nil, a default configuration is used. An empty ServerName
	// is set from the Address host.
	TLSConfig *tls.Config

	// Dial, if set, opens the network connection to Address in place of a
	// plain net.Dialer, for example to go through a proxy or to wrap the
	// connection. It is called with network "tcp" and the context of the
	// connection attempt. With TLS, the handshake runs over the connection
	// it returns. (*net.Dialer).DialContext has this signature.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

// Handlers are the client callbacks. They are called one at a time, in
// order, on a goroutine owned by the client, the way libmosquitto calls
// callbacks from its network loop. Callbacks may call Client methods, but
// must not block waiting for network activity (for example waiting for an
// acknowledgement), since later callbacks queue behind them.
type Handlers struct {
	// OnPreConnect is called just before each network connection attempt,
	// synchronously from Connect or Run (mosquitto_pre_connect_callback_set).
	// ServerIndex reports the server about to be tried, and SetCredentials
	// and SetWill change what is sent to it in CONNECT.
	OnPreConnect func(c *Client)

	// OnConnect is called when CONNACK is received, whether the connection
	// was accepted or refused.
	OnConnect func(c *Client, ack ConnAck)

	// OnDisconnect is called once for every network connection that was
	// established, after it has closed.
	OnDisconnect func(c *Client, ev DisconnectEvent)

	// OnConnectError is called by Run for every connection attempt that
	// fails, whichever server it was to: the network connection, the TLS
	// handshake, CONNACK not arriving within ConnectTimeout, or a refusal
	// (which OnConnect and OnDisconnect also report).
	OnConnectError func(c *Client, err error)

	// OnMessage is called for every message received from the server: QoS 0
	// and 1 on arrival (after PUBACK is sent), QoS 2 when PUBREL arrives
	// (after PUBCOMP is sent). The handler owns m.
	OnMessage func(c *Client, m *Message)

	// OnPublish is called when a published message is complete: QoS 0 once
	// written to the network, QoS 1 on PUBACK, QoS 2 on PUBCOMP, or on a
	// PUBREC with a failure reason code (MQTT 5). reason and props are the
	// acknowledgement's reason code and properties.
	OnPublish func(c *Client, mid uint16, reason byte, props *Properties)

	// OnSubscribe is called when SUBACK arrives, with one granted QoS or
	// reason code per topic filter, in request order.
	OnSubscribe func(c *Client, mid uint16, granted []byte, props *Properties)

	// OnUnsubscribe is called when UNSUBACK arrives, with one reason code per
	// topic filter. MQTT 3.x has no reason codes, so they are all 0 (success).
	OnUnsubscribe func(c *Client, mid uint16, reasons []byte, props *Properties)
}

// ConnAck describes a received CONNACK.
type ConnAck struct {
	ReasonCode     byte // 0 on success; a 3.x return code or a 5 reason code otherwise
	SessionPresent bool
	Properties     *Properties // MQTT 5 only
}

// DisconnectEvent describes why a connection ended.
type DisconnectEvent struct {
	// Err is nil when the application called Disconnect. Otherwise it wraps
	// one of ErrConnectionLost, ErrKeepalive, ErrProtocol,
	// ErrMalformedPacket, ErrServerDisconnect, or is a *ConnRefusedError.
	Err error

	// ReasonCode and Properties are set when the server sent DISCONNECT
	// (MQTT 5).
	ReasonCode byte
	Properties *Properties
}

// splitAddress checks a Server.Address and returns its host.
func splitAddress(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("%w: address %q: %v", ErrInvalid, addr, err)
	}
	if host == "" {
		return "", fmt.Errorf("%w: address %q: missing host", ErrInvalid, addr)
	}
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
		return "", fmt.Errorf("%w: address %q: port must be 1-65535", ErrInvalid, addr)
	}
	return host, nil
}

// validate checks and normalises opts in place.
func (o *Options) validate() error {
	if len(o.Servers) == 0 {
		return fmt.Errorf("%w: no servers", ErrInvalid)
	}
	o.Servers = slices.Clone(o.Servers)
	for i := range o.Servers {
		if err := o.Servers[i].validate(o.NoTopicCheck); err != nil {
			if len(o.Servers) > 1 {
				return fmt.Errorf("server %d (%s): %w", i, o.Servers[i].Address, err)
			}
			return err
		}
	}
	return nil
}

// validate checks and normalises s in place.
func (s *Server) validate(noTopicCheck bool) error {
	if _, err := splitAddress(s.Address); err != nil {
		return err
	}
	if s.TLSConfig != nil && !s.TLS {
		return fmt.Errorf("%w: TLSConfig set without TLS", ErrInvalid)
	}
	if s.ProtocolVersion == 0 {
		s.ProtocolVersion = MQTT311
	}
	switch s.ProtocolVersion {
	case MQTT31, MQTT311, MQTT5:
	default:
		return fmt.Errorf("%w: protocol version %d", ErrInvalid, s.ProtocolVersion)
	}
	if s.ClientID == "" && !s.CleanStart {
		return fmt.Errorf("%w: an empty client id requires CleanStart", ErrInvalid)
	}
	if err := checkUTF8String(s.ClientID); err != nil {
		return fmt.Errorf("%w: client id: %v", ErrInvalid, err)
	}
	if s.ClientID == "" && s.ProtocolVersion == MQTT31 {
		s.ClientID = randomClientID()
	}
	if s.KeepAlive != 0 && s.KeepAlive < 5 {
		return fmt.Errorf("%w: keepalive must be 0 or at least 5 seconds", ErrInvalid)
	}
	if err := checkCredentials(s.ProtocolVersion, s.Username, s.Password); err != nil {
		return err
	}
	if err := checkWill(s.ProtocolVersion, noTopicCheck, s.Will); err != nil {
		return err
	}
	if s.ConnectProperties != nil && s.ProtocolVersion != MQTT5 {
		return fmt.Errorf("%w: CONNECT properties require MQTT 5", ErrNotSupported)
	}
	if s.ReceiveMaximum == 0 {
		s.ReceiveMaximum = DefaultReceiveMaximum
	}
	if s.MaxInflight == 0 {
		s.MaxInflight = DefaultMaxInflight
	}
	return s.validateRun()
}

// validateRun checks and defaults the options used by Run.
func (s *Server) validateRun() error {
	if s.ReconnectDelay < 0 || s.ReconnectDelayMax < 0 || s.ConnectTimeout < 0 {
		return fmt.Errorf("%w: negative reconnect delay or connect timeout", ErrInvalid)
	}
	if s.ReconnectDelay == 0 {
		s.ReconnectDelay = time.Second
	}
	if s.ReconnectDelayMax == 0 {
		s.ReconnectDelayMax = s.ReconnectDelay
	}
	if s.ConnectTimeout == 0 {
		s.ConnectTimeout = 30 * time.Second
	}
	return nil
}

func checkCredentials(version byte, username string, password []byte) error {
	if err := checkUTF8String(username); err != nil {
		return fmt.Errorf("%w: username: %v", ErrInvalid, err)
	}
	if len(password) > math.MaxUint16 {
		return fmt.Errorf("%w: password too long", ErrInvalid)
	}
	if version < MQTT5 && username == "" && len(password) > 0 {
		return fmt.Errorf("%w: MQTT 3.x does not allow a password without a username", ErrInvalid)
	}
	return nil
}

func checkWill(version byte, noTopicCheck bool, w *Message) error {
	if w == nil {
		return nil
	}
	if err := checkPublishTopic(w.Topic, noTopicCheck); err != nil {
		return fmt.Errorf("%w: will topic: %v", ErrInvalid, err)
	}
	if w.QoS > 2 {
		return fmt.Errorf("%w: will qos %d", ErrInvalid, w.QoS)
	}
	if len(w.Payload) > math.MaxUint16 {
		return fmt.Errorf("%w: will payload too long", ErrInvalid)
	}
	if w.Properties != nil && version != MQTT5 {
		return fmt.Errorf("%w: will properties require MQTT 5", ErrNotSupported)
	}
	return nil
}

// checkUTF8String applies the MQTT UTF-8 string rules: valid UTF-8, no
// U+0000, at most 65535 bytes.
func checkUTF8String(s string) error {
	if len(s) > math.MaxUint16 {
		return fmt.Errorf("longer than 65535 bytes")
	}
	if !utf8.ValidString(s) {
		return fmt.Errorf("not valid UTF-8")
	}
	if strings.IndexByte(s, 0) >= 0 {
		return fmt.Errorf("contains U+0000")
	}
	return nil
}

// checkPublishTopic is mosquitto_pub_topic_check: non-empty, valid UTF-8
// string, no wildcards. With noCheck only the encoding's length limit is
// applied.
func checkPublishTopic(topic string, noCheck bool) error {
	if noCheck {
		return checkTopicLength(topic)
	}
	if topic == "" {
		return fmt.Errorf("empty topic")
	}
	if err := checkUTF8String(topic); err != nil {
		return err
	}
	if strings.ContainsAny(topic, "+#") {
		return fmt.Errorf("wildcards are not allowed in a topic name")
	}
	return nil
}

// checkSubscribeTopic is mosquitto_sub_topic_check: non-empty, valid UTF-8
// string, and wildcards that fill a whole level, with '#' only last. With
// noCheck only the encoding's length limit is applied.
func checkSubscribeTopic(filter string, noCheck bool) error {
	if noCheck {
		return checkTopicLength(filter)
	}
	if filter == "" {
		return fmt.Errorf("empty topic filter")
	}
	if err := checkUTF8String(filter); err != nil {
		return err
	}
	levels := strings.Split(filter, "/")
	for i, l := range levels {
		if strings.Contains(l, "+") && l != "+" {
			return fmt.Errorf("'+' must be a whole topic level")
		}
		if strings.Contains(l, "#") && (l != "#" || i != len(levels)-1) {
			return fmt.Errorf("'#' must be the whole last topic level")
		}
	}
	return nil
}

// checkTopicLength is the one topic check Options.NoTopicCheck keeps: a
// longer string cannot be encoded.
func checkTopicLength(topic string) error {
	if len(topic) > math.MaxUint16 {
		return fmt.Errorf("longer than 65535 bytes")
	}
	return nil
}

// randomClientID generates an id the way mosquitto__connect_init does for
// MQTT 3.1: "mosq-" followed by 18 random alphanumerics.
func randomClientID() string {
	const alphanum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 18)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = alphanum[int(b[i]&0x7F)%len(alphanum)]
	}
	return "mosq-" + string(b)
}

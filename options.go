package mqttclient

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/url"
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
}

// Options configures a Client. Server is the only required field.
type Options struct {
	// Server is the broker URL: mqtt:// or tcp:// for plain TCP (default port
	// 1883), mqtts://, ssl:// or tls:// for TLS (default port 8883).
	Server string

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
	// long Run waits before connecting again (mosquitto_reconnect_delay_set).
	// The n-th consecutive wait (n from 0) is ReconnectDelay*(n+1), or
	// ReconnectDelay*(n+1)*(n+1) with ReconnectExponential, capped at
	// ReconnectDelayMax. A successful CONNACK resets n. Zero values mean 1s
	// and ReconnectDelay, libmosquitto's defaults: a fixed 1s.
	ReconnectDelay       time.Duration
	ReconnectDelayMax    time.Duration
	ReconnectExponential bool

	// ConnectTimeout bounds each of Run's connection attempts, from dialling
	// to CONNACK. Zero means 30s.
	ConnectTimeout time.Duration

	// TLSConfig is used for mqtts/ssl/tls servers. If nil, a default
	// configuration with ServerName set from the URL host is used.
	TLSConfig *tls.Config

	// Logger receives debug output. Nil discards it.
	Logger *slog.Logger
}

// Handlers are the client callbacks. They are called one at a time, in
// order, on a goroutine owned by the client, the way libmosquitto calls
// callbacks from its network loop. Callbacks may call Client methods, but
// must not block waiting for network activity (for example waiting for an
// acknowledgement), since later callbacks queue behind them.
type Handlers struct {
	// OnPreConnect is called just before each network connection attempt,
	// synchronously from Connect (mosquitto_pre_connect_callback_set). It may
	// call SetCredentials or SetWill to change what is sent in CONNECT.
	OnPreConnect func(c *Client)

	// OnConnect is called when CONNACK is received, whether the connection
	// was accepted or refused.
	OnConnect func(c *Client, ack ConnAck)

	// OnDisconnect is called once for every network connection that was
	// established, after it has closed.
	OnDisconnect func(c *Client, ev DisconnectEvent)

	// OnConnectError is called by Run for every connection attempt that
	// fails: the network connection, the TLS handshake, CONNACK not
	// arriving within ConnectTimeout, or a refusal (which OnConnect and
	// OnDisconnect also report).
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

// server is a parsed Options.Server.
type server struct {
	addr   string // host:port
	host   string
	useTLS bool
}

func parseServer(s string) (server, error) {
	u, err := url.Parse(s)
	if err != nil {
		return server{}, fmt.Errorf("%w: server %q: %v", ErrInvalid, s, err)
	}
	var sv server
	port := "1883"
	switch strings.ToLower(u.Scheme) {
	case "mqtt", "tcp":
	case "mqtts", "ssl", "tls":
		sv.useTLS = true
		port = "8883"
	default:
		return server{}, fmt.Errorf("%w: server %q: unsupported scheme %q", ErrInvalid, s, u.Scheme)
	}
	sv.host = u.Hostname()
	if sv.host == "" {
		return server{}, fmt.Errorf("%w: server %q: missing host", ErrInvalid, s)
	}
	if p := u.Port(); p != "" {
		port = p
	}
	sv.addr = net.JoinHostPort(sv.host, port)
	return sv, nil
}

// validate checks and normalises opts in place.
func (o *Options) validate() error {
	if o.ProtocolVersion == 0 {
		o.ProtocolVersion = MQTT311
	}
	switch o.ProtocolVersion {
	case MQTT31, MQTT311, MQTT5:
	default:
		return fmt.Errorf("%w: protocol version %d", ErrInvalid, o.ProtocolVersion)
	}
	if o.ClientID == "" && !o.CleanStart {
		return fmt.Errorf("%w: an empty client id requires CleanStart", ErrInvalid)
	}
	if err := checkUTF8String(o.ClientID); err != nil {
		return fmt.Errorf("%w: client id: %v", ErrInvalid, err)
	}
	if o.ClientID == "" && o.ProtocolVersion == MQTT31 {
		o.ClientID = randomClientID()
	}
	if o.KeepAlive != 0 && o.KeepAlive < 5 {
		return fmt.Errorf("%w: keepalive must be 0 or at least 5 seconds", ErrInvalid)
	}
	if err := checkCredentials(o.ProtocolVersion, o.Username, o.Password); err != nil {
		return err
	}
	if err := checkWill(o.ProtocolVersion, o.Will); err != nil {
		return err
	}
	if o.ConnectProperties != nil && o.ProtocolVersion != MQTT5 {
		return fmt.Errorf("%w: CONNECT properties require MQTT 5", ErrNotSupported)
	}
	if o.ReceiveMaximum == 0 {
		o.ReceiveMaximum = DefaultReceiveMaximum
	}
	if o.MaxInflight == 0 {
		o.MaxInflight = DefaultMaxInflight
	}
	return o.validateRun()
}

// validateRun checks and defaults the options used by Run.
func (o *Options) validateRun() error {
	if o.ReconnectDelay < 0 || o.ReconnectDelayMax < 0 || o.ConnectTimeout < 0 {
		return fmt.Errorf("%w: negative reconnect delay or connect timeout", ErrInvalid)
	}
	if o.ReconnectDelay == 0 {
		o.ReconnectDelay = time.Second
	}
	if o.ReconnectDelayMax == 0 {
		o.ReconnectDelayMax = o.ReconnectDelay
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 30 * time.Second
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

func checkWill(version byte, w *Message) error {
	if w == nil {
		return nil
	}
	if err := checkPublishTopic(w.Topic); err != nil {
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
// string, no wildcards.
func checkPublishTopic(topic string) error {
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
// string, and wildcards that fill a whole level, with '#' only last.
func checkSubscribeTopic(filter string) error {
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

package clitest

import (
	"bytes"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/capture/capturetest"
)

// BrokerAddr is the server address of every connection a Capture builds.
const BrokerAddr = "10.0.0.2:1883"

// Capture builds a packet capture of scripted MQTT 3.1.1 sessions, for
// tests of the tools' -replay flags and of mqttpcap. Packets are a
// millisecond apart.
type Capture struct {
	t     testing.TB
	conns []*capture.Conn
	ms    int
}

// NewCapture starts an empty capture.
func NewCapture(t testing.TB) *Capture { return &Capture{t: t} }

// Session is one connection in a Capture.
type Session struct {
	c    *Capture
	conn *capture.Conn
}

// Session starts a connection from client (host:port) to BrokerAddr.
func (c *Capture) Session(client string) *Session {
	conn := &capture.Conn{Client: netip.MustParseAddrPort(client), Server: netip.MustParseAddrPort(BrokerAddr)}
	c.conns = append(c.conns, conn)
	return &Session{c: c, conn: conn}
}

func (c *Capture) next() time.Time {
	c.ms++
	return time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC).Add(time.Duration(c.ms) * time.Millisecond)
}

func (s *Session) add(d capture.Dir, b []byte) *Session {
	s.conn.Segments = append(s.conn.Segments, capture.Segment{Dir: d, Time: s.c.next(), Data: b})
	return s
}

// Client adds bytes sent by the client.
func (s *Session) Client(b []byte) *Session { return s.add(capture.FromClient, b) }

// Server adds bytes sent by the broker.
func (s *Session) Server(b []byte) *Session { return s.add(capture.FromServer, b) }

// ClientClose adds a FIN from the client.
func (s *Session) ClientClose() *Session {
	t := s.c.next()
	s.conn.ClientClose = &t
	return s
}

// ServerClose adds a FIN from the broker.
func (s *Session) ServerClose() *Session {
	t := s.c.next()
	s.conn.ServerClose = &t
	return s
}

// Write writes the capture to a pcap file in a temporary directory and
// returns its path.
func (c *Capture) Write() string {
	c.t.Helper()
	conns := make([]capture.Conn, len(c.conns))
	for i, conn := range c.conns {
		conns[i] = *conn
	}
	var buf bytes.Buffer
	if err := capturetest.Write(&buf, conns); err != nil {
		c.t.Fatal(err)
	}
	path := filepath.Join(c.t.TempDir(), "capture.pcap")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		c.t.Fatal(err)
	}
	return path
}

// MQTT 3.1.1 packets, as the tools send them by default.

// Connect is a CONNECT with a clean session and a 60 s keepalive.
func Connect(id string) []byte { return connect(id, nil, nil, "", nil) }

// ConnectAuth is Connect with a username and password.
func ConnectAuth(id, user, password string) []byte {
	return connect(id, []byte(user), []byte(password), "", nil)
}

// ConnectWill is Connect with a QoS 1 Will.
func ConnectWill(id, topic string, payload []byte) []byte {
	return connect(id, nil, nil, topic, payload)
}

func connect(id string, user, password []byte, willTopic string, willPayload []byte) []byte {
	flags := byte(0x02) // clean session
	if willTopic != "" {
		flags |= 0x04 | 1<<3 // Will, QoS 1
	}
	if user != nil {
		flags |= 0x80
	}
	if password != nil {
		flags |= 0x40
	}
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, flags, 0, 60}
	body = appendString(body, []byte(id))
	if willTopic != "" {
		body = appendString(body, []byte(willTopic))
		body = appendString(body, willPayload)
	}
	if user != nil {
		body = appendString(body, user)
	}
	if password != nil {
		body = appendString(body, password)
	}
	return packet(0x10, body)
}

func appendString(b, s []byte) []byte {
	return append(append(b, byte(len(s)>>8), byte(len(s))), s...)
}

// Connack accepts a connection, with no session present.
func Connack() []byte { return []byte{0x20, 2, 0, 0} }

// Subscribe subscribes to one filter.
func Subscribe(id uint16, filter string, qos byte) []byte {
	body := appendString([]byte{byte(id >> 8), byte(id)}, []byte(filter))
	return packet(0x82, append(body, qos))
}

// Suback grants one filter qos.
func Suback(id uint16, qos byte) []byte { return []byte{0x90, 3, byte(id >> 8), byte(id), qos} }

// Publish is a PUBLISH; id is ignored at QoS 0.
func Publish(topic string, qos byte, id uint16, payload []byte) []byte {
	body := appendString(nil, []byte(topic))
	if qos > 0 {
		body = append(body, byte(id>>8), byte(id))
	}
	return packet(0x30|qos<<1, append(body, payload...))
}

// Puback acknowledges a QoS 1 PUBLISH.
func Puback(id uint16) []byte { return []byte{0x40, 2, byte(id >> 8), byte(id)} }

// Disconnect is a DISCONNECT.
func Disconnect() []byte { return []byte{0xe0, 0} }

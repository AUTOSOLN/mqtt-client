// Package replay decodes the MQTT packets of a captured connection into an
// editable Timeline, and serves timelines to a live mqttclient.Client
// through Server.Dial, so that a test or tool runs the real client against
// what a broker once sent.
//
// A test reads and edits a timeline with ordinary Go: the fields of each
// Packet, slices functions on Timeline.Packets, and Packet.WithPayload.
// Inbound turns what publishing clients sent into what a broker would
// deliver to a subscriber. The package knows nothing about Sparkplug;
// tests combine it with packages sparkplug and sparkplug/verify.
package replay

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/capture"
)

// Control packet types, as numbered by the MQTT specification.
const (
	Connect byte = iota + 1
	Connack
	Publish
	Puback
	Pubrec
	Pubrel
	Pubcomp
	Subscribe
	Suback
	Unsubscribe
	Unsuback
	Pingreq
	Pingresp
	Disconnect
	Auth
)

var typeNames = [...]string{"RESERVED", "CONNECT", "CONNACK", "PUBLISH", "PUBACK", "PUBREC", "PUBREL",
	"PUBCOMP", "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK", "PINGREQ", "PINGRESP", "DISCONNECT", "AUTH"}

// maxRemaining is the largest remaining length MQTT can encode.
const maxRemaining = 268435455

// ConnectInfo is what a CONNECT says about the client. The username and
// password are deliberately left out; Packet.Raw still holds them.
type ConnectInfo struct {
	Version   byte // protocol level: 3 (MQTT 3.1), 4 (3.1.1) or 5
	ClientID  string
	Clean     bool
	KeepAlive uint16              // seconds
	Will      *mqttclient.Message // nil without a Will
}

// Packet is one decoded MQTT control packet, or a captured close. Fields
// that don't apply to the packet type are zero. Packet is a value: Decode
// gives each Packet its own Raw and Payload, so editing one never changes
// another.
type Packet struct {
	Dir  capture.Dir
	Time time.Time // when the packet's last byte was captured; zero for packets read from a live client

	// Close marks a FIN or RST from Dir. A close has no bytes.
	Close bool

	// Auto marks a packet the Replayer fills in from the live client
	// instead of sending or comparing it as captured. An Auto client
	// packet is matched by type only. An Auto broker CONNACK, SUBACK or
	// UNSUBACK accepts what the live client asked for, granting the
	// requested QoS. Inbound uses it; tests that build timelines by hand
	// may too.
	Auto bool

	Type        byte // control packet type: Connect ... Auth
	Dup         bool // PUBLISH only
	QoS         byte // PUBLISH only
	Retain      bool // PUBLISH only
	PacketID    uint16
	Topic       string                    // PUBLISH
	Properties  *mqttclient.Properties    // MQTT 5 only; nil otherwise
	ReasonCodes []byte                    // CONNACK, the four PUBLISH acknowledgements (MQTT 5), SUBACK, UNSUBACK, DISCONNECT (MQTT 5)
	Payload     []byte                    // PUBLISH
	Connect     *ConnectInfo              // CONNECT
	Filters     []mqttclient.Subscription // SUBSCRIBE; UNSUBSCRIBE (Topic only)

	// Raw is the whole packet as captured, or as rebuilt by WithPayload.
	// It is what the Replayer sends. Editing the decoded fields above
	// doesn't change Raw. The Raw of a CONNECT holds the password: don't
	// print it.
	Raw []byte

	// Err says why the packet couldn't be decoded; it wraps
	// mqttclient.ErrMalformedPacket. Raw is still set and still replayed.
	Err error
}

// TypeName is the specification's name for the packet type, or "close".
func (p Packet) TypeName() string {
	if p.Close {
		return "close"
	}
	if int(p.Type) < len(typeNames) {
		return typeNames[p.Type]
	}
	return fmt.Sprintf("type %d", p.Type)
}

// WithPayload returns a copy of a PUBLISH with its payload replaced and Raw
// rebuilt. The topic, packet identifier, flags and properties bytes are
// kept as captured. It returns an error for any other packet, or for a
// PUBLISH that couldn't be decoded.
func (p Packet) WithPayload(b []byte) (Packet, error) {
	if p.Close || p.Type != Publish || p.Err != nil {
		return Packet{}, fmt.Errorf("replay: WithPayload needs a well-formed PUBLISH, not %s", p.TypeName())
	}
	_, hdr, err := frame(p.Raw)
	if err != nil || hdr == 0 || len(p.Raw)-hdr < len(p.Payload) {
		return Packet{}, errors.New("replay: WithPayload: Raw doesn't hold this packet")
	}
	varHeader := p.Raw[hdr : len(p.Raw)-len(p.Payload)]
	rem := len(varHeader) + len(b)
	if rem > maxRemaining {
		return Packet{}, fmt.Errorf("replay: WithPayload: packet of %d bytes is too large for MQTT", rem)
	}
	raw := make([]byte, 0, 5+rem)
	raw = append(raw, p.Raw[0])
	raw = appendLength(raw, rem)
	raw = append(raw, varHeader...)
	raw = append(raw, b...)
	p.Raw = raw
	p.Payload = bytes.Clone(b)
	return p, nil
}

// Timeline is one connection's packets in capture order.
type Timeline struct {
	Client, Server netip.AddrPort
	Version        byte // MQTT protocol level of the connection: 3, 4 or 5
	Packets        []Packet
}

// Decode turns a plaintext connection into a timeline, in capture order.
// A packet is placed at the segment that completes it; closes are placed
// by time. version is used when the connection has no CONNECT; 0 means
// read it from the CONNECT.
//
// It returns an error wrapping capture.ErrEncrypted for a TLS connection.
// If the connection's streams end early (c.Err), or end inside a packet,
// it returns the packets decoded so far with an error saying so: the
// timeline is good up to that point and not after.
func Decode(c capture.Conn, version byte) (Timeline, error) {
	tl := Timeline{Client: c.Client, Server: c.Server, Version: version}
	if c.Transport == capture.TLS {
		return tl, fmt.Errorf("replay: %v to %v: %w", c.Client, c.Server, capture.ErrEncrypted)
	}
	var bufs [2][]byte // pending bytes, by capture.Dir
	var fail error
	for _, s := range c.Segments {
		d := s.Dir
		bufs[d] = append(bufs[d], s.Data...)
		for {
			n, hdr, err := frame(bufs[d])
			if err != nil {
				fail = fmt.Errorf("replay: %s stream: %w", d, err)
				break
			}
			if n == 0 {
				break
			}
			raw := bytes.Clone(bufs[d][:n])
			bufs[d] = bufs[d][n:]
			if tl.Version == 0 {
				if raw[0]>>4 != Connect {
					fail = errors.New("replay: the capture has no CONNECT; give the protocol version")
					break
				}
				tl.Version = connectVersion(raw[hdr:])
			}
			p := decodePacket(raw, hdr, tl.Version)
			p.Dir, p.Time = d, s.Time
			tl.Packets = append(tl.Packets, p)
		}
		if fail != nil {
			break
		}
	}
	tl.Packets = insertClose(tl.Packets, capture.FromClient, c.ClientClose)
	tl.Packets = insertClose(tl.Packets, capture.FromServer, c.ServerClose)
	switch {
	case fail != nil:
		return tl, fail
	case c.Err != nil:
		return tl, fmt.Errorf("replay: %v to %v: %w", c.Client, c.Server, c.Err)
	}
	for d, b := range bufs {
		if len(b) > 0 {
			return tl, fmt.Errorf("replay: %w: %d bytes of an incomplete packet at the end of the %s stream",
				capture.ErrTruncated, len(b), capture.Dir(d))
		}
	}
	return tl, nil
}

// insertClose puts a close from d at time *t, after every packet captured
// at or before it.
func insertClose(ps []Packet, d capture.Dir, t *time.Time) []Packet {
	if t == nil {
		return ps
	}
	i := len(ps)
	for i > 0 && ps[i-1].Time.After(*t) {
		i--
	}
	cl := Packet{Dir: d, Time: *t, Close: true}
	return append(ps[:i], append([]Packet{cl}, ps[i:]...)...)
}

// connectVersion reads the protocol level from a CONNECT body: a two-byte
// length, the protocol name, then the level. It returns 0 if the body is
// too short, which decodePacket then reports.
func connectVersion(body []byte) byte {
	if len(body) < 2 {
		return 0
	}
	i := 2 + int(binary.BigEndian.Uint16(body))
	if i >= len(body) {
		return 0
	}
	return body[i]
}

// frame finds the first packet in b. It returns the packet's length and
// the length of its fixed header, or n == 0 if b doesn't hold a whole
// packet yet.
func frame(b []byte) (n, hdr int, err error) {
	rem, mul := 0, 1
	for i := 1; ; i++ {
		if i > 4 {
			return 0, 0, fmt.Errorf("%w: remaining length longer than 4 bytes", mqttclient.ErrMalformedPacket)
		}
		if i >= len(b) {
			return 0, 0, nil
		}
		rem += int(b[i]&0x7f) * mul
		mul *= 128
		if b[i]&0x80 == 0 {
			hdr = i + 1
			break
		}
	}
	if len(b) < hdr+rem {
		return 0, 0, nil
	}
	return hdr + rem, hdr, nil
}

// appendLength appends n as an MQTT variable byte integer.
func appendLength(b []byte, n int) []byte {
	for {
		c := byte(n % 128)
		n /= 128
		if n > 0 {
			c |= 0x80
		}
		b = append(b, c)
		if n == 0 {
			return b
		}
	}
}

// decodePacket decodes raw, whose fixed header is hdr bytes long, for a
// connection at protocol level version. It never fails: a packet that
// can't be decoded comes back with Err set.
func decodePacket(raw []byte, hdr int, version byte) Packet {
	p := Packet{Raw: raw, Type: raw[0] >> 4}
	var fh packets.FixedHeader
	if err := fh.Decode(raw[0]); err != nil {
		p.Err = fmt.Errorf("%w: %s fixed header: %v", mqttclient.ErrMalformedPacket, p.TypeName(), err)
		return p
	}
	// The body is a private copy: comqtt's decoders return slices of it.
	body := bytes.Clone(raw[hdr:])
	fh.Remaining = len(body)
	pk := packets.Packet{FixedHeader: fh, ProtocolVersion: version}
	if err := decodeBody(&pk, body); err != nil {
		p.Err = fmt.Errorf("%w: %s: %v", mqttclient.ErrMalformedPacket, p.TypeName(), err)
		return p
	}
	if version == mqttclient.MQTT5 {
		switch p.Type {
		case Pingreq, Pingresp:
		default:
			props := pk.Properties
			p.Properties = &props
		}
	}
	switch p.Type {
	case Connect:
		ci := &ConnectInfo{Version: pk.ProtocolVersion, ClientID: pk.Connect.ClientIdentifier,
			Clean: pk.Connect.Clean, KeepAlive: pk.Connect.Keepalive}
		if pk.Connect.WillFlag {
			ci.Will = &mqttclient.Message{Topic: pk.Connect.WillTopic, Payload: pk.Connect.WillPayload,
				QoS: pk.Connect.WillQos, Retain: pk.Connect.WillRetain}
			if pk.ProtocolVersion == mqttclient.MQTT5 {
				wp := pk.Connect.WillProperties
				ci.Will.Properties = &wp
			}
		}
		p.Connect = ci
	case Connack:
		p.ReasonCodes = []byte{pk.ReasonCode}
	case Publish:
		p.Dup, p.QoS, p.Retain = fh.Dup, fh.Qos, fh.Retain
		p.PacketID, p.Topic, p.Payload = pk.PacketID, pk.TopicName, pk.Payload
	case Puback, Pubrec, Pubrel, Pubcomp:
		p.PacketID = pk.PacketID
		if version == mqttclient.MQTT5 {
			p.ReasonCodes = []byte{pk.ReasonCode}
		}
	case Subscribe, Unsubscribe:
		p.PacketID = pk.PacketID
		for _, f := range pk.Filters {
			p.Filters = append(p.Filters, mqttclient.Subscription{Topic: f.Filter, QoS: f.Qos,
				NoLocal: f.NoLocal, RetainAsPublished: f.RetainAsPublished, RetainHandling: f.RetainHandling})
		}
	case Suback, Unsuback:
		p.PacketID, p.ReasonCodes = pk.PacketID, pk.ReasonCodes
	case Disconnect:
		if version == mqttclient.MQTT5 {
			p.ReasonCodes = []byte{pk.ReasonCode}
		}
	}
	return p
}

func decodeBody(pk *packets.Packet, body []byte) error {
	switch pk.FixedHeader.Type {
	case Connect:
		return pk.ConnectDecode(body)
	case Connack:
		return pk.ConnackDecode(body)
	case Publish:
		return pk.PublishDecode(body)
	case Puback, Pubrec, Pubrel, Pubcomp:
		if pk.ProtocolVersion < mqttclient.MQTT5 && len(body) != 2 {
			return fmt.Errorf("remaining length %d", len(body))
		}
		return pk.PubackDecode(body)
	case Subscribe, Unsubscribe:
		return decodeFilters(pk, body)
	case Suback:
		return pk.SubackDecode(body)
	case Unsuback:
		return pk.UnsubackDecode(body)
	case Pingreq, Pingresp:
		if len(body) != 0 {
			return fmt.Errorf("remaining length %d", len(body))
		}
		return nil
	case Disconnect:
		// MQTT 5 §3.14.2.1: the properties may be left out.
		if pk.ProtocolVersion == mqttclient.MQTT5 && len(body) == 1 {
			pk.ReasonCode = body[0]
			return nil
		}
		return pk.DisconnectDecode(body)
	case Auth:
		return pk.AuthDecode(body)
	}
	return errors.New("reserved packet type")
}

// decodeFilters decodes SUBSCRIBE and UNSUBSCRIBE here rather than with
// comqtt, whose SUBSCRIBE decoder reads past the end of a truncated MQTT 5
// packet.
func decodeFilters(pk *packets.Packet, b []byte) error {
	if len(b) < 2 {
		return errors.New("no packet identifier")
	}
	pk.PacketID = binary.BigEndian.Uint16(b)
	b = b[2:]
	if pk.ProtocolVersion == mqttclient.MQTT5 {
		n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(b))
		if err != nil {
			return fmt.Errorf("properties: %v", err)
		}
		b = b[n:]
	}
	for len(b) > 0 {
		if len(b) < 2 || len(b) < 2+int(binary.BigEndian.Uint16(b)) {
			return errors.New("truncated topic filter")
		}
		n := int(binary.BigEndian.Uint16(b))
		f := packets.Subscription{Filter: string(b[2 : 2+n])}
		b = b[2+n:]
		if pk.FixedHeader.Type == Subscribe {
			if len(b) == 0 {
				return errors.New("topic filter without options")
			}
			opts := b[0]
			b = b[1:]
			f.Qos = opts & 0x03
			if f.Qos > 2 {
				return errors.New("QoS 3 requested")
			}
			if pk.ProtocolVersion == mqttclient.MQTT5 {
				f.NoLocal = opts&0x04 != 0
				f.RetainAsPublished = opts&0x08 != 0
				f.RetainHandling = (opts >> 4) & 0x03
			}
		}
		pk.Filters = append(pk.Filters, f)
	}
	return nil
}

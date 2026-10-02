// This file adapts the comqtt packets codec, which was written for a broker,
// to client use. Framing follows comqtt's mqtt/clients.go
// (ReadFixedHeader/ReadPacket); the client-side checks follow libmosquitto's
// handle_*.c.

package mqttclient

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

var (
	protocolNameMQTT   = []byte("MQTT")
	protocolNameMQIsdp = []byte("MQIsdp")
)

// readPacket reads one control packet from r. Network errors are returned
// unwrapped; protocol violations wrap ErrMalformedPacket, ErrProtocol or
// ErrOversizePacket.
//
// For an MQTT 5 connection a CONNACK with remaining length 2 is decoded as
// MQTT 3.x, and the returned packet has ProtocolVersion 4. A 3.x broker
// rejecting a v5 CONNECT answers this way.
func readPacket(r *bufio.Reader, version byte, maxSize uint32) (packets.Packet, error) {
	var pk packets.Packet
	pk.ProtocolVersion = version

	hb, err := r.ReadByte()
	if err != nil {
		return pk, err
	}
	if err := pk.FixedHeader.Decode(hb); err != nil {
		return pk, fmt.Errorf("%w: fixed header: %v", ErrMalformedPacket, err)
	}
	n, lenBytes, err := packets.DecodeLength(r)
	if err != nil {
		if errors.Is(err, packets.ErrMalformedVariableByteInteger) {
			return pk, fmt.Errorf("%w: remaining length", ErrMalformedPacket)
		}
		return pk, err
	}
	if maxSize > 0 && uint64(1+lenBytes+n) > uint64(maxSize) {
		return pk, fmt.Errorf("%w: %s of %d bytes", ErrOversizePacket, packets.PacketNames[pk.FixedHeader.Type], 1+lenBytes+n)
	}
	pk.FixedHeader.Remaining = n

	// A fresh buffer per packet: comqtt decoders return slices of it
	// (Payload, ReasonCodes).
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return pk, err
	}
	return pk, decodeBody(&pk, buf)
}

func decodeBody(pk *packets.Packet, buf []byte) error {
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connack:
		// MQTT 3.1.1/5: only bit 0 (session present) may be set.
		if len(buf) > 0 && buf[0]&0xFE != 0 && pk.ProtocolVersion >= MQTT311 {
			return fmt.Errorf("%w: CONNACK with invalid flags 0x%02x", ErrProtocol, buf[0])
		}
		if pk.ProtocolVersion < MQTT5 && len(buf) != 2 {
			return fmt.Errorf("%w: CONNACK remaining length %d", ErrMalformedPacket, len(buf))
		}
		if pk.ProtocolVersion == MQTT5 && len(buf) == 2 {
			pk.ProtocolVersion = MQTT311
		}
		err = pk.ConnackDecode(buf)
	case packets.Disconnect:
		// MQTT 5 §3.14.2.1: the properties may be omitted. Older comqtt
		// commits skip the reason code in that case, so read it here.
		if pk.ProtocolVersion == MQTT5 && len(buf) == 1 {
			pk.ReasonCode = buf[0]
			return nil
		}
		err = pk.DisconnectDecode(buf)
	case packets.Pingreq, packets.Pingresp:
		if len(buf) != 0 {
			return fmt.Errorf("%w: %s remaining length %d", ErrMalformedPacket, packets.PacketNames[pk.FixedHeader.Type], len(buf))
		}
	case packets.Publish:
		err = pk.PublishDecode(buf)
	case packets.Puback, packets.Pubrec, packets.Pubrel, packets.Pubcomp:
		// The four acknowledgements share one layout; MQTT 3.x has only
		// the packet identifier.
		if pk.ProtocolVersion < MQTT5 && len(buf) != 2 {
			return fmt.Errorf("%w: %s remaining length %d", ErrMalformedPacket, packets.PacketNames[pk.FixedHeader.Type], len(buf))
		}
		err = pk.PubackDecode(buf)
	case packets.Suback:
		err = pk.SubackDecode(buf)
	case packets.Unsuback:
		err = pk.UnsubackDecode(buf)
	case packets.Auth:
		err = pk.AuthDecode(buf)
	default:
		return fmt.Errorf("%w: unexpected %s packet from server", ErrProtocol, packetName(pk.FixedHeader.Type))
	}
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrMalformedPacket, packets.PacketNames[pk.FixedHeader.Type], err)
	}
	return nil
}

// encodePacket encodes pk, which must have FixedHeader.Type and
// ProtocolVersion set.
func encodePacket(pk *packets.Packet) ([]byte, error) {
	// Without this the comqtt encoder drops Response Topic and Correlation
	// Data, which it treats as a server-side negotiation.
	pk.Mods.AllowResponseInfo = true

	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Publish:
		err = pk.PublishEncode(&buf)
	case packets.Puback:
		err = pk.PubackEncode(&buf)
	case packets.Pubrec:
		err = pk.PubrecEncode(&buf)
	case packets.Pubrel:
		err = pk.PubrelEncode(&buf)
	case packets.Pubcomp:
		err = pk.PubcompEncode(&buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(&buf)
	case packets.Unsubscribe:
		err = pk.UnsubscribeEncode(&buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(&buf)
	case packets.Pingresp:
		err = pk.PingrespEncode(&buf)
	case packets.Disconnect:
		err = encodeDisconnect(pk, &buf)
	case packets.Auth:
		err = pk.AuthEncode(&buf)
	default:
		return nil, fmt.Errorf("%w: cannot encode %s", ErrInvalid, packetName(pk.FixedHeader.Type))
	}
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeDisconnect writes DISCONNECT the way libmosquitto's send__disconnect
// does: MQTT 5 sends the reason code only when it is non-zero or there are
// properties, and omits an empty property block.
func encodeDisconnect(pk *packets.Packet, buf *bytes.Buffer) error {
	if pk.ProtocolVersion == MQTT5 && pk.ReasonCode == reasonNormalDisconnection && !hasProperties(pk.FixedHeader.Type, &pk.Properties) {
		buf.Write([]byte{packets.Disconnect << 4, 0})
		return nil
	}
	if pk.ProtocolVersion == MQTT5 && !hasProperties(pk.FixedHeader.Type, &pk.Properties) {
		buf.Write([]byte{packets.Disconnect << 4, 1, pk.ReasonCode})
		return nil
	}
	return pk.DisconnectEncode(buf)
}

// hasProperties reports whether p encodes to a non-empty property block for
// packet type pkt.
func hasProperties(pkt byte, p *Properties) bool {
	var b bytes.Buffer
	p.Encode(pkt, packets.Mods{AllowResponseInfo: true}, &b, 0)
	return b.Len() > 1
}

func packetName(t byte) string {
	if s, ok := packets.PacketNames[t]; ok {
		return strings.ToUpper(s)
	}
	return fmt.Sprintf("type %d", t)
}

// connectParams is what goes into one CONNECT packet.
type connectParams struct {
	version         byte
	clientID        string
	cleanStart      bool
	keepAlive       uint16
	username        string
	password        []byte
	will            *Message
	retainAvailable bool
	properties      *Properties
	receiveMaximum  uint16
}

func newConnect(p connectParams) packets.Packet {
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: p.version,
	}
	pk.Connect.ProtocolName = protocolNameMQTT
	if p.version == MQTT31 {
		pk.Connect.ProtocolName = protocolNameMQIsdp
	}
	pk.Connect.ClientIdentifier = p.clientID
	pk.Connect.Clean = p.cleanStart
	pk.Connect.Keepalive = p.keepAlive
	if p.username != "" {
		pk.Connect.UsernameFlag = true
		pk.Connect.Username = []byte(p.username)
	}
	if len(p.password) > 0 {
		pk.Connect.PasswordFlag = true
		pk.Connect.Password = p.password
	}
	if w := p.will; w != nil {
		pk.Connect.WillFlag = true
		pk.Connect.WillTopic = w.Topic
		pk.Connect.WillPayload = w.Payload
		pk.Connect.WillQos = w.QoS
		// send__connect only sets will retain while the server allows retain.
		pk.Connect.WillRetain = w.Retain && p.retainAvailable
		if w.Properties != nil && p.version == MQTT5 {
			pk.Connect.WillProperties = *w.Properties
		}
	}
	if pk.Connect.WillFlag && p.version == MQTT5 {
		setPropertyFlags(&pk.Connect.WillProperties)
	}
	if p.version == MQTT5 {
		if p.properties != nil {
			pk.Properties = *p.properties
		}
		// send__connect always sends Receive Maximum.
		if pk.Properties.ReceiveMaximum == 0 {
			pk.Properties.ReceiveMaximum = p.receiveMaximum
		}
	}
	return pk
}

func newDisconnect(version, reason byte, props *Properties) packets.Packet {
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Disconnect},
		ProtocolVersion: version,
		ReasonCode:      reason,
	}
	if props != nil {
		pk.Properties = *props
	}
	return pk
}

// newPublish builds a PUBLISH for m; m.Mid is the packet identifier when
// m.QoS > 0. dup marks a resend.
func newPublish(version byte, m *Message, dup bool) packets.Packet {
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: m.QoS, Retain: m.Retain, Dup: dup && m.QoS > 0},
		ProtocolVersion: version,
		TopicName:       m.Topic,
		Payload:         m.Payload,
		PacketID:        m.Mid,
	}
	if version == MQTT5 && m.Properties != nil {
		pk.Properties = *m.Properties
		setPropertyFlags(&pk.Properties)
	}
	return pk
}

// newAck builds a PUBACK, PUBREC, PUBREL or PUBCOMP. The codec writes the
// short form (packet identifier only) unless the reason code is a failure,
// as libmosquitto's send__command_with_mid does for reason code 0.
func newAck(version, typ byte, mid uint16, reason byte) packets.Packet {
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: typ},
		ProtocolVersion: version,
		PacketID:        mid,
		ReasonCode:      reason,
	}
	if typ == packets.Pubrel {
		pk.FixedHeader.Qos = 1 // fixed header flags 0010 [MQTT-3.6.1-1]
	}
	return pk
}

// setPropertyFlags sets the codec's presence flags for values the caller
// set without them (codec gap 6). Payload Format Indicator 0 is the default,
// so leaving it out does not change its meaning.
func setPropertyFlags(p *Properties) {
	if p.PayloadFormat != 0 {
		p.PayloadFormatFlag = true
	}
}

func newPingreq(version byte) packets.Packet {
	return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingreq}, ProtocolVersion: version}
}

func newPingresp(version byte) packets.Packet {
	return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingresp}, ProtocolVersion: version}
}

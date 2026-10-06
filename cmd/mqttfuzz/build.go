package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// packetSteps are the step types that encode to an MQTT control packet.
var packetSteps = map[string]byte{
	"connect":     packets.Connect,
	"connack":     packets.Connack,
	"publish":     packets.Publish,
	"puback":      packets.Puback,
	"pubrec":      packets.Pubrec,
	"pubrel":      packets.Pubrel,
	"pubcomp":     packets.Pubcomp,
	"subscribe":   packets.Subscribe,
	"suback":      packets.Suback,
	"unsubscribe": packets.Unsubscribe,
	"unsuback":    packets.Unsuback,
	"pingreq":     packets.Pingreq,
	"pingresp":    packets.Pingresp,
	"disconnect":  packets.Disconnect,
	"auth":        packets.Auth,
}

// encodeStep returns the raw bytes a step sends. version is the run's
// protocol version. It handles both "raw" (verbatim hex) and the control
// packets.
func encodeStep(s *Step, version byte) ([]byte, error) {
	if s.Type == "raw" {
		clean := strings.Map(func(r rune) rune {
			if r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == ':' {
				return -1
			}
			return r
		}, s.Hex)
		b, err := hex.DecodeString(clean)
		if err != nil {
			return nil, fmt.Errorf("raw hex: %w", err)
		}
		return b, nil
	}

	typ, ok := packetSteps[s.Type]
	if !ok {
		return nil, fmt.Errorf("unknown step type %q", s.Type)
	}

	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: typ},
		ProtocolVersion: version,
	}
	// Reserved fixed-header bits default to what the spec requires. A step's
	// `flags` value replaces the whole low nibble after encoding (see the
	// stamp below), so the body is always built from a well-formed header
	// here and `flags` can carry bit patterns the encoders would reject
	// (QoS 3, set reserved bits, and so on).
	if typ == packets.Subscribe || typ == packets.Unsubscribe || typ == packets.Pubrel {
		pk.FixedHeader.Qos = 1 // flags 0010
	}

	if s.Properties != nil {
		pk.Properties = s.Properties.toPacket(typ)
	}
	pk.Mods.AllowResponseInfo = true

	var buf bytes.Buffer
	var err error
	switch typ {
	case packets.Connect:
		applyConnect(&pk, s, version)
		err = pk.ConnectEncode(&buf)
	case packets.Connack:
		pk.ReasonCode = s.Reason
		err = pk.ConnackEncode(&buf)
	case packets.Publish:
		pk.TopicName = s.Topic
		pk.Payload, err = publishPayload(s)
		if err != nil {
			return nil, err
		}
		pk.PacketID = s.PacketID
		if s.Flags == nil {
			pk.FixedHeader.Qos = s.QoS
			pk.FixedHeader.Retain = s.Retain
			pk.FixedHeader.Dup = s.Dup
		}
		err = pk.PublishEncode(&buf)
	case packets.Puback:
		pk.PacketID, pk.ReasonCode = s.PacketID, s.Reason
		err = pk.PubackEncode(&buf)
	case packets.Pubrec:
		pk.PacketID, pk.ReasonCode = s.PacketID, s.Reason
		err = pk.PubrecEncode(&buf)
	case packets.Pubrel:
		pk.PacketID, pk.ReasonCode = s.PacketID, s.Reason
		err = pk.PubrelEncode(&buf)
	case packets.Pubcomp:
		pk.PacketID, pk.ReasonCode = s.PacketID, s.Reason
		err = pk.PubcompEncode(&buf)
	case packets.Subscribe:
		pk.PacketID = s.PacketID
		pk.Filters = toSubscriptions(s.Filters)
		err = pk.SubscribeEncode(&buf)
	case packets.Suback:
		pk.PacketID = s.PacketID
		pk.ReasonCodes = reasonCodes(s)
		err = pk.SubackEncode(&buf)
	case packets.Unsubscribe:
		pk.PacketID = s.PacketID
		pk.Filters = toSubscriptions(s.Filters)
		err = pk.UnsubscribeEncode(&buf)
	case packets.Unsuback:
		pk.PacketID = s.PacketID
		pk.ReasonCodes = reasonCodes(s)
		err = pk.UnsubackEncode(&buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(&buf)
	case packets.Pingresp:
		err = pk.PingrespEncode(&buf)
	case packets.Disconnect:
		pk.ReasonCode = s.Reason
		err = pk.DisconnectEncode(&buf)
	case packets.Auth:
		pk.ReasonCode = s.Reason
		err = pk.AuthEncode(&buf)
	}
	if err != nil {
		return nil, fmt.Errorf("encoding %s: %w", strings.ToUpper(s.Type), err)
	}

	b := buf.Bytes()
	// -flags overrides the whole low nibble, including bits the typed
	// encoders do not expose.
	if s.Flags != nil && len(b) > 0 {
		b[0] = (b[0] & 0xF0) | (*s.Flags & 0x0F)
	}
	return b, nil
}

// publishPayload is the step's payload, or payload-size bytes of
// payload-pattern (default ascii) when either is set.
func publishPayload(s *Step) ([]byte, error) {
	if s.PayloadPattern == "" && s.PayloadSize == 0 {
		return []byte(s.Payload), nil
	}
	if s.Payload != "" {
		return nil, fmt.Errorf("give payload or payload-size/payload-pattern, not both")
	}
	pattern := s.PayloadPattern
	if pattern == "" {
		pattern = "ascii"
	}
	return cli.MakePattern(pattern, s.PayloadSize)
}

func applyConnect(pk *packets.Packet, s *Step, version byte) {
	name := s.ProtocolName
	if name == "" {
		name = "MQTT"
		if version == 3 {
			name = "MQIsdp"
		}
	}
	pk.Connect.ProtocolName = []byte(name)
	pk.Connect.ClientIdentifier = s.ClientID
	pk.Connect.Clean = s.Clean == nil || *s.Clean
	pk.Connect.Keepalive = s.KeepAlive
	if s.Username != "" {
		pk.Connect.UsernameFlag = true
		pk.Connect.Username = []byte(s.Username)
	}
	if s.Password != "" {
		pk.Connect.PasswordFlag = true
		pk.Connect.Password = []byte(s.Password)
	}
	if s.WillTopic != "" {
		pk.Connect.WillFlag = true
		pk.Connect.WillTopic = s.WillTopic
		pk.Connect.WillPayload = []byte(s.WillPayload)
		pk.Connect.WillQos = s.WillQoS
		pk.Connect.WillRetain = s.WillRetain
	}
}

func toSubscriptions(fs []Filter) packets.Subscriptions {
	subs := make(packets.Subscriptions, len(fs))
	for i, f := range fs {
		subs[i] = packets.Subscription{
			Filter:            f.Filter,
			Qos:               f.QoS,
			NoLocal:           f.NoLocal,
			RetainAsPublished: f.RetainAsPub,
			RetainHandling:    f.RetainHandling,
		}
	}
	return subs
}

// reasonCodes builds the reason-code payload for a SUBACK/UNSUBACK. One code
// per filter when filters are given, otherwise the single Reason field.
func reasonCodes(s *Step) []byte {
	if len(s.Filters) == 0 {
		return []byte{s.Reason}
	}
	codes := make([]byte, len(s.Filters))
	for i, f := range s.Filters {
		codes[i] = f.QoS // reuse qos as the granted/returned code
	}
	return codes
}

func (p *Props) toPacket(pkt byte) packets.Properties {
	var pr packets.Properties
	if p.SessionExpiry != nil {
		pr.SessionExpiryInterval = *p.SessionExpiry
		pr.SessionExpiryIntervalFlag = true
	}
	pr.ReceiveMaximum = p.ReceiveMaximum
	pr.MaximumPacketSize = p.MaximumPacketSize
	pr.TopicAliasMaximum = p.TopicAliasMaximum
	if p.TopicAlias != 0 {
		pr.TopicAlias = p.TopicAlias
		pr.TopicAliasFlag = true
	}
	if p.PayloadFormat != nil {
		pr.PayloadFormat = *p.PayloadFormat
		pr.PayloadFormatFlag = true
	}
	pr.MessageExpiryInterval = p.MessageExpiry
	pr.ContentType = p.ContentType
	pr.ResponseTopic = p.ResponseTopic
	if p.SubscriptionID != 0 {
		pr.SubscriptionIdentifier = []int{p.SubscriptionID}
	}
	pr.ReasonString = p.ReasonString
	pr.AuthenticationMethod = p.AuthMethod
	if p.AuthData != "" {
		pr.AuthenticationData = []byte(p.AuthData)
	}
	for _, u := range p.User {
		pr.User = append(pr.User, packets.UserProperty{Key: u.Key, Val: u.Val})
	}
	return pr
}

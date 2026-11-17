package replay

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/capture"
)

// reasonDisconnectWithWill is the MQTT 5 DISCONNECT reason code that asks
// the broker to publish the Will anyway.
const reasonDisconnectWithWill = 0x04

// Inbound turns timelines captured from publishing clients (edge nodes)
// into one timeline that delivers their messages to a subscriber, as a
// broker would:
//
//   - It starts with an Auto CONNECT, CONNACK, SUBSCRIBE and SUBACK, so the
//     subscriber may use any client ID and topic filters.
//   - Every client PUBLISH becomes a broker PUBLISH. Timelines are merged
//     by capture time; one client's messages keep their order. The payload
//     is kept byte for byte; QoS is kept; RETAIN and DUP are cleared, as
//     for a message forwarded to an existing subscription; MQTT 5 topic
//     aliases are resolved and the Topic Alias property removed; packet
//     identifiers are renumbered from 1, since clients may have used the
//     same ones.
//   - A QoS 2 PUBLISH is followed by a broker PUBREL.
//   - A client whose connection ends without a normal DISCONNECT (no
//     DISCONNECT, or MQTT 5 reason 0x04) has its Will delivered at the end
//     of its timeline. The MQTT 5 Will Delay Interval is ignored.
//
// All timelines must have the same protocol version and start with a
// CONNECT. Inbound returns an error, naming the timeline and packet, for a
// malformed PUBLISH or a topic alias with no topic before it; drop those
// packets first to go on without them.
func Inbound(clients []Timeline) (Timeline, error) {
	if len(clients) == 0 {
		return Timeline{}, errors.New("replay: Inbound needs at least one timeline")
	}
	version := clients[0].Version
	queues := make([][]Packet, len(clients)) // broker PUBLISHes to deliver, per client, without identifiers yet
	for ci, tl := range clients {
		if tl.Version != version {
			return Timeline{}, fmt.Errorf("replay: Inbound: timeline %d has protocol version %d, timeline 0 has %d", ci, tl.Version, version)
		}
		q, err := inboundMessages(tl)
		if err != nil {
			return Timeline{}, fmt.Errorf("replay: Inbound: timeline %d: %w", ci, err)
		}
		queues[ci] = q
	}

	out := Timeline{Server: clients[0].Server, Version: version}
	start := earliest(queues)
	out.Packets = []Packet{
		{Dir: capture.FromClient, Time: start, Auto: true, Type: Connect},
		{Dir: capture.FromServer, Time: start, Auto: true, Type: Connack},
		{Dir: capture.FromClient, Time: start, Auto: true, Type: Subscribe},
		{Dir: capture.FromServer, Time: start, Auto: true, Type: Suback},
	}
	var id uint16
	next := make([]int, len(queues))
	for {
		best := -1 // the client whose next message was captured first
		for ci, q := range queues {
			if next[ci] < len(q) && (best < 0 || q[next[ci]].Time.Before(queues[best][next[best]].Time)) {
				best = ci
			}
		}
		if best < 0 {
			break
		}
		m := queues[best][next[best]]
		next[best]++
		if m.QoS > 0 {
			id++
			if id == 0 {
				id = 1
			}
		}
		p, err := encodePublish(version, m.Topic, m.QoS, id, m.Properties, m.Payload)
		if err != nil {
			return Timeline{}, fmt.Errorf("replay: Inbound: %w", err)
		}
		p.Dir, p.Time = capture.FromServer, m.Time
		out.Packets = append(out.Packets, p)
		if m.QoS == 2 {
			rel := []byte{Pubrel<<4 | 0x02, 2, byte(id >> 8), byte(id)}
			out.Packets = append(out.Packets, Packet{Dir: capture.FromServer, Time: m.Time, Type: Pubrel, PacketID: id, Raw: rel})
		}
	}
	return out, nil
}

// inboundMessages returns what a broker would deliver from one client's
// timeline: its PUBLISHes, then its Will if the connection ended
// abnormally. Only Topic, QoS, Properties, Payload and Time are set.
func inboundMessages(tl Timeline) ([]Packet, error) {
	var connect *ConnectInfo
	var msgs []Packet
	aliases := make(map[uint16]string)
	normalEnd := false
	var end time.Time
	for i, p := range tl.Packets {
		if p.Time.After(end) {
			end = p.Time
		}
		if p.Dir != capture.FromClient || p.Close {
			continue
		}
		if connect == nil {
			if p.Type != Connect || p.Connect == nil {
				return nil, fmt.Errorf("packet %d: the timeline doesn't start with a CONNECT", i)
			}
			connect = p.Connect
			continue
		}
		switch p.Type {
		case Publish:
			if p.Err != nil {
				return nil, fmt.Errorf("packet %d: %w", i, p.Err)
			}
			m := Packet{Topic: p.Topic, QoS: p.QoS, Payload: p.Payload, Time: p.Time}
			if p.Properties != nil {
				props := *p.Properties
				if props.TopicAliasFlag && props.TopicAlias > 0 {
					switch t, ok := aliases[props.TopicAlias]; {
					case m.Topic != "":
						aliases[props.TopicAlias] = m.Topic
					case ok:
						m.Topic = t
					default:
						return nil, fmt.Errorf("packet %d: topic alias %d with no topic before it", i, props.TopicAlias)
					}
					props.TopicAlias, props.TopicAliasFlag = 0, false
				}
				m.Properties = &props
			}
			msgs = append(msgs, m)
		case Disconnect:
			normalEnd = tl.Version != mqttclient.MQTT5 || len(p.ReasonCodes) == 0 || p.ReasonCodes[0] != reasonDisconnectWithWill
		}
	}
	if connect == nil {
		return nil, errors.New("the timeline has no CONNECT")
	}
	if w := connect.Will; w != nil && !normalEnd {
		msgs = append(msgs, Packet{Topic: w.Topic, QoS: w.QoS, Properties: w.Properties, Payload: w.Payload, Time: end})
	}
	return msgs, nil
}

// earliest is the capture time of the first message in queues.
func earliest(queues [][]Packet) time.Time {
	var t time.Time
	for _, q := range queues {
		if len(q) > 0 && (t.IsZero() || q[0].Time.Before(t)) {
			t = q[0].Time
		}
	}
	return t
}

// encodePublish builds a broker PUBLISH, RETAIN and DUP clear, and decodes
// it back so that its fields match its Raw bytes.
func encodePublish(version byte, topic string, qos byte, id uint16, props *mqttclient.Properties, payload []byte) (Packet, error) {
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: qos},
		ProtocolVersion: version,
		TopicName:       topic,
		PacketID:        id,
		Payload:         payload,
	}
	if props != nil && version == mqttclient.MQTT5 {
		pk.Properties = *props
	}
	// Without this the comqtt encoder drops Response Topic and Correlation
	// Data.
	pk.Mods.AllowResponseInfo = true
	var buf bytes.Buffer
	if err := pk.PublishEncode(&buf); err != nil {
		return Packet{}, fmt.Errorf("encode PUBLISH to %q: %v", topic, err)
	}
	raw := buf.Bytes()
	_, hdr, err := frame(raw)
	if err != nil {
		return Packet{}, err
	}
	p := decodePacket(raw, hdr, version)
	if p.Err != nil {
		return Packet{}, fmt.Errorf("encode PUBLISH to %q: %w", topic, p.Err)
	}
	return p, nil
}

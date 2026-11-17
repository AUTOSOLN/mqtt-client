package replay_test

import (
	"errors"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/replay"
)

// publisher builds a publishing client's timeline: CONNECT (with a Will if
// will is set), then one QoS 1 PUBLISH per topic at the given millisecond
// offsets, retained and with DUP set so the test can see them cleared.
func publisher(t *testing.T, addr netip.AddrPort, will string, topics []string, ms []int) replay.Timeline {
	t.Helper()
	cp := packets.ConnectParams{ClientIdentifier: addr.String(), Clean: true}
	if will != "" {
		cp.WillFlag, cp.WillTopic, cp.WillPayload = true, will, []byte("gone")
	}
	s := newSession(t, addr, brokerAddr)
	s.client(packets.Packet{FixedHeader: fh(packets.Connect), Connect: cp})
	s.server(packets.Packet{FixedHeader: fh(packets.Connack)})
	for i, topic := range topics {
		pk := publishPk(topic, 1, 1, []byte(topic)) // every client reuses identifier 1
		pk.FixedHeader.Retain, pk.FixedHeader.Dup = true, true
		s.ms = ms[i]
		s.client(pk)
	}
	return decode(t, s.conn)
}

type delivered struct {
	topic string
	id    uint16
}

// publishes returns the broker PUBLISHes of an Inbound timeline, checking
// the flags every one of them must have.
func publishes(t *testing.T, tl replay.Timeline) []delivered {
	t.Helper()
	var got []delivered
	for _, p := range tl.Packets {
		if p.Type != replay.Publish {
			continue
		}
		if p.Dir != capture.FromServer || p.Retain || p.Dup || p.Err != nil {
			t.Errorf("PUBLISH %q: got dir %v retain %v dup %v err %v, want from the server, retain and dup clear",
				p.Topic, p.Dir, p.Retain, p.Dup, p.Err)
		}
		got = append(got, delivered{p.Topic, p.PacketID})
	}
	return got
}

func TestInbound_TwoClients_MergedByTimeWithFreshIdentifiers(t *testing.T) {
	a := publisher(t, netip.MustParseAddrPort("10.0.0.3:1000"), "", []string{"a/1", "a/2"}, []int{10, 30})
	b := publisher(t, netip.MustParseAddrPort("10.0.0.4:1000"), "", []string{"b/1", "b/2"}, []int{20, 40})

	in, err := replay.Inbound([]replay.Timeline{a, b})
	if err != nil {
		t.Fatal(err)
	}
	want := []delivered{{"a/1", 1}, {"b/1", 2}, {"a/2", 3}, {"b/2", 4}}
	if got := publishes(t, in); !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	var head []string
	for _, p := range in.Packets[:4] {
		if !p.Auto {
			t.Errorf("handshake packet %s: got Auto false, want true", p.TypeName())
		}
		head = append(head, p.TypeName())
	}
	if want := []string{"CONNECT", "CONNACK", "SUBSCRIBE", "SUBACK"}; !slices.Equal(head, want) {
		t.Errorf("handshake: got %v, want %v", head, want)
	}
}

func TestInbound_ClockStepsBack_KeepsEachClientsOrder(t *testing.T) {
	// a's second message has an earlier timestamp than its first.
	a := publisher(t, netip.MustParseAddrPort("10.0.0.3:1000"), "", []string{"a/1", "a/2"}, []int{30, 10})
	b := publisher(t, netip.MustParseAddrPort("10.0.0.4:1000"), "", []string{"b/1"}, []int{20})

	in, err := replay.Inbound([]replay.Timeline{a, b})
	if err != nil {
		t.Fatal(err)
	}
	var topics []string
	for _, d := range publishes(t, in) {
		topics = append(topics, d.topic)
	}
	if slices.Index(topics, "a/1") > slices.Index(topics, "a/2") {
		t.Errorf("got order %v, want a/1 before a/2", topics)
	}
}

func TestInbound_Will_DeliveredOnlyAfterAbnormalEnd(t *testing.T) {
	addr := netip.MustParseAddrPort("10.0.0.3:1000")
	normal := publisher(t, addr, "will/t", []string{"a/1"}, []int{10})
	normal.Packets = append(normal.Packets, replay.Packet{Dir: capture.FromClient, Time: t0.Add(time.Second),
		Type: replay.Disconnect, Raw: []byte{packets.Disconnect << 4, 0}})
	dropped := publisher(t, addr, "will/t", []string{"a/1"}, []int{10})

	tests := []struct {
		name string
		tl   replay.Timeline
		want []delivered
	}{
		{name: "DISCONNECT", tl: normal, want: []delivered{{"a/1", 1}}},
		{name: "no DISCONNECT", tl: dropped, want: []delivered{{"a/1", 1}, {"will/t", 0}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in, err := replay.Inbound([]replay.Timeline{tt.tl})
			if err != nil {
				t.Fatal(err)
			}
			if got := publishes(t, in); !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInbound_MQTT5TopicAlias_ResolvedAndRemoved(t *testing.T) {
	s := newSession(t, edgeAddr, brokerAddr)
	s.client(packets.Packet{FixedHeader: fh(packets.Connect), ProtocolVersion: 5,
		Connect: packets.ConnectParams{ClientIdentifier: "e", Clean: true}})
	first := publishPk("long/topic/name", 0, 0, []byte("1"))
	first.ProtocolVersion = 5
	first.Properties = packets.Properties{TopicAlias: 3, TopicAliasFlag: true}
	second := publishPk("", 0, 0, []byte("2"))
	second.ProtocolVersion = 5
	second.Properties = packets.Properties{TopicAlias: 3, TopicAliasFlag: true}
	s.client(first).client(second)

	in, err := replay.Inbound([]replay.Timeline{decode(t, s.conn)})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for _, p := range in.Packets {
		if p.Type != replay.Publish {
			continue
		}
		n++
		if p.Topic != "long/topic/name" || p.Properties == nil || p.Properties.TopicAliasFlag {
			t.Errorf("PUBLISH %q: got topic %q, alias flag %v; want the full topic and no alias",
				p.Payload, p.Topic, p.Properties != nil && p.Properties.TopicAliasFlag)
		}
	}
	if n != 2 {
		t.Errorf("got %d PUBLISHes, want 2", n)
	}
}

func TestInbound_UnusableTimelines_Error(t *testing.T) {
	good := publisher(t, edgeAddr, "", []string{"a/1"}, []int{10})
	malformed := publisher(t, edgeAddr, "", []string{"a/1"}, []int{10})
	malformed.Packets[2].Err = mqttclient.ErrMalformedPacket
	v5 := good
	v5.Version = mqttclient.MQTT5
	noConnect := good
	noConnect.Packets = good.Packets[1:]

	tests := []struct {
		name string
		tls  []replay.Timeline
		want error
	}{
		{name: "none", tls: nil},
		{name: "malformed PUBLISH", tls: []replay.Timeline{malformed}, want: mqttclient.ErrMalformedPacket},
		{name: "mixed versions", tls: []replay.Timeline{good, v5}},
		{name: "no CONNECT", tls: []replay.Timeline{noConnect}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := replay.Inbound(tt.tls)
			if err == nil || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Errorf("got %v, want an error (wrapping %v)", err, tt.want)
			}
		})
	}
}

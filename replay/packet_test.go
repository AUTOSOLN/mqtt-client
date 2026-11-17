package replay_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/wind-c/comqtt/v2/mqtt/packets"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/replay"
)

func TestDecode_HostSession_DecodesHeadersAndPayloads(t *testing.T) {
	tl := decode(t, hostSession(t))
	if tl.Version != mqttclient.MQTT311 || tl.Client != hostAddr || tl.Server != brokerAddr {
		t.Errorf("got version %d client %v server %v, want 4 %v %v", tl.Version, tl.Client, tl.Server, hostAddr, brokerAddr)
	}
	want := []struct {
		dir  capture.Dir
		name string
	}{
		{capture.FromClient, "CONNECT"}, {capture.FromServer, "CONNACK"},
		{capture.FromClient, "SUBSCRIBE"}, {capture.FromServer, "SUBACK"},
		{capture.FromServer, "PUBLISH"}, {capture.FromClient, "PUBACK"},
		{capture.FromServer, "PUBLISH"}, {capture.FromClient, "PUBACK"},
		{capture.FromServer, "PUBLISH"}, {capture.FromClient, "PUBACK"},
		{capture.FromServer, "PUBLISH"}, {capture.FromClient, "PUBACK"},
		{capture.FromClient, "DISCONNECT"}, {capture.FromClient, "close"},
	}
	if len(tl.Packets) != len(want) {
		t.Fatalf("got %d packets, want %d", len(tl.Packets), len(want))
	}
	for i, w := range want {
		p := tl.Packets[i]
		if p.Dir != w.dir || p.TypeName() != w.name || p.Err != nil {
			t.Errorf("packet %d: got %v %s (err %v), want %v %s", i, p.Dir, p.TypeName(), p.Err, w.dir, w.name)
		}
	}
	if c := tl.Packets[0].Connect; c == nil || c.ClientID != "host1" || !c.Clean || c.KeepAlive != 60 || c.Will != nil {
		t.Errorf("CONNECT: got %+v, want client ID host1, clean, keepalive 60, no Will", c)
	}
	if f := tl.Packets[2].Filters; len(f) != 1 || f[0].Topic != "spBv1.0/#" || f[0].QoS != 1 || tl.Packets[2].PacketID != 1 {
		t.Errorf("SUBSCRIBE: got id %d filters %+v, want id 1, spBv1.0/# at QoS 1", tl.Packets[2].PacketID, f)
	}
	if p := tl.Packets[4]; p.QoS != 1 || p.PacketID != 1 || p.Retain || p.Dup || p.Topic != "spBv1.0/plant1/NBIRTH/press7" || len(p.Payload) == 0 {
		t.Errorf("first PUBLISH: got %q QoS %d id %d retain %v dup %v with %d bytes, want the NBIRTH at QoS 1, id 1",
			p.Topic, p.QoS, p.PacketID, p.Retain, p.Dup, len(p.Payload))
	}
}

func TestDecode_SegmentsSplitAtEveryOffset_SamePackets(t *testing.T) {
	conn := hostSession(t)
	base := decode(t, conn)
	for si, s := range conn.Segments {
		for k := 1; k < len(s.Data); k++ {
			split := conn
			split.Segments = append([]capture.Segment(nil), conn.Segments[:si]...)
			split.Segments = append(split.Segments,
				capture.Segment{Dir: s.Dir, Time: s.Time, Data: s.Data[:k]},
				capture.Segment{Dir: s.Dir, Time: s.Time, Data: s.Data[k:]})
			split.Segments = append(split.Segments, conn.Segments[si+1:]...)
			if got := decode(t, split); !reflect.DeepEqual(got, base) {
				t.Fatalf("segment %d split at byte %d: got a different timeline", si, k)
			}
		}
	}
}

func TestDecode_EditingOnePacket_LeavesOthersUnchanged(t *testing.T) {
	conn := hostSession(t)
	tl := decode(t, conn)
	pub := tl.Packets[4]
	raw := bytes.Clone(pub.Raw)

	pub.Payload[0] ^= 0xff
	if !bytes.Equal(pub.Raw, raw) {
		t.Errorf("editing Payload changed Raw")
	}
	tl.Packets[6].Raw[0] ^= 0xff

	again := decode(t, conn)
	again.Packets[4].Payload[0] ^= 0xff // the edit made above
	again.Packets[6].Raw[0] ^= 0xff
	if !reflect.DeepEqual(again, tl) {
		t.Errorf("a second Decode of the same Conn differs by more than the edits: editing one timeline changed the Conn")
	}
}

func TestWithPayload_RemainingLengthBoundaries_RoundTrips(t *testing.T) {
	props := &mqttclient.Properties{User: []mqttclient.UserProperty{{Key: "k", Val: "v"}}}
	for _, version := range []byte{mqttclient.MQTT311, mqttclient.MQTT5} {
		pk := publishPk("a/b", 1, 7, []byte("old"))
		pk.ProtocolVersion = version
		pk.FixedHeader.Retain = true
		if version == mqttclient.MQTT5 {
			pk.Properties = *props
		}
		orig := decodeOne(t, version, enc(t, pk))
		varHeader := len(orig.Raw) - 2 - len(orig.Payload)
		// Remaining lengths either side of each change in the length's size.
		for _, rem := range []int{127, 128, 16383, 16384} {
			payload := bytes.Repeat([]byte{'x'}, rem-varHeader)
			p, err := orig.WithPayload(payload)
			if err != nil {
				t.Fatalf("version %d, remaining %d: %v", version, rem, err)
			}
			got := decodeOne(t, version, p.Raw)
			if !bytes.Equal(got.Payload, payload) || got.Topic != "a/b" || got.PacketID != 7 || got.QoS != 1 || !got.Retain {
				t.Errorf("version %d, remaining %d: got %q id %d QoS %d retain %v with %d bytes, want a/b id 7 QoS 1 retain with %d",
					version, rem, got.Topic, got.PacketID, got.QoS, got.Retain, len(got.Payload), len(payload))
			}
			if version == mqttclient.MQTT5 && !reflect.DeepEqual(got.Properties.User, props.User) {
				t.Errorf("remaining %d: got user properties %+v, want %+v", rem, got.Properties.User, props.User)
			}
		}
	}
}

func TestWithPayload_NotAPublish_Error(t *testing.T) {
	tl := decode(t, hostSession(t))
	if _, err := tl.Packets[1].WithPayload([]byte("x")); err == nil {
		t.Errorf("WithPayload on a CONNACK: got no error, want one")
	}
}

// decodeOne decodes raw, one broker packet, at protocol level version.
func decodeOne(t *testing.T, version byte, raw []byte) replay.Packet {
	t.Helper()
	tl, err := replay.Decode(capture.Conn{Segments: []capture.Segment{{Dir: capture.FromServer, Data: raw}}}, version)
	if err != nil || len(tl.Packets) != 1 {
		t.Fatalf("Decode: %d packets, err %v; want 1, nil", len(tl.Packets), err)
	}
	if tl.Packets[0].Err != nil {
		t.Fatalf("Decode: %v", tl.Packets[0].Err)
	}
	return tl.Packets[0]
}

func TestDecode_ConnectionProblems_ErrorAfterGoodPackets(t *testing.T) {
	connect := enc(t, packets.Packet{FixedHeader: fh(packets.Connect), Connect: packets.ConnectParams{ClientIdentifier: "c", Clean: true}})
	tests := []struct {
		name    string
		conn    capture.Conn
		want    error
		packets int
	}{
		{
			name: "TLS",
			conn: capture.Conn{Transport: capture.TLS, Segments: []capture.Segment{{Dir: capture.FromClient, Data: []byte{0x16, 3, 1}}}},
			want: capture.ErrEncrypted,
		},
		{
			name:    "gap",
			conn:    capture.Conn{Err: capture.ErrGap, Segments: []capture.Segment{{Dir: capture.FromClient, Data: connect}}},
			want:    capture.ErrGap,
			packets: 1,
		},
		{
			name:    "incomplete packet at the end",
			conn:    capture.Conn{Segments: []capture.Segment{{Dir: capture.FromClient, Data: append(bytes.Clone(connect), 0x20, 2, 0)}}},
			want:    capture.ErrTruncated,
			packets: 1,
		},
		{
			name: "malformed remaining length",
			conn: capture.Conn{Segments: []capture.Segment{{Dir: capture.FromClient, Data: append(bytes.Clone(connect), 0x20, 0xff, 0xff, 0xff, 0xff, 0x01)}}},
			want: mqttclient.ErrMalformedPacket, packets: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tl, err := replay.Decode(tt.conn, 0)
			if !errors.Is(err, tt.want) {
				t.Errorf("got error %v, want %v", err, tt.want)
			}
			if len(tl.Packets) != tt.packets {
				t.Errorf("got %d packets, want the %d before the problem", len(tl.Packets), tt.packets)
			}
		})
	}
}

func TestDecode_NoConnectOrVersion_Error(t *testing.T) {
	conn := capture.Conn{Segments: []capture.Segment{{Dir: capture.FromServer, Data: []byte{0x20, 2, 0, 0}}}}
	if _, err := replay.Decode(conn, 0); err == nil {
		t.Errorf("got no error for a capture with no CONNECT and no version, want one")
	}
	if tl, err := replay.Decode(conn, mqttclient.MQTT311); err != nil || len(tl.Packets) != 1 {
		t.Errorf("with version 4: got %d packets, err %v; want 1, nil", len(tl.Packets), err)
	}
}

func FuzzDecode(f *testing.F) {
	conn := hostSession(f)
	var client, server []byte
	for _, s := range conn.Segments {
		if s.Dir == capture.FromClient {
			client = append(client, s.Data...)
		} else {
			server = append(server, s.Data...)
		}
	}
	f.Add(client, server, byte(4))
	f.Add([]byte{0x82, 0x05, 0, 1, 0, 0, 'a'}, []byte{0x90, 3, 0, 1, 0}, byte(5))
	f.Fuzz(func(t *testing.T, client, server []byte, version byte) {
		c := capture.Conn{Segments: []capture.Segment{{Dir: capture.FromClient, Data: client}, {Dir: capture.FromServer, Data: server}}}
		tl, _ := replay.Decode(c, version)
		for _, p := range tl.Packets {
			if p.Type == replay.Publish && p.Err == nil {
				if _, err := p.WithPayload([]byte("x")); err != nil {
					t.Errorf("WithPayload on a decoded PUBLISH: %v", err)
				}
			}
		}
	})
}

package replay_test

import (
	"bytes"
	"context"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/replay"
)

func connectPk(id string, clean bool) packets.Packet {
	return packets.Packet{FixedHeader: fh(packets.Connect),
		Connect: packets.ConnectParams{ClientIdentifier: id, Clean: clean, Keepalive: 60}}
}

func ack(typ byte, id uint16) packets.Packet {
	return packets.Packet{FixedHeader: fh(typ), PacketID: id}
}

func TestReplay_BrokerQoS2Publish_DeliveredAfterPubrel(t *testing.T) {
	s := newSession(t, hostAddr, brokerAddr)
	s.client(connectPk("host1", true)).server(packets.Packet{FixedHeader: fh(packets.Connack)})
	s.client(packets.Packet{FixedHeader: fh(packets.Subscribe), PacketID: 1, Filters: packets.Subscriptions{{Filter: "t", Qos: 2}}})
	s.server(packets.Packet{FixedHeader: fh(packets.Suback), PacketID: 1, ReasonCodes: []byte{2}})
	s.server(publishPk("t", 2, 9, []byte("exactly once")))
	s.client(ack(packets.Pubrec, 9)).server(ack(packets.Pubrel, 9)).client(ack(packets.Pubcomp, 9))
	s.client(packets.Packet{FixedHeader: fh(packets.Disconnect)})
	tl := decode(t, s.clientClose().conn)

	r := startReplay(t, []replay.Timeline{tl}, hostServer())
	r.subscribeQoS("t", 2)
	got := r.receive(1)
	res := r.finish()

	if m := got[0]; m.Topic != "t" || m.QoS != 2 || string(m.Payload) != "exactly once" {
		t.Errorf("got %q QoS %d %q, want t QoS 2 \"exactly once\"", m.Topic, m.QoS, m.Payload)
	}
	if len(res.Divergences) != 0 {
		t.Errorf("got divergences %v, want none", res.Divergences)
	}
}

func TestReplay_ClientQoS2Publish_CompletesWithMappedIdentifiers(t *testing.T) {
	s := newSession(t, hostAddr, brokerAddr)
	s.client(connectPk("host1", true)).server(packets.Packet{FixedHeader: fh(packets.Connack)})
	s.client(publishPk("t", 2, 7, []byte("x"))) // the live client will use identifier 1
	s.server(ack(packets.Pubrec, 7)).client(ack(packets.Pubrel, 7)).server(ack(packets.Pubcomp, 7))
	s.client(packets.Packet{FixedHeader: fh(packets.Disconnect)})
	tl := decode(t, s.clientClose().conn)

	r := startReplay(t, []replay.Timeline{tl}, hostServer())
	if _, err := r.c.Connect(r.ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	p, err := r.c.Publish(r.ctx, &mqttclient.Message{Topic: "t", Payload: []byte("x"), QoS: 2})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := p.Wait(r.ctx); err != nil {
		t.Errorf("publish: got %v, want completion after PUBCOMP", err)
	}
	res := r.finish()
	if len(res.Divergences) != 0 {
		t.Errorf("got divergences %v, want none", res.Divergences)
	}
}

// Regression for the reconnect risk in the plan: the second Dial gets the
// second captured connection, and the client resends its unacknowledged
// QoS 1 PUBLISH with DUP set.
func TestReplay_ReconnectSequence_SecondConnectionGetsTheDupResend(t *testing.T) {
	first := newSession(t, hostAddr, brokerAddr)
	first.client(connectPk("c", false)).server(packets.Packet{FixedHeader: fh(packets.Connack)})
	first.client(publishPk("t", 1, 1, []byte("x")))
	first.serverClose() // lost before PUBACK

	second := newSession(t, netip.MustParseAddrPort("10.0.0.1:50001"), brokerAddr)
	second.ms = 100
	second.client(connectPk("c", false)).server(packets.Packet{FixedHeader: fh(packets.Connack), SessionPresent: true})
	resend := publishPk("t", 1, 1, []byte("x"))
	resend.FixedHeader.Dup = true
	second.client(resend).server(ack(packets.Puback, 1))
	second.client(packets.Packet{FixedHeader: fh(packets.Disconnect)})
	second.clientClose()

	tls := []replay.Timeline{decode(t, first.conn), decode(t, second.conn)}
	rp, err := replay.New(tls)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	defer wg.Wait()
	var res replay.Result
	var runErr error
	wg.Go(func() { res, runErr = rp.Run(ctx) })

	connected := make(chan struct{}, 2)
	acked := make(chan byte, 1)
	srv := mqttclient.Server{Address: brokerAddr.String(), ProtocolVersion: mqttclient.MQTT311, ClientID: "c",
		KeepAlive: 60, ReconnectDelay: time.Millisecond, Dial: rp.Dial}
	c, err := mqttclient.New(mqttclient.Options{Servers: []mqttclient.Server{srv}}, mqttclient.Handlers{
		OnConnect: func(*mqttclient.Client, mqttclient.ConnAck) { connected <- struct{}{} },
		OnPublish: func(_ *mqttclient.Client, _ uint16, reason byte, _ *mqttclient.Properties) { acked <- reason },
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	wg.Go(func() { _ = c.Run(runCtx) })

	select {
	case <-connected:
	case <-ctx.Done():
		t.Fatal("never connected")
	}
	if _, err := c.Publish(ctx, &mqttclient.Message{Topic: "t", Payload: []byte("x"), QoS: 1}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case reason := <-acked:
		if reason != 0 {
			t.Errorf("got PUBACK reason 0x%02x, want 0", reason)
		}
	case <-ctx.Done():
		t.Fatal("PUBLISH never acknowledged")
	}
	if err := c.Disconnect(ctx, 0, nil); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
	stopRun()
	wg.Wait()

	if runErr != nil {
		t.Errorf("Run: %v", runErr)
	}
	if res.Replayed != 2 || len(res.Divergences) != 0 {
		t.Errorf("got %d replayed, divergences %v; want 2 and none", res.Replayed, res.Divergences)
	}
}

func TestReplay_LivePingreq_AnsweredAtOnce(t *testing.T) {
	s := newSession(t, hostAddr, brokerAddr)
	connect := enc(t, connectPk("host1", true))
	s.client(connectPk("host1", true)).server(packets.Packet{FixedHeader: fh(packets.Connack)})
	s.client(packets.Packet{FixedHeader: fh(packets.Disconnect)})
	rp, err := replay.New([]replay.Timeline{decode(t, s.clientClose().conn)})
	if err != nil {
		t.Fatal(err)
	}

	rc, wait := dialRaw(t, rp)
	rc.send(connect)
	rc.expect([]byte{0x20, 2, 0, 0})
	rc.send([]byte{0xc0, 0}) // PINGREQ, not in the capture
	rc.expect([]byte{0xd0, 0})
	rc.send([]byte{0xe0, 0})
	if res := wait(); len(res.Divergences) != 0 {
		t.Errorf("got divergences %v, want none", res.Divergences)
	}
}

func TestReplay_BrokerAckForUnknownIdentifier_SentUnchangedWithDivergence(t *testing.T) {
	s := newSession(t, hostAddr, brokerAddr)
	connect := enc(t, connectPk("host1", true))
	s.client(connectPk("host1", true)).server(packets.Packet{FixedHeader: fh(packets.Connack)})
	s.server(ack(packets.Puback, 5)) // for a PUBLISH the capture doesn't have
	rp, err := replay.New([]replay.Timeline{decode(t, s.conn)})
	if err != nil {
		t.Fatal(err)
	}

	rc, wait := dialRaw(t, rp)
	rc.send(connect)
	rc.expect([]byte{0x20, 2, 0, 0})
	rc.expect([]byte{0x40, 2, 0, 5})
	res := wait()
	if len(res.Divergences) != 1 || !strings.Contains(res.Divergences[0].Detail, "PUBACK for packet 5, which the live client never sent") {
		t.Errorf("got divergences %v, want one about PUBACK 5", res.Divergences)
	}
}

func TestReplay_LivePublishDiffers_DivergenceNamesEachField(t *testing.T) {
	s := newSession(t, hostAddr, brokerAddr)
	connect := enc(t, connectPk("host1", true))
	s.client(connectPk("host1", true)).server(packets.Packet{FixedHeader: fh(packets.Connack)})
	s.client(publishPk("a", 0, 0, []byte("12345")))
	rp, err := replay.New([]replay.Timeline{decode(t, s.conn)})
	if err != nil {
		t.Fatal(err)
	}

	rc, wait := dialRaw(t, rp)
	rc.send(connect)
	rc.expect([]byte{0x20, 2, 0, 0})
	live := publishPk("b", 0, 0, []byte("123"))
	live.FixedHeader.Retain = true
	rc.send(enc(t, live))
	res := wait()
	want := `PUBLISH "b" QoS 0 retain true dup false with 3 bytes, capture has "a" QoS 0 retain false dup false with 5 bytes`
	if len(res.Divergences) != 1 || res.Divergences[0].Detail != want {
		t.Errorf("got divergences %v, want one: %s", res.Divergences, want)
	}
}

func TestReplayer_Played_ClosedWhenTheLastTimelineEndsBeforeDisconnect(t *testing.T) {
	s := newSession(t, hostAddr, brokerAddr)
	connect := enc(t, connectPk("host1", true))
	s.client(connectPk("host1", true)).server(packets.Packet{FixedHeader: fh(packets.Connack)})
	s.server(publishPk("t", 0, 0, []byte("last")))
	rp, err := replay.New([]replay.Timeline{decode(t, s.conn)})
	if err != nil {
		t.Fatal(err)
	}

	rc, wait := dialRaw(t, rp)
	rc.send(connect)
	rc.expect([]byte{0x20, 2, 0, 0})
	rc.expect(enc(t, publishPk("t", 0, 0, []byte("last"))))
	select {
	case <-rp.Played():
	case <-time.After(5 * time.Second):
		t.Fatal("Played not closed after the last broker packet")
	}
	if res := wait(); len(res.Divergences) != 0 || res.Replayed != 1 {
		t.Errorf("got %d replayed, divergences %v; want 1 and none", res.Replayed, res.Divergences)
	}
}

func TestNew_TimelineWithoutConnect_Error(t *testing.T) {
	// A capture that starts mid-session: decodable with a version, not
	// replayable.
	conn := capture.Conn{Segments: []capture.Segment{{Dir: capture.FromServer, Data: enc(t, publishPk("t", 0, 0, []byte("x")))}}}
	tl, err := replay.Decode(conn, mqttclient.MQTT311)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replay.New([]replay.Timeline{tl}); err == nil || !strings.Contains(err.Error(), "middle of a session") {
		t.Errorf("got %v, want an error saying the capture starts in the middle of a session", err)
	}
}

func TestInbound_QoS2Publish_SubscriberReceivesAfterPubrel(t *testing.T) {
	s := newSession(t, edgeAddr, brokerAddr)
	s.client(connectPk("edge", true)).server(packets.Packet{FixedHeader: fh(packets.Connack)})
	s.client(publishPk("t", 2, 3, []byte("once"))).server(ack(packets.Pubrec, 3)).client(ack(packets.Pubrel, 3)).server(ack(packets.Pubcomp, 3))
	s.client(packets.Packet{FixedHeader: fh(packets.Disconnect)})
	in, err := replay.Inbound([]replay.Timeline{decode(t, s.clientClose().conn)})
	if err != nil {
		t.Fatal(err)
	}

	r := startReplay(t, []replay.Timeline{in}, hostServer())
	r.subscribeQoS("t", 2)
	got := r.receive(1)
	res := r.finish()
	if m := got[0]; m.QoS != 2 || !bytes.Equal(m.Payload, []byte("once")) {
		t.Errorf("got QoS %d %q, want QoS 2 \"once\"", m.QoS, m.Payload)
	}
	if len(res.Divergences) != 0 {
		t.Errorf("got divergences %v, want none", res.Divergences)
	}
}

func TestReplay_LiveResendWithoutDup_Divergence(t *testing.T) {
	s := newSession(t, hostAddr, brokerAddr)
	connect := enc(t, connectPk("c", false))
	s.client(connectPk("c", false)).server(packets.Packet{FixedHeader: fh(packets.Connack), SessionPresent: true})
	resend := publishPk("t", 1, 1, []byte("x"))
	resend.FixedHeader.Dup = true
	s.client(resend)
	rp, err := replay.New([]replay.Timeline{decode(t, s.conn)})
	if err != nil {
		t.Fatal(err)
	}

	rc, wait := dialRaw(t, rp)
	rc.send(connect)
	rc.expect([]byte{0x20, 2, 1, 0})
	rc.send(enc(t, publishPk("t", 1, 1, []byte("x")))) // DUP clear
	res := wait()
	if len(res.Divergences) == 0 || !strings.Contains(res.Divergences[0].Detail, "dup false") {
		t.Errorf("got divergences %v, want one about DUP", res.Divergences)
	}
}

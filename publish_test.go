package mqttclient

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// pubRecorder collects the publish-related callbacks.
type pubRecorder struct {
	*recorder
	messages  chan *Message
	published chan publishEvent
}

type publishEvent struct {
	mid    uint16
	reason byte
}

func newPubClient(t *testing.T, b *fakeBroker, opts Options) (*Client, *pubRecorder) {
	t.Helper()
	opts.Server = b.url()
	if opts.ClientID == "" {
		opts.ClientID = "c"
	}
	opts.CleanStart = true
	rec := &pubRecorder{
		recorder:  &recorder{connects: make(chan ConnAck, 10), disconnects: make(chan DisconnectEvent, 10)},
		messages:  make(chan *Message, 10),
		published: make(chan publishEvent, 10),
	}
	c, err := New(opts, Handlers{
		OnConnect:    func(_ *Client, a ConnAck) { rec.connects <- a },
		OnDisconnect: func(_ *Client, e DisconnectEvent) { rec.disconnects <- e },
		OnMessage:    func(_ *Client, m *Message) { rec.messages <- m },
		OnPublish:    func(_ *Client, mid uint16, reason byte, _ *Properties) { rec.published <- publishEvent{mid, reason} },
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, rec
}

// handshake runs Connect against b, answering with connack.
func handshake(t *testing.T, b *fakeBroker, c *Client, connack []byte) *fakeConn {
	t.Helper()
	res := connectAsync(c, 5*time.Second)
	fc := b.accept()
	fc.readConnect()
	fc.send(connack...)
	if r := waitResult(t, res); r.err != nil {
		t.Fatal(r.err)
	}
	return fc
}

func (r *pubRecorder) message(t *testing.T) *Message {
	t.Helper()
	select {
	case m := <-r.messages:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("OnMessage not called")
		return nil
	}
}

func (r *pubRecorder) publish(t *testing.T) publishEvent {
	t.Helper()
	select {
	case e := <-r.published:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("OnPublish not called")
		return publishEvent{}
	}
}

func (r *pubRecorder) noMessage(t *testing.T) {
	t.Helper()
	select {
	case m := <-r.messages:
		t.Fatalf("unexpected OnMessage %+v", m)
	case <-time.After(100 * time.Millisecond):
	}
}

func publishOK(t *testing.T, c *Client, m *Message) *Pending {
	t.Helper()
	p, err := c.Publish(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func wait(t *testing.T, p *Pending) (Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.Wait(ctx)
}

func msg(qos byte) *Message { return &Message{Topic: "t", Payload: []byte("hi"), QoS: qos} }

func TestPublishQoS0(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))

	p := publishOK(t, c, &Message{Topic: "t", Payload: []byte("hi"), Retain: true})
	fc.expect(0x31, 0x05, 0x00, 0x01, 't', 'h', 'i')
	if _, err := wait(t, p); err != nil || p.Mid != 1 {
		t.Fatalf("mid %d err %v", p.Mid, err)
	}
	if e := rec.publish(t); e.mid != 1 {
		t.Fatalf("OnPublish %+v", e)
	}
	// QoS 0 takes a mid too, as in libmosquitto.
	if p := publishOK(t, c, msg(0)); p.Mid != 2 {
		t.Fatalf("second mid %d", p.Mid)
	}
}

func TestPublishQoS1(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))

	p := publishOK(t, c, msg(1))
	fc.expect(0x32, 0x07, 0x00, 0x01, 't', 0x00, 0x01, 'h', 'i')
	select {
	case <-p.Done():
		t.Fatal("complete before PUBACK")
	case <-time.After(50 * time.Millisecond):
	}
	fc.send(0x40, 0x02, 0x00, 0x01)
	if _, err := wait(t, p); err != nil {
		t.Fatal(err)
	}
	if e := rec.publish(t); e.mid != 1 {
		t.Fatalf("OnPublish %+v", e)
	}
}

func TestPublishQoS2(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))

	p := publishOK(t, c, msg(2))
	fc.expect(0x34, 0x07, 0x00, 0x01, 't', 0x00, 0x01, 'h', 'i')
	fc.send(0x50, 0x02, 0x00, 0x01)
	fc.expect(0x62, 0x02, 0x00, 0x01)
	fc.send(0x70, 0x02, 0x00, 0x01)
	if _, err := wait(t, p); err != nil {
		t.Fatal(err)
	}
	if e := rec.publish(t); e.mid != 1 {
		t.Fatalf("OnPublish %+v", e)
	}
}

func TestPublishV5Properties(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newPubClient(t, b, Options{ProtocolVersion: MQTT5})
	fc := handshake(t, b, c, connackBytes(t, 5, 0, false, nil))

	// Response Topic is only encoded by comqtt with AllowResponseInfo, and
	// Payload Format only with its flag: the client takes care of both.
	publishOK(t, c, &Message{Topic: "t", Properties: &Properties{PayloadFormat: 1, ResponseTopic: "r"}})
	fc.expect(0x30, 0x0A, 0x00, 0x01, 't', 0x06, 0x01, 0x01, 0x08, 0x00, 0x01, 'r')
}

// PUBREC with a failure reason code ends a QoS 2 flow (MQTT 5).
func TestPubrecFailure(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, Options{ProtocolVersion: MQTT5})
	fc := handshake(t, b, c, connackBytes(t, 5, 0, false, &Properties{ReceiveMaximum: 1}))

	p1 := publishOK(t, c, msg(2))
	p2 := publishOK(t, c, msg(2))
	fc.expect(0x34, 0x08, 0x00, 0x01, 't', 0x00, 0x01, 0x00, 'h', 'i')
	fc.send(0x50, 0x03, 0x00, 0x01, 0x87)
	_, err := wait(t, p1)
	var rce *ReasonCodeError
	if !errors.As(err, &rce) || rce.ReasonCode != 0x87 || rce.Packet != "PUBREC" {
		t.Fatalf("Wait err %v", err)
	}
	if e := rec.publish(t); e.mid != 1 || e.reason != 0x87 {
		t.Fatalf("OnPublish %+v", e)
	}
	// The quota is free again: the second message goes out, with no PUBREL
	// for the first.
	fc.expect(0x34, 0x08, 0x00, 0x01, 't', 0x00, 0x02, 0x00, 'h', 'i')
	fc.send(0x50, 0x02, 0x00, 0x02)
	fc.expect(0x62, 0x02, 0x00, 0x02)
	fc.send(0x70, 0x02, 0x00, 0x02)
	if _, err := wait(t, p2); err != nil {
		t.Fatal(err)
	}
}

func TestSendQuota(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		opts    Options
		props   *Properties
		quota   int
	}{
		{"v5 server receive maximum", 5, Options{ProtocolVersion: MQTT5}, &Properties{ReceiveMaximum: 2}, 2},
		{"v311 max inflight", 4, Options{MaxInflight: 3}, nil, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(t)
			c, _ := newPubClient(t, b, tc.opts)
			fc := handshake(t, b, c, connackBytes(t, tc.version, 0, false, tc.props))
			for range tc.quota + 2 {
				publishOK(t, c, msg(1))
			}
			for range tc.quota {
				fc.readRaw()
			}
			if raw, err := fc.tryReadRaw(100 * time.Millisecond); err == nil {
				t.Fatalf("message beyond the quota sent: % x", raw)
			}
			fc.send(0x40, 0x02, 0x00, 0x01)
			if raw := fc.readRaw(); raw[6] != byte(tc.quota+1) {
				t.Fatalf("after PUBACK got % x, want mid %d", raw, tc.quota+1)
			}
		})
	}
}

func TestPublishChecks(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newPubClient(t, b, Options{ProtocolVersion: MQTT5})
	fc := handshake(t, b, c, connackBytes(t, 5, 0, false, &Properties{
		MaximumQos: 1, MaximumQosFlag: true, RetainAvailable: 0, RetainAvailableFlag: true, MaximumPacketSize: 20}))

	cases := []struct {
		name string
		m    *Message
		want error
	}{
		{"qos above server maximum", msg(2), ErrQoSNotSupported},
		{"oversize", &Message{Topic: "t", Payload: make([]byte, 15)}, ErrOversizePacket},
		{"wildcard topic", &Message{Topic: "a/+"}, ErrInvalid},
		{"empty topic", &Message{}, ErrInvalid},
		{"qos 3", &Message{Topic: "t", QoS: 3}, ErrInvalid},
		{"topic alias", &Message{Topic: "t", Properties: &Properties{TopicAlias: 1}}, ErrNotSupported},
		{"subscription identifier", &Message{Topic: "t", Properties: &Properties{SubscriptionIdentifier: []int{1}}}, ErrInvalid},
	}
	for _, tc := range cases {
		if _, err := c.Publish(context.Background(), tc.m); !errors.Is(err, tc.want) {
			t.Errorf("%s: err %v, want %v", tc.name, err, tc.want)
		}
	}
	// 20 bytes exactly fits; the refused messages took no mid; retain is
	// cleared because the server does not support it.
	p := publishOK(t, c, &Message{Topic: "t", Payload: make([]byte, 14), Retain: true})
	if p.Mid != 1 {
		t.Fatalf("mid %d", p.Mid)
	}
	if raw := fc.readRaw(); len(raw) != 20 || raw[0] != 0x30 {
		t.Fatalf("got % x", raw)
	}
}

func TestPublishV311Properties(t *testing.T) {
	c, err := New(Options{Server: "mqtt://h", CleanStart: true}, Handlers{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(context.Background(), &Message{Topic: "t", Properties: &Properties{}}); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("err %v", err)
	}
}

// QoS 0 needs a connection; QoS 1 and 2 are queued and sent after the next
// CONNACK, as with mosquitto_publish.
func TestPublishWhileDisconnected(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newPubClient(t, b, Options{})
	if _, err := c.Publish(context.Background(), msg(0)); !errors.Is(err, ErrNoConn) {
		t.Fatalf("QoS 0 err %v", err)
	}
	p := publishOK(t, c, msg(1))
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
	fc.expect(0x32, 0x07, 0x00, 0x01, 't', 0x00, 0x01, 'h', 'i') // first send: no DUP
	fc.send(0x40, 0x02, 0x00, 0x01)
	if _, err := wait(t, p); err != nil {
		t.Fatal(err)
	}
}

// In-flight messages are sent again on the next connection: PUBLISH with
// DUP set, or PUBREL once PUBREC has arrived (message__reconnect_reset).
func TestResendAfterReconnect(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
	p1 := publishOK(t, c, msg(1))
	p2 := publishOK(t, c, msg(2))
	fc.readRaw()
	fc.readRaw()
	fc.send(0x50, 0x02, 0x00, 0x02) // PUBREC for the QoS 2 message
	fc.expect(0x62, 0x02, 0x00, 0x02)
	fc.nc.Close()
	rec.disconnect(t)

	fc = handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
	fc.expect(0x3A, 0x07, 0x00, 0x01, 't', 0x00, 0x01, 'h', 'i')
	fc.expect(0x62, 0x02, 0x00, 0x02)
	fc.send(0x40, 0x02, 0x00, 0x01)
	fc.send(0x70, 0x02, 0x00, 0x02)
	for _, p := range []*Pending{p1, p2} {
		if _, err := wait(t, p); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReceiveQoS0And1(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))

	fc.send(0x31, 0x05, 0x00, 0x01, 'a', 'h', 'i')
	if m := rec.message(t); m.Topic != "a" || string(m.Payload) != "hi" || m.QoS != 0 || !m.Retain {
		t.Fatalf("message %+v", m)
	}
	fc.send(0x32, 0x07, 0x00, 0x01, 'a', 0x12, 0x34, 'h', 'i')
	fc.expect(0x40, 0x02, 0x12, 0x34)
	if m := rec.message(t); m.Mid != 0x1234 || m.QoS != 1 {
		t.Fatalf("message %+v", m)
	}
}

// Inbound QoS 2 is delivered on PUBREL. A resent PUBLISH for a stored
// message replaces it, so it is delivered once.
func TestReceiveQoS2(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))

	fc.send(0x34, 0x07, 0x00, 0x01, 'a', 0x00, 0x07, 'h', 'i')
	fc.expect(0x50, 0x02, 0x00, 0x07)
	rec.noMessage(t)
	fc.send(0x3C, 0x07, 0x00, 0x01, 'a', 0x00, 0x07, 'h', '2')
	fc.expect(0x50, 0x02, 0x00, 0x07)
	fc.send(0x62, 0x02, 0x00, 0x07)
	fc.expect(0x70, 0x02, 0x00, 0x07)
	if m := rec.message(t); string(m.Payload) != "h2" || m.Mid != 7 {
		t.Fatalf("message %+v", m)
	}
	rec.noMessage(t)

	// A PUBREL for an unknown mid is still answered.
	fc.send(0x62, 0x02, 0x00, 0x08)
	fc.expect(0x70, 0x02, 0x00, 0x08)
	rec.noMessage(t)
}

func TestReceiveV5Properties(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, Options{ProtocolVersion: MQTT5})
	fc := handshake(t, b, c, connackBytes(t, 5, 0, false, nil))
	fc.send(0x30, 0x0A, 0x00, 0x01, 'a', 0x05, 0x03, 0x00, 0x02, 'c', 't', 'x')
	m := rec.message(t)
	if m.Properties == nil || m.Properties.ContentType != "ct" || string(m.Payload) != "x" {
		t.Fatalf("message %+v", m)
	}
}

// Unknown acknowledgements are ignored; ones that do not match the
// message's QoS, or carry a reason code not allowed for the packet, are
// protocol errors.
func TestUnexpectedAcks(t *testing.T) {
	t.Run("unknown puback and pubcomp", func(t *testing.T) {
		b := newFakeBroker(t)
		c, rec := newPubClient(t, b, Options{})
		fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
		fc.send(0x40, 0x02, 0x00, 0x09)
		fc.send(0x70, 0x02, 0x00, 0x09)
		fc.send(0xC0, 0x00) // still alive: PINGREQ is answered
		fc.expect(0xD0, 0x00)
		select {
		case e := <-rec.published:
			t.Fatalf("OnPublish %+v", e)
		default:
		}
	})
	t.Run("unknown pubrec gets pubrel", func(t *testing.T) {
		b := newFakeBroker(t)
		c, _ := newPubClient(t, b, Options{})
		fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
		fc.send(0x50, 0x02, 0x00, 0x09)
		fc.expect(0x62, 0x02, 0x00, 0x09)
	})
	cases := []struct {
		name string
		qos  byte
		ack  []byte
		want []byte
	}{
		{"puback for qos 2", 2, []byte{0x40, 0x02, 0x00, 0x01}, []byte{0xE0, 0x01, 0x82}},
		{"pubrec for qos 1", 1, []byte{0x50, 0x02, 0x00, 0x01}, []byte{0xE0, 0x01, 0x82}},
		{"invalid puback reason", 1, []byte{0x40, 0x03, 0x00, 0x01, 0x92}, []byte{0xE0, 0x01, 0x82}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(t)
			c, rec := newPubClient(t, b, Options{ProtocolVersion: MQTT5})
			fc := handshake(t, b, c, connackBytes(t, 5, 0, false, nil))
			publishOK(t, c, msg(tc.qos))
			fc.readRaw()
			fc.send(tc.ack...)
			fc.expect(tc.want...)
			if ev := rec.disconnect(t); !errors.Is(ev.Err, ErrProtocol) {
				t.Fatalf("OnDisconnect err %v", ev.Err)
			}
		})
	}
}

func TestInboundProtocolErrors(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		opts    Options
		send    [][]byte
		want    []byte
	}{
		{"receive maximum exceeded", 5, Options{ProtocolVersion: MQTT5, ReceiveMaximum: 1}, [][]byte{
			{0x34, 0x06, 0x00, 0x01, 'a', 0x00, 0x01, 0x00},
			{0x34, 0x06, 0x00, 0x01, 'a', 0x00, 0x02, 0x00},
		}, []byte{0xE0, 0x01, 0x93}},
		{"topic alias", 5, Options{ProtocolVersion: MQTT5}, [][]byte{
			{0x30, 0x07, 0x00, 0x01, 'a', 0x03, 0x23, 0x00, 0x01},
		}, []byte{0xE0, 0x01, 0x94}},
		{"wildcard topic", 5, Options{ProtocolVersion: MQTT5}, [][]byte{
			{0x30, 0x04, 0x00, 0x01, '#', 0x00},
		}, []byte{0xE0, 0x01, 0x81}},
		{"mid 0", 5, Options{ProtocolVersion: MQTT5}, [][]byte{
			{0x32, 0x06, 0x00, 0x01, 'a', 0x00, 0x00, 0x00},
		}, []byte{0xE0, 0x01, 0x82}},
		{"oversize for client maximum", 5, Options{ProtocolVersion: MQTT5, ConnectProperties: &Properties{MaximumPacketSize: 8}}, [][]byte{
			{0x30, 0x07, 0x00, 0x01, 'a', 0x00, 'x', 'y', 'z'},
		}, []byte{0xE0, 0x01, 0x95}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(t)
			c, rec := newPubClient(t, b, tc.opts)
			fc := handshake(t, b, c, connackBytes(t, tc.version, 0, false, nil))
			for i, p := range tc.send {
				fc.send(p...)
				if i < len(tc.send)-1 {
					fc.readRaw() // PUBREC
				}
			}
			fc.expect(tc.want...)
			if ev := rec.disconnect(t); ev.Err == nil {
				t.Fatal("OnDisconnect without error")
			}
		})
	}
}

// Stored inbound QoS 2 messages are dropped when the server has no session:
// their PUBREL will never come, and they must not use up Receive Maximum.
func TestInboundQoS2ClearedWithoutSession(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, Options{ProtocolVersion: MQTT5, ReceiveMaximum: 1})
	fc := handshake(t, b, c, connackBytes(t, 5, 0, false, nil))
	fc.send(0x34, 0x06, 0x00, 0x01, 'a', 0x00, 0x01, 0x00)
	fc.readRaw()
	fc.nc.Close()
	rec.disconnect(t)

	fc = handshake(t, b, c, connackBytes(t, 5, 0, false, nil))
	fc.send(0x34, 0x06, 0x00, 0x01, 'a', 0x00, 0x02, 0x00)
	fc.expect(0x50, 0x02, 0x00, 0x02) // accepted: the quota is free
	fc.send(0x62, 0x02, 0x00, 0x01)   // PUBREL for the dropped message
	fc.expect(0x70, 0x02, 0x00, 0x01)
	rec.noMessage(t)
}

func TestMidAllocation(t *testing.T) {
	var s session
	s.init()
	s.lastMid = 65534
	s.outByMid[65535] = &outMsg{}
	s.outByMid[1] = &outMsg{}
	if mid, err := s.nextMid(); err != nil || mid != 2 {
		t.Fatalf("mid %d err %v, want 2 (skip in-use 65535, 0 and 1)", mid, err)
	}
	for m := range 65536 {
		s.outByMid[uint16(m)] = &outMsg{}
	}
	if _, err := s.nextMid(); !errors.Is(err, ErrNoMid) {
		t.Fatalf("err %v", err)
	}
}

// OnMessage for QoS 1 runs only after PUBACK is written, so a handler that
// publishes cannot overtake the acknowledgement (11-prop-recv-qos1).
func TestAckBeforeDelivery(t *testing.T) {
	b := newFakeBroker(t)
	c, err := New(Options{Server: b.url(), ClientID: "c", CleanStart: true}, Handlers{
		OnMessage: func(c *Client, _ *Message) { _, _ = c.Publish(context.Background(), &Message{Topic: "ok"}) },
	})
	if err != nil {
		t.Fatal(err)
	}
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
	fc.send(0x32, 0x07, 0x00, 0x01, 'a', 0x00, 0x05, 'h', 'i')
	fc.expect(0x40, 0x02, 0x00, 0x05)
	if raw := fc.readRaw(); !bytes.HasPrefix(raw, []byte{0x30, 0x04, 0x00, 0x02, 'o', 'k'}) {
		t.Fatalf("got % x", raw)
	}
}

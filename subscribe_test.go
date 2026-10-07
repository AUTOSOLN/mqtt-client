package mqttclient

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// subRecorder collects OnSubscribe and OnUnsubscribe.
type subRecorder struct {
	*pubRecorder
	subacks   chan []byte
	unsubacks chan []byte
}

func newSubClient(t *testing.T, b *fakeBroker, opts Options) (*Client, *subRecorder) {
	t.Helper()
	c, pr := newPubClient(t, b, opts)
	rec := &subRecorder{pubRecorder: pr, subacks: make(chan []byte, 10), unsubacks: make(chan []byte, 10)}
	c.h.OnSubscribe = func(_ *Client, _ uint16, granted []byte, _ *Properties) { rec.subacks <- granted }
	c.h.OnUnsubscribe = func(_ *Client, _ uint16, reasons []byte, _ *Properties) { rec.unsubacks <- reasons }
	return c, rec
}

func recv(t *testing.T, ch chan []byte, what string) []byte {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(5 * time.Second):
		t.Fatalf("%s not called", what)
		return nil
	}
}

func subscribeOK(t *testing.T, c *Client, subs []Subscription, props *Properties) *Pending {
	t.Helper()
	p, err := c.Subscribe(context.Background(), subs, props)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSubscribeV311(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newSubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))

	// MQTT 5 options are dropped for 3.1.1, as libmosquitto does.
	p := subscribeOK(t, c, []Subscription{{Topic: "a/b", QoS: 1, NoLocal: true}}, nil)
	fc.expect(0x82, 0x08, 0x00, 0x01, 0x00, 0x03, 'a', '/', 'b', 0x01)
	fc.send(0x90, 0x03, 0x00, 0x01, 0x01)
	res, err := wait(t, p)
	if err != nil || !slices.Equal(res.ReasonCodes, []byte{1}) {
		t.Fatalf("result %+v err %v", res, err)
	}
	if g := recv(t, rec.subacks, "OnSubscribe"); !slices.Equal(g, []byte{1}) {
		t.Fatalf("OnSubscribe %v", g)
	}
	// Subscriptions and publishes share the mid sequence.
	if p := publishOK(t, c, msg(0)); p.Mid != 2 {
		t.Fatalf("publish mid %d", p.Mid)
	}
}

func TestSubscribeV5OptionsAndProperties(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newSubClient(t, b, Options{ProtocolVersion: MQTT5})
	fc := handshake(t, b, c, connackBytes(t, 5, 0, false, nil))

	p := subscribeOK(t, c, []Subscription{{Topic: "a", QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 1}},
		&Properties{SubscriptionIdentifier: []int{5}, User: []UserProperty{{Key: "k", Val: "v"}}})
	fc.expect(0x82, 0x10, 0x00, 0x01,
		0x09, 0x0B, 0x05, 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v',
		0x00, 0x01, 'a', 0x1E)
	fc.send(0x90, 0x04, 0x00, 0x01, 0x00, 0x02)
	if res, err := wait(t, p); err != nil || !slices.Equal(res.ReasonCodes, []byte{2}) {
		t.Fatalf("result %+v err %v", res, err)
	}
}

// A refused filter makes Wait return a *ReasonCodeError, with every code
// still in the Result.
func TestSubackFailure(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newSubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))

	p := subscribeOK(t, c, []Subscription{{Topic: "a"}, {Topic: "b", QoS: 1}}, nil)
	fc.readRaw()
	fc.send(0x90, 0x04, 0x00, 0x01, 0x00, 0x80)
	res, err := wait(t, p)
	var rce *ReasonCodeError
	if !errors.As(err, &rce) || rce.ReasonCode != 0x80 || rce.Packet != "SUBACK" || !slices.Equal(res.ReasonCodes, []byte{0, 0x80}) {
		t.Fatalf("result %+v err %v", res, err)
	}
	recv(t, rec.subacks, "OnSubscribe")
}

func TestUnsubscribe(t *testing.T) {
	t.Run("v311 has no reason codes", func(t *testing.T) {
		b := newFakeBroker(t)
		c, rec := newSubClient(t, b, Options{})
		fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
		p, err := c.Unsubscribe(context.Background(), []string{"a", "b"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		fc.expect(0xA2, 0x08, 0x00, 0x01, 0x00, 0x01, 'a', 0x00, 0x01, 'b')
		fc.send(0xB0, 0x02, 0x00, 0x01)
		if res, err := wait(t, p); err != nil || !slices.Equal(res.ReasonCodes, []byte{0, 0}) {
			t.Fatalf("result %+v err %v", res, err)
		}
		if r := recv(t, rec.unsubacks, "OnUnsubscribe"); !slices.Equal(r, []byte{0, 0}) {
			t.Fatalf("OnUnsubscribe %v", r)
		}
	})
	t.Run("v5 reason codes and properties", func(t *testing.T) {
		b := newFakeBroker(t)
		c, _ := newSubClient(t, b, Options{ProtocolVersion: MQTT5})
		fc := handshake(t, b, c, connackBytes(t, 5, 0, false, nil))
		p, err := c.Unsubscribe(context.Background(), []string{"a", "b"}, &Properties{User: []UserProperty{{Key: "k", Val: "v"}}})
		if err != nil {
			t.Fatal(err)
		}
		fc.expect(0xA2, 0x10, 0x00, 0x01, 0x07, 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v', 0x00, 0x01, 'a', 0x00, 0x01, 'b')
		fc.send(0xB0, 0x05, 0x00, 0x01, 0x00, 0x00, 0x11)
		if res, err := wait(t, p); err != nil || !slices.Equal(res.ReasonCodes, []byte{0, 0x11}) {
			t.Fatalf("result %+v err %v", res, err)
		}
	})
}

func TestSubscribeChecks(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newSubClient(t, b, Options{ProtocolVersion: MQTT5})
	if _, err := c.Subscribe(context.Background(), []Subscription{{Topic: "a"}}, nil); !errors.Is(err, ErrNoConn) {
		t.Fatalf("disconnected: %v", err)
	}
	fc := handshake(t, b, c, connackBytes(t, 5, 0, false, &Properties{MaximumPacketSize: 20}))

	subs := []struct {
		name string
		subs []Subscription
		prop *Properties
		want error
	}{
		{"none", nil, nil, ErrInvalid},
		{"empty filter", []Subscription{{}}, nil, ErrInvalid},
		{"# not last", []Subscription{{Topic: "#/a"}}, nil, ErrInvalid},
		{"# inside a level", []Subscription{{Topic: "a/b#"}}, nil, ErrInvalid},
		{"+ inside a level", []Subscription{{Topic: "a+/b"}}, nil, ErrInvalid},
		{"qos 3", []Subscription{{Topic: "a", QoS: 3}}, nil, ErrInvalid},
		{"retain handling 3", []Subscription{{Topic: "a", RetainHandling: 3}}, nil, ErrInvalid},
		{"subscription id 0", []Subscription{{Topic: "a"}}, &Properties{SubscriptionIdentifier: []int{0}}, ErrInvalid},
		{"two subscription ids", []Subscription{{Topic: "a"}}, &Properties{SubscriptionIdentifier: []int{1, 2}}, ErrInvalid},
		{"oversize", []Subscription{{Topic: "0123456789abcdef"}}, nil, ErrOversizePacket},
	}
	for _, tc := range subs {
		if _, err := c.Subscribe(context.Background(), tc.subs, tc.prop); !errors.Is(err, tc.want) {
			t.Errorf("subscribe %s: err %v, want %v", tc.name, err, tc.want)
		}
	}
	if _, err := c.Unsubscribe(context.Background(), []string{"a"}, &Properties{SubscriptionIdentifier: []int{1}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("unsubscribe with subscription id: %v", err)
	}
	if _, err := c.Unsubscribe(context.Background(), []string{"a/#/b"}, nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("unsubscribe bad filter: %v", err)
	}
	// Valid wildcards; the refused requests took no mid.
	p := subscribeOK(t, c, []Subscription{{Topic: "+/a/#"}}, nil)
	if raw := fc.readRaw(); p.Mid != 1 || raw[3] != 1 {
		t.Fatalf("mid %d, packet % x", p.Mid, raw)
	}
}

func TestNoTopicCheck(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newSubClient(t, b, Options{NoTopicCheck: true})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))

	subscribeOK(t, c, []Subscription{{Topic: "a/#/+"}}, nil)
	fc.expect(0x82, 0x0A, 0x00, 0x01, 0x00, 0x05, 'a', '/', '#', '/', '+', 0x00)
	if _, err := c.Unsubscribe(context.Background(), []string{"b+"}, nil); err != nil {
		t.Fatal(err)
	}
	fc.expect(0xA2, 0x06, 0x00, 0x02, 0x00, 0x02, 'b', '+')
	publishOK(t, c, &Message{Topic: "+"})
	fc.expect(0x30, 0x03, 0x00, 0x01, '+')
}

func TestSubscribeV311Properties(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newSubClient(t, b, Options{})
	handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
	if _, err := c.Subscribe(context.Background(), []Subscription{{Topic: "a"}}, &Properties{}); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("err %v", err)
	}
}

// SUBSCRIBE is not resent: a request still waiting for SUBACK fails when the
// connection closes, and its mid is free again.
func TestSubscribeFailsOnConnectionLoss(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newSubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
	p := subscribeOK(t, c, []Subscription{{Topic: "a"}}, nil)
	fc.readRaw()
	fc.nc.Close()
	if _, err := wait(t, p); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Wait err %v", err)
	}
	rec.disconnect(t)

	fc = handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
	if raw, err := fc.tryReadRaw(100 * time.Millisecond); err == nil {
		t.Fatalf("resent % x", raw)
	}
	p = subscribeOK(t, c, []Subscription{{Topic: "a"}}, nil)
	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := wait(t, p); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("Wait after Disconnect: %v", err)
	}
}

func TestSubackProtocolErrors(t *testing.T) {
	cases := []struct {
		name string
		ack  []byte
	}{
		{"suback without reason codes", []byte{0x90, 0x03, 0x00, 0x01, 0x00}},
		{"unsuback for a subscribe", []byte{0xB0, 0x03, 0x00, 0x01, 0x00}},
		{"suback mid 0", []byte{0x90, 0x04, 0x00, 0x00, 0x00, 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(t)
			c, rec := newSubClient(t, b, Options{ProtocolVersion: MQTT5})
			fc := handshake(t, b, c, connackBytes(t, 5, 0, false, nil))
			p := subscribeOK(t, c, []Subscription{{Topic: "a"}}, nil)
			fc.readRaw()
			fc.send(tc.ack...)
			fc.expect(0xE0, 0x01, 0x82)
			if ev := rec.disconnect(t); !errors.Is(ev.Err, ErrProtocol) {
				t.Fatalf("OnDisconnect err %v", ev.Err)
			}
			if _, err := wait(t, p); !errors.Is(err, ErrProtocol) {
				t.Fatalf("Wait err %v", err)
			}
		})
	}
	t.Run("unknown suback ignored", func(t *testing.T) {
		b := newFakeBroker(t)
		c, rec := newSubClient(t, b, Options{})
		fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
		fc.send(0x90, 0x03, 0x00, 0x09, 0x00)
		fc.send(0xC0, 0x00)
		fc.expect(0xD0, 0x00)
		select {
		case g := <-rec.subacks:
			t.Fatalf("OnSubscribe %v", g)
		default:
		}
	})
}

// A subscribed client receives its own message back (03-publish-loop).
func TestSubscribeAndReceive(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newSubClient(t, b, Options{})
	fc := handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
	subscribeOK(t, c, []Subscription{{Topic: "a/+"}}, nil)
	fc.readRaw()
	fc.send(0x90, 0x03, 0x00, 0x01, 0x00)
	recv(t, rec.subacks, "OnSubscribe")
	fc.send(0x30, 0x06, 0x00, 0x03, 'a', '/', 'x', '!')
	if m := rec.message(t); m.Topic != "a/x" || string(m.Payload) != "!" {
		t.Fatalf("message %+v", m)
	}
}

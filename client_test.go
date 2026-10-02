package mqttclient

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// recorder collects callbacks.
type recorder struct {
	connects    chan ConnAck
	disconnects chan DisconnectEvent
}

func newTestClient(t *testing.T, b *fakeBroker, opts Options) (*Client, *recorder) {
	t.Helper()
	opts.Server = b.url()
	rec := &recorder{connects: make(chan ConnAck, 10), disconnects: make(chan DisconnectEvent, 10)}
	c, err := New(opts, Handlers{
		OnConnect:    func(_ *Client, a ConnAck) { rec.connects <- a },
		OnDisconnect: func(_ *Client, e DisconnectEvent) { rec.disconnects <- e },
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, rec
}

// connectAsync runs Connect in the background and returns its result.
func connectAsync(c *Client, timeout time.Duration) <-chan connackResult {
	ch := make(chan connackResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		ack, err := c.Connect(ctx)
		ch <- connackResult{ack, err}
	}()
	return ch
}

func waitResult(t *testing.T, ch <-chan connackResult) connackResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return")
		return connackResult{}
	}
}

func (r *recorder) disconnect(t *testing.T) DisconnectEvent {
	t.Helper()
	select {
	case ev := <-r.disconnects:
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("OnDisconnect not called")
		return DisconnectEvent{}
	}
}

func (r *recorder) connect(t *testing.T) ConnAck {
	t.Helper()
	select {
	case a := <-r.connects:
		return a
	case <-time.After(5 * time.Second):
		t.Fatal("OnConnect not called")
		return ConnAck{}
	}
}

func TestConnectDisconnectV311(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newTestClient(t, b, Options{ClientID: "c1", CleanStart: true, KeepAlive: 60,
		Username: "user", Password: []byte("pw"),
		Will: &Message{Topic: "will/t", Payload: []byte("bye"), QoS: 1, Retain: true}})

	for round := 0; round < 2; round++ { // a client can connect again after Disconnect
		res := connectAsync(c, 5*time.Second)
		fc := b.accept()
		pk := fc.readConnect()
		if string(pk.Connect.ProtocolName) != "MQTT" || pk.ProtocolVersion != 4 {
			t.Fatalf("protocol %q v%d", pk.Connect.ProtocolName, pk.ProtocolVersion)
		}
		if pk.Connect.ClientIdentifier != "c1" || !pk.Connect.Clean || pk.Connect.Keepalive != 60 {
			t.Fatalf("connect params %+v", pk.Connect)
		}
		if string(pk.Connect.Username) != "user" || string(pk.Connect.Password) != "pw" {
			t.Fatalf("credentials %q %q", pk.Connect.Username, pk.Connect.Password)
		}
		if !pk.Connect.WillFlag || pk.Connect.WillTopic != "will/t" || string(pk.Connect.WillPayload) != "bye" ||
			pk.Connect.WillQos != 1 || !pk.Connect.WillRetain {
			t.Fatalf("will %+v", pk.Connect)
		}
		fc.send(connackBytes(t, 4, 0, false, nil)...)
		r := waitResult(t, res)
		if r.err != nil {
			t.Fatal(r.err)
		}
		if a := rec.connect(t); a.ReasonCode != 0 {
			t.Fatalf("OnConnect %+v", a)
		}
		if !c.IsConnected() {
			t.Fatal("IsConnected false after CONNACK")
		}
		if _, err := c.Connect(context.Background()); !errors.Is(err, ErrAlreadyConnected) {
			t.Fatalf("second Connect: %v", err)
		}

		if err := c.Disconnect(context.Background(), 0, nil); err != nil {
			t.Fatal(err)
		}
		fc.expect(0xE0, 0x00)
		fc.expectClosed()
		if ev := rec.disconnect(t); ev.Err != nil {
			t.Fatalf("OnDisconnect err %v", ev.Err)
		}
		if c.IsConnected() {
			t.Fatal("IsConnected true after Disconnect")
		}
	}
	if err := c.Disconnect(context.Background(), 0, nil); !errors.Is(err, ErrNoConn) {
		t.Fatalf("Disconnect when disconnected: %v", err)
	}
}

func TestConnectV5Properties(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newTestClient(t, b, Options{CleanStart: true, ProtocolVersion: MQTT5,
		ConnectProperties: &Properties{SessionExpiryInterval: 30, SessionExpiryIntervalFlag: true,
			User: []UserProperty{{Key: "k", Val: "v"}}}})
	res := connectAsync(c, 5*time.Second)
	fc := b.accept()
	pk := fc.readConnect()
	p := pk.Properties
	if p.ReceiveMaximum != DefaultReceiveMaximum {
		t.Fatalf("receive maximum %d", p.ReceiveMaximum)
	}
	if !p.SessionExpiryIntervalFlag || p.SessionExpiryInterval != 30 || len(p.User) != 1 {
		t.Fatalf("properties %+v", p)
	}
	fc.send(connackBytes(t, 5, 0, false, &Properties{AssignedClientID: "assigned-1", ReasonString: "hi"})...)
	r := waitResult(t, res)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.ack.Properties == nil || r.ack.Properties.AssignedClientID != "assigned-1" {
		t.Fatalf("ack %+v", r.ack)
	}
	if got := c.ClientID(); got != "assigned-1" {
		t.Fatalf("ClientID %q", got)
	}
	rec.connect(t)
}

func TestReceiveMaximumFromConnectProperties(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newTestClient(t, b, Options{CleanStart: true, ProtocolVersion: MQTT5, ReceiveMaximum: 7,
		ConnectProperties: &Properties{ReceiveMaximum: 3}})
	connectAsync(c, 5*time.Second)
	if got := b.accept().readConnect().Properties.ReceiveMaximum; got != 3 {
		t.Fatalf("receive maximum %d, want 3 from ConnectProperties", got)
	}
}

func TestConnackRefused(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		connack func(t *testing.T) []byte
		code    byte
	}{
		{"v311 not authorized", 4, func(t *testing.T) []byte { return connackBytes(t, 4, 5, false, nil) }, 5},
		{"v5 not authorized", 5, func(t *testing.T) []byte { return connackBytes(t, 5, 0x87, false, nil) }, 0x87},
		// A 3.x broker answering an MQTT 5 CONNECT.
		{"v5 to v3 broker", 5, func(*testing.T) []byte { return []byte{0x20, 0x02, 0x00, 0x01} }, 0x84},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(t)
			c, rec := newTestClient(t, b, Options{ClientID: "c", CleanStart: true, ProtocolVersion: tc.version})
			res := connectAsync(c, 5*time.Second)
			fc := b.accept()
			fc.readConnect()
			fc.send(tc.connack(t)...)
			r := waitResult(t, res)
			var refused *ConnRefusedError
			if !errors.As(r.err, &refused) || refused.ReasonCode != tc.code {
				t.Fatalf("Connect err %v, want refused 0x%02x", r.err, tc.code)
			}
			if a := rec.connect(t); a.ReasonCode != tc.code {
				t.Fatalf("OnConnect code 0x%02x", a.ReasonCode)
			}
			if ev := rec.disconnect(t); !errors.As(ev.Err, &refused) {
				t.Fatalf("OnDisconnect err %v", ev.Err)
			}
			fc.expectClosed() // no DISCONNECT after a refusal
			if c.IsConnected() {
				t.Fatal("IsConnected after refusal")
			}
		})
	}
}

// Protocol errors on an MQTT 5 connection are reported to the server with
// DISCONNECT 0x82 (or 0x81 for malformed packets) before closing.
func TestProtocolErrors(t *testing.T) {
	cases := []struct {
		name       string
		version    byte
		clientID   string
		script     func(t *testing.T, fc *fakeConn)
		want       error
		disconnect []byte // DISCONNECT expected from the client, nil for none
	}{
		{"duplicate connack", 5, "c", func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 5, 0, false, nil)...)
			fc.send(connackBytes(t, 5, 0, false, nil)...)
		}, ErrProtocol, []byte{0xE0, 0x01, 0x82}},
		{"session present with clean start", 5, "c", func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 5, 0, true, nil)...)
		}, ErrProtocol, []byte{0xE0, 0x01, 0x82}},
		{"assigned id when we have one", 5, "c", func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 5, 0, false, &Properties{AssignedClientID: "x"})...)
		}, ErrProtocol, []byte{0xE0, 0x01, 0x82}},
		{"packet before connack", 5, "c", func(t *testing.T, fc *fakeConn) {
			fc.send(0xD0, 0x00)
		}, ErrProtocol, []byte{0xE0, 0x01, 0x82}},
		{"invalid connack flags", 5, "c", func(t *testing.T, fc *fakeConn) {
			fc.send(0x20, 0x03, 0x02, 0x00, 0x00)
		}, ErrProtocol, []byte{0xE0, 0x01, 0x82}},
		{"malformed pingresp", 5, "c", func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 5, 0, false, nil)...)
			fc.send(0xD0, 0x01, 0x00)
		}, ErrMalformedPacket, []byte{0xE0, 0x01, 0x81}},
		{"subscribe from server", 5, "c", func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 5, 0, false, nil)...)
			fc.send(0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x00)
		}, ErrProtocol, []byte{0xE0, 0x01, 0x82}},
		{"v311 duplicate connack", 4, "c", func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 4, 0, false, nil)...)
			fc.send(connackBytes(t, 4, 0, false, nil)...)
		}, ErrProtocol, nil},
		{"v311 disconnect from server", 4, "c", func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 4, 0, false, nil)...)
			fc.send(0xE0, 0x00)
		}, ErrProtocol, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(t)
			c, rec := newTestClient(t, b, Options{ClientID: tc.clientID, CleanStart: true, ProtocolVersion: tc.version})
			connectAsync(c, 5*time.Second)
			fc := b.accept()
			fc.readConnect()
			tc.script(t, fc)
			if tc.disconnect != nil {
				fc.expect(tc.disconnect...)
			}
			fc.expectClosed()
			if ev := rec.disconnect(t); !errors.Is(ev.Err, tc.want) {
				t.Fatalf("OnDisconnect err %v, want %v", ev.Err, tc.want)
			}
		})
	}
}

func TestServerDisconnect(t *testing.T) {
	cases := []struct {
		name   string
		packet []byte
		reason string
	}{
		{"reason code only", []byte{0xE0, 0x01, 0x8B}, ""},
		{"with reason string", append([]byte{0xE0, 0x08, 0x8B, 0x06, 0x1F, 0x00, 0x03}, "bye"...), "bye"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(t)
			c, rec := newTestClient(t, b, Options{ClientID: "c", CleanStart: true, ProtocolVersion: MQTT5})
			res := connectAsync(c, 5*time.Second)
			fc := b.accept()
			fc.readConnect()
			fc.send(connackBytes(t, 5, 0, false, nil)...)
			if r := waitResult(t, res); r.err != nil {
				t.Fatal(r.err)
			}
			fc.send(tc.packet...)
			fc.expectClosed()
			ev := rec.disconnect(t)
			if !errors.Is(ev.Err, ErrServerDisconnect) || ev.ReasonCode != 0x8B {
				t.Fatalf("event %+v", ev)
			}
			if ev.Properties == nil || ev.Properties.ReasonString != tc.reason {
				t.Fatalf("properties %+v", ev.Properties)
			}
		})
	}
}

func TestKeepalive(t *testing.T) {
	b := newFakeBroker(t)
	// Server Keep Alive overrides the client's 60s so the test runs quickly.
	c, rec := newTestClient(t, b, Options{ClientID: "c", CleanStart: true, KeepAlive: 60, ProtocolVersion: MQTT5})
	res := connectAsync(c, 5*time.Second)
	fc := b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 5, 0, false, &Properties{ServerKeepAlive: 1, ServerKeepAliveFlag: true})...)
	if r := waitResult(t, res); r.err != nil {
		t.Fatal(r.err)
	}

	start := time.Now()
	fc.expect(0xC0, 0x00)
	if d := time.Since(start); d < 800*time.Millisecond || d > 2*time.Second {
		t.Fatalf("PINGREQ after %v, want about 1s", d)
	}
	fc.send(0xD0, 0x00)
	fc.expect(0xC0, 0x00)

	// Server PINGREQ is answered.
	fc.send(0xC0, 0x00)
	fc.expect(0xD0, 0x00)

	// Stop answering: the client gives up after another keepalive period.
	fc.expectClosed()
	if ev := rec.disconnect(t); !errors.Is(ev.Err, ErrKeepalive) {
		t.Fatalf("OnDisconnect err %v, want ErrKeepalive", ev.Err)
	}
}

func TestConnectContextCancelled(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newTestClient(t, b, Options{ClientID: "c", CleanStart: true})
	res := connectAsync(c, 200*time.Millisecond)
	fc := b.accept()
	fc.readConnect() // never answered
	if r := waitResult(t, res); !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("Connect err %v", r.err)
	}
	if ev := rec.disconnect(t); !errors.Is(ev.Err, context.DeadlineExceeded) {
		t.Fatalf("OnDisconnect err %v", ev.Err)
	}
	fc.expectClosed()
}

func TestConnectionLost(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newTestClient(t, b, Options{ClientID: "c", CleanStart: true})
	res := connectAsync(c, 5*time.Second)
	fc := b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 4, 0, false, nil)...)
	if r := waitResult(t, res); r.err != nil {
		t.Fatal(r.err)
	}
	fc.nc.Close()
	if ev := rec.disconnect(t); !errors.Is(ev.Err, ErrConnectionLost) {
		t.Fatalf("OnDisconnect err %v", ev.Err)
	}
}

func TestDisconnectV5Reason(t *testing.T) {
	cases := []struct {
		name   string
		reason byte
		props  *Properties
		want   []byte
	}{
		{"normal", 0, nil, []byte{0xE0, 0x00}},
		{"with will message", 0x04, nil, []byte{0xE0, 0x01, 0x04}},
		{"session expiry", 0, &Properties{SessionExpiryInterval: 10, SessionExpiryIntervalFlag: true},
			[]byte{0xE0, 0x07, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x0A}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(t)
			c, rec := newTestClient(t, b, Options{ClientID: "c", CleanStart: true, ProtocolVersion: MQTT5})
			res := connectAsync(c, 5*time.Second)
			fc := b.accept()
			fc.readConnect()
			fc.send(connackBytes(t, 5, 0, false, nil)...)
			if r := waitResult(t, res); r.err != nil {
				t.Fatal(r.err)
			}
			if err := c.Disconnect(context.Background(), tc.reason, tc.props); err != nil {
				t.Fatal(err)
			}
			fc.expect(tc.want...)
			if ev := rec.disconnect(t); ev.Err != nil {
				t.Fatal(ev.Err)
			}
		})
	}
}

func TestDisconnectFromCallback(t *testing.T) {
	b := newFakeBroker(t)
	done := make(chan DisconnectEvent, 1)
	c, err := New(Options{Server: b.url(), ClientID: "c", CleanStart: true}, Handlers{
		OnConnect:    func(c *Client, _ ConnAck) { _ = c.Disconnect(context.Background(), 0, nil) },
		OnDisconnect: func(_ *Client, ev DisconnectEvent) { done <- ev },
	})
	if err != nil {
		t.Fatal(err)
	}
	connectAsync(c, 5*time.Second)
	fc := b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 4, 0, false, nil)...)
	fc.expect(0xE0, 0x00)
	select {
	case ev := <-done:
		if ev.Err != nil {
			t.Fatal(ev.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnDisconnect not called")
	}
}

func TestPreConnectAndSetters(t *testing.T) {
	b := newFakeBroker(t)
	c, err := New(Options{Server: b.url(), ClientID: "c", CleanStart: true,
		Will: &Message{Topic: "w", Payload: []byte("x")}}, Handlers{
		OnPreConnect: func(c *Client) { _ = c.SetCredentials("late", []byte("pw")) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetWill(nil); err != nil {
		t.Fatal(err)
	}
	connectAsync(c, 5*time.Second)
	pk := b.accept().readConnect()
	if string(pk.Connect.Username) != "late" || pk.Connect.WillFlag {
		t.Fatalf("connect %+v", pk.Connect)
	}
	if err := c.SetCredentials("", []byte("pw")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("password without username on 3.1.1: %v", err)
	}
}

func TestMQTT31(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newTestClient(t, b, Options{CleanStart: true, ProtocolVersion: MQTT31})
	if id := c.ClientID(); !strings.HasPrefix(id, "mosq-") || len(id) != 23 {
		t.Fatalf("generated id %q", id)
	}
	connectAsync(c, 5*time.Second)
	pk := b.accept().readConnect()
	if string(pk.Connect.ProtocolName) != "MQIsdp" || pk.ProtocolVersion != 3 || pk.Connect.ClientIdentifier != c.ClientID() {
		t.Fatalf("connect %q v%d id %q", pk.Connect.ProtocolName, pk.ProtocolVersion, pk.Connect.ClientIdentifier)
	}
}

func TestOptionsValidation(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want error
	}{
		{"empty id without clean start", Options{Server: "mqtt://h"}, ErrInvalid},
		{"keepalive below 5", Options{Server: "mqtt://h", CleanStart: true, KeepAlive: 4}, ErrInvalid},
		{"bad protocol version", Options{Server: "mqtt://h", CleanStart: true, ProtocolVersion: 6}, ErrInvalid},
		{"password without username", Options{Server: "mqtt://h", CleanStart: true, Password: []byte("p")}, ErrInvalid},
		{"wildcard will topic", Options{Server: "mqtt://h", CleanStart: true, Will: &Message{Topic: "a/#"}}, ErrInvalid},
		{"will qos 3", Options{Server: "mqtt://h", CleanStart: true, Will: &Message{Topic: "a", QoS: 3}}, ErrInvalid},
		{"connect properties on 3.1.1", Options{Server: "mqtt://h", CleanStart: true, ConnectProperties: &Properties{}}, ErrNotSupported},
		{"will properties on 3.1.1", Options{Server: "mqtt://h", CleanStart: true, Will: &Message{Topic: "a", Properties: &Properties{}}}, ErrNotSupported},
		{"websocket scheme", Options{Server: "ws://h", CleanStart: true}, ErrInvalid},
		{"missing host", Options{Server: "mqtt://", CleanStart: true}, ErrInvalid},
		{"tls config on plain server", Options{Server: "mqtt://h", CleanStart: true, TLSConfig: tlsConfig()}, ErrInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.opts, Handlers{}); !errors.Is(err, tc.want) {
				t.Fatalf("New err %v, want %v", err, tc.want)
			}
		})
	}
	if _, err := New(Options{Server: "mqtts://h", CleanStart: true, TLSConfig: tlsConfig()}, Handlers{}); err != nil {
		t.Fatalf("valid TLS options: %v", err)
	}
}

func TestDisconnectV311RejectsReason(t *testing.T) {
	c, err := New(Options{Server: "mqtt://h", CleanStart: true}, Handlers{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Disconnect(context.Background(), 0x04, nil); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("err %v", err)
	}
}

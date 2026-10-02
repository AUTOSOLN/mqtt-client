package mqttclient

import (
	"context"
	"errors"
	"net"
	"slices"
	"testing"
	"time"
)

func runAsync(c *Client, ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- c.Run(ctx) }()
	return ch
}

func runResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// fast reconnects quickly so the tests run in milliseconds.
func fast(opts Options) Options {
	opts.ReconnectDelay = 20 * time.Millisecond
	opts.ConnectTimeout = 2 * time.Second
	return opts
}

// accepted answers a connection attempt with an accepting CONNACK.
func accepted(t *testing.T, b *fakeBroker, version byte) *fakeConn {
	t.Helper()
	fc := b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, version, 0, false, nil)...)
	return fc
}

// Run reconnects after a lost connection and resends what was in flight.
func TestRunReconnects(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, fast(Options{}))
	res := runAsync(c, context.Background())

	fc := accepted(t, b, 4)
	rec.connect(t)
	if _, err := c.Connect(context.Background()); !errors.Is(err, ErrRunning) {
		t.Fatalf("Connect during Run: %v", err)
	}
	if err := c.Run(context.Background()); !errors.Is(err, ErrRunning) {
		t.Fatalf("second Run: %v", err)
	}
	p := publishOK(t, c, msg(1))
	fc.readRaw()
	fc.nc.Close()
	if ev := rec.disconnect(t); !errors.Is(ev.Err, ErrConnectionLost) {
		t.Fatalf("OnDisconnect %v", ev.Err)
	}

	fc = accepted(t, b, 4)
	rec.connect(t)
	fc.expect(0x3A, 0x07, 0x00, 0x01, 't', 0x00, 0x01, 'h', 'i')
	fc.send(0x40, 0x02, 0x00, 0x01)
	if _, err := wait(t, p); err != nil {
		t.Fatal(err)
	}

	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	fc.expect(0xE0, 0x00)
	if err := runResult(t, res); err != nil {
		t.Fatalf("Run returned %v after Disconnect", err)
	}
}

// Run keeps trying while the server is down, and reports each failure.
func TestRunServerDownAtStart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	failures := make(chan error, 100)
	connects := make(chan ConnAck, 1)
	c, err := New(fast(Options{Server: "mqtt://" + addr, ClientID: "c", CleanStart: true}), Handlers{
		OnConnectError: func(_ *Client, err error) { failures <- err },
		OnConnect:      func(_ *Client, a ConnAck) { connects <- a },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	res := runAsync(c, ctx)
	for range 2 {
		select {
		case <-failures:
		case <-time.After(5 * time.Second):
			t.Fatal("OnConnectError not called")
		}
	}

	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("port %s taken meanwhile: %v", addr, err)
	}
	b := &fakeBroker{t: t, ln: ln}
	t.Cleanup(func() { ln.Close() })
	fc := accepted(t, b, 4)
	select {
	case <-connects:
	case <-time.After(5 * time.Second):
		t.Fatal("OnConnect not called")
	}

	// Ending ctx disconnects cleanly, so the server does not publish the Will.
	cancel()
	fc.expect(0xE0, 0x00)
	if err := runResult(t, res); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
}

func TestRunStopsOnPermanentErrors(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		script  func(t *testing.T, fc *fakeConn)
		check   func(err error) bool
	}{
		{"not authorized", 4, func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 4, 5, false, nil)...)
		}, func(err error) bool {
			var r *ConnRefusedError
			return errors.As(err, &r) && r.ReasonCode == 5
		}},
		{"v5 bad credentials", 5, func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 5, 0x86, false, nil)...)
		}, func(err error) bool {
			var r *ConnRefusedError
			return errors.As(err, &r) && r.ReasonCode == 0x86
		}},
		{"protocol error", 4, func(t *testing.T, fc *fakeConn) {
			fc.send(connackBytes(t, 4, 0, false, nil)...)
			fc.send(0xE0, 0x00) // DISCONNECT from a 3.1.1 server
		}, func(err error) bool { return errors.Is(err, ErrProtocol) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(t)
			c, _ := newPubClient(t, b, fast(Options{ProtocolVersion: tc.version}))
			res := runAsync(c, context.Background())
			fc := b.accept()
			fc.readConnect()
			tc.script(t, fc)
			if err := runResult(t, res); !tc.check(err) {
				t.Fatalf("Run returned %v", err)
			}
		})
	}
}

// Transient refusals and missing CONNACKs are retried.
func TestRunRetries(t *testing.T) {
	b := newFakeBroker(t)
	opts := fast(Options{ProtocolVersion: MQTT5})
	opts.ConnectTimeout = 200 * time.Millisecond
	c, rec := newPubClient(t, b, opts)
	res := runAsync(c, context.Background())

	fc := b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 5, 0x89, false, nil)...) // server busy
	if a := rec.connect(t); a.ReasonCode != 0x89 {
		t.Fatalf("OnConnect %+v", a)
	}
	rec.disconnect(t)

	fc = b.accept()
	fc.readConnect() // never answered: the attempt times out

	accepted(t, b, 5)
	rec.connect(t)
	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := runResult(t, res); err != nil {
		t.Fatal(err)
	}
}

// Disconnect stops Run while it waits to reconnect.
func TestRunDisconnectWhileWaiting(t *testing.T) {
	b := newFakeBroker(t)
	opts := fast(Options{})
	opts.ReconnectDelay = time.Hour
	c, rec := newPubClient(t, b, opts)
	res := runAsync(c, context.Background())
	fc := accepted(t, b, 4)
	rec.connect(t)
	fc.nc.Close()
	rec.disconnect(t)
	time.Sleep(20 * time.Millisecond) // let Run start waiting
	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatalf("Disconnect between connections: %v", err)
	}
	if err := runResult(t, res); err != nil {
		t.Fatal(err)
	}
	// Afterwards the client is idle again: Disconnect has nothing to do.
	if err := c.Disconnect(context.Background(), 0, nil); !errors.Is(err, ErrNoConn) {
		t.Fatalf("second Disconnect: %v", err)
	}
}

// Run keeps the server's Retain Available across reconnects, so a retained
// Will is sent without retain to a server that does not support it
// (mosquitto_reconnect). Connect starts from "available" again.
func TestRunKeepsRetainAvailable(t *testing.T) {
	b := newFakeBroker(t)
	c, rec := newPubClient(t, b, fast(Options{ProtocolVersion: MQTT5,
		Will: &Message{Topic: "w", Payload: []byte("x"), Retain: true}}))
	res := runAsync(c, context.Background())

	fc := b.accept()
	if !fc.readConnect().Connect.WillRetain {
		t.Fatal("first CONNECT without will retain")
	}
	fc.send(connackBytes(t, 5, 0, false, &Properties{RetainAvailable: 0, RetainAvailableFlag: true})...)
	rec.connect(t)
	fc.nc.Close()
	rec.disconnect(t)

	fc = b.accept()
	if fc.readConnect().Connect.WillRetain {
		t.Fatal("reconnect CONNECT kept will retain")
	}
	fc.send(connackBytes(t, 5, 0, false, nil)...)
	rec.connect(t)
	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := runResult(t, res); err != nil {
		t.Fatal(err)
	}

	connectAsync(c, 5*time.Second)
	if !b.accept().readConnect().Connect.WillRetain {
		t.Fatal("Connect did not reset retain available")
	}
}

func TestReconnectDelay(t *testing.T) {
	s := time.Second
	cases := []struct {
		name string
		opts Options
		want []time.Duration
	}{
		{"default fixed", Options{}, []time.Duration{s, s, s, s}},
		{"linear", Options{ReconnectDelay: s, ReconnectDelayMax: 3 * s}, []time.Duration{s, 2 * s, 3 * s, 3 * s, 3 * s}},
		{"exponential", Options{ReconnectDelay: s, ReconnectDelayMax: 10 * s, ReconnectExponential: true},
			[]time.Duration{s, 4 * s, 9 * s, 10 * s, 10 * s}},
	}
	for _, tc := range cases {
		opts := tc.opts
		opts.Server, opts.CleanStart = "mqtt://h", true
		c, err := New(opts, Handlers{})
		if err != nil {
			t.Fatal(err)
		}
		var got []time.Duration
		n := 0
		for range tc.want {
			var d time.Duration
			d, n = c.reconnectDelay(n)
			got = append(got, d)
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: delays %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Disconnect during a connection attempt (CONNECT sent, no CONNACK yet)
// stops Run without an error.
func TestRunDisconnectDuringAttempt(t *testing.T) {
	b := newFakeBroker(t)
	c, _ := newPubClient(t, b, fast(Options{}))
	res := runAsync(c, context.Background())
	fc := b.accept()
	fc.readConnect()
	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := runResult(t, res); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if _, err := b.tryAccept(300 * time.Millisecond); err == nil {
		t.Fatal("Run connected again after Disconnect")
	}
}

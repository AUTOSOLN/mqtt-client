package mqttclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// newServersClient creates a client for several servers. Its buffers are
// large enough for the OnConnect and OnDisconnect calls of many refused
// attempts.
func newServersClient(t *testing.T, opts Options, h Handlers) (*Client, *pubRecorder) {
	t.Helper()
	rec := &pubRecorder{
		recorder:  &recorder{connects: make(chan ConnAck, 100), disconnects: make(chan DisconnectEvent, 100)},
		messages:  make(chan *Message, 100),
		published: make(chan publishEvent, 100),
	}
	h.OnConnect = func(_ *Client, a ConnAck) { rec.connects <- a }
	h.OnDisconnect = func(_ *Client, e DisconnectEvent) { rec.disconnects <- e }
	h.OnPublish = func(_ *Client, mid uint16, reason byte, _ *Properties) { rec.published <- publishEvent{mid, reason} }
	c, err := New(opts, h)
	if err != nil {
		t.Fatal(err)
	}
	return c, rec
}

// acceptedConnect waits for an OnConnect that accepted the connection,
// skipping refusals.
func acceptedConnect(t *testing.T, rec *pubRecorder) ConnAck {
	t.Helper()
	for {
		if a := rec.connect(t); a.ReasonCode == 0 {
			return a
		}
	}
}

// expectConnect reads a CONNECT and checks its version and client id.
func expectConnect(t *testing.T, fc *fakeConn, version byte, clientID string) {
	t.Helper()
	pk := fc.readConnect()
	if pk.ProtocolVersion != version || pk.Connect.ClientIdentifier != clientID {
		t.Fatalf("CONNECT version %d client id %q, want %d %q", pk.ProtocolVersion, pk.Connect.ClientIdentifier, version, clientID)
	}
}

// Run moves to the next server after a failed attempt, cycles back to the
// first, retries the same server first after an accepted connection is
// lost, connects to each with its own settings, and resends in-flight
// messages to whichever server it reaches.
func TestRunCyclesServers(t *testing.T) {
	a, b := newFakeBroker(t), newFakeBroker(t)
	sa := fast(Server{Address: a.address(), ClientID: "a", ProtocolVersion: MQTT311})
	sa.ConnectTimeout = 200 * time.Millisecond
	sb := fast(Server{Address: b.address(), ClientID: "b", ProtocolVersion: MQTT5})
	c, rec := newServersClient(t, Options{Servers: []Server{sa, sb}}, Handlers{})
	res := runAsync(c, context.Background())

	fc := a.accept()
	expectConnect(t, fc, MQTT311, "a") // never answered: the attempt times out
	fc = b.accept()
	expectConnect(t, fc, MQTT5, "b")
	fc.send(connackBytes(t, 5, 0x88, false, nil)...) // server unavailable

	fc = a.accept()
	expectConnect(t, fc, MQTT311, "a")
	fc.send(connackBytes(t, 4, 0, false, nil)...)
	acceptedConnect(t, rec)
	if i := c.ServerIndex(); i != 0 {
		t.Fatalf("ServerIndex %d, want 0", i)
	}
	p := publishOK(t, c, msg(1))
	fc.readRaw()
	fc.nc.Close()

	fc = a.accept() // the server that was working is tried again first
	expectConnect(t, fc, MQTT311, "a")
	fc.send(connackBytes(t, 4, 3, false, nil)...) // server unavailable

	fc = b.accept()
	expectConnect(t, fc, MQTT5, "b")
	fc.send(connackBytes(t, 5, 0, false, nil)...)
	acceptedConnect(t, rec)
	if i := c.ServerIndex(); i != 1 {
		t.Fatalf("ServerIndex %d, want 1", i)
	}
	// Resent with DUP, encoded for MQTT 5.
	fc.expect(0x3A, 0x08, 0x00, 0x01, 't', 0x00, 0x01, 0x00, 'h', 'i')
	fc.send(0x40, 0x02, 0x00, 0x01)
	if _, err := wait(t, p); err != nil {
		t.Fatal(err)
	}

	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	if raw := fc.readRaw(); raw[0] != 0xE0 {
		t.Fatalf("got % x, want DISCONNECT", raw)
	}
	if err := runResult(t, res); err != nil {
		t.Fatalf("Run returned %v after Disconnect", err)
	}
}

// A server that fails permanently is not tried again; Run returns once
// every server has.
func TestRunDropsPermanentlyFailedServers(t *testing.T) {
	a, b := newFakeBroker(t), newFakeBroker(t)
	sa := fast(Server{Address: a.address(), ClientID: "a"})
	sb := fast(Server{Address: b.address(), ClientID: "b"})
	c, rec := newServersClient(t, Options{Servers: []Server{sa, sb}}, Handlers{})
	res := runAsync(c, context.Background())

	fc := a.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 4, 4, false, nil)...) // bad username or password

	fc = b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 4, 0, false, nil)...)
	acceptedConnect(t, rec)
	fc.nc.Close()

	fc = b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 4, 3, false, nil)...) // server unavailable: b again, not a

	fc = b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 4, 5, false, nil)...) // not authorized

	err := runResult(t, res)
	var refused *ConnRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("Run returned %v, want a *ConnRefusedError", err)
	}
	for _, addr := range []string{a.address(), b.address()} {
		if !strings.Contains(err.Error(), addr) {
			t.Errorf("Run error %q does not name %s", err, addr)
		}
	}
	if _, err := a.tryAccept(100 * time.Millisecond); err == nil {
		t.Fatal("server that refused the credentials was tried again")
	}
}

// Connect tries each server once from the current one, and OnPreConnect can
// tell which server is about to be tried.
func TestConnectTriesEachServer(t *testing.T) {
	a, b := newFakeBroker(t), newFakeBroker(t)
	opts := Options{Servers: []Server{
		{Address: a.address(), ClientID: "a"},
		{Address: b.address(), ClientID: "b", ProtocolVersion: MQTT5},
	}}
	c, rec := newServersClient(t, opts, Handlers{
		OnPreConnect: func(c *Client) {
			_ = c.SetCredentials(fmt.Sprintf("user%d", c.ServerIndex()), nil)
		},
	})
	expectUser := func(fc *fakeConn, want string) {
		t.Helper()
		if pk := fc.readConnect(); pk.Connect.Username == nil || string(pk.Connect.Username) != want {
			t.Fatalf("CONNECT username %q, want %q", pk.Connect.Username, want)
		}
	}

	res := connectAsync(c, 5*time.Second)
	fc := a.accept()
	expectUser(fc, "user0")
	fc.send(connackBytes(t, 4, 3, false, nil)...)
	fc = b.accept()
	expectUser(fc, "user1")
	fc.send(connackBytes(t, 5, 0, false, nil)...)
	if r := <-res; r.err != nil {
		t.Fatalf("Connect: %v", r.err)
	}
	acceptedConnect(t, rec)
	if i := c.ServerIndex(); i != 1 {
		t.Fatalf("ServerIndex %d, want 1", i)
	}
	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	fc.readRaw()

	// Starts from the current server; when both refuse, both errors are
	// returned and the server after the last one tried becomes current.
	res = connectAsync(c, 5*time.Second)
	fc = b.accept()
	expectUser(fc, "user1")
	fc.send(connackBytes(t, 5, 0x87, false, nil)...) // not authorized
	fc = a.accept()
	expectUser(fc, "user0")
	fc.send(connackBytes(t, 4, 5, false, nil)...) // not authorized
	r := <-res
	var refused *ConnRefusedError
	if !errors.As(r.err, &refused) || !strings.Contains(r.err.Error(), "server 0") || !strings.Contains(r.err.Error(), "server 1") {
		t.Fatalf("Connect err %v", r.err)
	}
	if i := c.ServerIndex(); i != 1 {
		t.Fatalf("ServerIndex %d, want 1", i)
	}
}

func TestServersValidation(t *testing.T) {
	if _, err := New(Options{}, Handlers{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("no servers: %v", err)
	}
	_, err := New(Options{Servers: []Server{
		{Address: "h:1883", CleanStart: true},
		{Address: "h:1884", CleanStart: true, KeepAlive: 4},
	}}, Handlers{})
	if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "server 1 (h:1884)") {
		t.Fatalf("invalid second server: %v", err)
	}
}

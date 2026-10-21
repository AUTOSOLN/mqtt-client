package mqttclient

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// recordingDialer dials TCP and keeps what it was asked for and what it
// returned.
type recordingDialer struct {
	mu      sync.Mutex
	network string
	address string
	nc      net.Conn
}

func (d *recordingDialer) dial(ctx context.Context, network, address string) (net.Conn, error) {
	var nd net.Dialer
	nc, err := nd.DialContext(ctx, network, address)
	d.mu.Lock()
	d.network, d.address, d.nc = network, address, nc
	d.mu.Unlock()
	return nc, err
}

func (d *recordingDialer) conn() net.Conn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nc
}

func TestDial(t *testing.T) {
	b := newFakeBroker(t)
	d := &recordingDialer{}
	lost := make(chan DisconnectEvent, 1)

	c, err := New(single(Server{Address: b.address(), ClientID: "c", CleanStart: true, Dial: d.dial}), Handlers{
		OnDisconnect: func(_ *Client, ev DisconnectEvent) { lost <- ev },
	})
	if err != nil {
		t.Fatal(err)
	}
	res := connectAsync(c, 5*time.Second)
	fc := b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 4, 0, false, nil)...)
	if r := waitResult(t, res); r.err != nil {
		t.Fatal(r.err)
	}
	if d.network != "tcp" || d.address != b.address() {
		t.Fatalf("Dial called with %q %q, want tcp %q", d.network, d.address, b.address())
	}

	// Closing the dialled connection drops the link without DISCONNECT.
	d.conn().Close()
	fc.expectClosed()
	select {
	case ev := <-lost:
		if !errors.Is(ev.Err, ErrConnectionLost) {
			t.Fatalf("OnDisconnect err %v, want ErrConnectionLost", ev.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnDisconnect not called")
	}
}

func TestDialError(t *testing.T) {
	errDial := errors.New("dial refused")
	c, err := New(single(Server{
		Address: "127.0.0.1:1", ClientID: "c", CleanStart: true,
		Dial: func(context.Context, string, string) (net.Conn, error) { return nil, errDial },
	}), Handlers{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Connect(t.Context()); !errors.Is(err, errDial) {
		t.Fatalf("Connect err %v, want %v", err, errDial)
	}
}

func TestDialTLS(t *testing.T) {
	cert, pool := selfSigned(t)
	b := newFakeTLSBroker(t, cert)
	d := &recordingDialer{}

	c, err := New(single(Server{
		Address: b.address(), TLS: true, ClientID: "c", CleanStart: true,
		TLSConfig: &tls.Config{RootCAs: pool}, Dial: d.dial,
	}), Handlers{})
	if err != nil {
		t.Fatal(err)
	}
	res := connectAsync(c, 5*time.Second)
	fc := b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 4, 0, false, nil)...)
	if r := waitResult(t, res); r.err != nil {
		t.Fatal(r.err)
	}
	if _, ok := d.conn().(*net.TCPConn); !ok {
		t.Fatalf("Dial returned %T, want the plain TCP connection", d.conn())
	}
	if err := c.Disconnect(t.Context(), 0, nil); err != nil {
		t.Fatal(err)
	}
	fc.expect(0xE0, 0x00)
}

package mqttclient

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// fakeBroker is a scripted broker for tests: each test accepts the client's
// connection and reads and writes raw packets.
type fakeBroker struct {
	t  *testing.T
	ln net.Listener
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return &fakeBroker{t: t, ln: ln}
}

func (b *fakeBroker) url() string { return "mqtt://" + b.ln.Addr().String() }

type fakeConn struct {
	t  *testing.T
	nc net.Conn
	br *bufio.Reader
}

// accept waits for the client to connect.
func (b *fakeBroker) accept() *fakeConn {
	b.t.Helper()
	type res struct {
		nc  net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		nc, err := b.ln.Accept()
		ch <- res{nc, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			b.t.Fatal(r.err)
		}
		b.t.Cleanup(func() { r.nc.Close() })
		return &fakeConn{t: b.t, nc: r.nc, br: bufio.NewReader(r.nc)}
	case <-time.After(5 * time.Second):
		b.t.Fatal("client did not connect")
		return nil
	}
}

// tryAccept waits up to d for a connection.
func (b *fakeBroker) tryAccept(d time.Duration) (net.Conn, error) {
	_ = b.ln.(*net.TCPListener).SetDeadline(time.Now().Add(d))
	defer b.ln.(*net.TCPListener).SetDeadline(time.Time{})
	nc, err := b.ln.Accept()
	if err == nil {
		b.t.Cleanup(func() { nc.Close() })
	}
	return nc, err
}

// readRaw reads one packet and returns its bytes, or fails the test.
func (f *fakeConn) readRaw() []byte {
	f.t.Helper()
	b, err := f.tryReadRaw(5 * time.Second)
	if err != nil {
		f.t.Fatalf("reading packet: %v", err)
	}
	return b
}

func (f *fakeConn) tryReadRaw(timeout time.Duration) ([]byte, error) {
	_ = f.nc.SetReadDeadline(time.Now().Add(timeout))
	hb, err := f.br.ReadByte()
	if err != nil {
		return nil, err
	}
	var hdr bytes.Buffer
	hdr.WriteByte(hb)
	tee := io.TeeReader(f.br, &hdr)
	n, _, err := packets.DecodeLength(byteReader{tee})
	if err != nil {
		return nil, err
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(f.br, body); err != nil {
		return nil, err
	}
	return append(hdr.Bytes(), body...), nil
}

// readConnect reads and decodes a CONNECT.
func (f *fakeConn) readConnect() packets.Packet {
	f.t.Helper()
	raw := f.readRaw()
	var pk packets.Packet
	if err := pk.FixedHeader.Decode(raw[0]); err != nil || pk.FixedHeader.Type != packets.Connect {
		f.t.Fatalf("expected CONNECT, got % x", raw)
	}
	body := raw[1+varintLen(raw[1:]):]
	pk.FixedHeader.Remaining = len(body)
	if err := pk.ConnectDecode(body); err != nil {
		f.t.Fatalf("decoding CONNECT: %v", err)
	}
	return pk
}

// expect reads one packet and checks it is exactly want.
func (f *fakeConn) expect(want ...byte) {
	f.t.Helper()
	if got := f.readRaw(); !bytes.Equal(got, want) {
		f.t.Fatalf("got packet % x, want % x", got, want)
	}
}

// expectClosed checks the client closes the connection without sending more.
func (f *fakeConn) expectClosed() {
	f.t.Helper()
	if b, err := f.tryReadRaw(5 * time.Second); err == nil {
		f.t.Fatalf("expected close, got packet % x", b)
	}
}

func (f *fakeConn) send(b ...byte) {
	f.t.Helper()
	if _, err := f.nc.Write(b); err != nil {
		f.t.Fatal(err)
	}
}

// connack encodes a CONNACK as a server would.
func connackBytes(t *testing.T, version, code byte, sessionPresent bool, props *Properties) []byte {
	t.Helper()
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connack},
		ProtocolVersion: version,
		ReasonCode:      code,
		SessionPresent:  sessionPresent,
	}
	if props != nil {
		pk.Properties = *props
	}
	var buf bytes.Buffer
	if err := pk.ConnackEncode(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type byteReader struct{ r io.Reader }

func (b byteReader) ReadByte() (byte, error) {
	var x [1]byte
	_, err := io.ReadFull(b.r, x[:])
	return x[0], err
}

func varintLen(b []byte) int {
	for i, c := range b {
		if c&0x80 == 0 {
			return i + 1
		}
	}
	return len(b)
}

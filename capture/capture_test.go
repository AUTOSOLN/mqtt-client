package capture_test

import (
	"bytes"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/capture/capturetest"
)

var (
	t0     = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	client = netip.MustParseAddrPort("10.0.0.1:50000")
	server = netip.MustParseAddrPort("10.0.0.2:1883")
)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

// stream joins the bytes a connection carried in one direction.
func stream(c capture.Conn, d capture.Dir) []byte {
	var b []byte
	for _, s := range c.Segments {
		if s.Dir == d {
			b = append(b, s.Data...)
		}
	}
	return b
}

// readOne reads a capture that must hold exactly one connection.
func readOne(t *testing.T, file []byte) capture.Conn {
	t.Helper()
	c, err := capture.Read(bytes.NewReader(file))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(c.Conns) != 1 {
		t.Fatalf("got %d connections, want 1", len(c.Conns))
	}
	return c.Conns[0]
}

// handshake starts a Writer with a three-way handshake from client to
// server: the client's next sequence number is 1001, the server's 5001.
func handshake(t *testing.T, buf *bytes.Buffer) *capturetest.Writer {
	t.Helper()
	w, err := capturetest.NewWriter(buf)
	if err != nil {
		t.Fatal(err)
	}
	must(t, w.TCP(at(0), client, server, 1000, 0, capturetest.SYN, nil))
	must(t, w.TCP(at(0), server, client, 5000, 1001, capturetest.SYN|capturetest.ACK, nil))
	must(t, w.TCP(at(0), client, server, 1001, 5001, capturetest.ACK, nil))
	return w
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRead_WrittenConn_ReassemblesBothDirections(t *testing.T) {
	up := bytes.Repeat([]byte("0123456789"), 300) // 3000 bytes: three segments
	down := []byte("hello")
	cc, sc := at(5), at(6)
	var buf bytes.Buffer
	must(t, capturetest.Write(&buf, []capture.Conn{{
		Client: client, Server: server,
		Segments: []capture.Segment{
			{Dir: capture.FromClient, Time: at(1), Data: up},
			{Dir: capture.FromServer, Time: at(2), Data: down},
		},
		ClientClose: &cc, ServerClose: &sc,
	}}))

	c := readOne(t, buf.Bytes())
	if c.Client != client || c.Server != server {
		t.Errorf("got client %v server %v, want %v %v", c.Client, c.Server, client, server)
	}
	if c.Err != nil || c.Transport != capture.Plain {
		t.Errorf("got Err %v Transport %v, want nil, Plain", c.Err, c.Transport)
	}
	if got := stream(c, capture.FromClient); !bytes.Equal(got, up) {
		t.Errorf("client stream: got %d bytes, want the %d written", len(got), len(up))
	}
	if got := stream(c, capture.FromServer); !bytes.Equal(got, down) {
		t.Errorf("server stream: got %q, want %q", got, down)
	}
	if c.ClientClose == nil || !c.ClientClose.Equal(cc) || c.ServerClose == nil || !c.ServerClose.Equal(sc) {
		t.Errorf("got closes %v %v, want %v %v", c.ClientClose, c.ServerClose, cc, sc)
	}
	if got := c.Segments[1].Time; !got.Equal(at(1)) {
		t.Errorf("second segment time: got %v, want %v", got, at(1))
	}
}

func TestRead_Retransmissions_KeepEachByteOnce(t *testing.T) {
	var buf bytes.Buffer
	w := handshake(t, &buf)
	must(t, w.TCP(at(1), client, server, 1001, 5001, capturetest.ACK, []byte("abc")))
	must(t, w.TCP(at(2), client, server, 1001, 5001, capturetest.ACK, []byte("abc")))   // duplicate
	must(t, w.TCP(at(3), client, server, 1002, 5001, capturetest.ACK, []byte("bcdef"))) // overlap with new bytes

	c := readOne(t, buf.Bytes())
	if got := stream(c, capture.FromClient); string(got) != "abcdef" || c.Err != nil {
		t.Errorf("got %q, Err %v; want \"abcdef\", nil", got, c.Err)
	}
}

func TestRead_MissingSegment_EndsStreamsWithErrGap(t *testing.T) {
	var buf bytes.Buffer
	w := handshake(t, &buf)
	must(t, w.TCP(at(1), client, server, 1001, 5001, capturetest.ACK, []byte("abc")))
	must(t, w.TCP(at(2), client, server, 1010, 5001, capturetest.ACK, []byte("xyz"))) // 1004..1009 missing
	must(t, w.TCP(at(3), client, server, 1013, 5001, capturetest.ACK, []byte("after")))
	must(t, w.TCP(at(4), server, client, 5001, 1018, capturetest.ACK, []byte("reply")))

	c := readOne(t, buf.Bytes())
	if !errors.Is(c.Err, capture.ErrGap) {
		t.Errorf("got Err %v, want ErrGap", c.Err)
	}
	if got := stream(c, capture.FromClient); string(got) != "abc" {
		t.Errorf("client stream: got %q, want only the bytes before the gap, \"abc\"", got)
	}
	if got := stream(c, capture.FromServer); len(got) != 0 {
		t.Errorf("server stream: got %q, want nothing after the gap", got)
	}
}

func TestRead_SnapLengthCutsPayload_ErrTruncated(t *testing.T) {
	var buf bytes.Buffer
	w := handshake(t, &buf)
	w.SnapLen = 60 // 54 bytes of headers, then 6 of the 100-byte payload
	must(t, w.TCP(at(1), client, server, 1001, 5001, capturetest.ACK, bytes.Repeat([]byte{'x'}, 100)))

	c := readOne(t, buf.Bytes())
	if !errors.Is(c.Err, capture.ErrTruncated) {
		t.Errorf("got Err %v, want ErrTruncated", c.Err)
	}
	if got := stream(c, capture.FromClient); len(got) != 0 {
		t.Errorf("got %d bytes, want none from a truncated frame", len(got))
	}
}

func TestRead_TLSClientHello_MarkedTLS(t *testing.T) {
	var buf bytes.Buffer
	w := handshake(t, &buf)
	must(t, w.TCP(at(1), client, server, 1001, 5001, capturetest.ACK, []byte{0x16, 0x03, 0x01, 0x00, 0x05, 1, 2, 3, 4, 5}))

	if c := readOne(t, buf.Bytes()); c.Transport != capture.TLS {
		t.Errorf("got Transport %v, want TLS", c.Transport)
	}
}

func TestRead_NoHandshake_ClientIsTheSideSendingCONNECT(t *testing.T) {
	srv := netip.MustParseAddrPort("10.0.0.2:9999") // not a well-known port
	var buf bytes.Buffer
	w, err := capturetest.NewWriter(&buf)
	must(t, err)
	// The server's packet is captured first, so it is first seen.
	must(t, w.TCP(at(1), srv, client, 7000, 3000, capturetest.ACK, []byte{0x20, 0x02, 0, 0}))
	must(t, w.TCP(at(2), client, srv, 3000, 7004, capturetest.ACK, []byte{0x10, 0x00}))

	c := readOne(t, buf.Bytes())
	if c.Client != client || c.Server != srv || c.Err != nil {
		t.Errorf("got client %v server %v Err %v, want %v %v nil", c.Client, c.Server, c.Err, client, srv)
	}
}

func TestRead_SamePortsAfterClose_TwoConnections(t *testing.T) {
	cl := at(3)
	conn := capture.Conn{Client: client, Server: server, ClientClose: &cl,
		Segments: []capture.Segment{{Dir: capture.FromClient, Time: at(1), Data: []byte("one")}}}
	second := conn
	cl2 := at(13)
	second.ClientClose = &cl2
	second.Segments = []capture.Segment{{Dir: capture.FromClient, Time: at(11), Data: []byte("two")}}
	var buf bytes.Buffer
	must(t, capturetest.Write(&buf, []capture.Conn{conn, second}))

	c, err := capture.Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Conns) != 2 {
		t.Fatalf("got %d connections, want 2", len(c.Conns))
	}
	for i, want := range []string{"one", "two"} {
		if got := stream(c.Conns[i], capture.FromClient); string(got) != want {
			t.Errorf("connection %d: got %q, want %q", i, got, want)
		}
	}
}

func TestRead_UnreadableFiles_ErrFormat(t *testing.T) {
	var good bytes.Buffer
	w := handshake(t, &good)
	must(t, w.TCP(at(1), client, server, 1001, 5001, capturetest.ACK, []byte("abc")))
	oversize := bytes.Clone(good.Bytes()[:24+16])
	oversize[24+8] = 0xff // included length of the first record: far over the limit
	oversize[24+9] = 0xff
	oversize[24+10] = 0x0f

	tests := []struct {
		name string
		file []byte
	}{
		{name: "empty", file: nil},
		{name: "not a capture", file: []byte("GET / HTTP/1.1\r\nHost: example\r\n\r\n")},
		{name: "pcapng", file: append([]byte{0x0a, 0x0d, 0x0d, 0x0a}, make([]byte, 20)...)},
		{name: "gzip", file: append([]byte{0x1f, 0x8b, 8, 0}, make([]byte, 20)...)},
		{name: "ends mid-record", file: good.Bytes()[:good.Len()-2]},
		{name: "record over the limit", file: oversize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := capture.Read(bytes.NewReader(tt.file)); !errors.Is(err, capture.ErrFormat) {
				t.Errorf("got %v, want ErrFormat", err)
			}
		})
	}
}

func FuzzRead(f *testing.F) {
	var buf bytes.Buffer
	cl := at(3)
	if err := capturetest.Write(&buf, []capture.Conn{{Client: client, Server: server, ClientClose: &cl,
		Segments: []capture.Segment{
			{Dir: capture.FromClient, Time: at(1), Data: []byte{0x10, 0x0c, 0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 60, 0, 0}},
			{Dir: capture.FromServer, Time: at(2), Data: []byte{0x20, 2, 0, 0}},
		}}}); err != nil {
		f.Fatal(err)
	}
	f.Add(buf.Bytes())
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := capture.Read(bytes.NewReader(b))
		if err == nil && c == nil {
			t.Fatal("nil Capture without an error")
		}
	})
}

package capture_test

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/capture/capturetest"
)

var (
	client6 = netip.MustParseAddrPort("[fd00::1]:50000")
	server6 = netip.MustParseAddrPort("[fd00::2]:1883")
)

// exchange is a small two-way connection whose streams the tests check.
func exchange(c, s netip.AddrPort, ms int) capture.Conn {
	cl := at(ms + 3)
	return capture.Conn{Client: c, Server: s, ClientClose: &cl, Segments: []capture.Segment{
		{Dir: capture.FromClient, Time: at(ms + 1), Data: []byte("ping")},
		{Dir: capture.FromServer, Time: at(ms + 2), Data: []byte("pong")},
	}}
}

// tcpFrames returns a TCPFunc that builds IP packets for the addresses'
// family, wraps them with wrap and passes the frames to write.
func tcpFrames(wrap func([]byte) []byte, write func(time.Time, []byte) error) capturetest.TCPFunc {
	return func(t time.Time, src, dst netip.AddrPort, seq, ack uint32, flags byte, payload []byte) error {
		build := capturetest.IPv4TCP
		if src.Addr().Is6() {
			build = capturetest.IPv6TCP
		}
		ip, err := build(src, dst, seq, ack, flags, payload)
		if err != nil {
			return err
		}
		return write(t, wrap(ip))
	}
}

// checkExchange checks that c is exchange(want.Client, want.Server, ...).
func checkExchange(t *testing.T, c capture.Conn, want capture.Conn) {
	t.Helper()
	if c.Client != want.Client || c.Server != want.Server || c.Err != nil {
		t.Errorf("got client %v server %v Err %v, want %v %v nil", c.Client, c.Server, c.Err, want.Client, want.Server)
	}
	if up, down := stream(c, capture.FromClient), stream(c, capture.FromServer); string(up) != "ping" || string(down) != "pong" {
		t.Errorf("got streams %q and %q, want \"ping\" and \"pong\"", up, down)
	}
}

func noWrap(ip []byte) []byte { return ip }

func TestRead_PcapLinkTypes_SameStreams(t *testing.T) {
	vlan := func(ip []byte) []byte { return capturetest.EthernetVLAN(7, ip) }
	nullLE := func(ip []byte) []byte { return capturetest.Null(binary.LittleEndian, ip) }
	nullBE := func(ip []byte) []byte { return capturetest.Null(binary.BigEndian, ip) }
	loop := func(ip []byte) []byte {
		af := uint32(2)
		if ip[0]>>4 == 6 {
			af = 24
		}
		return append(binary.BigEndian.AppendUint32(nil, af), ip...)
	}
	tests := []struct {
		name     string
		link     uint32
		wrap     func([]byte) []byte
		ipv6     bool
		big, ns  bool
		wantTime time.Time
	}{
		{name: "Ethernet IPv4", link: capturetest.LinkEthernet, wrap: capturetest.Ethernet},
		{name: "Ethernet VLAN", link: capturetest.LinkEthernet, wrap: vlan},
		{name: "Ethernet IPv6", link: capturetest.LinkEthernet, wrap: capturetest.Ethernet, ipv6: true},
		{name: "Linux cooked v1", link: capturetest.LinkLinuxSLL, wrap: capturetest.LinuxSLL},
		{name: "Linux cooked v2 IPv6", link: capturetest.LinkLinuxSLL2, wrap: capturetest.LinuxSLL2, ipv6: true},
		{name: "raw IPv4", link: capturetest.LinkRaw, wrap: noWrap},
		{name: "raw IPv6", link: capturetest.LinkRaw, wrap: noWrap, ipv6: true},
		{name: "null, little-endian host", link: capturetest.LinkNull, wrap: nullLE},
		{name: "null, big-endian host, IPv6", link: capturetest.LinkNull, wrap: nullBE, ipv6: true},
		{name: "OpenBSD loop", link: capturetest.LinkLoop, wrap: loop},
		{name: "big-endian file, nanoseconds", link: capturetest.LinkEthernet, wrap: capturetest.Ethernet, big: true, ns: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bo binary.ByteOrder = binary.LittleEndian
			if tt.big {
				bo = binary.BigEndian
			}
			var buf bytes.Buffer
			w, err := capturetest.NewWriterLink(&buf, tt.link, bo, tt.ns)
			must(t, err)
			conn := exchange(client, server, 0)
			if tt.ipv6 {
				conn = exchange(client6, server6, 0)
			}
			if tt.ns {
				conn.Segments[0].Time = conn.Segments[0].Time.Add(123) // nanoseconds survive
			}
			must(t, capturetest.WriteConn(tcpFrames(tt.wrap, w.Frame), conn))

			c := readOne(t, buf.Bytes())
			checkExchange(t, c, conn)
			if tt.ns && !c.Segments[0].Time.Equal(conn.Segments[0].Time) {
				t.Errorf("got time %v, want %v", c.Segments[0].Time, conn.Segments[0].Time)
			}
		})
	}
}

func TestRead_Pcapng_SectionsInterfacesAndPacketBlocks(t *testing.T) {
	var buf bytes.Buffer
	p, err := capturetest.NewPcapngWriter(&buf, binary.LittleEndian)
	must(t, err)
	eth, err := p.Interface(capturetest.LinkEthernet, 0) // microseconds
	must(t, err)
	sll2, err := p.Interface(capturetest.LinkLinuxSLL2, 9) // nanoseconds
	must(t, err)
	a := exchange(client, server, 0)
	b := exchange(netip.MustParseAddrPort("10.0.0.3:40000"), server, 10)
	b.Segments[0].Time = b.Segments[0].Time.Add(321)
	must(t, capturetest.WriteConn(tcpFrames(capturetest.Ethernet, func(t time.Time, f []byte) error {
		return p.Packet(eth, t, f, 0)
	}), a))
	must(t, p.Block(4, make([]byte, 4)))      // a name resolution block, skipped
	must(t, p.Block(0x0bad, make([]byte, 8))) // an unknown block, skipped
	must(t, capturetest.WriteConn(tcpFrames(capturetest.LinuxSLL2, func(t time.Time, f []byte) error {
		return p.Packet(sll2, t, f, 0)
	}), b))

	// A second section, big-endian, with one raw interface in units of
	// 2^-20 s, written with the obsolete Packet Block.
	must(t, p.Section(binary.BigEndian))
	raw, err := p.Interface(capturetest.LinkRaw, 0x80|20)
	must(t, err)
	c := exchange(client6, server6, 20)
	must(t, capturetest.WriteConn(tcpFrames(noWrap, func(t time.Time, f []byte) error {
		return p.ObsoletePacket(raw, t, f)
	}), c))

	got, err := capture.Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Conns) != 3 {
		t.Fatalf("got %d connections, want 3", len(got.Conns))
	}
	for i, want := range []capture.Conn{a, b, c} {
		checkExchange(t, got.Conns[i], want)
	}
	if g, w := got.Conns[0].Segments[0].Time, a.Segments[0].Time; !g.Equal(w) {
		t.Errorf("microsecond interface: got time %v, want %v", g, w)
	}
	if g, w := got.Conns[1].Segments[0].Time, b.Segments[0].Time; !g.Equal(w) {
		t.Errorf("nanosecond interface: got time %v, want %v", g, w)
	}
	if d := got.Conns[2].Segments[0].Time.Sub(c.Segments[0].Time).Abs(); d > time.Microsecond {
		t.Errorf("2^-20 s interface: got time %v, want %v (within 1µs)", got.Conns[2].Segments[0].Time, c.Segments[0].Time)
	}
}

func TestRead_PcapngSimplePackets_StreamsWithoutTimes(t *testing.T) {
	var buf bytes.Buffer
	p, err := capturetest.NewPcapngWriter(&buf, binary.LittleEndian)
	must(t, err)
	_, err = p.Interface(capturetest.LinkEthernet, 0)
	must(t, err)
	conn := exchange(client, server, 0)
	must(t, capturetest.WriteConn(tcpFrames(capturetest.Ethernet, func(_ time.Time, f []byte) error {
		return p.SimplePacket(f)
	}), conn))

	c := readOne(t, buf.Bytes())
	checkExchange(t, c, conn)
	if !c.Segments[0].Time.IsZero() {
		t.Errorf("got time %v, want zero: simple packets have no timestamp", c.Segments[0].Time)
	}
}

func TestRead_PcapngUnsupportedLinkType_PacketsIgnored(t *testing.T) {
	var buf bytes.Buffer
	p, err := capturetest.NewPcapngWriter(&buf, binary.LittleEndian)
	must(t, err)
	other, err := p.Interface(147, 0) // a user-defined link type
	must(t, err)
	eth, err := p.Interface(capturetest.LinkEthernet, 0)
	must(t, err)
	must(t, p.Packet(other, at(0), []byte("not a frame we read"), 0))
	conn := exchange(client, server, 0)
	must(t, capturetest.WriteConn(tcpFrames(capturetest.Ethernet, func(t time.Time, f []byte) error {
		return p.Packet(eth, t, f, 0)
	}), conn))

	got, err := capture.Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Conns) != 1 || got.Ignored != 1 {
		t.Fatalf("got %d connections, %d ignored; want 1 and 1", len(got.Conns), got.Ignored)
	}
	checkExchange(t, got.Conns[0], conn)
}

func TestRead_Gzip_SameAsUncompressed(t *testing.T) {
	var plain bytes.Buffer
	conn := exchange(client, server, 0)
	must(t, capturetest.Write(&plain, []capture.Conn{conn}))
	var zipped bytes.Buffer
	zw := gzip.NewWriter(&zipped)
	_, err := zw.Write(plain.Bytes())
	must(t, err)
	must(t, zw.Close())

	checkExchange(t, readOne(t, zipped.Bytes()), conn)
}

func TestRead_BadPcapng_ErrFormat(t *testing.T) {
	good := func(t *testing.T) (*bytes.Buffer, *capturetest.PcapngWriter) {
		var buf bytes.Buffer
		p, err := capturetest.NewPcapngWriter(&buf, binary.LittleEndian)
		must(t, err)
		return &buf, p
	}
	tests := []struct {
		name  string
		build func(t *testing.T) []byte
	}{
		{name: "packet on an undescribed interface", build: func(t *testing.T) []byte {
			buf, p := good(t)
			must(t, p.Packet(0, at(0), []byte{1, 2, 3, 4}, 0))
			return buf.Bytes()
		}},
		{name: "trailing length differs", build: func(t *testing.T) []byte {
			buf, _ := good(t)
			b := buf.Bytes()
			b[len(b)-4]++
			return b
		}},
		{name: "length not a multiple of 4", build: func(t *testing.T) []byte {
			buf, _ := good(t)
			b := buf.Bytes()
			b[4]++
			return b
		}},
		{name: "bad byte-order magic", build: func(t *testing.T) []byte {
			buf, _ := good(t)
			b := buf.Bytes()
			b[8] = 0
			return b
		}},
		{name: "block over the limit", build: func(t *testing.T) []byte {
			buf, _ := good(t)
			b := append(buf.Bytes(), 6, 0, 0, 0, 0, 0, 0, 0x7f) // 2 GiB block
			return b
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := capture.Read(bytes.NewReader(tt.build(t))); !errors.Is(err, capture.ErrFormat) {
				t.Errorf("got %v, want ErrFormat", err)
			}
		})
	}
}

func TestRead_OutOfOrderSegments_Reassembled(t *testing.T) {
	var buf bytes.Buffer
	w := handshake(t, &buf)
	must(t, w.TCP(at(1), client, server, 1001, 5001, capturetest.ACK, []byte("abc")))
	must(t, w.TCP(at(2), client, server, 1007, 5001, capturetest.ACK, []byte("ghi"))) // early
	must(t, w.TCP(at(3), client, server, 1004, 5001, capturetest.ACK, []byte("def"))) // fills the gap

	c := readOne(t, buf.Bytes())
	if got := stream(c, capture.FromClient); string(got) != "abcdefghi" || c.Err != nil {
		t.Errorf("got %q, Err %v; want \"abcdefghi\", nil", got, c.Err)
	}
}

func TestRead_EveryFrameTwice_StreamsOnce(t *testing.T) {
	// tcpdump -i any can see a segment on two interfaces.
	var buf bytes.Buffer
	w, err := capturetest.NewWriter(&buf)
	must(t, err)
	conn := exchange(client, server, 0)
	must(t, capturetest.WriteConn(func(ts time.Time, src, dst netip.AddrPort, seq, ack uint32, flags byte, payload []byte) error {
		if err := w.TCP(ts, src, dst, seq, ack, flags, payload); err != nil {
			return err
		}
		return w.TCP(ts, src, dst, seq, ack, flags, payload)
	}, conn))

	checkExchange(t, readOne(t, buf.Bytes()), conn)
}

// withIPv6Header inserts an extension header of type typ, body hdr (whose
// first byte is overwritten with the next header), after the fixed IPv6
// header.
func withIPv6Header(ip []byte, typ byte, hdr []byte) []byte {
	hdr = bytes.Clone(hdr)
	hdr[0] = ip[6]
	out := append(bytes.Clone(ip[:40]), hdr...)
	out = append(out, ip[40:]...)
	out[6] = typ
	binary.BigEndian.PutUint16(out[4:], uint16(len(out)-40))
	return out
}

func TestRead_IPv6ExtensionHeaders(t *testing.T) {
	hopByHop := func(ip []byte) []byte { return withIPv6Header(ip, 0, make([]byte, 8)) }
	fragment := func(ip []byte) []byte {
		if len(ip) == 60 { // no payload: leave handshake and FIN segments whole
			return ip
		}
		return withIPv6Header(ip, 44, []byte{0, 0, 0, 1, 0, 0, 0, 9}) // offset 0, more fragments
	}
	tests := []struct {
		name    string
		wrap    func([]byte) []byte
		wantErr error
	}{
		{name: "hop-by-hop options", wrap: hopByHop},
		{name: "first fragment", wrap: fragment, wantErr: capture.ErrTruncated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w, err := capturetest.NewWriterLink(&buf, capturetest.LinkRaw, binary.LittleEndian, false)
			must(t, err)
			conn := exchange(client6, server6, 0)
			must(t, capturetest.WriteConn(tcpFrames(tt.wrap, w.Frame), conn))

			c := readOne(t, buf.Bytes())
			if tt.wantErr != nil {
				if !errors.Is(c.Err, tt.wantErr) {
					t.Errorf("got Err %v, want %v", c.Err, tt.wantErr)
				}
				return
			}
			checkExchange(t, c, conn)
		})
	}
}

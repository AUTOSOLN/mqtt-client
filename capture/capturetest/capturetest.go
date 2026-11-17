// Package capturetest writes capture files for tests of code that reads
// them.
//
// Write turns capture.Conn values into a classic pcap file with Ethernet,
// IPv4 and TCP framing and a full TCP handshake. Writer gives
// segment-level control of a classic pcap file, for tests of
// retransmissions, gaps and truncated frames, and PcapngWriter writes
// pcapng. The frame builders (IPv4TCP, IPv6TCP, Ethernet, LinuxSLL, ...)
// make the frames for either.
package capturetest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"time"

	"github.com/AUTOSOLN/mqtt-client/capture"
)

// TCP flags.
const (
	FIN = 0x01
	SYN = 0x02
	RST = 0x04
	PSH = 0x08
	ACK = 0x10
)

// Link-layer header types (https://www.tcpdump.org/linktypes.html).
const (
	LinkNull      = 0
	LinkEthernet  = 1
	LinkRaw       = 101
	LinkLoop      = 108
	LinkLinuxSLL  = 113
	LinkLinuxSLL2 = 276
)

// mss is the largest payload Write puts in one segment.
const mss = 1460

// Writer writes a classic pcap file.
type Writer struct {
	w    io.Writer
	bo   binary.ByteOrder
	nano bool
	link uint32

	// SnapLen, if non-zero, cuts each frame to this many bytes, the way a
	// capture with a small snap length does. Set it before writing frames.
	SnapLen int
}

// NewWriter writes the header of a little-endian, microsecond, Ethernet
// pcap file to w.
func NewWriter(w io.Writer) (*Writer, error) {
	return NewWriterLink(w, LinkEthernet, binary.LittleEndian, false)
}

// NewWriterLink writes the header of a pcap file with the given link type,
// byte order and timestamp precision (nanoseconds if nano, otherwise
// microseconds) to w.
func NewWriterLink(w io.Writer, link uint32, bo binary.ByteOrder, nano bool) (*Writer, error) {
	var h [24]byte
	magic := uint32(0xa1b2c3d4)
	if nano {
		magic = 0xa1b23c4d
	}
	bo.PutUint32(h[0:], magic)
	bo.PutUint16(h[4:], 2) // version 2.4
	bo.PutUint16(h[6:], 4)
	bo.PutUint32(h[16:], 262144) // snap length
	bo.PutUint32(h[20:], link)
	if _, err := w.Write(h[:]); err != nil {
		return nil, err
	}
	return &Writer{w: w, bo: bo, nano: nano, link: link}, nil
}

// TCP writes one IPv4 TCP segment from src to dst in an Ethernet frame.
// The Writer's link type must be Ethernet.
func (w *Writer) TCP(t time.Time, src, dst netip.AddrPort, seq, ack uint32, flags byte, payload []byte) error {
	if w.link != LinkEthernet {
		return errors.New("capturetest: Writer.TCP needs an Ethernet file; use Frame")
	}
	ip, err := IPv4TCP(src, dst, seq, ack, flags, payload)
	if err != nil {
		return err
	}
	return w.Frame(t, Ethernet(ip))
}

// Frame writes one frame, which must match the Writer's link type.
func (w *Writer) Frame(t time.Time, f []byte) error {
	incl := f
	if w.SnapLen > 0 && len(f) > w.SnapLen {
		incl = f[:w.SnapLen]
	}
	frac := t.Nanosecond() / 1000
	if w.nano {
		frac = t.Nanosecond()
	}
	var h [16]byte
	w.bo.PutUint32(h[0:], uint32(t.Unix()))
	w.bo.PutUint32(h[4:], uint32(frac))
	w.bo.PutUint32(h[8:], uint32(len(incl)))
	w.bo.PutUint32(h[12:], uint32(len(f)))
	if _, err := w.w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.w.Write(incl)
	return err
}

// IPv4TCP builds an IPv4 packet carrying one TCP segment. The TCP
// checksum is left zero: readers of captures don't check it, and captures
// of offloaded traffic often have it wrong anyway.
func IPv4TCP(src, dst netip.AddrPort, seq, ack uint32, flags byte, payload []byte) ([]byte, error) {
	if !src.Addr().Is4() || !dst.Addr().Is4() {
		return nil, errors.New("capturetest: IPv4TCP needs IPv4 addresses")
	}
	n := 20 + 20 + len(payload)
	if n > 0xffff {
		return nil, fmt.Errorf("capturetest: payload of %d bytes is too large for one segment", len(payload))
	}
	ip := make([]byte, n)
	ip[0] = 0x45 // IPv4, 20-byte header
	binary.BigEndian.PutUint16(ip[2:], uint16(n))
	binary.BigEndian.PutUint16(ip[6:], 0x4000) // don't fragment
	ip[8] = 64                                 // TTL
	ip[9] = 6                                  // TCP
	s, d := src.Addr().As4(), dst.Addr().As4()
	copy(ip[12:16], s[:])
	copy(ip[16:20], d[:])
	binary.BigEndian.PutUint16(ip[10:], checksum(ip[:20]))
	putTCP(ip[20:], src, dst, seq, ack, flags, payload)
	return ip, nil
}

// IPv6TCP builds an IPv6 packet carrying one TCP segment, with no
// extension headers.
func IPv6TCP(src, dst netip.AddrPort, seq, ack uint32, flags byte, payload []byte) ([]byte, error) {
	if !src.Addr().Is6() || src.Addr().Is4In6() || !dst.Addr().Is6() || dst.Addr().Is4In6() {
		return nil, errors.New("capturetest: IPv6TCP needs IPv6 addresses")
	}
	n := 20 + len(payload)
	if n > 0xffff {
		return nil, fmt.Errorf("capturetest: payload of %d bytes is too large for one segment", len(payload))
	}
	ip := make([]byte, 40+n)
	ip[0] = 0x60
	binary.BigEndian.PutUint16(ip[4:], uint16(n))
	ip[6] = 6 // next header: TCP
	ip[7] = 64
	s, d := src.Addr().As16(), dst.Addr().As16()
	copy(ip[8:24], s[:])
	copy(ip[24:40], d[:])
	putTCP(ip[40:], src, dst, seq, ack, flags, payload)
	return ip, nil
}

func putTCP(b []byte, src, dst netip.AddrPort, seq, ack uint32, flags byte, payload []byte) {
	binary.BigEndian.PutUint16(b[0:], src.Port())
	binary.BigEndian.PutUint16(b[2:], dst.Port())
	binary.BigEndian.PutUint32(b[4:], seq)
	binary.BigEndian.PutUint32(b[8:], ack)
	b[12] = 5 << 4 // 20-byte header
	b[13] = flags
	binary.BigEndian.PutUint16(b[14:], 65535) // window
	copy(b[20:], payload)
}

func checksum(b []byte) uint16 {
	var sum uint32
	for i := 0; i+1 < len(b); i += 2 {
		sum += uint32(binary.BigEndian.Uint16(b[i:]))
	}
	for sum > 0xffff {
		sum = sum&0xffff + sum>>16
	}
	return ^uint16(sum)
}

// etherType is the EtherType of an IP packet, from its version.
func etherType(ip []byte) uint16 {
	if len(ip) > 0 && ip[0]>>4 == 6 {
		return 0x86dd
	}
	return 0x0800
}

// Ethernet wraps an IP packet in an Ethernet frame.
func Ethernet(ip []byte) []byte {
	f := make([]byte, 14, 14+len(ip))
	copy(f[0:6], []byte{0x02, 0, 0, 0, 0, 2}) // locally administered addresses
	copy(f[6:12], []byte{0x02, 0, 0, 0, 0, 1})
	binary.BigEndian.PutUint16(f[12:], etherType(ip))
	return append(f, ip...)
}

// EthernetVLAN wraps an IP packet in an Ethernet frame with an 802.1Q tag.
func EthernetVLAN(vlan uint16, ip []byte) []byte {
	f := make([]byte, 18, 18+len(ip))
	copy(f[0:6], []byte{0x02, 0, 0, 0, 0, 2})
	copy(f[6:12], []byte{0x02, 0, 0, 0, 0, 1})
	binary.BigEndian.PutUint16(f[12:], 0x8100)
	binary.BigEndian.PutUint16(f[14:], vlan&0x0fff)
	binary.BigEndian.PutUint16(f[16:], etherType(ip))
	return append(f, ip...)
}

// LinuxSLL wraps an IP packet in a Linux cooked capture v1 header.
func LinuxSLL(ip []byte) []byte {
	f := make([]byte, 16, 16+len(ip))
	binary.BigEndian.PutUint16(f[2:], 772) // ARPHRD_LOOPBACK
	binary.BigEndian.PutUint16(f[4:], 6)   // address length
	binary.BigEndian.PutUint16(f[14:], etherType(ip))
	return append(f, ip...)
}

// LinuxSLL2 wraps an IP packet in a Linux cooked capture v2 header.
func LinuxSLL2(ip []byte) []byte {
	f := make([]byte, 20, 20+len(ip))
	binary.BigEndian.PutUint16(f[0:], etherType(ip))
	binary.BigEndian.PutUint32(f[4:], 1)   // interface index
	binary.BigEndian.PutUint16(f[8:], 772) // ARPHRD_LOOPBACK
	f[11] = 6                              // address length
	return append(f, ip...)
}

// Null wraps an IP packet in a BSD or Npcap loopback header, whose address
// family is in the capturing machine's byte order, bo. IPv6 uses 24, the
// value on NetBSD, OpenBSD and Windows.
func Null(bo binary.ByteOrder, ip []byte) []byte {
	af := uint32(2)
	if etherType(ip) == 0x86dd {
		af = 24
	}
	f := make([]byte, 4, 4+len(ip))
	bo.PutUint32(f, af)
	return append(f, ip...)
}

// Write writes conns as a classic pcap file, one after another. Each
// connection gets a three-way handshake a millisecond before its first
// segment, its segments (split into 1460-byte pieces), and a FIN at
// ClientClose and ServerClose if they are set. Conn.Transport and Conn.Err
// are ignored.
func Write(w io.Writer, conns []capture.Conn) error {
	pw, err := NewWriter(w)
	if err != nil {
		return err
	}
	for i, c := range conns {
		if err := WriteConn(pw.TCP, c); err != nil {
			return fmt.Errorf("capturetest: connection %d: %w", i, err)
		}
	}
	return nil
}

// TCPFunc writes one TCP segment, as Writer.TCP does.
type TCPFunc func(t time.Time, src, dst netip.AddrPort, seq, ack uint32, flags byte, payload []byte) error

// WriteConn writes one connection, as Write does, through tcp. Use it to
// write a connection in another format or link type.
func WriteConn(tcp TCPFunc, c capture.Conn) error {
	start := time.Unix(0, 0)
	if len(c.Segments) > 0 {
		start = c.Segments[0].Time.Add(-time.Millisecond)
	} else if c.ClientClose != nil {
		start = c.ClientClose.Add(-time.Millisecond)
	}
	cseq, sseq := uint32(1000), uint32(5000) // initial sequence numbers
	if err := tcp(start, c.Client, c.Server, cseq, 0, SYN, nil); err != nil {
		return err
	}
	if err := tcp(start, c.Server, c.Client, sseq, cseq+1, SYN|ACK, nil); err != nil {
		return err
	}
	cseq, sseq = cseq+1, sseq+1
	if err := tcp(start, c.Client, c.Server, cseq, sseq, ACK, nil); err != nil {
		return err
	}
	for _, s := range c.Segments {
		src, dst, seq, ack := c.Client, c.Server, &cseq, sseq
		if s.Dir == capture.FromServer {
			src, dst, seq, ack = c.Server, c.Client, &sseq, cseq
		}
		for d := s.Data; len(d) > 0; {
			n := min(len(d), mss)
			if err := tcp(s.Time, src, dst, *seq, ack, PSH|ACK, d[:n]); err != nil {
				return err
			}
			*seq += uint32(n)
			d = d[n:]
		}
	}
	// Closes go out in time order, client first on a tie.
	type fin struct {
		t        time.Time
		src, dst netip.AddrPort
		seq, ack *uint32
	}
	var fins []fin
	if c.ClientClose != nil {
		fins = append(fins, fin{*c.ClientClose, c.Client, c.Server, &cseq, &sseq})
	}
	if c.ServerClose != nil {
		f := fin{*c.ServerClose, c.Server, c.Client, &sseq, &cseq}
		if len(fins) == 1 && f.t.Before(fins[0].t) {
			fins = []fin{f, fins[0]}
		} else {
			fins = append(fins, f)
		}
	}
	for _, f := range fins {
		if err := tcp(f.t, f.src, f.dst, *f.seq, *f.ack, FIN|ACK, nil); err != nil {
			return err
		}
		*f.seq++
	}
	return nil
}

// Package capturetest writes classic pcap files, with Ethernet, IPv4 and
// TCP framing, for tests of code that reads captures.
//
// Write turns capture.Conn values into a file with a full TCP handshake.
// Writer gives segment-level control, for tests of retransmissions, gaps
// and truncated frames.
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

// TCP flags for Writer.TCP.
const (
	FIN = 0x01
	SYN = 0x02
	RST = 0x04
	PSH = 0x08
	ACK = 0x10
)

// mss is the largest payload Write puts in one segment.
const mss = 1460

// Writer writes a classic pcap file: little-endian, microsecond
// timestamps, Ethernet link type.
type Writer struct {
	w io.Writer

	// SnapLen, if non-zero, cuts each frame to this many bytes, the way a
	// capture with a small snap length does. Set it before writing frames.
	SnapLen int
}

// NewWriter writes the file header to w.
func NewWriter(w io.Writer) (*Writer, error) {
	var h [24]byte
	binary.LittleEndian.PutUint32(h[0:], 0xa1b2c3d4)
	binary.LittleEndian.PutUint16(h[4:], 2) // version 2.4
	binary.LittleEndian.PutUint16(h[6:], 4)
	binary.LittleEndian.PutUint32(h[16:], 262144) // snap length
	binary.LittleEndian.PutUint32(h[20:], 1)      // Ethernet
	if _, err := w.Write(h[:]); err != nil {
		return nil, err
	}
	return &Writer{w: w}, nil
}

// TCP writes one IPv4 TCP segment from src to dst.
func (w *Writer) TCP(t time.Time, src, dst netip.AddrPort, seq, ack uint32, flags byte, payload []byte) error {
	if !src.Addr().Is4() || !dst.Addr().Is4() {
		return errors.New("capturetest: only IPv4 addresses are supported")
	}
	ipLen := 20 + 20 + len(payload)
	if ipLen > 0xffff {
		return fmt.Errorf("capturetest: payload of %d bytes is too large for one segment", len(payload))
	}
	f := make([]byte, 14+ipLen)
	// Ethernet: locally administered MAC addresses, then type IPv4.
	copy(f[0:6], []byte{0x02, 0, 0, 0, 0, 2})
	copy(f[6:12], []byte{0x02, 0, 0, 0, 0, 1})
	binary.BigEndian.PutUint16(f[12:], 0x0800)

	ip := f[14:]
	ip[0] = 0x45 // IPv4, 20-byte header
	binary.BigEndian.PutUint16(ip[2:], uint16(ipLen))
	binary.BigEndian.PutUint16(ip[6:], 0x4000) // don't fragment
	ip[8] = 64                                 // TTL
	ip[9] = 6                                  // TCP
	s, d := src.Addr().As4(), dst.Addr().As4()
	copy(ip[12:16], s[:])
	copy(ip[16:20], d[:])
	binary.BigEndian.PutUint16(ip[10:], checksum(ip[:20]))

	tcp := ip[20:]
	binary.BigEndian.PutUint16(tcp[0:], src.Port())
	binary.BigEndian.PutUint16(tcp[2:], dst.Port())
	binary.BigEndian.PutUint32(tcp[4:], seq)
	binary.BigEndian.PutUint32(tcp[8:], ack)
	tcp[12] = 5 << 4 // 20-byte header
	tcp[13] = flags
	binary.BigEndian.PutUint16(tcp[14:], 65535) // window
	copy(tcp[20:], payload)
	// The TCP checksum is left zero: readers of captures don't check it,
	// and captures of offloaded traffic often have it wrong anyway.

	return w.frame(t, f)
}

func (w *Writer) frame(t time.Time, f []byte) error {
	incl := f
	if w.SnapLen > 0 && len(f) > w.SnapLen {
		incl = f[:w.SnapLen]
	}
	var h [16]byte
	binary.LittleEndian.PutUint32(h[0:], uint32(t.Unix()))
	binary.LittleEndian.PutUint32(h[4:], uint32(t.Nanosecond()/1000))
	binary.LittleEndian.PutUint32(h[8:], uint32(len(incl)))
	binary.LittleEndian.PutUint32(h[12:], uint32(len(f)))
	if _, err := w.w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.w.Write(incl)
	return err
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

// Write writes conns as a pcap file, one after another. Each connection
// gets a three-way handshake a millisecond before its first segment, its
// segments (split into 1460-byte pieces), and a FIN at ClientClose and
// ServerClose if they are set. Conn.Transport and Conn.Err are ignored.
func Write(w io.Writer, conns []capture.Conn) error {
	pw, err := NewWriter(w)
	if err != nil {
		return err
	}
	for i, c := range conns {
		if err := pw.conn(c); err != nil {
			return fmt.Errorf("capturetest: connection %d: %w", i, err)
		}
	}
	return nil
}

func (w *Writer) conn(c capture.Conn) error {
	start := time.Unix(0, 0)
	if len(c.Segments) > 0 {
		start = c.Segments[0].Time.Add(-time.Millisecond)
	} else if c.ClientClose != nil {
		start = c.ClientClose.Add(-time.Millisecond)
	}
	cseq, sseq := uint32(1000), uint32(5000) // initial sequence numbers
	if err := w.TCP(start, c.Client, c.Server, cseq, 0, SYN, nil); err != nil {
		return err
	}
	if err := w.TCP(start, c.Server, c.Client, sseq, cseq+1, SYN|ACK, nil); err != nil {
		return err
	}
	cseq, sseq = cseq+1, sseq+1
	if err := w.TCP(start, c.Client, c.Server, cseq, sseq, ACK, nil); err != nil {
		return err
	}
	for _, s := range c.Segments {
		src, dst, seq, ack := c.Client, c.Server, &cseq, sseq
		if s.Dir == capture.FromServer {
			src, dst, seq, ack = c.Server, c.Client, &sseq, cseq
		}
		for d := s.Data; len(d) > 0; {
			n := min(len(d), mss)
			if err := w.TCP(s.Time, src, dst, *seq, ack, PSH|ACK, d[:n]); err != nil {
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
		if err := w.TCP(f.t, f.src, f.dst, *f.seq, *f.ack, FIN|ACK, nil); err != nil {
			return err
		}
		*f.seq++
	}
	return nil
}

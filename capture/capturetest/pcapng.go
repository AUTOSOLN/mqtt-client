package capturetest

import (
	"encoding/binary"
	"io"
	"time"
)

// PcapngWriter writes a pcapng file one block at a time.
type PcapngWriter struct {
	w      io.Writer
	bo     binary.AppendByteOrder
	ifaces []int64 // timestamp units per second, per interface of the current section
}

// NewPcapngWriter writes a Section Header Block in byte order bo to w.
func NewPcapngWriter(w io.Writer, bo binary.AppendByteOrder) (*PcapngWriter, error) {
	p := &PcapngWriter{w: w}
	return p, p.Section(bo)
}

// Section starts a new section, in byte order bo. Interfaces described
// before it no longer apply.
func (p *PcapngWriter) Section(bo binary.AppendByteOrder) error {
	p.bo, p.ifaces = bo, nil
	body := bo.AppendUint32(nil, 0x1a2b3c4d)
	body = bo.AppendUint16(body, 1) // version 1.0
	body = bo.AppendUint16(body, 0)
	body = bo.AppendUint64(body, ^uint64(0)) // section length not given
	return p.Block(0x0a0d0d0a, body)
}

// Interface writes an Interface Description Block and returns its index.
// tsresol is the if_tsresol option byte (6 for microseconds, 9 for
// nanoseconds, 0x80|n for 2^-n seconds); 0 leaves the option out, which
// means microseconds.
func (p *PcapngWriter) Interface(link uint16, tsresol byte) (int, error) {
	body := p.bo.AppendUint16(nil, link)
	body = p.bo.AppendUint16(body, 0)
	body = p.bo.AppendUint32(body, 262144) // snap length
	units := int64(1_000_000)
	if tsresol != 0 {
		body = p.bo.AppendUint16(body, 9) // if_tsresol
		body = p.bo.AppendUint16(body, 1)
		body = append(body, tsresol, 0, 0, 0)
		body = p.bo.AppendUint32(body, 0) // opt_endofopt
		if tsresol&0x80 != 0 {
			units = 1 << (tsresol & 0x7f)
		} else {
			units = 1
			for range tsresol {
				units *= 10
			}
		}
	}
	if err := p.Block(1, body); err != nil {
		return 0, err
	}
	p.ifaces = append(p.ifaces, units)
	return len(p.ifaces) - 1, nil
}

// Packet writes an Enhanced Packet Block with frame f on interface iface.
// origLen is the frame's length on the wire; 0 means len(f).
func (p *PcapngWriter) Packet(iface int, t time.Time, f []byte, origLen int) error {
	if origLen == 0 {
		origLen = len(f)
	}
	high, low := p.stamp(iface, t)
	body := p.bo.AppendUint32(nil, uint32(iface))
	body = p.bo.AppendUint32(body, high)
	body = p.bo.AppendUint32(body, low)
	body = p.bo.AppendUint32(body, uint32(len(f)))
	body = p.bo.AppendUint32(body, uint32(origLen))
	return p.Block(6, pad(append(body, f...)))
}

// ObsoletePacket writes a Packet Block, as old Wireshark versions did.
func (p *PcapngWriter) ObsoletePacket(iface int, t time.Time, f []byte) error {
	high, low := p.stamp(iface, t)
	body := p.bo.AppendUint16(nil, uint16(iface))
	body = p.bo.AppendUint16(body, 0) // drops
	body = p.bo.AppendUint32(body, high)
	body = p.bo.AppendUint32(body, low)
	body = p.bo.AppendUint32(body, uint32(len(f)))
	body = p.bo.AppendUint32(body, uint32(len(f)))
	return p.Block(2, pad(append(body, f...)))
}

// SimplePacket writes a Simple Packet Block, which has no timestamp and is
// on interface 0.
func (p *PcapngWriter) SimplePacket(f []byte) error {
	body := p.bo.AppendUint32(nil, uint32(len(f)))
	return p.Block(3, pad(append(body, f...)))
}

// Block writes a block of any type; body must be padded to 32 bits.
func (p *PcapngWriter) Block(typ uint32, body []byte) error {
	total := uint32(12 + len(body))
	b := p.bo.AppendUint32(nil, typ)
	b = p.bo.AppendUint32(b, total)
	b = append(b, body...)
	b = p.bo.AppendUint32(b, total)
	_, err := p.w.Write(b)
	return err
}

func (p *PcapngWriter) stamp(iface int, t time.Time) (high, low uint32) {
	units := int64(1_000_000)
	if iface < len(p.ifaces) {
		units = p.ifaces[iface]
	}
	ts := uint64(t.Unix())*uint64(units) + uint64(int64(t.Nanosecond())*units/1e9)
	return uint32(ts >> 32), uint32(ts)
}

func pad(b []byte) []byte {
	for len(b)%4 != 0 {
		b = append(b, 0)
	}
	return b
}

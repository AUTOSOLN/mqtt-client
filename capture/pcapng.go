package capture

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"time"
)

// pcapng block types (https://datatracker.ietf.org/doc/draft-ietf-opsawg-pcapng/).
const (
	blockSection   = 0x0a0d0d0a
	blockInterface = 0x00000001
	blockObsolete  = 0x00000002 // Packet Block, written by old Wireshark versions
	blockSimple    = 0x00000003
	blockEnhanced  = 0x00000006
)

// maxBlock bounds one pcapng block: a packet of maxRecord bytes plus room
// for options and other blocks (name resolution, statistics, secrets).
const maxBlock = 16 << 20

// pcapngInterface is what an Interface Description Block says.
type pcapngInterface struct {
	decode    linkDecoder // nil for an unsupported link type
	link      uint16
	snap      uint32
	unitsPerS uint64 // timestamp units per second
	offset    int64  // seconds added to every timestamp (if_tsoffset)
}

// readPcapng reads a pcapng file: one or more sections, each with its own
// byte order and interfaces.
func readPcapng(br *bufio.Reader, a *assembler) error {
	var bo binary.ByteOrder = binary.LittleEndian
	var ifaces []pcapngInterface
	for n := 1; ; n++ {
		var hdr [8]byte
		if _, err := io.ReadFull(br, hdr[:]); err == io.EOF && n > 1 {
			return nil
		} else if err != nil {
			return fmt.Errorf("%w: block %d header: %v", ErrFormat, n, err)
		}
		typ := binary.LittleEndian.Uint32(hdr[0:4]) // the section header type reads the same in both byte orders
		if typ == blockSection {
			bom, err := br.Peek(4)
			if err != nil {
				return fmt.Errorf("%w: block %d: section header: %v", ErrFormat, n, err)
			}
			switch binary.LittleEndian.Uint32(bom) {
			case 0x1a2b3c4d:
				bo = binary.LittleEndian
			case 0x4d3c2b1a:
				bo = binary.BigEndian
			default:
				return fmt.Errorf("%w: block %d: bad byte-order magic % x", ErrFormat, n, bom)
			}
			ifaces = nil // interfaces belong to their section
		} else if n == 1 {
			return fmt.Errorf("%w: pcapng file doesn't start with a section header", ErrFormat)
		} else {
			typ = bo.Uint32(hdr[0:4])
		}
		total := bo.Uint32(hdr[4:8])
		if total < 12 || total%4 != 0 || total > maxBlock {
			return fmt.Errorf("%w: block %d has length %d", ErrFormat, n, total)
		}
		body := make([]byte, total-8)
		if _, err := io.ReadFull(br, body); err != nil {
			return fmt.Errorf("%w: block %d: %v", ErrFormat, n, err)
		}
		if trailer := bo.Uint32(body[len(body)-4:]); trailer != total {
			return fmt.Errorf("%w: block %d: trailing length %d differs from %d", ErrFormat, n, trailer, total)
		}
		body = body[:len(body)-4]

		var err error
		switch typ {
		case blockSection:
			if len(body) < 16 {
				err = fmt.Errorf("section header of %d bytes", len(body))
			}
		case blockInterface:
			var ifc pcapngInterface
			ifc, err = parseInterface(bo, body)
			ifaces = append(ifaces, ifc)
		case blockEnhanced:
			err = enhancedPacket(a, bo, body, ifaces)
		case blockSimple:
			err = simplePacket(a, bo, body, ifaces)
		case blockObsolete:
			err = obsoletePacket(a, bo, body, ifaces)
		}
		// Other blocks (name resolution, statistics, decryption secrets,
		// custom) are skipped.
		if err != nil {
			return fmt.Errorf("%w: block %d: %v", ErrFormat, n, err)
		}
	}
}

func parseInterface(bo binary.ByteOrder, b []byte) (pcapngInterface, error) {
	if len(b) < 8 {
		return pcapngInterface{}, fmt.Errorf("interface description of %d bytes", len(b))
	}
	ifc := pcapngInterface{link: bo.Uint16(b[0:2]), snap: bo.Uint32(b[4:8]), unitsPerS: 1_000_000}
	ifc.decode = linkTypes[uint32(ifc.link)]
	for opts := b[8:]; len(opts) >= 4; {
		code, n := bo.Uint16(opts[0:2]), int(bo.Uint16(opts[2:4]))
		if code == 0 { // opt_endofopt
			break
		}
		if len(opts) < 4+n {
			return ifc, fmt.Errorf("interface option %d overruns the block", code)
		}
		v := opts[4 : 4+n]
		switch {
		case code == 9 && n == 1: // if_tsresol
			exp := uint(v[0] & 0x7f)
			if v[0]&0x80 != 0 { // a power of two
				if exp > 30 {
					return ifc, fmt.Errorf("timestamp resolution 2^-%d is not supported", exp)
				}
				ifc.unitsPerS = 1 << exp
			} else {
				if exp > 9 {
					return ifc, fmt.Errorf("timestamp resolution 10^-%d is not supported", exp)
				}
				ifc.unitsPerS = 1
				for range exp {
					ifc.unitsPerS *= 10
				}
			}
		case code == 14 && n == 8: // if_tsoffset
			ifc.offset = int64(bo.Uint64(v))
		}
		padded := 4 + (n+3)&^3 // values are padded to 32 bits
		if padded > len(opts) {
			break
		}
		opts = opts[padded:]
	}
	return ifc, nil
}

// iface returns interface i of the section.
func iface(ifaces []pcapngInterface, i uint32) (pcapngInterface, error) {
	if int64(i) >= int64(len(ifaces)) {
		return pcapngInterface{}, fmt.Errorf("packet on interface %d, but only %d described", i, len(ifaces))
	}
	return ifaces[i], nil
}

// packet passes one captured frame to the interface's link-layer decoder.
func (ifc pcapngInterface) packet(a *assembler, t time.Time, data []byte, orig uint32) error {
	if uint32(len(data)) > maxRecord {
		return fmt.Errorf("packet of %d bytes, more than the limit of %d", len(data), maxRecord)
	}
	if ifc.decode == nil {
		a.ignored++ // pcapng allows several link types; skip the ones we can't read
		return nil
	}
	ifc.decode(a, t, data, uint32(len(data)) < orig)
	return nil
}

func (ifc pcapngInterface) time(high, low uint32) time.Time {
	ts := uint64(high)<<32 | uint64(low)
	sec := ts / ifc.unitsPerS
	hi, lo := bits.Mul64(ts%ifc.unitsPerS, 1e9)
	ns, _ := bits.Div64(hi, lo, ifc.unitsPerS)
	return time.Unix(int64(sec)+ifc.offset, int64(ns)).UTC()
}

func enhancedPacket(a *assembler, bo binary.ByteOrder, b []byte, ifaces []pcapngInterface) error {
	if len(b) < 20 {
		return fmt.Errorf("enhanced packet block of %d bytes", len(b))
	}
	ifc, err := iface(ifaces, bo.Uint32(b[0:4]))
	if err != nil {
		return err
	}
	capLen, orig := bo.Uint32(b[12:16]), bo.Uint32(b[16:20])
	if uint64(capLen) > uint64(len(b)-20) {
		return fmt.Errorf("captured length %d overruns the block", capLen)
	}
	return ifc.packet(a, ifc.time(bo.Uint32(b[4:8]), bo.Uint32(b[8:12])), b[20:20+capLen], orig)
}

func obsoletePacket(a *assembler, bo binary.ByteOrder, b []byte, ifaces []pcapngInterface) error {
	if len(b) < 20 {
		return fmt.Errorf("packet block of %d bytes", len(b))
	}
	ifc, err := iface(ifaces, uint32(bo.Uint16(b[0:2])))
	if err != nil {
		return err
	}
	capLen, orig := bo.Uint32(b[12:16]), bo.Uint32(b[16:20])
	if uint64(capLen) > uint64(len(b)-20) {
		return fmt.Errorf("captured length %d overruns the block", capLen)
	}
	return ifc.packet(a, ifc.time(bo.Uint32(b[4:8]), bo.Uint32(b[8:12])), b[20:20+capLen], orig)
}

// simplePacket reads a Simple Packet Block: interface 0, no timestamp, so
// its segments have the zero time.
func simplePacket(a *assembler, bo binary.ByteOrder, b []byte, ifaces []pcapngInterface) error {
	if len(b) < 4 {
		return fmt.Errorf("simple packet block of %d bytes", len(b))
	}
	ifc, err := iface(ifaces, 0)
	if err != nil {
		return err
	}
	orig := bo.Uint32(b[0:4])
	capLen := min(uint64(orig), uint64(len(b)-4))
	if ifc.snap > 0 {
		capLen = min(capLen, uint64(ifc.snap))
	}
	return ifc.packet(a, time.Time{}, b[4:4+capLen], orig)
}

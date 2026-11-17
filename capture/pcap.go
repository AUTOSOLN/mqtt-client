package capture

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net/netip"
	"time"
)

// maxRecord bounds one captured frame, whatever snap length the file
// declares: 256 KiB is the largest snap length tcpdump and Wireshark use.
const maxRecord = 256 << 10

// Link-layer header types (https://www.tcpdump.org/linktypes.html).
const linkEthernet = 1

// Read reads a capture file and returns its TCP connections.
//
// It returns an error wrapping ErrFormat if the file isn't a capture it can
// read, or ends in the middle of a record. Problems with one connection (a
// gap, a truncated frame) don't fail the whole file: they are in that
// connection's Err.
func Read(r io.Reader) (*Capture, error) {
	br := bufio.NewReader(r)
	var hdr [24]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, fmt.Errorf("%w: file header: %v", ErrFormat, err)
	}
	var bo binary.ByteOrder
	var nano bool
	switch m := binary.LittleEndian.Uint32(hdr[:4]); m {
	case 0xa1b2c3d4:
		bo = binary.LittleEndian
	case 0xa1b23c4d:
		bo, nano = binary.LittleEndian, true
	case 0xd4c3b2a1:
		bo = binary.BigEndian
	case 0x4d3cb2a1:
		bo, nano = binary.BigEndian, true
	case 0x0a0d0d0a:
		return nil, fmt.Errorf("%w: pcapng is not supported yet; save the capture as pcap", ErrFormat)
	default:
		if hdr[0] == 0x1f && hdr[1] == 0x8b {
			return nil, fmt.Errorf("%w: compressed captures are not supported yet; save it uncompressed", ErrFormat)
		}
		return nil, fmt.Errorf("%w: not a pcap file (magic number %08x)", ErrFormat, m)
	}
	// The top bits of the link type field may carry FCS information.
	if link := bo.Uint32(hdr[20:24]) & 0x0fffffff; link != linkEthernet {
		return nil, fmt.Errorf("%w: link-layer type %d is not supported yet", ErrFormat, link)
	}
	limit := uint32(maxRecord)
	if snap := bo.Uint32(hdr[16:20]); snap > 0 && snap < limit {
		limit = snap
	}

	a := newAssembler()
	var rec [16]byte
	for n := 1; ; n++ {
		if _, err := io.ReadFull(br, rec[:]); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("%w: record %d header: %v", ErrFormat, n, err)
		}
		sec, frac := bo.Uint32(rec[0:4]), bo.Uint32(rec[4:8])
		incl, orig := bo.Uint32(rec[8:12]), bo.Uint32(rec[12:16])
		if incl > limit {
			return nil, fmt.Errorf("%w: record %d has %d bytes, more than the limit of %d", ErrFormat, n, incl, limit)
		}
		data := make([]byte, incl)
		if _, err := io.ReadFull(br, data); err != nil {
			return nil, fmt.Errorf("%w: record %d: %v", ErrFormat, n, err)
		}
		ns := int64(frac)
		if !nano {
			ns *= 1000
		}
		a.ethernet(time.Unix(int64(sec), ns).UTC(), data, incl < orig)
	}
	return a.finish(), nil
}

// ethernet handles one Ethernet frame. truncated says the capture kept
// only the start of it.
func (a *assembler) ethernet(t time.Time, b []byte, truncated bool) {
	if len(b) < 14 {
		a.ignored++
		return
	}
	etype := binary.BigEndian.Uint16(b[12:14])
	b = b[14:]
	for etype == 0x8100 || etype == 0x88a8 { // 802.1Q and 802.1ad VLAN tags
		if len(b) < 4 {
			a.ignored++
			return
		}
		etype = binary.BigEndian.Uint16(b[2:4])
		b = b[4:]
	}
	if etype != 0x0800 {
		a.ignored++
		return
	}
	a.ipv4(t, b, truncated)
}

func (a *assembler) ipv4(t time.Time, b []byte, truncated bool) {
	if len(b) < 20 || b[0]>>4 != 4 {
		a.ignored++
		return
	}
	ihl := int(b[0]&0x0f) * 4
	total := int(binary.BigEndian.Uint16(b[2:4]))
	if ihl < 20 || total < ihl || len(b) < ihl || b[9] != 6 { // 6: TCP
		a.ignored++
		return
	}
	frag := binary.BigEndian.Uint16(b[6:8])
	if frag&0x1fff != 0 { // a later fragment: no TCP header to place it
		a.ignored++
		return
	}
	src := netip.AddrFrom4([4]byte(b[12:16]))
	dst := netip.AddrFrom4([4]byte(b[16:20]))
	if len(b) >= total {
		b = b[:total] // drop Ethernet padding
	} else {
		truncated = true
	}
	why := ""
	switch {
	case frag&0x2000 != 0: // more fragments follow
		why = "IP fragment"
	case truncated:
		why = "frame cut short by the capture's snap length"
	}
	a.tcp(t, src, dst, b[ihl:], why)
}

// tcp handles one TCP segment. A non-empty why says its payload is
// incomplete.
func (a *assembler) tcp(t time.Time, src, dst netip.Addr, b []byte, why string) {
	if len(b) < 20 {
		a.ignored++
		return
	}
	off := int(b[12]>>4) * 4
	if off < 20 || len(b) < off {
		a.ignored++
		return
	}
	a.segment(t,
		netip.AddrPortFrom(src, binary.BigEndian.Uint16(b[0:2])),
		netip.AddrPortFrom(dst, binary.BigEndian.Uint16(b[2:4])),
		binary.BigEndian.Uint32(b[4:8]), b[13], b[off:], why)
}

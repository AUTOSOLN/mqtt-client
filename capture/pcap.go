package capture

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// readPcap reads a classic pcap file
// (https://datatracker.ietf.org/doc/draft-ietf-opsawg-pcap/).
func readPcap(br *bufio.Reader, a *assembler) error {
	var hdr [24]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return fmt.Errorf("%w: file header: %v", ErrFormat, err)
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
	default:
		return fmt.Errorf("%w: not a pcap or pcapng file (magic number %08x)", ErrFormat, m)
	}
	// The top bits of the link type field may carry FCS information.
	link := bo.Uint32(hdr[20:24]) & 0x0fffffff
	decode, ok := linkTypes[link]
	if !ok {
		return fmt.Errorf("%w: link-layer type %d is not supported", ErrFormat, link)
	}
	limit := uint32(maxRecord)
	if snap := bo.Uint32(hdr[16:20]); snap > 0 && snap < limit {
		limit = snap
	}

	var rec [16]byte
	for n := 1; ; n++ {
		if _, err := io.ReadFull(br, rec[:]); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("%w: record %d header: %v", ErrFormat, n, err)
		}
		sec, frac := bo.Uint32(rec[0:4]), bo.Uint32(rec[4:8])
		incl, orig := bo.Uint32(rec[8:12]), bo.Uint32(rec[12:16])
		if incl > limit {
			return fmt.Errorf("%w: record %d has %d bytes, more than the limit of %d", ErrFormat, n, incl, limit)
		}
		data := make([]byte, incl)
		if _, err := io.ReadFull(br, data); err != nil {
			return fmt.Errorf("%w: record %d: %v", ErrFormat, n, err)
		}
		ns := int64(frac)
		if !nano {
			ns *= 1000
		}
		decode(a, time.Unix(int64(sec), ns).UTC(), data, incl < orig)
	}
}

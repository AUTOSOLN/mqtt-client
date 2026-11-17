package capture

import (
	"encoding/binary"
	"net/netip"
	"time"
)

// A linkDecoder handles one captured frame of its link-layer type.
// truncated says the capture kept only the start of the frame.
type linkDecoder func(a *assembler, t time.Time, b []byte, truncated bool)

// linkTypes maps the link-layer header types we read
// (https://www.tcpdump.org/linktypes.html) to their decoders. It is never
// changed.
var linkTypes = map[uint32]linkDecoder{
	0:   (*assembler).null,      // BSD loopback, Windows Npcap loopback
	1:   (*assembler).ethernet,  // Ethernet
	101: (*assembler).raw,       // raw IP
	108: (*assembler).loop,      // OpenBSD loopback
	113: (*assembler).linuxSLL,  // Linux cooked capture v1 (tcpdump -i any, before libpcap 1.10)
	228: (*assembler).raw,       // raw IPv4
	229: (*assembler).raw,       // raw IPv6
	276: (*assembler).linuxSLL2, // Linux cooked capture v2 (tcpdump -i any)
}

// EtherTypes.
const (
	etherIPv4 = 0x0800
	etherIPv6 = 0x86dd
)

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
	a.network(t, etype, b, truncated)
}

// linuxSLL handles Linux cooked capture v1: a 16-byte header ending with
// the protocol.
func (a *assembler) linuxSLL(t time.Time, b []byte, truncated bool) {
	if len(b) < 16 {
		a.ignored++
		return
	}
	a.network(t, binary.BigEndian.Uint16(b[14:16]), b[16:], truncated)
}

// linuxSLL2 handles Linux cooked capture v2: a 20-byte header starting
// with the protocol.
func (a *assembler) linuxSLL2(t time.Time, b []byte, truncated bool) {
	if len(b) < 20 {
		a.ignored++
		return
	}
	a.network(t, binary.BigEndian.Uint16(b[0:2]), b[20:], truncated)
}

// raw handles frames that are an IP packet with no link-layer header.
func (a *assembler) raw(t time.Time, b []byte, truncated bool) {
	if len(b) == 0 {
		a.ignored++
		return
	}
	switch b[0] >> 4 {
	case 4:
		a.network(t, etherIPv4, b, truncated)
	case 6:
		a.network(t, etherIPv6, b, truncated)
	default:
		a.ignored++
	}
}

// Address family values in loopback headers. IPv6 differs between
// operating systems.
var familyIPv6 = map[uint32]bool{10: true, 24: true, 28: true, 30: true}

// null handles BSD and Npcap loopback: a 4-byte address family in the byte
// order of the machine that captured, which the file doesn't record, so
// both orders are tried.
func (a *assembler) null(t time.Time, b []byte, truncated bool) {
	if len(b) < 4 {
		a.ignored++
		return
	}
	le, be := binary.LittleEndian.Uint32(b), binary.BigEndian.Uint32(b)
	switch {
	case le == 2 || be == 2:
		a.network(t, etherIPv4, b[4:], truncated)
	case familyIPv6[le] || familyIPv6[be]:
		a.network(t, etherIPv6, b[4:], truncated)
	default:
		a.ignored++
	}
}

// loop handles OpenBSD loopback: like null, but in network byte order.
func (a *assembler) loop(t time.Time, b []byte, truncated bool) {
	if len(b) < 4 {
		a.ignored++
		return
	}
	switch af := binary.BigEndian.Uint32(b); {
	case af == 2:
		a.network(t, etherIPv4, b[4:], truncated)
	case familyIPv6[af]:
		a.network(t, etherIPv6, b[4:], truncated)
	default:
		a.ignored++
	}
}

func (a *assembler) network(t time.Time, etype uint16, b []byte, truncated bool) {
	switch etype {
	case etherIPv4:
		a.ipv4(t, b, truncated)
	case etherIPv6:
		a.ipv6(t, b, truncated)
	default:
		a.ignored++
	}
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
		b = b[:total] // drop link-layer padding
	} else {
		truncated = true
	}
	a.tcp(t, src, dst, b[ihl:], incomplete(frag&0x2000 != 0, truncated))
}

// IPv6 next-header values.
const (
	ipv6HopByHop = 0
	ipv6TCP      = 6
	ipv6Routing  = 43
	ipv6Fragment = 44
	ipv6AH       = 51
	ipv6DestOpts = 60
)

func (a *assembler) ipv6(t time.Time, b []byte, truncated bool) {
	if len(b) < 40 || b[0]>>4 != 6 {
		a.ignored++
		return
	}
	total := 40 + int(binary.BigEndian.Uint16(b[4:6]))
	next := b[6]
	src := netip.AddrFrom16([16]byte(b[8:24]))
	dst := netip.AddrFrom16([16]byte(b[24:40]))
	if total == 40 { // empty, or a jumbogram: not something MQTT sends
		a.ignored++
		return
	}
	if len(b) >= total {
		b = b[:total]
	} else {
		truncated = true
	}
	b = b[40:]
	fragment := false
	for next != ipv6TCP {
		var n int
		switch next {
		case ipv6HopByHop, ipv6Routing, ipv6DestOpts:
			if len(b) < 2 {
				a.ignored++
				return
			}
			n = (int(b[1]) + 1) * 8
		case ipv6Fragment:
			if len(b) < 8 {
				a.ignored++
				return
			}
			if binary.BigEndian.Uint16(b[2:4])>>3 != 0 { // a later fragment
				a.ignored++
				return
			}
			fragment = b[3]&1 != 0
			n = 8
		case ipv6AH:
			if len(b) < 2 {
				a.ignored++
				return
			}
			n = (int(b[1]) + 2) * 4
		default: // UDP, ICMPv6, no next header, ...
			a.ignored++
			return
		}
		if len(b) < n {
			a.ignored++
			return
		}
		next, b = b[0], b[n:]
	}
	a.tcp(t, src, dst, b, incomplete(fragment, truncated))
}

// incomplete says why a TCP payload is incomplete, or "" if it isn't.
func incomplete(fragment, truncated bool) string {
	switch {
	case fragment:
		return "IP fragment"
	case truncated:
		return "frame cut short by the capture's snap length"
	}
	return ""
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

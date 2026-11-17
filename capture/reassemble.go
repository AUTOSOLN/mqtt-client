package capture

import (
	"bytes"
	"fmt"
	"net/netip"
	"time"
)

// TCP flags.
const (
	flagFIN = 0x01
	flagSYN = 0x02
	flagRST = 0x04
	flagACK = 0x10
)

// half is one direction of a flow.
type half struct {
	started bool       // next is known
	next    uint32     // sequence number of the next expected byte
	closed  *time.Time // FIN or RST seen
}

type rawSegment struct {
	side int // index into flow.ends of the sender
	t    time.Time
	data []byte
}

// flow is one TCP connection being reassembled. Its two ends are in the
// order they were first seen; which one is the client is decided by SYN,
// or at the end by finish.
type flow struct {
	ends        [2]netip.AddrPort
	halves      [2]half
	client      int // index into ends; valid if clientKnown
	clientKnown bool
	segs        []rawSegment
	err         error
}

func (f *flow) side(ap netip.AddrPort) int {
	if ap == f.ends[0] {
		return 0
	}
	return 1
}

func (f *flow) closed() bool { return f.halves[0].closed != nil || f.halves[1].closed != nil }

// assembler collects segments into flows.
type assembler struct {
	open    map[[2]netip.AddrPort]*flow // the latest flow for each pair of endpoints
	flows   []*flow                     // every flow, in order of first packet
	ignored int
}

func newAssembler() *assembler {
	return &assembler{open: make(map[[2]netip.AddrPort]*flow)}
}

// pairKey is the same for both directions of a connection.
func pairKey(a, b netip.AddrPort) [2]netip.AddrPort {
	if a.Compare(b) > 0 {
		a, b = b, a
	}
	return [2]netip.AddrPort{a, b}
}

// segment adds one TCP segment. A non-empty why says its payload is
// incomplete, which ends the flow with ErrTruncated.
func (a *assembler) segment(t time.Time, src, dst netip.AddrPort, seq uint32, flags byte, payload []byte, why string) {
	key := pairKey(src, dst)
	f := a.open[key]
	syn := flags&flagSYN != 0
	if f == nil || (syn && flags&flagACK == 0 && f.closed()) { // a new connection, or one reusing the ports of a closed one
		f = &flow{ends: [2]netip.AddrPort{src, dst}}
		a.open[key] = f
		a.flows = append(a.flows, f)
	}
	i := f.side(src)
	h := &f.halves[i]
	if syn {
		if !f.clientKnown {
			f.client, f.clientKnown = i, true
			if flags&flagACK != 0 { // SYN-ACK: the other end is the client
				f.client = 1 - i
			}
		}
		h.started, h.next = true, seq+1
		return
	}
	if f.err != nil {
		return
	}
	if len(payload) > 0 {
		if why != "" {
			f.err = fmt.Errorf("%w: %s from %v at %v", ErrTruncated, why, src, t)
			return
		}
		if !h.started { // the capture began after the handshake
			h.started, h.next = true, seq
		}
		switch d := int64(int32(seq - h.next)); {
		case d > 0:
			f.err = fmt.Errorf("%w: %d bytes from %v missing at %v", ErrGap, d, src, t)
			return
		case d < 0: // a retransmission, maybe with new bytes at the end
			if -d >= int64(len(payload)) {
				payload = nil
			} else {
				payload = payload[-d:]
			}
		}
		if len(payload) > 0 {
			f.segs = append(f.segs, rawSegment{side: i, t: t, data: bytes.Clone(payload)})
			h.next += uint32(len(payload))
		}
	}
	if flags&(flagFIN|flagRST) != 0 && h.closed == nil {
		h.closed = &t
	}
}

// Well-known MQTT server ports, the last resort for telling the client
// from the server.
var serverPorts = map[uint16]bool{1883: true, 8883: true}

func (a *assembler) finish() *Capture {
	c := &Capture{Ignored: a.ignored, Conns: make([]Conn, 0, len(a.flows))}
	for _, f := range a.flows {
		client, ok := f.client, f.clientKnown
		if !ok {
			client, ok = clientByContent(f)
		}
		if !ok {
			switch {
			case serverPorts[f.ends[1].Port()] && !serverPorts[f.ends[0].Port()]:
				client, ok = 0, true
			case serverPorts[f.ends[0].Port()] && !serverPorts[f.ends[1].Port()]:
				client, ok = 1, true
			}
		}
		conn := Conn{Client: f.ends[client], Server: f.ends[1-client], Err: f.err}
		if !ok && conn.Err == nil {
			conn.Err = fmt.Errorf("%w: %v and %v", ErrDirection, f.ends[0], f.ends[1])
		}
		conn.ClientClose = f.halves[client].closed
		conn.ServerClose = f.halves[1-client].closed
		conn.Segments = make([]Segment, len(f.segs))
		var head []byte // the first bytes from the client
		for j, s := range f.segs {
			d := FromServer
			if s.side == client {
				d = FromClient
				if len(head) < 3 {
					head = append(head, s.data[:min(len(s.data), 3-len(head))]...)
				}
			}
			conn.Segments[j] = Segment{Dir: d, Time: s.t, Data: s.data}
		}
		if len(head) == 3 && head[0] == 0x16 && head[1] == 0x03 && head[2] <= 0x04 { // a TLS handshake record
			conn.Transport = TLS
		}
		c.Conns = append(c.Conns, conn)
	}
	return c
}

// clientByContent picks the client of a flow whose SYN wasn't captured:
// the end whose first bytes are an MQTT CONNECT.
func clientByContent(f *flow) (int, bool) {
	seen := [2]bool{}
	for _, s := range f.segs {
		if seen[s.side] {
			continue
		}
		seen[s.side] = true
		if s.data[0] == 0x10 {
			return s.side, true
		}
	}
	return 0, false
}

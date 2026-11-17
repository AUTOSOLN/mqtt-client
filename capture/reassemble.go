package capture

import (
	"bytes"
	"fmt"
	"net/netip"
	"slices"
	"time"
)

// TCP flags.
const (
	flagFIN = 0x01
	flagSYN = 0x02
	flagRST = 0x04
	flagACK = 0x10
)

// maxPending bounds the out-of-order bytes held for one direction while
// waiting for a missing segment. Past it, the bytes are taken as lost.
const maxPending = 4 << 20

// early is a segment that arrived before the bytes in front of it.
type early struct {
	seq  uint32
	data []byte
}

// half is one direction of a flow.
type half struct {
	started bool       // next is known
	next    uint32     // sequence number of the next expected byte
	closed  *time.Time // FIN or RST seen
	pending []early    // out-of-order segments, in arrival order
	held    int        // bytes in pending
	gapAt   int        // len(flow.segs) when pending last became non-empty
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
	open       map[[2]netip.AddrPort]*flow // the latest flow for each pair of endpoints
	flows      []*flow                     // every flow, in order of first packet
	ignored    int
	maxPending int // maxPending, except in tests
}

func newAssembler() *assembler {
	return &assembler{open: make(map[[2]netip.AddrPort]*flow), maxPending: maxPending}
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
		if int32(seq-h.next) > 0 { // bytes in front of it are missing, for now
			if len(h.pending) == 0 {
				h.gapAt = len(f.segs)
			}
			h.pending = append(h.pending, early{seq: seq, data: bytes.Clone(payload)})
			h.held += len(payload)
			if h.held > a.maxPending {
				f.err = fmt.Errorf("%w: more than %d bytes from %v waited for a missing segment at %v", ErrGap, a.maxPending, src, t)
				f.segs = f.segs[:h.gapAt]
			}
			return
		}
		f.deliver(i, t, seq, payload)
		f.drainPending(i, t)
	}
	if flags&(flagFIN|flagRST) != 0 && h.closed == nil {
		h.closed = &t
	}
}

// deliver appends the bytes of a segment that aren't already in the
// stream; seq is at or before the next expected byte.
func (f *flow) deliver(i int, t time.Time, seq uint32, payload []byte) {
	h := &f.halves[i]
	if behind := int64(h.next - seq); behind >= int64(len(payload)) { // a retransmission of bytes we have
		return
	} else if behind > 0 {
		payload = payload[behind:]
	}
	f.segs = append(f.segs, rawSegment{side: i, t: t, data: bytes.Clone(payload)})
	h.next += uint32(len(payload))
}

// drainPending delivers held segments that the stream has caught up with.
func (f *flow) drainPending(i int, t time.Time) {
	h := &f.halves[i]
	for {
		j := slices.IndexFunc(h.pending, func(e early) bool { return int32(e.seq-h.next) <= 0 })
		if j < 0 {
			return
		}
		e := h.pending[j]
		h.pending = slices.Delete(h.pending, j, j+1)
		h.held -= len(e.data)
		f.deliver(i, t, e.seq, e.data)
	}
}

// Well-known MQTT server ports, the last resort for telling the client
// from the server.
var serverPorts = map[uint16]bool{1883: true, 8883: true}

func (a *assembler) finish() *Capture {
	c := &Capture{Ignored: a.ignored, Conns: make([]Conn, 0, len(a.flows))}
	for _, f := range a.flows {
		// A segment still missing at the end is a gap: nothing either side
		// sent after it opened can be placed reliably.
		cut := len(f.segs)
		for i, h := range f.halves {
			if len(h.pending) == 0 {
				continue
			}
			if f.err == nil {
				f.err = fmt.Errorf("%w: bytes from %v before sequence number %d were never captured", ErrGap, f.ends[i], h.pending[0].seq)
			}
			cut = min(cut, h.gapAt)
		}
		f.segs = f.segs[:cut]
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

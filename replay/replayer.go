package replay

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/capture"
)

// Errors returned by a Replayer.
var (
	// ErrExhausted is returned by Dial once every timeline has been
	// replayed, or Run has returned.
	ErrExhausted = errors.New("replay: no more captured connections")

	// ErrRunning is returned by a second call to Run.
	ErrRunning = errors.New("replay: Run already called")
)

// Divergence is a difference between what the live client did and what
// the timeline says the captured client did.
type Divergence struct {
	Conn   int    // index of the timeline
	Index  int    // index of the timeline packet involved, or -1
	Detail string // what differed
}

func (d Divergence) String() string {
	if d.Index < 0 {
		return fmt.Sprintf("connection %d: %s", d.Conn, d.Detail)
	}
	return fmt.Sprintf("connection %d, packet %d: %s", d.Conn, d.Index, d.Detail)
}

// Result is what a replay found.
type Result struct {
	Replayed    int // timelines a live client connected for
	Divergences []Divergence
}

// Replayer serves timelines to a live client through Server.Dial: set
// Server.Dial to its Dial method, and run Run in a goroutine you wait for.
//
// For each timeline it plays the broker's packets in order, in lockstep
// with the live client:
//
//   - A client packet that starts an exchange (CONNECT, PUBLISH, SUBSCRIBE,
//     UNSUBSCRIBE, PUBREL, DISCONNECT, AUTH) waits for the live client's
//     next such packet. Differences are recorded in Result; a different
//     packet type ends the connection.
//   - Client acknowledgements of broker PUBLISHes are matched by packet
//     identifier, not position, so a timeline whose broker PUBLISHes were
//     reordered or dropped still replays. A broker PUBREL waits for the
//     live PUBREC with its identifier.
//   - Broker packets go out as their Raw bytes, except that the packet
//     identifiers of acknowledgements to the client's own requests are
//     rewritten to the ones the live client chose.
//   - Captured PINGREQ and PINGRESP are skipped; a live PINGREQ is
//     answered at once.
//   - A captured broker close closes the connection. A captured client
//     close waits for the live client to close.
//   - At the end of a timeline without a close, the Replayer keeps
//     answering until the live client disconnects, so the caller decides
//     when the run ends; Played tells it when.
type Replayer struct {
	timelines []Timeline
	conns     chan net.Conn // server ends of pipes, from Dial to Run
	done      chan struct{} // closed when Run returns
	played    chan struct{} // see Played
	playOnce  sync.Once

	mu      sync.Mutex
	running bool
}

// New returns a Replayer for timelines, which are replayed in order, one
// per Dial. Each timeline must start with the client's CONNECT (Auto or
// captured): a capture that begins in the middle of a session can be
// decoded and read, but not replayed.
func New(timelines []Timeline) (*Replayer, error) {
	for i, tl := range timelines {
		switch tl.Version {
		case mqttclient.MQTT31, mqttclient.MQTT311, mqttclient.MQTT5:
		default:
			return nil, fmt.Errorf("replay: timeline %d has protocol version %d", i, tl.Version)
		}
		if j := slices.IndexFunc(tl.Packets, func(p Packet) bool { return p.Dir == capture.FromClient && !p.Close }); j < 0 || tl.Packets[j].Type != Connect {
			return nil, fmt.Errorf("replay: timeline %d doesn't start with the client's CONNECT; a capture taken in the middle of a session can't be replayed", i)
		}
	}
	return &Replayer{timelines: timelines, conns: make(chan net.Conn), done: make(chan struct{}), played: make(chan struct{})}, nil
}

// Played is closed once the last timeline has been played to its end (the
// live client may still be sending acknowledgements, and the Replayer
// keeps answering until it disconnects), or Run has returned. A tool
// replaying a capture waits on it to know when to stop.
func (r *Replayer) Played() <-chan struct{} { return r.played }

func (r *Replayer) markPlayed() { r.playOnce.Do(func() { close(r.played) }) }

// Dial has the signature of Server.Dial. Each call hands a new in-memory
// connection to Run, which serves the next timeline on it, and returns the
// client's end. It returns ErrExhausted once Run has returned.
func (r *Replayer) Dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case r.conns <- server:
		return client, nil
	case <-ctx.Done():
		_ = client.Close() // never used
		_ = server.Close()
		return nil, ctx.Err()
	case <-r.done:
		_ = client.Close()
		_ = server.Close()
		return nil, ErrExhausted
	}
}

// Run serves the timelines, each on the connection of one Dial, until all
// are replayed or ctx is cancelled. It returns what it found, and ctx's
// error if ctx ended it early.
func (r *Replayer) Run(ctx context.Context) (Result, error) {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return Result{}, ErrRunning
	}
	r.running = true
	r.mu.Unlock()
	defer close(r.done)
	defer r.markPlayed()

	var res Result
	for i := range r.timelines {
		var nc net.Conn
		select {
		case nc = <-r.conns:
		case <-ctx.Done():
			return res, ctx.Err()
		}
		res.Replayed++
		s := &session{tl: &r.timelines[i], conn: i, nc: nc, res: &res,
			idMap: make(map[uint16]uint16), want: make(map[uint16]byte)}
		if i == len(r.timelines)-1 {
			s.played = r.markPlayed
		}
		s.serve(ctx)
		if err := ctx.Err(); err != nil {
			return res, err
		}
	}
	return res, nil
}

// session replays one timeline on one connection.
type session struct {
	tl   *Timeline
	conn int
	nc   net.Conn
	res  *Result
	in   chan liveEvent

	idMap map[uint16]uint16 // captured identifier of a client request -> live identifier
	want  map[uint16]byte   // identifier of a broker PUBLISH -> the client acknowledgement still expected
	last  map[byte]Packet   // the latest live CONNECT, SUBSCRIBE and UNSUBSCRIBE, for Auto answers

	played func() // called when the timeline has been played, for the last timeline; nil otherwise
}

// liveEvent is a packet from the live client, or the error that ended its
// stream.
type liveEvent struct {
	p   Packet
	err error
}

func (s *session) diverge(i int, format string, args ...any) {
	s.res.Divergences = append(s.res.Divergences, Divergence{Conn: s.conn, Index: i, Detail: fmt.Sprintf(format, args...)})
}

// serve replays the timeline. It owns the reader goroutine and waits for
// it before returning; it closes nc on return or when ctx is cancelled.
func (s *session) serve(ctx context.Context) {
	s.in = make(chan liveEvent, 16)
	s.last = make(map[byte]Packet)
	quit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { s.read(quit) })
	stop := context.AfterFunc(ctx, func() { _ = s.nc.Close() })
	defer func() {
		stop()
		_ = s.nc.Close() // also ends the reader
		close(quit)
		wg.Wait()
	}()

	for i, p := range s.tl.Packets {
		if !s.step(i, p) {
			return
		}
	}
	if s.played != nil {
		s.played()
	}
	s.drain(-1)
}

// read decodes packets from the live client and sends them to serve,
// ending with the error that ended the stream.
func (s *session) read(quit <-chan struct{}) {
	br := bufio.NewReader(s.nc)
	version := s.tl.Version
	for {
		raw, hdr, err := readRaw(br)
		var ev liveEvent
		if err != nil {
			ev.err = err
		} else {
			ev.p = decodePacket(raw, hdr, version)
		}
		select {
		case s.in <- ev:
		case <-quit:
			return
		}
		if err != nil {
			return
		}
	}
}

// readRaw reads one packet's bytes, and returns them with the length of
// the fixed header.
func readRaw(br *bufio.Reader) ([]byte, int, error) {
	first, err := br.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	raw := []byte{first}
	rem, mul := 0, 1
	for {
		b, err := br.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		raw = append(raw, b)
		rem += int(b&0x7f) * mul
		mul *= 128
		if b&0x80 == 0 {
			break
		}
		if len(raw) == 5 {
			return nil, 0, fmt.Errorf("%w: remaining length longer than 4 bytes", mqttclient.ErrMalformedPacket)
		}
	}
	hdr := len(raw)
	raw = append(raw, make([]byte, rem)...)
	if _, err := io.ReadFull(br, raw[hdr:]); err != nil {
		return nil, 0, err
	}
	return raw, hdr, nil
}

// step plays timeline packet i. It returns false when the connection is
// over.
func (s *session) step(i int, p Packet) bool {
	switch {
	case p.Close && p.Dir == capture.FromServer:
		return false
	case p.Close:
		s.drain(i)
		return false
	case p.Dir == capture.FromClient:
		return s.clientStep(i, p)
	default:
		return s.brokerStep(i, p)
	}
}

func (s *session) clientStep(i int, p Packet) bool {
	switch p.Type {
	case Puback, Pubrec, Pubcomp, Pingreq:
		return true // acknowledgements are matched by identifier; pings depend on timing
	}
	live, ok := s.nextExchange(i, p)
	if !ok {
		return false
	}
	if live.Type != p.Type {
		s.diverge(i, "live client sent %s, capture has %s", live.TypeName(), p.TypeName())
		return false
	}
	if live.Err != nil {
		s.diverge(i, "live client sent a malformed %s: %v", live.TypeName(), live.Err)
		return false
	}
	switch live.Type {
	case Connect, Subscribe, Unsubscribe:
		s.last[live.Type] = live
	}
	if !p.Auto {
		if !s.compare(i, p, live) {
			return false
		}
	}
	switch p.Type {
	case Subscribe, Unsubscribe:
		s.idMap[p.PacketID] = live.PacketID
	case Publish:
		if p.QoS > 0 {
			s.idMap[p.PacketID] = live.PacketID
		}
	case Pubrel:
		if got, ok := s.idMap[p.PacketID]; ok && got != live.PacketID {
			s.diverge(i, "PUBREL for packet %d, want %d", live.PacketID, got)
		}
	}
	return true
}

// nextExchange returns the live client's next packet that isn't an
// acknowledgement or a ping, handling those on the way.
func (s *session) nextExchange(i int, want Packet) (Packet, bool) {
	for {
		ev := <-s.in
		if ev.err != nil {
			s.diverge(i, "live client closed the connection (%v) before sending %s", ev.err, want.TypeName())
			return Packet{}, false
		}
		if !s.aside(i, ev.p) {
			return ev.p, true
		}
	}
}

// aside handles a live acknowledgement or PINGREQ, and reports whether p
// was one.
func (s *session) aside(i int, p Packet) bool {
	switch p.Type {
	case Pingreq:
		s.write(i, pingresp)
		return true
	case Puback, Pubrec, Pubcomp:
		if p.Err != nil {
			s.diverge(i, "live client sent a malformed %s: %v", p.TypeName(), p.Err)
			return true
		}
		switch want, ok := s.want[p.PacketID]; {
		case !ok:
			s.diverge(i, "live client sent %s for packet %d, which the broker never sent", p.TypeName(), p.PacketID)
		case want != p.Type:
			s.diverge(i, "live client sent %s for packet %d, want %s", p.TypeName(), p.PacketID, typeNames[want])
		case p.Type == Pubrec:
			s.want[p.PacketID] = Pubrel // the broker's PUBREL is next
		default:
			delete(s.want, p.PacketID)
		}
		return true
	}
	return false
}

// compare records the differences between a captured client packet and
// the live one. It returns false if they are too different to go on.
func (s *session) compare(i int, want, live Packet) bool {
	switch want.Type {
	case Connect:
		c, l := want.Connect, live.Connect
		if c.Version != l.Version {
			s.diverge(i, "live client uses protocol version %d, the capture %d", l.Version, c.Version)
			return false
		}
		if c.ClientID != l.ClientID {
			s.diverge(i, "client ID %q, capture has %q", l.ClientID, c.ClientID)
		}
		if c.Clean != l.Clean {
			s.diverge(i, "clean %v, capture has %v", l.Clean, c.Clean)
		}
		if c.KeepAlive != l.KeepAlive {
			s.diverge(i, "keepalive %d, capture has %d", l.KeepAlive, c.KeepAlive)
		}
		if (c.Will == nil) != (l.Will == nil) || (c.Will != nil && c.Will.Topic != l.Will.Topic) {
			s.diverge(i, "Will %s, capture has %s", willTopic(l.Will), willTopic(c.Will))
		}
	case Publish:
		if want.Topic != live.Topic || want.QoS != live.QoS || want.Retain != live.Retain || want.Dup != live.Dup || len(want.Payload) != len(live.Payload) {
			s.diverge(i, "PUBLISH %q QoS %d retain %v dup %v with %d bytes, capture has %q QoS %d retain %v dup %v with %d bytes",
				live.Topic, live.QoS, live.Retain, live.Dup, len(live.Payload), want.Topic, want.QoS, want.Retain, want.Dup, len(want.Payload))
		}
	case Subscribe, Unsubscribe:
		if !slices.Equal(want.Filters, live.Filters) {
			s.diverge(i, "%s filters %+v, capture has %+v", live.TypeName(), live.Filters, want.Filters)
		}
	}
	return true
}

func willTopic(w *mqttclient.Message) string {
	if w == nil {
		return "none"
	}
	return fmt.Sprintf("%q", w.Topic)
}

func (s *session) brokerStep(i int, p Packet) bool {
	if p.Close {
		return false
	}
	raw := p.Raw
	switch {
	case p.Type == Pingresp:
		return true
	case p.Auto:
		var err error
		if raw, err = s.autoAnswer(p); err != nil {
			s.diverge(i, "%v", err)
			return false
		}
	case p.Type == Puback || p.Type == Pubrec || p.Type == Pubcomp || p.Type == Suback || p.Type == Unsuback:
		if p.Err == nil {
			live, ok := s.idMap[p.PacketID]
			if !ok {
				s.diverge(i, "broker %s for packet %d, which the live client never sent", p.TypeName(), p.PacketID)
			} else {
				raw = withPacketID(raw, live)
			}
		}
	case p.Type == Pubrel && p.Err == nil:
		if !s.awaitPubrec(i, p.PacketID) {
			return false
		}
		s.want[p.PacketID] = Pubcomp
	}
	if !s.write(i, raw) {
		return false
	}
	if p.Type == Publish && p.Err == nil && p.QoS > 0 {
		s.want[p.PacketID] = Puback
		if p.QoS == 2 {
			s.want[p.PacketID] = Pubrec
		}
	}
	return true
}

// awaitPubrec waits until the live client has sent PUBREC for id.
func (s *session) awaitPubrec(i int, id uint16) bool {
	for s.want[id] == Pubrec {
		ev := <-s.in
		if ev.err != nil {
			s.diverge(i, "live client closed the connection (%v) before PUBREC for packet %d", ev.err, id)
			return false
		}
		if !s.aside(i, ev.p) {
			s.diverge(i, "live client sent %s while the broker waited for PUBREC %d", ev.p.TypeName(), id)
			return false
		}
	}
	return true
}

// drain handles the live client after the timeline's last exchange, until
// it closes, then reports acknowledgements it never sent.
func (s *session) drain(i int) {
	for {
		ev := <-s.in
		if ev.err != nil {
			break
		}
		if s.aside(i, ev.p) || ev.p.Type == Disconnect {
			continue
		}
		s.diverge(i, "live client sent %s after the end of the captured connection", ev.p.TypeName())
	}
	for id, want := range s.want {
		s.diverge(i, "live client never sent %s for packet %d", typeNames[want], id)
	}
}

// write sends raw to the live client.
func (s *session) write(i int, raw []byte) bool {
	if _, err := s.nc.Write(raw); err != nil {
		s.diverge(i, "live client closed the connection: %v", err)
		return false
	}
	return true
}

var pingresp = []byte{Pingresp << 4, 0}

// withPacketID returns a copy of an acknowledgement with its packet
// identifier, the two bytes after the fixed header, set to id.
func withPacketID(raw []byte, id uint16) []byte {
	_, hdr, err := frame(raw)
	if err != nil || len(raw) < hdr+2 {
		return raw
	}
	out := slices.Clone(raw)
	out[hdr], out[hdr+1] = byte(id>>8), byte(id)
	return out
}

// autoAnswer builds an Auto CONNACK, SUBACK or UNSUBACK that accepts the
// live client's latest request.
func (s *session) autoAnswer(p Packet) ([]byte, error) {
	v5 := s.tl.Version == mqttclient.MQTT5
	var body []byte
	switch p.Type {
	case Connack:
		body = []byte{0, 0} // no session present, accepted
		if v5 {
			body = append(body, 0) // no properties
		}
	case Suback, Unsuback:
		reqType := Subscribe
		if p.Type == Unsuback {
			reqType = Unsubscribe
		}
		req, ok := s.last[reqType]
		if !ok {
			return nil, fmt.Errorf("Auto %s with no live %s to answer", p.TypeName(), typeNames[reqType])
		}
		body = []byte{byte(req.PacketID >> 8), byte(req.PacketID)}
		if v5 {
			body = append(body, 0)
		}
		for _, f := range req.Filters {
			switch {
			case p.Type == Suback:
				body = append(body, f.QoS)
			case v5:
				body = append(body, 0) // success
			}
		}
	default:
		return nil, fmt.Errorf("Auto %s: only CONNACK, SUBACK and UNSUBACK can be Auto from the broker", p.TypeName())
	}
	return append(appendLength([]byte{p.Type << 4}, len(body)), body...), nil
}

package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/replay"
)

// ErrDiverged is returned by Session.CheckReplay when the live client did
// something other than the captured client did. Tools exit 1 for it.
var ErrDiverged = errors.New("the live client diverged from the capture")

// RegisterReplayInbound adds -replay-inbound to fs. Only tools that
// subscribe register it.
func (f *ConnFlags) RegisterReplayInbound(fs *flag.FlagSet) {
	fs.BoolVar(&f.ReplayInbound, "replay-inbound", false,
		"with -replay, the capture is of publishing clients (edge nodes): deliver their messages to this tool as a subscriber")
}

// checkReplayFlags checks the -replay flags against each other and the
// connection flags.
func (f *ConnFlags) checkReplayFlags() error {
	if f.Replay == "" {
		if f.ReplayConns != "" || f.ReplayInbound {
			return errors.New("-replay-conn and -replay-inbound need -replay")
		}
		return nil
	}
	switch {
	case f.TLS:
		return errors.New("-replay can't be used with -tls: a replay runs in memory, without TLS")
	case f.Config != "":
		return errors.New("-replay can't be used with -config: the capture replaces the servers")
	}
	return nil
}

// replaySetup is a capture loaded for -replay.
type replaySetup struct {
	rp        *replay.Replayer
	file      string
	conns     []int          // indexes of the captured connections replayed
	timelines int            // timelines the Replayer serves: len(conns), or 1 with -replay-inbound
	server    netip.AddrPort // the captured server's address
}

// loadReplay reads the -replay capture and builds its Replayer.
func (f *ConnFlags) loadReplay() (*replaySetup, error) {
	file, err := os.Open(f.Replay)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	c, err := capture.Read(file)
	if err != nil {
		return nil, fmt.Errorf("-replay %s: %w", f.Replay, err)
	}
	idx, err := replayConns(f.ReplayConns, c)
	if err != nil {
		return nil, err
	}
	tls := make([]replay.Timeline, 0, len(idx))
	for _, i := range idx {
		tl, err := replay.Decode(c.Conns[i], 0)
		if err != nil {
			return nil, fmt.Errorf("-replay connection %d (%v to %v): %w; pick others with -replay-conn",
				i, c.Conns[i].Client, c.Conns[i].Server, err)
		}
		tls = append(tls, tl)
	}
	if f.ReplayInbound {
		in, err := replay.Inbound(tls)
		if err != nil {
			return nil, err
		}
		tls = []replay.Timeline{in}
	}
	rp, err := replay.New(tls)
	if err != nil {
		return nil, err
	}
	return &replaySetup{rp: rp, file: f.Replay, conns: idx, timelines: len(tls), server: c.Conns[idx[0]].Server}, nil
}

// replayConns parses -replay-conn. Without it, every plain-TCP connection
// is replayed, in capture order.
func replayConns(list string, c *capture.Capture) ([]int, error) {
	var idx []int
	if list == "" {
		for i, conn := range c.Conns {
			if conn.Transport == capture.Plain {
				idx = append(idx, i)
			}
		}
		if len(idx) == 0 {
			return nil, fmt.Errorf("-replay: the capture has no plain-TCP connections (%d connections, %d frames ignored)", len(c.Conns), c.Ignored)
		}
		return idx, nil
	}
	for s := range strings.SplitSeq(list, ",") {
		i, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || i < 0 || i >= len(c.Conns) {
			return nil, fmt.Errorf("-replay-conn: %q is not a connection index from 0 to %d (see mqttpcap -list)", s, len(c.Conns)-1)
		}
		idx = append(idx, i)
	}
	return idx, nil
}

// addressSet reports whether -address was given on the command line.
func (f *ConnFlags) addressSet() bool {
	set := false
	if f.fs != nil {
		f.fs.Visit(func(fl *flag.Flag) { set = set || fl.Name == "address" })
	}
	return set
}

// replayRun is a Replayer running for a Session.
type replayRun struct {
	setup    *replaySetup
	stop     context.CancelFunc // nil until startReplay
	wg       sync.WaitGroup
	res      replay.Result
	err      error
	finished bool // finishReplay has run
}

// startReplay starts the session's Replayer; finishReplay stops it.
func (s *Session) startReplay() {
	if s.replay == nil || s.replay.stop != nil {
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	s.replay.stop = stop
	s.replay.wg.Go(func() { s.replay.res, s.replay.err = s.replay.setup.rp.Run(ctx) })
}

// finishReplay stops the Replayer, once the client has disconnected, and
// logs what it found. It is safe to call more than once.
func (s *Session) finishReplay() {
	r := s.replay
	if r == nil || r.stop == nil || r.finished {
		return
	}
	r.finished = true
	r.stop()
	r.wg.Wait()
	if errors.Is(r.err, context.Canceled) {
		r.err = nil // the session stopped it
	}
	Logf("replay: %d of %d captured connections replayed, %d divergences", r.res.Replayed, r.setup.timelines, len(r.res.Divergences))
	for _, d := range r.res.Divergences {
		Logf("  DIVERGENCE %s", d)
	}
}

// Played is closed once the whole -replay capture has been played (see
// replay.Replayer.Played). Without -replay it is nil, so it never fires in
// a select.
func (s *Session) Played() <-chan struct{} {
	if s.replay == nil {
		return nil
	}
	return s.replay.setup.rp.Played()
}

// CheckReplay fails, with an error wrapping ErrDiverged, if the live client
// diverged from the -replay capture or didn't get through all of it. Call
// it after Disconnect. Without -replay it returns nil.
func (s *Session) CheckReplay() error {
	r := s.replay
	if r == nil {
		return nil
	}
	s.finishReplay()
	switch {
	case r.err != nil:
		return fmt.Errorf("replay: %w", r.err)
	case len(r.res.Divergences) > 0:
		return fmt.Errorf("%w: %d divergences, the first: %s", ErrDiverged, len(r.res.Divergences), r.res.Divergences[0])
	case r.res.Replayed < r.setup.timelines:
		return fmt.Errorf("%w: only %d of %d captured connections were replayed", ErrDiverged, r.res.Replayed, r.setup.timelines)
	}
	return nil
}

// replayReport is the report's replay section, or nil without -replay.
func (s *Session) replayReport() *ReportReplay {
	r := s.replay
	if r == nil {
		return nil
	}
	s.finishReplay()
	rr := &ReportReplay{File: r.setup.file, Connections: r.setup.conns, Inbound: s.replayInbound,
		Replayed: r.res.Replayed, Divergences: []string{}}
	for _, d := range r.res.Divergences {
		rr.Divergences = append(rr.Divergences, d.String())
	}
	return rr
}

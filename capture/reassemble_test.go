package capture

import (
	"errors"
	"net/netip"
	"testing"
	"time"
)

// Tested here, with a small limit, because reaching the real 4 MiB limit
// through a capture file takes too long for a unit test.
func TestAssembler_TooMuchWaitingForAGap_ErrGapBeforeTheGap(t *testing.T) {
	c := netip.MustParseAddrPort("10.0.0.1:50000")
	s := netip.MustParseAddrPort("10.0.0.2:1883")
	t0 := time.Unix(0, 0)
	a := newAssembler()
	a.maxPending = 10
	a.segment(t0, c, s, 1000, flagSYN, nil, "")
	a.segment(t0, c, s, 1001, flagACK, []byte("abc"), "")
	a.segment(t0, s, c, 5000, flagACK, []byte("reply"), "")  // before the gap: kept
	a.segment(t0, c, s, 1010, flagACK, []byte("123456"), "") // 1004..1009 missing
	a.segment(t0, s, c, 5005, flagACK, []byte("later"), "")  // after the gap opened: dropped
	a.segment(t0, c, s, 1016, flagACK, []byte("7890X"), "")  // 11 bytes held: over the limit

	conns := a.finish().Conns
	if len(conns) != 1 {
		t.Fatalf("got %d connections, want 1", len(conns))
	}
	conn := conns[0]
	if !errors.Is(conn.Err, ErrGap) {
		t.Errorf("got Err %v, want ErrGap", conn.Err)
	}
	var up, down string
	for _, seg := range conn.Segments {
		if seg.Dir == FromClient {
			up += string(seg.Data)
		} else {
			down += string(seg.Data)
		}
	}
	if up != "abc" || down != "reply" {
		t.Errorf("got streams %q and %q, want \"abc\" and \"reply\": only what came before the gap", up, down)
	}
}

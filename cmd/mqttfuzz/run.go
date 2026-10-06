package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// runner executes a scenario over one connection and tracks whether any
// expectation failed.
type runner struct {
	rc          *rawConn
	recvTimeout time.Duration
	writeTO     time.Duration
	failed      bool
}

// run executes every step in order. It returns an error only for a setup or
// I/O problem that aborts the run; a failed expectation is recorded and
// reported through runner.failed (and the process exit code) so the rest of
// the scenario still runs.
func (r *runner) run(ctx context.Context, sc *Scenario, version byte) error {
	for i, s := range sc.Steps {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if s.Delay > 0 {
			time.Sleep(s.Delay)
		}
		label := s.Label
		if label == "" {
			label = s.Type
		}
		logf("step %d: %s", i+1, label)
		if err := r.step(&s, version); err != nil {
			return fmt.Errorf("step %d (%s): %w", i+1, label, err)
		}
	}
	return nil
}

func (r *runner) step(s *Step, version byte) error {
	switch s.Type {
	case "sleep":
		d := s.Duration
		if d == 0 {
			d = s.Timeout
		}
		time.Sleep(d)
		return nil

	case "recv":
		return r.recv(s)

	case "expect-close":
		return r.expectClose(s)

	default:
		b, err := encodeStep(s, version)
		if err != nil {
			return err
		}
		logf("  send %s (%d bytes) %s", strings.ToUpper(s.Type), len(b), hexPreview(b))
		if err := r.rc.writeBytes(b, r.writeTO); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		return nil
	}
}

// recv reads Count packets (default 1) and, if Expect is set, checks the
// first against it. Expect may be a packet type name, "close" (the server
// hung up) or "none" (nothing arrived before the timeout).
func (r *runner) recv(s *Step) error {
	to := s.Timeout
	if to == 0 {
		to = r.recvTimeout
	}
	count := s.Count
	if count < 1 {
		count = 1
	}

	var first *inPacket
	var readErr error
	for n := 0; n < count; n++ {
		in, err := r.rc.readPacket(to)
		if err != nil {
			readErr = err
			break
		}
		if first == nil {
			first = in
		}
		logf("  recv %s", describe(in))
	}

	if s.Expect == "" {
		if readErr != nil && !isTimeout(readErr) && first == nil {
			logf("  (read ended: %v)", classify(readErr))
		}
		return nil
	}

	switch strings.ToLower(s.Expect) {
	case "close":
		if isClosed(readErr) {
			logf("  OK expected close: %v", classify(readErr))
		} else {
			r.fail("expected connection close, got %s", outcome(first, readErr))
		}
	case "none":
		if isTimeout(readErr) && first == nil {
			logf("  OK expected silence (timeout, no packet)")
		} else {
			r.fail("expected no packet, got %s", outcome(first, readErr))
		}
	default:
		want, ok := packetTypeByName(s.Expect)
		if !ok {
			return fmt.Errorf("unknown expect value %q", s.Expect)
		}
		switch {
		case first == nil:
			r.fail("expected %s, got %s", strings.ToUpper(s.Expect), outcome(nil, readErr))
		case first.Type == want:
			logf("  OK expected %s", strings.ToUpper(s.Expect))
		default:
			r.fail("expected %s, got %s", strings.ToUpper(s.Expect), first.Name)
		}
	}
	return nil
}

// expectClose asserts the server closes the connection within the timeout,
// draining any packets it sends first.
func (r *runner) expectClose(s *Step) error {
	to := s.Timeout
	if to == 0 {
		to = r.recvTimeout
	}
	deadline := time.Now().Add(to)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			r.fail("expected connection close within %s, still open", to)
			return nil
		}
		in, err := r.rc.readPacket(remaining)
		if err != nil {
			if isClosed(err) {
				logf("  OK connection closed: %v", classify(err))
			} else {
				r.fail("expected close, read ended with: %v", classify(err))
			}
			return nil
		}
		logf("  recv %s (while awaiting close)", describe(in))
	}
}

func (r *runner) fail(format string, args ...any) {
	r.failed = true
	logf("  FAIL: "+format, args...)
}

func describe(in *inPacket) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s", in.Name)
	if in.Flags != 0 {
		fmt.Fprintf(&b, " flags=0x%x", in.Flags)
	}
	if in.Type == packets.Connack {
		fmt.Fprintf(&b, " session-present=%t", in.Session)
	}
	if in.PacketID != 0 {
		fmt.Fprintf(&b, " id=%d", in.PacketID)
	}
	if in.Topic != "" {
		fmt.Fprintf(&b, " topic=%q", in.Topic)
	}
	if in.ReasonSet {
		fmt.Fprintf(&b, " reason=0x%02x", in.Reason)
	}
	if len(in.Reasons) > 0 {
		fmt.Fprintf(&b, " reasons=%s", hexBytes(in.Reasons))
	}
	if in.Err != nil {
		fmt.Fprintf(&b, " [decode: %v]", in.Err)
	}
	fmt.Fprintf(&b, " %s", hexPreview(in.Raw))
	return b.String()
}

func outcome(in *inPacket, err error) string {
	if in != nil {
		return in.Name
	}
	if err != nil {
		return classify(err)
	}
	return "nothing"
}

// classify renders a read error compactly.
func classify(err error) string {
	switch {
	case err == nil:
		return "no error"
	case errors.Is(err, io.EOF):
		return "EOF (server closed)"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected EOF mid-frame"
	case isTimeout(err):
		return "timeout"
	}
	return err.Error()
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// isClosed reports whether err means the peer closed the connection.
func isClosed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "broken pipe")
}

func hexPreview(b []byte) string {
	const max = 32
	if len(b) <= max {
		return hexBytes(b)
	}
	return hexBytes(b[:max]) + fmt.Sprintf("…(+%d)", len(b)-max)
}

func hexBytes(b []byte) string {
	var sb strings.Builder
	for i, c := range b {
		if i > 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "%02x", c)
	}
	return sb.String()
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

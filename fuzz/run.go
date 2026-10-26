package fuzz

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/AUTOSOLN/mqtt-client/internal/pattern"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

var (
	// ErrSetup is wrapped by Run's error when the run could not take place
	// or finish: the broker was unreachable, the context ended, or a step
	// could not be encoded. It says nothing about the broker's behaviour.
	ErrSetup = errors.New("fuzz: setup failed")
	// ErrExpectation is wrapped by Run's error when the run completed but
	// an expectation failed: the broker did not behave as the scenario
	// asserts.
	ErrExpectation = errors.New("fuzz: expectation failed")
)

// RunOptions configures one run of a scenario. The zero value runs the
// scenario as written.
type RunOptions struct {
	// Address (host:port), Version (3, 4 or 5) and TLS override the
	// scenario's. TLS only turns TLS on. Without an address anywhere,
	// localhost:1883 is used.
	Address   string
	Version   byte
	TLS       bool
	TLSConfig *tls.Config

	// Dial, if set, opens the TCP connection; a TLS handshake still
	// follows when TLS is on.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)

	// Vars are token values for this run, on top of the ones the scenario
	// was loaded with. They re-resolve the steps.
	Vars map[string]string

	// Username and Password are sent by every CONNECT step that sets
	// neither username: nor password: itself.
	Username string
	Password string

	// RecvTimeout is the wait for a recv or expect-close step without a
	// timeout of its own (default 2s). DialTimeout bounds the connection
	// and TLS handshake (default 10s), WriteTimeout each write (default 5s).
	RecvTimeout  time.Duration
	DialTimeout  time.Duration
	WriteTimeout time.Duration

	// Logf, if set, receives a line per step and per packet as the run
	// goes, such as testing.T.Logf.
	Logf func(format string, args ...any)
}

// Run connects and executes every step in order. The Result is filled in
// whatever happens. The error is nil when every expectation held, wraps
// ErrExpectation when one failed, and wraps ErrSetup when the run could not
// complete.
//
// A failed expectation does not stop the run. If the server closes the
// connection, later sending steps are skipped and later reads see the close.
// A write that times out (the server stopped reading) fails the step and
// ends the run.
func (sc *Scenario) Run(ctx context.Context, o RunOptions) (Result, error) {
	res := Result{Scenario: sc.Name}
	steps := sc.Steps
	if len(o.Vars) > 0 {
		n, err := sc.withVars(o.Vars)
		if err != nil {
			res.Error = err.Error()
			return res, fmt.Errorf("%w: %w", ErrSetup, err)
		}
		steps = n.Steps
	}
	o.defaults(sc)
	res.Address, res.Version = o.Address, o.Version

	scheme := "tcp"
	if o.TLS {
		scheme = "tls"
	}
	o.Logf("mqttfuzz %s: %s %s  v%s  %d step(s)", sc.Name, scheme, o.Address, versionName(o.Version), len(steps))

	rc, err := dial(ctx, o.Dial, o.Address, o.TLS, o.TLSConfig, o.DialTimeout, o.Version)
	if err != nil {
		res.Error = err.Error()
		return res, fmt.Errorf("%w: %w", ErrSetup, err)
	}
	defer rc.close()

	r := &runner{rc: rc, o: o, version: o.Version, start: time.Now(), res: &res}
	if err := r.run(ctx, steps); err != nil {
		res.Error = err.Error()
		o.Logf("ABORT: %v", err)
		return res, fmt.Errorf("%w: %w", ErrSetup, err)
	}
	if err := res.expectationErr(); err != nil {
		o.Logf("RESULT: %d expectation(s) FAILED", len(res.Failures))
		return res, err
	}
	o.Logf("RESULT: OK")
	return res, nil
}

// expectationErr summarizes the failures, or returns nil when there are
// none.
func (r *Result) expectationErr() error {
	n := len(r.Failures)
	if n == 0 {
		return nil
	}
	more := ""
	if n > 1 {
		more = fmt.Sprintf(" (and %d more)", n-1)
	}
	return fmt.Errorf("%w: %s%s", ErrExpectation, r.Failures[0], more)
}

// defaults fills in what the options leave unset, from sc or the defaults.
func (o *RunOptions) defaults(sc *Scenario) {
	if o.Version == 0 {
		o.Version, _ = ParseVersion(sc.Version) // checked by Parse
	}
	if o.Address == "" {
		o.Address = sc.Address
	}
	if o.Address == "" {
		o.Address = "localhost:1883"
	}
	o.TLS = o.TLS || sc.TLS
	if o.RecvTimeout <= 0 {
		o.RecvTimeout = 2 * time.Second
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 10 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 5 * time.Second
	}
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
}

// errStopped ends a run early after a failure that leaves the connection
// unusable; the failure is already recorded.
var errStopped = errors.New("run stopped")

// runner executes a scenario over one connection and records the result.
type runner struct {
	rc      *rawConn
	o       RunOptions
	version byte
	start   time.Time
	res     *Result

	cur *StepResult // the step running now
	// closed is set once the server has closed the connection.
	closed bool
	// lastReason is the reason code of the latest DISCONNECT or refusing
	// CONNACK, the server's reason for a close that follows.
	lastReason *byte
}

func (r *runner) run(ctx context.Context, steps []Step) error {
	for i := range steps {
		s := &steps[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.Delay > 0 {
			if err := sleep(ctx, s.Delay); err != nil {
				return err
			}
		}
		label := s.Label
		if label == "" {
			label = s.Type
		}
		r.res.Steps = append(r.res.Steps, StepResult{Step: i + 1, Type: s.Type, Label: s.Label, Start: time.Since(r.start)})
		r.cur = &r.res.Steps[len(r.res.Steps)-1]
		r.o.Logf("step %d: %s", i+1, label)
		err := r.step(ctx, s)
		r.cur.Elapsed = time.Since(r.start) - r.cur.Start
		if errors.Is(err, errStopped) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("step %d (%s): %w", i+1, label, err)
		}
	}
	return nil
}

func (r *runner) step(ctx context.Context, s *Step) error {
	switch s.Type {
	case "sleep":
		d := s.Duration
		if d == 0 {
			d = s.Timeout
		}
		return sleep(ctx, d)
	case "recv":
		if s.expectsClose() {
			r.expectClose(s)
			return nil
		}
		return r.recv(s)
	case "expect-close":
		r.expectClose(s)
		return nil
	default:
		return r.send(s)
	}
}

// send encodes and writes a packet or raw step, Repeat times.
func (r *runner) send(s *Step) error {
	st := *s
	if st.Type == "connect" {
		if st.Username == nil && st.Password == nil {
			st.Username, st.Password = &r.o.Username, &r.o.Password
		}
	}
	n := max(st.Repeat, 1)
	for i := 0; i < n; i++ {
		if r.closed {
			r.cur.Skipped = true
			r.o.Logf("  skipped: the server closed the connection")
			return nil
		}
		b, err := encodeStep(&st, r.version)
		if err != nil {
			return err
		}
		if i < 3 {
			r.o.Logf("  send %s (%d bytes) %s", strings.ToUpper(st.Type), len(b), hexPreview(b))
		} else if i == 3 {
			r.o.Logf("  … %d more", n-3)
		}
		if err := r.write(b); err != nil {
			return err
		}
		if st.PacketID != 0 {
			st.PacketID++
			if st.PacketID == 0 {
				st.PacketID = 1
			}
		}
	}
	return nil
}

// write sends b. A connection the server closed is recorded, not an error.
func (r *runner) write(b []byte) error {
	err := r.rc.writeBytes(b, r.o.WriteTimeout)
	switch {
	case err == nil:
		r.cur.Sent += len(b)
		return nil
	case isClosed(err):
		r.o.Logf("  write failed, the server closed the connection: %v", err)
		r.markClosed()
		return nil
	case isTimeout(err):
		r.fail("write timed out after %s: the server stopped reading", r.o.WriteTimeout)
		return errStopped
	}
	return fmt.Errorf("write: %w", err)
}

// read reads one packet and records it, or records the close.
func (r *runner) read(timeout time.Duration) (*Packet, error) {
	if r.closed {
		return nil, io.EOF
	}
	in, err := r.rc.readPacket(timeout)
	if err != nil {
		if isClosed(err) {
			r.o.Logf("  connection closed: %s", classify(err))
			r.markClosed()
		}
		return nil, err
	}
	r.cur.Received = append(r.cur.Received, *in)
	switch {
	case in.typ == packets.Disconnect,
		in.typ == packets.Connack && in.ReasonCode != nil && *in.ReasonCode != 0:
		r.lastReason = in.ReasonCode
	}
	return in, nil
}

func (r *runner) markClosed() {
	if r.closed {
		return
	}
	r.closed = true
	r.cur.Closed = true
	r.cur.CloseReason = r.lastReason
}

// recv reads Count packets (default 1) and, if Expect is set, checks the
// first against it: a packet type name, or "none" (nothing arrived before
// the timeout). Reason and Reasons are checked against the first packet.
func (r *runner) recv(s *Step) error {
	to := s.Timeout
	if to == 0 {
		to = r.o.RecvTimeout
	}
	count := max(s.Count, 1)

	var first *Packet
	var readErr error
	for n := 0; n < count; n++ {
		in, err := r.read(to)
		if err != nil {
			readErr = err
			break
		}
		if first == nil {
			first = in
		}
		r.o.Logf("  recv %s", in.describe())
		r.verifyPayload(s, in)
	}

	expect := strings.ToLower(s.Expect)
	switch expect {
	case "":
		if readErr != nil && !isTimeout(readErr) && first == nil {
			r.o.Logf("  (read ended: %v)", classify(readErr))
		}
	case "none":
		if isTimeout(readErr) && first == nil {
			r.o.Logf("  OK expected silence (timeout, no packet)")
		} else {
			r.fail("expected no packet, got %s", outcome(first, readErr))
		}
		return nil
	default:
		want, _ := packetTypeByName(expect) // checked by Parse
		switch {
		case first == nil:
			r.fail("expected %s, got %s", strings.ToUpper(s.Expect), outcome(nil, readErr))
			return nil
		case first.typ != want:
			r.fail("expected %s, got %s", strings.ToUpper(s.Expect), first.describe())
			return nil
		}
		r.o.Logf("  OK expected %s", strings.ToUpper(s.Expect))
	}
	if first != nil {
		r.checkReasons(s, first)
	} else if s.Reason != nil || len(s.Reasons) > 0 {
		r.fail("expected a packet with reason codes, got %s", outcome(nil, readErr))
	}
	return nil
}

// checkReasons compares a received packet with the step's reason and
// reasons.
func (r *runner) checkReasons(s *Step, in *Packet) {
	if s.Reason != nil {
		switch {
		case in.ReasonCode == nil:
			r.fail("expected reason 0x%02x, %s carries no single reason code", *s.Reason, in.Type)
		case *in.ReasonCode != *s.Reason:
			r.fail("expected %s reason 0x%02x, got 0x%02x", in.Type, *s.Reason, *in.ReasonCode)
		default:
			r.o.Logf("  OK reason 0x%02x", *s.Reason)
		}
	}
	if len(s.Reasons) > 0 {
		if !bytes.Equal(in.ReasonCodes, s.Reasons) {
			r.fail("expected %s reasons %s, got %s", in.Type, hexBytes(s.Reasons), hexBytes(in.ReasonCodes))
		} else {
			r.o.Logf("  OK reasons %s", hexBytes(s.Reasons))
		}
	}
}

// verifyPayload checks a received PUBLISH against the recv step's
// payload-pattern and payload-size, when the step sets either.
func (r *runner) verifyPayload(s *Step, in *Packet) {
	if in.typ != packets.Publish || (s.PayloadPattern == "" && s.PayloadSize == 0) {
		return
	}
	name, size := s.PayloadPattern, s.PayloadSize
	if name == "" {
		name = "ascii"
	}
	if size == 0 {
		size = -1
	}
	if err := pattern.Verify(name, in.Payload, size); err != nil {
		r.fail("payload: %v", err)
		return
	}
	r.o.Logf("  OK payload %d bytes of %s", len(in.Payload), name)
}

// expectClose asserts the server closes the connection within the timeout,
// draining any packets it sends first. With Reason, a DISCONNECT or refusing
// CONNACK carrying that reason code must come before the close (unless
// ReasonOptional, when it is checked only if one came).
func (r *runner) expectClose(s *Step) {
	to := s.Timeout
	if to == 0 {
		to = r.o.RecvTimeout
	}
	deadline := time.Now().Add(to)
	for !r.closed {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			r.fail("expected connection close within %s, still open", to)
			return
		}
		in, err := r.read(remaining)
		if err != nil {
			if !r.closed {
				r.fail("expected close, read ended with: %v", classify(err))
				return
			}
			break
		}
		r.o.Logf("  recv %s (while awaiting close)", in.describe())
	}
	r.o.Logf("  OK connection closed")
	r.checkCloseReason(s)
}

func (r *runner) checkCloseReason(s *Step) {
	if s.Reason == nil {
		return
	}
	switch {
	case r.lastReason == nil && s.ReasonOptional:
		r.o.Logf("  OK no reason code before the close (reason-optional)")
	case r.lastReason == nil:
		r.fail("expected reason 0x%02x before the close, got no DISCONNECT or refusing CONNACK", *s.Reason)
	case *r.lastReason != *s.Reason:
		r.fail("expected reason 0x%02x before the close, got 0x%02x", *s.Reason, *r.lastReason)
	default:
		r.o.Logf("  OK reason 0x%02x before the close", *s.Reason)
	}
}

func (r *runner) fail(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if r.cur.Failure == "" {
		r.cur.Failure = msg
	} else {
		r.cur.Failure += "; " + msg
	}
	label := r.cur.Label
	if label == "" {
		label = r.cur.Type
	}
	r.res.Failures = append(r.res.Failures, fmt.Sprintf("step %d (%s): %s", r.cur.Step, label, msg))
	r.o.Logf("  FAIL: %s", msg)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func outcome(in *Packet, err error) string {
	if in != nil {
		return in.Type
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

func versionName(v byte) string {
	switch v {
	case 3:
		return "3.1"
	case 4:
		return "3.1.1"
	case 5:
		return "5"
	}
	return fmt.Sprint(v)
}

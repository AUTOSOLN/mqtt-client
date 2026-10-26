package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/clitest"
	"github.com/AUTOSOLN/mqtt-client/internal/pattern"
)

func runTool(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut clitest.Buffer
	code = run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func decode(t *testing.T, s string) cli.Report {
	t.Helper()
	var r cli.Report
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		t.Fatalf("report is not JSON: %v\n%s", err, s)
	}
	return r
}

func readFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func makePattern(t *testing.T, name string, size int) []byte {
	t.Helper()
	b, err := pattern.Make(name, size)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReportVerified(t *testing.T) {
	b := &clitest.Broker{Messages: [][]byte{makePattern(t, "binary", 300), makePattern(t, "binary", 300)}}
	b.Start(t)
	code, stdout, stderr := runTool(t, "-address", b.Addr(), "-t", "t/a", "-C", "2", "-W", "5s",
		"-verify", "binary", "-verify-size", "300", "-report", "-")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	r := decode(t, stdout)
	if !r.OK || r.Tool != "mqttsub" || r.ExitReason != "received 2 messages" {
		t.Errorf("verdict: %+v", r)
	}
	if r.Counts != (cli.ReportCounts{Received: 2}) {
		t.Errorf("counts: %+v", r.Counts)
	}
	if r.Verify != (cli.ReportVerify{Pattern: "binary", Size: 300, FirstBadOffset: -1}) {
		t.Errorf("verify: %+v", r.Verify)
	}
	if r.SessionPresent == nil || r.ConnackReason == nil || *r.ConnackReason != 0 {
		t.Errorf("CONNACK: session_present=%v connack_reason=%v", r.SessionPresent, r.ConnackReason)
	}
}

func TestReportVerifyFailed(t *testing.T) {
	bad := makePattern(t, "binary", 300)
	bad[7] ^= 0xff
	b := &clitest.Broker{Messages: [][]byte{makePattern(t, "binary", 300), bad, makePattern(t, "binary", 299)}}
	b.Start(t)
	file := filepath.Join(t.TempDir(), "report.json")
	code, _, _ := runTool(t, "-address", b.Addr(), "-t", "t/a", "-C", "3", "-W", "5s",
		"-verify", "binary", "-verify-size", "300", "-quiet", "-report", file)
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	r := decode(t, readFile(t, file))
	if r.OK || r.Exit != 1 || r.ExitReason != "2 payloads failed verification" {
		t.Errorf("verdict: %+v", r)
	}
	if r.Counts.Received != 3 || r.Counts.VerifyFailed != 2 {
		t.Errorf("counts: %+v", r.Counts)
	}
	if r.Verify.FirstBadOffset != 7 || r.Verify.FirstBadMessage != 2 {
		t.Errorf("verify: %+v", r.Verify)
	}
}

func TestReportTooFew(t *testing.T) {
	b := &clitest.Broker{SessionPresent: true}
	b.Start(t)
	code, stdout, _ := runTool(t, "-address", b.Addr(), "-id", "s1", "-clean=false", "-t", "t/a", "-C", "1", "-W", "100ms", "-report", "-")
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	r := decode(t, stdout)
	if r.OK || r.ExitReason != "received 0 of 1 messages" || r.SessionPresent == nil || !*r.SessionPresent {
		t.Errorf("report: %+v", r)
	}
}

func TestExitConnectError(t *testing.T) {
	b := &clitest.Broker{ConnackCode: 4}
	b.Start(t)
	code, stdout, _ := runTool(t, "-address", b.Addr(), "-t", "t/a", "-report", "-")
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if r := decode(t, stdout); r.OK || r.Exit != 2 || r.ConnackReason == nil || *r.ConnackReason != 4 {
		t.Errorf("report: %+v", r)
	}
}

// TestExitConnectionLost checks that losing an established connection is
// a test failure (1), not a setup error (2).
func TestExitConnectionLost(t *testing.T) {
	b := &clitest.Broker{Drop: true}
	b.Start(t)
	code, stdout, _ := runTool(t, "-address", b.Addr(), "-t", "t/a", "-W", "5s", "-report", "-")
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if r := decode(t, stdout); r.OK || r.Exit != 1 || !strings.Contains(r.ExitReason, "connection lost") {
		t.Errorf("report: %+v", r)
	}
}

func TestReportArgumentError(t *testing.T) {
	code, stdout, _ := runTool(t, "-report", "-")
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if r := decode(t, stdout); r.OK || r.Exit != 2 || !strings.Contains(r.ExitReason, "-t") {
		t.Errorf("report: %+v", r)
	}
}

// TestLogUnchanged pins the log of a run without -report, and checks that
// -report to a file does not change it.
func TestLogUnchanged(t *testing.T) {
	b := &clitest.Broker{Messages: [][]byte{[]byte("hi")}}
	b.Start(t)
	args := []string{"-address", b.Addr(), "-id", "sub1", "-t", "t/a", "-C", "1", "-W", "5s"}
	want := clitest.Log(`connected in 1ms client_id="sub1" session_present=false
OnConnect reason=0x00 session_present=false
SUBACK mid=1
  "t/a": granted QoS 0
waiting for 1 messages, at most 5s
OnMessage mid=0 qos=0 retain=false topic="t/a" payload="hi"
disconnected
OnDisconnect err=<nil> reason=0x00
received 1 messages
OK
`)
	code, stdout, stderr := runTool(t, args...)
	if got := clitest.Log(stdout); code != 0 || stderr != "" || !slices.Equal(got, want) {
		t.Errorf("without -report: exit %d stderr %q, log\n%s\nwant\n%s", code, stderr, strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	file := filepath.Join(t.TempDir(), "report.json")
	code, stdout, stderr = runTool(t, append(args, "-report", file)...)
	if got := clitest.Log(stdout); code != 0 || stderr != "" || !slices.Equal(got, want) {
		t.Errorf("with -report file: exit %d stderr %q, log\n%s", code, stderr, strings.Join(got, "\n"))
	}
}

func TestExpectSession(t *testing.T) {
	b := &clitest.Broker{SessionPresent: true, Messages: [][]byte{[]byte("hi")}}
	b.Start(t)
	args := []string{"-address", b.Addr(), "-id", "s1", "-clean=false", "-t", "t/a", "-C", "1", "-W", "5s", "-report", "-"}
	code, stdout, _ := runTool(t, append(args, "-expect-session", "1")...)
	if r := decode(t, stdout); code != 0 || !r.OK || r.Counts.Received != 1 {
		t.Errorf("-expect-session 1: exit %d %+v", code, r)
	}
	code, stdout, _ = runTool(t, append(args, "-expect-session", "0")...)
	r := decode(t, stdout)
	if code != 1 || r.OK || r.Counts.Received != 0 || r.SessionPresent == nil || !*r.SessionPresent {
		t.Errorf("-expect-session 0: exit %d %+v", code, r)
	}
}

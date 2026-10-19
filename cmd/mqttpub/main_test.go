package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/clitest"
)

// runTool runs mqttpub with args and returns its exit code and output.
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

func TestReportStdout(t *testing.T) {
	b := &clitest.Broker{}
	b.Start(t)
	code, stdout, stderr := runTool(t, "-address", b.Addr(), "-q", "1", "-n", "5", "-quiet", "-report", "-")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	r := decode(t, stdout)
	if !r.OK || r.Exit != 0 || r.Tool != "mqttpub" || r.Schema != 1 || r.ExitReason != "all 5 messages acknowledged" {
		t.Errorf("verdict: %+v", r)
	}
	if r.Server != (cli.ReportServer{Address: b.Addr(), Protocol: "311"}) {
		t.Errorf("server: %+v", r.Server)
	}
	if r.SessionPresent == nil || *r.SessionPresent || r.ConnackReason == nil || *r.ConnackReason != 0 {
		t.Errorf("CONNACK: session_present=%v connack_reason=%v", r.SessionPresent, r.ConnackReason)
	}
	if r.Counts != (cli.ReportCounts{Published: 5, Acked: 5}) {
		t.Errorf("counts: %+v", r.Counts)
	}
	if r.Verify != (cli.ReportVerify{Size: -1, FirstBadOffset: -1}) {
		t.Errorf("verify: %+v", r.Verify)
	}
	if l := r.LatencyMs; l == nil || l.P50 > l.P95 || l.P95 > l.P99 || l.P99 > l.Max || l.Max <= 0 {
		t.Errorf("latency: %+v", l)
	}
	// The log moved to stderr.
	if !strings.Contains(stderr, " OK\n") {
		t.Errorf("stderr has no log:\n%s", stderr)
	}
}

func TestReportSessionPresentQoS2Pattern(t *testing.T) {
	b := &clitest.Broker{SessionPresent: true}
	b.Start(t)
	file := filepath.Join(t.TempDir(), "report.json")
	code, _, _ := runTool(t, "-address", b.Addr(), "-id", "s1", "-clean=false", "-q", "2", "-n", "3",
		"-size", "100", "-pattern", "binary", "-report", file)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	r := decode(t, string(data))
	if r.SessionPresent == nil || !*r.SessionPresent {
		t.Errorf("session_present = %v, want true", r.SessionPresent)
	}
	if r.Counts.Acked != 3 || r.Verify != (cli.ReportVerify{Pattern: "binary", Size: 100, FirstBadOffset: -1}) {
		t.Errorf("counts %+v verify %+v", r.Counts, r.Verify)
	}
}

func TestReportArgumentError(t *testing.T) {
	code, stdout, stderr := runTool(t, "-size-step", "5", "-report", "-")
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	r := decode(t, stdout)
	if r.OK || r.Exit != 2 || r.ExitReason != "-size-step needs -size" || r.ConnackReason != nil {
		t.Errorf("report: %+v", r)
	}
	if stderr != "mqttpub: -size-step needs -size\n" {
		t.Errorf("stderr: %q", stderr)
	}
}

func TestReportConnackRefused(t *testing.T) {
	b := &clitest.Broker{ConnackCode: 5}
	b.Start(t)
	code, stdout, _ := runTool(t, "-address", b.Addr(), "-report", "-")
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	r := decode(t, stdout)
	if r.OK || r.Exit != 2 || r.ConnackReason == nil || *r.ConnackReason != 5 || r.SessionPresent == nil || *r.SessionPresent {
		t.Errorf("report: %+v connack_reason=%v", r, r.ConnackReason)
	}
}

func TestReportConnectError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	code, stdout, _ := runTool(t, "-address", addr, "-timeout", "2s", "-report", "-")
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	r := decode(t, stdout)
	if r.OK || r.Exit != 2 || r.ConnackReason != nil || r.SessionPresent != nil || !strings.Contains(r.ExitReason, "connect") {
		t.Errorf("report: %+v", r)
	}
}

// TestLogUnchanged pins the log of a run without -report, and checks that
// -report to a file does not change it.
func TestLogUnchanged(t *testing.T) {
	b := &clitest.Broker{}
	b.Start(t)
	args := []string{"-address", b.Addr(), "-id", "pub1", "-t", "t/a", "-q", "1", "-n", "2"}
	want := clitest.Log(`connected in 1ms client_id="pub1" session_present=false
OnConnect reason=0x00 session_present=false
Publish mid=1 qos=1 retain=false topic="t/a" bytes=7
Publish mid=2 qos=1 retain=false topic="t/a" bytes=7
OnPublish mid=1 reason=0x00
OnPublish mid=2 reason=0x00
completed 2 of 2 in 1ms (2 msg/s), 0 refused
disconnected
OnDisconnect err=<nil> reason=0x00
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
	for _, tc := range []struct {
		brokerSP bool
		expect   string
		code     int
	}{
		{true, "1", 0},
		{false, "1", 1},
		{false, "0", 0},
		{true, "0", 1},
	} {
		b := &clitest.Broker{SessionPresent: tc.brokerSP}
		b.Start(t)
		code, stdout, stderr := runTool(t, "-address", b.Addr(), "-id", "s1", "-clean=false", "-q", "1",
			"-expect-session", tc.expect, "-report", "-")
		if code != tc.code {
			t.Errorf("broker session present %v, -expect-session %s: exit %d, want %d\n%s", tc.brokerSP, tc.expect, code, tc.code, stderr)
			continue
		}
		r := decode(t, stdout)
		if r.SessionPresent == nil || *r.SessionPresent != tc.brokerSP || r.ExpectSession == nil || *r.ExpectSession != (tc.expect == "1") {
			t.Errorf("-expect-session %s: session_present=%v expect_session=%v", tc.expect, r.SessionPresent, r.ExpectSession)
		}
		if tc.code == 0 {
			if !r.OK || r.Counts.Acked != 1 || !strings.Contains(stderr, "session present is "+tc.expect+", as expected") {
				t.Errorf("-expect-session %s: %+v\n%s", tc.expect, r, stderr)
			}
		} else if r.OK || r.Counts.Published != 0 || !strings.Contains(r.ExitReason, "want "+tc.expect+" (-expect-session)") {
			t.Errorf("-expect-session %s: %+v", tc.expect, r)
		}
	}
}

func TestExpectSessionArguments(t *testing.T) {
	for _, args := range [][]string{
		{"-expect-session", "1"}, // -clean defaults to true
		{"-expect-session", "yes", "-clean=false", "-id", "s1"},
	} {
		code, stdout, _ := runTool(t, append(args, "-report", "-")...)
		if code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
		if r := decode(t, stdout); r.OK || !strings.Contains(r.ExitReason, "-expect-session") {
			t.Errorf("%v: %+v", args, r)
		}
	}
}

func TestNoExpectSession(t *testing.T) {
	b := &clitest.Broker{SessionPresent: true}
	b.Start(t)
	code, stdout, _ := runTool(t, "-address", b.Addr(), "-id", "s1", "-clean=false", "-report", "-")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if r := decode(t, stdout); r.ExpectSession != nil || r.SessionPresent == nil || !*r.SessionPresent {
		t.Errorf("expect_session=%v session_present=%v", r.ExpectSession, r.SessionPresent)
	}
}

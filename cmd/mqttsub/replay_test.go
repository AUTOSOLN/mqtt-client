package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/AUTOSOLN/mqtt-client/cmd/internal/clitest"
)

// subCapture is a capture of mqttsub -id sub1 -t filter -q qos receiving
// messages on t/a, then disconnecting.
func subCapture(t *testing.T, filter string, qos byte, messages ...string) string {
	t.Helper()
	c := clitest.NewCapture(t)
	s := c.Session("10.0.0.1:50000")
	s.Client(clitest.Connect("sub1")).Server(clitest.Connack())
	s.Client(clitest.Subscribe(1, filter, qos)).Server(clitest.Suback(1, qos))
	for i, m := range messages {
		id := uint16(i + 1)
		s.Server(clitest.Publish("t/a", qos, id, []byte(m)))
		if qos > 0 {
			s.Client(clitest.Puback(id))
		}
	}
	s.Client(clitest.Disconnect()).ClientClose()
	return c.Write()
}

func TestReplay_MatchingCapture_ReceivesAndReports(t *testing.T) {
	path := subCapture(t, "t/a", 1, "one", "two")
	code, stdout, stderr := runTool(t, "-replay", path, "-id", "sub1", "-t", "t/a", "-q", "1", "-report", "-")
	if code != 0 {
		t.Fatalf("exit %d, want 0\n%s", code, stderr)
	}
	r := decode(t, stdout)
	if r.Counts.Received != 2 {
		t.Errorf("got %d received, want 2", r.Counts.Received)
	}
	if r.Server.Address != clitest.BrokerAddr {
		t.Errorf("got server %q, want the captured %q", r.Server.Address, clitest.BrokerAddr)
	}
	if r.Replay == nil || r.Replay.File != path || !slices.Equal(r.Replay.Connections, []int{0}) ||
		r.Replay.Replayed != 1 || len(r.Replay.Divergences) != 0 {
		t.Errorf("got replay %+v, want file %s, connection 0 replayed, no divergences", r.Replay, path)
	}
}

// The plan's check that a tool behaves the same against a replay as
// against a broker: the same exchange, once each way, gives the same
// report.
func TestReplay_SameExchangeAsBroker_SameReport(t *testing.T) {
	b := &clitest.Broker{Messages: [][]byte{[]byte("one"), []byte("two")}}
	b.Start(t)
	args := []string{"-id", "sub1", "-t", "t/a", "-C", "2", "-W", "5s", "-report", "-"}
	code, live, stderr := runTool(t, append([]string{"-address", b.Addr()}, args...)...)
	if code != 0 {
		t.Fatalf("against the broker: exit %d\n%s", code, stderr)
	}
	path := subCapture(t, "t/a", 0, "one", "two")
	code, replayed, stderr := runTool(t, append([]string{"-replay", path}, args...)...)
	if code != 0 {
		t.Fatalf("against the replay: exit %d\n%s", code, stderr)
	}

	got, want := decode(t, replayed), decode(t, live)
	if got.OK != want.OK || got.Exit != want.Exit || got.ExitReason != want.ExitReason || got.Counts != want.Counts {
		t.Errorf("replay: got ok=%v exit=%d %q %+v, want the broker run's ok=%v exit=%d %q %+v",
			got.OK, got.Exit, got.ExitReason, got.Counts, want.OK, want.Exit, want.ExitReason, want.Counts)
	}
	if *got.SessionPresent != *want.SessionPresent || *got.ConnackReason != *want.ConnackReason {
		t.Errorf("replay: got CONNACK %v/%v, want %v/%v", *got.SessionPresent, *got.ConnackReason, *want.SessionPresent, *want.ConnackReason)
	}
}

func TestReplay_ToolDiffersFromCapture_Exit1(t *testing.T) {
	path := subCapture(t, "t/other", 1, "one")
	code, stdout, _ := runTool(t, "-replay", path, "-id", "sub1", "-t", "t/a", "-q", "1", "-report", "-")
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	r := decode(t, stdout)
	if r.Replay == nil || len(r.Replay.Divergences) != 1 || !strings.Contains(r.Replay.Divergences[0], "SUBSCRIBE filters") {
		t.Errorf("got replay %+v, want one divergence about the SUBSCRIBE filters", r.Replay)
	}
	if !strings.Contains(r.ExitReason, "diverged from the capture") {
		t.Errorf("got exit reason %q, want it to say the client diverged", r.ExitReason)
	}
}

func TestReplay_Inbound_EdgeCaptureDeliveredWithWill(t *testing.T) {
	c := clitest.NewCapture(t)
	s := c.Session("10.0.0.3:40000")
	s.Client(clitest.ConnectWill("edge1", "e/will", []byte("gone"))).Server(clitest.Connack())
	s.Client(clitest.Publish("e/data", 1, 1, []byte("1"))).Server(clitest.Puback(1))
	s.Client(clitest.Publish("e/data", 1, 2, []byte("2"))).Server(clitest.Puback(2))
	s.ServerClose() // dropped: the Will is published
	path := c.Write()

	code, stdout, stderr := runTool(t, "-replay", path, "-replay-inbound", "-id", "host", "-t", "e/#", "-q", "1", "-report", "-")
	if code != 0 {
		t.Fatalf("exit %d, want 0\n%s", code, stderr)
	}
	r := decode(t, stdout)
	if r.Counts.Received != 3 || r.Replay == nil || !r.Replay.Inbound || len(r.Replay.Divergences) != 0 {
		t.Errorf("got %d received, replay %+v; want 3 (two messages and the Will), inbound, no divergences",
			r.Counts.Received, r.Replay)
	}
}

func TestReplay_BadReplayFlags_Exit2(t *testing.T) {
	path := subCapture(t, "t/a", 1, "one")
	notCapture := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(notCapture, []byte("not a capture"), 0o644); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args []string
	}{
		{name: "with -tls", args: []string{"-replay", path, "-tls"}},
		{name: "missing file", args: []string{"-replay", filepath.Join(t.TempDir(), "none.pcap")}},
		{name: "not a capture", args: []string{"-replay", notCapture}},
		{name: "-replay-conn without -replay", args: []string{"-replay-conn", "0"}},
		{name: "-replay-inbound without -replay", args: []string{"-replay-inbound"}},
		{name: "connection out of range", args: []string{"-replay", path, "-replay-conn", "3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, _ := runTool(t, append(tt.args, "-id", "sub1", "-t", "t/a")...)
			if code != 2 {
				t.Errorf("exit %d, want 2\n%s", code, stdout)
			}
		})
	}
}

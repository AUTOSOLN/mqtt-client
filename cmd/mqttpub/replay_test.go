package main

import (
	"testing"

	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/clitest"
)

func TestReplay_PublisherCapture_AckedWithoutDivergence(t *testing.T) {
	c := clitest.NewCapture(t)
	s := c.Session("10.0.0.1:50000")
	s.Client(clitest.Connect("pub1")).Server(clitest.Connack())
	s.Client(clitest.Publish("t/a", 1, 1, []byte("hello"))).Server(clitest.Puback(1))
	s.Client(clitest.Disconnect()).ClientClose()
	path := c.Write()

	code, stdout, stderr := runTool(t, "-replay", path, "-id", "pub1", "-t", "t/a", "-m", "hello", "-q", "1", "-report", "-")
	if code != 0 {
		t.Fatalf("exit %d, want 0\n%s", code, stderr)
	}
	r := decode(t, stdout)
	if r.Counts != (cli.ReportCounts{Published: 1, Acked: 1}) {
		t.Errorf("got counts %+v, want 1 published, 1 acked", r.Counts)
	}
	if r.Replay == nil || r.Replay.Replayed != 1 || len(r.Replay.Divergences) != 0 {
		t.Errorf("got replay %+v, want 1 replayed, no divergences", r.Replay)
	}
}

func TestReplay_PublishedPayloadDiffers_Exit1(t *testing.T) {
	c := clitest.NewCapture(t)
	s := c.Session("10.0.0.1:50000")
	s.Client(clitest.Connect("pub1")).Server(clitest.Connack())
	s.Client(clitest.Publish("t/a", 1, 1, []byte("hello"))).Server(clitest.Puback(1))
	s.Client(clitest.Disconnect()).ClientClose()
	path := c.Write()

	code, stdout, _ := runTool(t, "-replay", path, "-id", "pub1", "-t", "t/a", "-m", "hello, longer", "-q", "1", "-report", "-")
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if r := decode(t, stdout); r.Replay == nil || len(r.Replay.Divergences) != 1 {
		t.Errorf("got replay %+v, want one divergence", r.Replay)
	}
}

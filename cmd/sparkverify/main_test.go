package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/AUTOSOLN/mqtt-client/cmd/internal/clitest"
	"github.com/AUTOSOLN/mqtt-client/sparkplug"
	"github.com/AUTOSOLN/mqtt-client/sparkplug/verify"
)

// edgeCapture is an edge node's session, captured at the edge: CONNECT
// with its NDEATH Will, NBIRTH, NDATA with the given seqs (counting from
// 1; 0 skips one), then the connection drops.
func edgeCapture(t *testing.T, skip int) string {
	t.Helper()
	e := sparkplug.NewEdge("plant1", "press7", 0)
	wt, wp := e.Death()
	c := clitest.NewCapture(t)
	s := c.Session("10.0.0.3:40000")
	s.Client(clitest.ConnectWill("press7", wt, wp)).Server(clitest.Connack())
	temp := sparkplug.Metric{Name: "Temperature", DataType: sparkplug.Double, Value: 21.5}
	bt, bp := e.Birth(temp)
	s.Client(clitest.Publish(bt, 1, 1, bp)).Server(clitest.Puback(1))
	for i := 1; i <= 3; i++ {
		dt, dp := e.Data(temp)
		if i == skip {
			continue // never published: a gap
		}
		id := uint16(i + 1)
		s.Client(clitest.Publish(dt, 1, id, dp)).Server(clitest.Puback(id))
	}
	s.ServerClose()
	return c.Write()
}

func runReport(t *testing.T, args ...string) (int, verify.Report) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "report.json")
	var out, errOut clitest.Buffer
	code := run(append(args, "-report", file), &out, &errOut)
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("exit %d, no report: %v\n%s", code, err, out.String())
	}
	var r verify.Report
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return code, r
}

func TestReplayInbound_EdgeCapture_NoViolations(t *testing.T) {
	code, r := runReport(t, "-replay", edgeCapture(t, 0), "-replay-inbound", "-id", "verifier")
	if code != 0 || !r.OK() || r.Totals.Births != 1 || r.Totals.Deaths != 1 || r.Totals.Data != 3 {
		t.Errorf("exit %d, report %v with %d births, %d deaths, %d data; want 0, OK, 1, 1, 3",
			code, r, r.Totals.Births, r.Totals.Deaths, r.Totals.Data)
	}
}

func TestReplayInbound_EdgeCaptureWithGap_Exit1(t *testing.T) {
	code, r := runReport(t, "-replay", edgeCapture(t, 2), "-replay-inbound", "-id", "verifier")
	if code != 1 || r.Totals.Gaps != 1 {
		t.Errorf("exit %d with %d gaps, want 1 and 1", code, r.Totals.Gaps)
	}
}

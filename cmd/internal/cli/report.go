package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"sync"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

// ReportSchema is the version of the Report format. Fields are added to it,
// never renamed or removed; a change that breaks readers bumps it.
const ReportSchema = 1

// Report is the JSON run report that mqttpub and mqttsub write with
// -report, so that a test harness reads the result instead of parsing the
// log. It is written once, on every exit path, including failures.
type Report struct {
	Tool       string `json:"tool"`
	Schema     int    `json:"schema"`
	OK         bool   `json:"ok"`
	Exit       int    `json:"exit"`
	ExitReason string `json:"exit_reason"`

	Server ReportServer `json:"server"`
	// SessionPresent and ConnackReason come from the latest CONNACK. They
	// are null when no CONNACK arrived (the connection failed before one).
	SessionPresent *bool `json:"session_present"`
	ConnackReason  *int  `json:"connack_reason"`
	// ExpectSession is -expect-session, null when not given.
	ExpectSession *bool `json:"expect_session"`

	Counts     ReportCounts `json:"counts"`
	Verify     ReportVerify `json:"verify"`
	LatencyMs  *Latency     `json:"latency_ms,omitempty"`
	DurationMs int64        `json:"duration_ms"`

	path           string
	stdout, stderr io.Writer
	start          time.Time
	conn           *ConnFlags
	session        *Session
}

// ReportServer is the server the run connected to: the one of the latest
// CONNACK, or before one, the first server.
type ReportServer struct {
	Address  string `json:"address"`
	Protocol string `json:"protocol"` // as -v: "31", "311" or "5"
	TLS      bool   `json:"tls"`
}

// ReportCounts counts messages. A tool leaves the counts that do not apply
// to it at zero.
type ReportCounts struct {
	Published    int64 `json:"published"`     // PUBLISH handed to the client
	Acked        int64 `json:"acked"`         // completed without a failure reason code (QoS 0: written)
	Refused      int64 `json:"refused"`       // completed with a reason code of 0x80 or above
	Dropped      int64 `json:"dropped"`       // QoS 0 not sent while reconnecting (-config)
	Received     int64 `json:"received"`      // PUBLISH from the server
	VerifyFailed int64 `json:"verify_failed"` // received payloads that failed -verify
}

// ReportVerify describes the payload pattern and the first payload that
// did not match it.
type ReportVerify struct {
	Pattern         string `json:"pattern"`           // "" when no pattern is used
	Size            int    `json:"size"`              // bytes sent (mqttpub -size) or required (mqttsub -verify-size); -1 if none
	FirstBadOffset  int    `json:"first_bad_offset"`  // first wrong, missing or extra byte; -1 if none
	FirstBadMessage int64  `json:"first_bad_message"` // number of that message, counting from 1; 0 if none
}

// NewReport starts the report for tool, to be written to path ("-" for
// stdout, "" for none) by Finish. It also directs Logf: to stdout, or to
// stderr when the report takes stdout, so that stdout is only the JSON.
func NewReport(tool, path string, conn *ConnFlags, stdout, stderr io.Writer) *Report {
	if path == "-" {
		SetLogOutput(stderr)
	} else {
		SetLogOutput(stdout)
	}
	return &Report{
		Tool:   tool,
		Schema: ReportSchema,
		Verify: ReportVerify{Size: -1, FirstBadOffset: -1},
		path:   path,
		stdout: stdout,
		stderr: stderr,
		start:  time.Now(),
		conn:   conn,
	}
}

// Enabled reports whether a report is to be written.
func (r *Report) Enabled() bool { return r.path != "" }

// SetSession gives the session whose server and CONNACK are reported.
func (r *Report) SetSession(s *Session) { r.session = s }

// Finish completes the report with the exit code and the error that caused
// it (nil on success, when ExitReason is kept if set), writes it, and
// returns the code to exit with: code, or 2 if the report could not be
// written. Without a report it only returns code.
func (r *Report) Finish(code int, err error) int {
	if !r.Enabled() {
		return code
	}
	r.Exit, r.OK = code, code == 0
	switch {
	case err != nil:
		r.ExitReason = err.Error()
	case r.ExitReason == "":
		r.ExitReason = "OK"
	}
	r.DurationMs = time.Since(r.start).Milliseconds()
	r.fillConnection(err)

	b, jerr := json.MarshalIndent(r, "", "  ")
	if jerr == nil {
		b = append(b, '\n')
		if r.path == "-" {
			_, jerr = r.stdout.Write(b)
		} else {
			jerr = os.WriteFile(r.path, b, 0o644)
		}
	}
	if jerr != nil {
		fmt.Fprintf(r.stderr, "%s: writing report: %v\n", r.Tool, jerr)
		return 2
	}
	return code
}

func (r *Report) fillConnection(err error) {
	var ack *mqttclient.ConnAck
	switch {
	case r.session != nil:
		var srv mqttclient.Server
		srv, ack = r.session.Connection()
		r.Server = ReportServer{Address: srv.Address, Protocol: versionName(srv.ProtocolVersion), TLS: srv.TLS}
	case r.conn.Config != "":
		// The file was not loaded, so there is no server to name.
		r.Server = ReportServer{Protocol: r.conn.Version}
	default:
		r.Server = ReportServer{Address: r.conn.Address, Protocol: r.conn.Version, TLS: r.conn.TLS}
		if v, verr := r.conn.ProtocolVersion(); verr == nil {
			r.Server.Protocol = versionName(v)
		}
	}
	// A refused CONNACK may reach OnConnect after Connect returns; the
	// error has its code.
	var refused *mqttclient.ConnRefusedError
	if ack == nil && errors.As(err, &refused) {
		ack = &mqttclient.ConnAck{ReasonCode: refused.ReasonCode}
	}
	if ack != nil {
		sp, code := ack.SessionPresent, int(ack.ReasonCode)
		r.SessionPresent, r.ConnackReason = &sp, &code
	}
}

func versionName(v byte) string {
	switch v {
	case mqttclient.MQTT31:
		return "31"
	case mqttclient.MQTT311:
		return "311"
	case mqttclient.MQTT5:
		return "5"
	}
	return fmt.Sprint(v)
}

// Latency summarizes durations in milliseconds.
type Latency struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

// Latencies collects durations from any goroutine.
type Latencies struct {
	mu sync.Mutex
	d  []time.Duration
}

// Add records d.
func (l *Latencies) Add(d time.Duration) {
	l.mu.Lock()
	l.d = append(l.d, d)
	l.mu.Unlock()
}

// Summary returns the nearest-rank percentiles of the durations so far, or
// nil if there are none.
func (l *Latencies) Summary() *Latency {
	l.mu.Lock()
	d := slices.Clone(l.d)
	l.mu.Unlock()
	if len(d) == 0 {
		return nil
	}
	slices.Sort(d)
	pct := func(p float64) float64 {
		i := int(math.Ceil(p*float64(len(d)))) - 1
		return ms(d[max(i, 0)])
	}
	return &Latency{P50: pct(0.50), P95: pct(0.95), P99: pct(0.99), Max: ms(d[len(d)-1])}
}

// ms is d in milliseconds, to the microsecond.
func ms(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Microsecond)) / 1000
}

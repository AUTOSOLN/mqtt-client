// Package verify checks a stream of Sparkplug B messages, as a subscriber
// receives them, against the spec's sequencing rules, per edge node:
//
//   - seq goes up by one, modulo 256, from NBIRTH through every NDATA,
//     DBIRTH, DDATA and DDEATH. A jump forward is a gap; a seq that a gap
//     skipped, arriving later, is a reorder; one already seen is a
//     duplicate.
//   - NBIRTH has seq 0 and a bdSeq metric.
//   - NDEATH carries the bdSeq of the NBIRTH it ends. One carrying another
//     bdSeq while a newer session is online is stale.
//   - No NDATA before NBIRTH (or after NDEATH), and no DDATA for a device
//     before its DBIRTH.
//
// Feed it every message received on spBv1.0/#; Report returns counters and
// violations, so a caller can show "3 lost, 0 reorders" rather than only
// pass or fail.
package verify

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/AUTOSOLN/mqtt-client/sparkplug"
)

// Kind classifies a violation.
type Kind string

// Violation kinds.
const (
	KindGap             Kind = "gap"               // seq jumped forward; Detail says how many are missing
	KindReorder         Kind = "reorder"           // a seq skipped by a gap arrived late
	KindDuplicate       Kind = "duplicate"         // a seq already seen (or too far behind to tell)
	KindBirthSeq        Kind = "birth-seq"         // NBIRTH seq is not 0
	KindDataBeforeBirth Kind = "data-before-birth" // NDATA/DDATA/DBIRTH/DDEATH with no NBIRTH, or DDATA with no DBIRTH
	KindDeathMismatch   Kind = "death-mismatch"    // NDEATH bdSeq differs from the NBIRTH it ends
	KindStaleDeath      Kind = "stale-death"       // NDEATH of an older session while a newer one is online
	KindBadPayload      Kind = "bad-payload"       // undecodable, or missing seq or bdSeq
	KindBadTopic        Kind = "bad-topic"         // under spBv1.0/ but not a valid Sparkplug topic
)

// Violation is one broken rule.
type Violation struct {
	Edge   string    `json:"edge"` // "<group>/<edge>"; empty for a bad topic
	Topic  string    `json:"topic"`
	Kind   Kind      `json:"kind"`
	Detail string    `json:"detail"`
	At     time.Time `json:"at"`
}

func (v Violation) String() string {
	return fmt.Sprintf("%s %s: %s (%s)", v.Kind, v.Topic, v.Detail, v.At.Format("15:04:05.000"))
}

// Counters count messages and problems.
type Counters struct {
	Messages   int `json:"messages"`
	Births     int `json:"births"`     // NBIRTH
	Deaths     int `json:"deaths"`     // NDEATH
	Data       int `json:"data"`       // NDATA and DDATA
	Gaps       int `json:"gaps"`       // seq jumps forward
	Lost       int `json:"lost"`       // seqs skipped by gaps that never arrived
	Reorders   int `json:"reorders"`   // seqs skipped by a gap that arrived later
	Duplicates int `json:"duplicates"` // seqs received again
	Violations int `json:"violations"` // all violations, those above included
}

func (c *Counters) add(o Counters) {
	c.Messages += o.Messages
	c.Births += o.Births
	c.Deaths += o.Deaths
	c.Data += o.Data
	c.Gaps += o.Gaps
	c.Lost += o.Lost
	c.Reorders += o.Reorders
	c.Duplicates += o.Duplicates
	c.Violations += o.Violations
}

// EdgeReport is one edge node's state and counters.
type EdgeReport struct {
	Counters
	Online bool   `json:"online"` // an NBIRTH with no matching NDEATH since
	BdSeq  uint64 `json:"bd_seq"` // of the latest NBIRTH
}

// Report is the verifier's result so far.
type Report struct {
	Edges      map[string]EdgeReport `json:"edges"` // by "<group>/<edge>"
	Totals     Counters              `json:"totals"`
	Violations []Violation           `json:"violations,omitempty"` // in arrival order, up to Options.MaxViolations
	Ignored    int                   `json:"ignored"`              // messages outside spBv1.0/, and STATE
}

// OK reports whether no rule was broken.
func (r Report) OK() bool { return r.Totals.Violations == 0 }

// String summarizes the report on one line, with the first violation.
func (r Report) String() string {
	t := r.Totals
	s := fmt.Sprintf("%d edges, %d messages: %d gaps (%d lost), %d reorders, %d duplicates, %d violations",
		len(r.Edges), t.Messages, t.Gaps, t.Lost, t.Reorders, t.Duplicates, t.Violations)
	if len(r.Violations) > 0 {
		s += "; first: " + r.Violations[0].String()
	}
	return s
}

// Options configures a Verifier.
type Options struct {
	// AnyBirthSeq accepts an NBIRTH with a seq other than 0. Sparkplug 3.0
	// allows any seq there; this package's Edge, and 2.2, use 0.
	AnyBirthSeq bool

	// MaxViolations bounds Report.Violations (default 1000); the counters
	// keep counting.
	MaxViolations int

	// OnViolation, if set, is called with each violation as it is found,
	// with the verifier's lock held: it must not call the verifier.
	OnViolation func(Violation)
}

// window is how far behind the expected seq a message may arrive and still
// count as a reorder: half the seq space.
const window = 128

// Verifier checks messages fed to it. It is safe for concurrent use.
type Verifier struct {
	opts Options

	mu         sync.Mutex
	edges      map[string]*edge
	violations []Violation
	ignored    int
	badTopics  int
}

type edge struct {
	c       Counters
	born    bool
	bdSeq   uint64
	expect  uint64          // next seq
	n       int             // seq-bearing messages since NBIRTH
	missing map[uint64]int  // seq skipped by a gap -> n when skipped
	devices map[string]bool // born devices
}

// New returns a Verifier.
func New(opts Options) *Verifier {
	if opts.MaxViolations <= 0 {
		opts.MaxViolations = 1000
	}
	return &Verifier{opts: opts, edges: map[string]*edge{}}
}

// Feed checks one received message.
func (v *Verifier) Feed(topic string, payload []byte, at time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !strings.HasPrefix(topic, sparkplug.Namespace+"/") {
		v.ignored++
		return
	}
	t, err := sparkplug.ParseTopic(topic)
	if err != nil {
		v.violate(nil, Violation{Topic: topic, Kind: KindBadTopic, Detail: err.Error(), At: at})
		return
	}
	if t.Type == sparkplug.STATE || t.Type == sparkplug.NCMD || t.Type == sparkplug.DCMD {
		v.ignored++
		return
	}
	id := t.EdgeID()
	e := v.edges[id]
	if e == nil {
		e = &edge{missing: map[uint64]int{}, devices: map[string]bool{}}
		v.edges[id] = e
	}
	e.c.Messages++
	viol := func(k Kind, format string, args ...any) {
		v.violate(e, Violation{Edge: id, Topic: topic, Kind: k, Detail: fmt.Sprintf(format, args...), At: at})
	}

	p, err := sparkplug.Unmarshal(payload)
	if err != nil {
		viol(KindBadPayload, "%v", err)
		return
	}
	switch t.Type {
	case sparkplug.NBIRTH:
		v.birth(e, p, viol)
	case sparkplug.NDEATH:
		v.death(e, p, viol)
	default:
		v.seqMessage(e, t, p, viol)
	}
}

type violateFunc func(k Kind, format string, args ...any)

func (v *Verifier) birth(e *edge, p *sparkplug.Payload, viol violateFunc) {
	e.c.Births++
	bd, ok := p.BdSeq()
	if !ok {
		viol(KindBadPayload, "NBIRTH without a %s metric", sparkplug.BdSeqMetric)
	}
	if p.Seq == nil {
		viol(KindBadPayload, "NBIRTH without seq")
	} else if *p.Seq != 0 && !v.opts.AnyBirthSeq {
		viol(KindBirthSeq, "NBIRTH seq %d, want 0", *p.Seq)
	}
	e.born, e.bdSeq = true, bd
	e.expect, e.n = 0, 0
	if p.Seq != nil {
		e.expect = (*p.Seq + 1) % 256
	}
	clear(e.missing)
	clear(e.devices)
}

func (v *Verifier) death(e *edge, p *sparkplug.Payload, viol violateFunc) {
	e.c.Deaths++
	bd, ok := p.BdSeq()
	switch {
	case !ok:
		viol(KindBadPayload, "NDEATH without a %s metric", sparkplug.BdSeqMetric)
	case e.born && bd != e.bdSeq:
		viol(KindStaleDeath, "NDEATH bdSeq %d while the session with bdSeq %d is online", bd, e.bdSeq)
		return // the online session is unaffected
	case !e.born && e.c.Births > 0 && bd != e.bdSeq:
		viol(KindDeathMismatch, "NDEATH bdSeq %d, the last NBIRTH had %d", bd, e.bdSeq)
	}
	e.born = false
}

func (v *Verifier) seqMessage(e *edge, t sparkplug.Topic, p *sparkplug.Payload, viol violateFunc) {
	if t.Type == sparkplug.NDATA || t.Type == sparkplug.DDATA {
		e.c.Data++
	}
	if !e.born {
		viol(KindDataBeforeBirth, "%s with no NBIRTH since the edge's last NDEATH (or ever)", t.Type)
		return
	}
	switch t.Type {
	case sparkplug.DBIRTH:
		e.devices[t.Device] = true
	case sparkplug.DDEATH:
		delete(e.devices, t.Device)
	case sparkplug.DDATA:
		if !e.devices[t.Device] {
			viol(KindDataBeforeBirth, "DDATA for device %q with no DBIRTH since NBIRTH", t.Device)
		}
	}
	if p.Seq == nil {
		viol(KindBadPayload, "%s without seq", t.Type)
		return
	}
	v.checkSeq(e, *p.Seq%256, viol)
}

func (v *Verifier) checkSeq(e *edge, got uint64, viol violateFunc) {
	e.n++
	if _, late := e.missing[got]; late {
		delete(e.missing, got)
		e.c.Lost--
		e.c.Reorders++
		viol(KindReorder, "seq %d arrived after later ones (expected %d)", got, e.expect)
		return
	}
	switch d := (got - e.expect + 256) % 256; {
	case d == 0:
		e.expect = (e.expect + 1) % 256
	case d < window:
		for s := e.expect; s != got; s = (s + 1) % 256 {
			e.missing[s] = e.n
		}
		e.c.Gaps++
		e.c.Lost += int(d)
		viol(KindGap, "expected seq %d, got %d: %d missing", e.expect, got, d)
		e.expect = (got + 1) % 256
	default:
		e.c.Duplicates++
		viol(KindDuplicate, "seq %d again (expected %d)", got, e.expect)
	}
	// A seq skipped more than a window ago is lost for good; forget it so
	// that the next time round the same value isn't taken for it.
	maps.DeleteFunc(e.missing, func(_ uint64, n int) bool { return e.n-n >= window })
}

// violate records a violation, against e or, for a bad topic (nil e), the
// verifier as a whole.
func (v *Verifier) violate(e *edge, vi Violation) {
	if e != nil {
		e.c.Violations++
	} else {
		v.badTopics++
	}
	if len(v.violations) < v.opts.MaxViolations {
		v.violations = append(v.violations, vi)
	}
	if v.opts.OnViolation != nil {
		v.opts.OnViolation(vi)
	}
}

// Report returns the counters and violations so far.
func (v *Verifier) Report() Report {
	v.mu.Lock()
	defer v.mu.Unlock()
	r := Report{Edges: make(map[string]EdgeReport, len(v.edges)), Violations: slices.Clone(v.violations), Ignored: v.ignored}
	for id, e := range v.edges {
		r.Edges[id] = EdgeReport{Counters: e.c, Online: e.born, BdSeq: e.bdSeq}
		r.Totals.add(e.c)
	}
	r.Totals.Violations += v.badTopics
	return r
}

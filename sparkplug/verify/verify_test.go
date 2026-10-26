package verify

import (
	"strings"
	"testing"
	"time"

	"github.com/AUTOSOLN/mqtt-client/sparkplug"
)

type msg struct {
	topic   string
	payload []byte
}

func feed(v *Verifier, msgs ...msg) Report {
	for _, m := range msgs {
		v.Feed(m.topic, m.payload, time.Now())
	}
	return v.Report()
}

func m(topic string, payload []byte) msg { return msg{topic, payload} }

// session returns n messages of one edge session: NBIRTH, DBIRTH, then
// alternating NDATA and DDATA.
func session(e *sparkplug.Edge, n int) []msg {
	out := []msg{m(e.Birth()), m(e.DeviceBirth("D"))}
	for i := 0; len(out) < n; i++ {
		if i%2 == 0 {
			out = append(out, m(e.Data()))
		} else {
			out = append(out, m(e.DeviceData("D")))
		}
	}
	return out
}

func TestCleanStream(t *testing.T) {
	v := New(Options{})
	a, b := sparkplug.NewEdge("G", "A", 0), sparkplug.NewEdge("G", "B", 9)
	sa, sb := session(a, 600), session(b, 600) // seq wraps twice
	for i := range sa {
		feed(v, sa[i], sb[i])
	}
	r := feed(v, m(a.DeviceDeath("D")), m(a.Death()), m("other/topic", nil), m(sparkplug.StateTopic("h"), sparkplug.StatePayload(true, time.Now())))
	if !r.OK() {
		t.Fatalf("%s", r)
	}
	if ra := r.Edges["G/A"]; ra.Messages != 602 || ra.Births != 1 || ra.Deaths != 1 || ra.Online || ra.Data != 598 {
		t.Fatalf("edge A %+v", ra)
	}
	if rb := r.Edges["G/B"]; !rb.Online || rb.BdSeq != 9 {
		t.Fatalf("edge B %+v", rb)
	}
	if r.Ignored != 2 || r.Totals.Messages != 1202 {
		t.Fatalf("report %+v", r.Totals)
	}
}

func TestSeqProblems(t *testing.T) {
	e := sparkplug.NewEdge("G", "E", 0)
	s := session(e, 12) // seq 0..11
	cases := []struct {
		name  string
		msgs  []msg
		check func(c Counters) bool
		kind  Kind
	}{
		{"gap", append(append([]msg{}, s[:5]...), s[8:]...), func(c Counters) bool { return c.Gaps == 1 && c.Lost == 3 && c.Reorders == 0 }, KindGap},
		{"reorder", []msg{s[0], s[1], s[3], s[2], s[4]}, func(c Counters) bool { return c.Gaps == 1 && c.Lost == 0 && c.Reorders == 1 }, KindGap},
		{"duplicate", []msg{s[0], s[1], s[2], s[2], s[3]}, func(c Counters) bool { return c.Duplicates == 1 && c.Gaps == 0 }, KindDuplicate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := feed(New(Options{}), tc.msgs...)
			if c := r.Edges["G/E"].Counters; !tc.check(c) || r.OK() {
				t.Fatalf("%+v", c)
			}
			if r.Violations[0].Kind != tc.kind {
				t.Fatalf("first violation %s", r.Violations[0])
			}
		})
	}
}

// A seq lost long ago isn't taken for a reorder when the value comes round
// again.
func TestLostSeqExpires(t *testing.T) {
	e := sparkplug.NewEdge("G", "E", 0)
	s := session(e, 600)
	r := feed(New(Options{}), append(append([]msg{}, s[:10]...), s[11:]...)...)
	if c := r.Edges["G/E"].Counters; c.Gaps != 1 || c.Lost != 1 || c.Reorders != 0 || c.Duplicates != 0 {
		t.Fatalf("%+v", c)
	}
}

func TestBirthDeathRules(t *testing.T) {
	e := sparkplug.NewEdge("G", "E", 3)
	death3 := m(e.Death())
	birth3 := m(e.Birth())
	e.NewSession()
	birth4 := m(e.Birth())
	data4 := m(e.Data())

	cases := []struct {
		name string
		msgs []msg
		kind Kind
	}{
		{"data before birth", []msg{data4}, KindDataBeforeBirth},
		{"data after death", []msg{birth3, death3, data4}, KindDataBeforeBirth},
		{"stale death after a new birth", []msg{birth3, birth4, death3, data4}, KindStaleDeath},
		{"death mismatch", []msg{birth4, m(e.Death()), death3}, KindDeathMismatch},
		{"ddata before dbirth", ddataBeforeDBirth(), KindDataBeforeBirth},
		{"bad payload", []msg{m(e.Topic(sparkplug.NDATA), []byte{0xff, 0xff})}, KindBadPayload},
		{"bad topic", []msg{m("spBv1.0/G/NOPE/E", nil)}, KindBadTopic},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var seen []Violation
			r := feed(New(Options{OnViolation: func(v Violation) { seen = append(seen, v) }}), tc.msgs...)
			if len(r.Violations) != 1 || r.Violations[0].Kind != tc.kind || len(seen) != 1 || r.Totals.Violations != 1 {
				t.Fatalf("violations %v, want one %s", r.Violations, tc.kind)
			}
		})
	}

	// The online session survives a stale NDEATH.
	r := feed(New(Options{}), birth3, birth4, death3)
	if !r.Edges["G/E"].Online || r.Edges["G/E"].BdSeq != 4 {
		t.Fatalf("%+v", r.Edges["G/E"])
	}
}

func ddataBeforeDBirth() []msg {
	e := sparkplug.NewEdge("G", "X", 0)
	return []msg{m(e.Birth()), m(e.DeviceData("X"))}
}

func TestBirthSeq(t *testing.T) {
	seq := uint64(5)
	b, _ := (&sparkplug.Payload{Seq: &seq, Metrics: []sparkplug.Metric{{Name: "bdSeq", DataType: sparkplug.UInt64, Value: uint64(0)}}}).Marshal()
	birth := m("spBv1.0/G/NBIRTH/E", b)
	if r := feed(New(Options{}), birth); r.OK() || r.Violations[0].Kind != KindBirthSeq {
		t.Fatalf("%s", r)
	}
	if r := feed(New(Options{AnyBirthSeq: true}), birth); !r.OK() {
		t.Fatalf("%s", r)
	}
}

func TestReportString(t *testing.T) {
	e := sparkplug.NewEdge("G", "E", 0)
	s := []msg{m(e.Birth()), m(e.Data()), m(e.Data()), m(e.Data()), m(e.Data())} // seq 0-4
	r := feed(New(Options{MaxViolations: 1}), s[0], s[3], s[1], s[4], s[4])
	if r.Totals.Violations != 3 || len(r.Violations) != 1 {
		t.Fatalf("%+v %v", r.Totals, r.Violations)
	}
	if str := r.String(); !strings.Contains(str, "1 gaps (1 lost), 1 reorders, 1 duplicates, 3 violations") {
		t.Fatal(str)
	}
}

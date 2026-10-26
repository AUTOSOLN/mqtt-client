package sparkplug

import (
	"bytes"
	"testing"
	"time"
)

func TestParseTopic(t *testing.T) {
	good := map[string]Topic{
		"spBv1.0/G/NBIRTH/E":   {Group: "G", Type: NBIRTH, Edge: "E"},
		"spBv1.0/G/DDATA/E/D":  {Group: "G", Type: DDATA, Edge: "E", Device: "D"},
		"spBv1.0/STATE/host-1": {Type: STATE, Host: "host-1"},
		"spBv1.0/G/NCMD/E":     {Group: "G", Type: NCMD, Edge: "E"},
	}
	for s, want := range good {
		got, err := ParseTopic(s)
		if err != nil || got != want || got.String() != s {
			t.Errorf("%s: %+v %v", s, got, err)
		}
	}
	for _, s := range []string{"spBv1.0/G/NBIRTH", "spBv1.0/G/DDATA/E", "spBv1.0/G/NDATA/E/D", "spBv1.0/G/BOGUS/E", "spAv1.0/G/NDATA/E", "spBv1.0/STATE", "spBv1.0//NDATA/E"} {
		if _, err := ParseTopic(s); err == nil {
			t.Errorf("%s parsed", s)
		}
	}
}

func TestPayloadRoundTrip(t *testing.T) {
	alias := uint64(7)
	seq := uint64(42)
	in := &Payload{Timestamp: 1700000000000, Seq: &seq, UUID: "u", Body: []byte{1, 2}, Metrics: []Metric{
		{Name: "i8", DataType: Int8, Value: int64(-5)},
		{Name: "i32", DataType: Int32, Value: -70000},
		{Name: "i64", DataType: Int64, Value: int64(-1)},
		{Name: "u16", DataType: UInt16, Value: uint16(65535)},
		{Name: "u64", DataType: UInt64, Value: uint64(1 << 63), Alias: &alias},
		{Name: "f", DataType: Float, Value: float32(1.5)},
		{Name: "d", DataType: Double, Value: 2.25},
		{Name: "b", DataType: Boolean, Value: true},
		{Name: "s", DataType: String, Value: "hi"},
		{Name: "bytes", DataType: Bytes, Value: []byte{0, 255}},
		{Name: "null", DataType: Int32},
	}}
	b, err := in.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	out, err := Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if *out.Seq != 42 || out.Timestamp != in.Timestamp || out.UUID != "u" || !bytes.Equal(out.Body, in.Body) {
		t.Fatalf("payload %+v", out)
	}
	want := map[string]any{"i8": int64(-5), "i32": int64(-70000), "i64": int64(-1), "u16": uint64(65535),
		"u64": uint64(1 << 63), "f": float32(1.5), "d": 2.25, "b": true, "s": "hi", "null": nil}
	for name, v := range want {
		if got := out.Metric(name).Value; got != v {
			t.Errorf("%s = %#v, want %#v", name, got, v)
		}
	}
	if !bytes.Equal(out.Metric("bytes").Value.([]byte), []byte{0, 255}) || *out.Metric("u64").Alias != 7 {
		t.Errorf("bytes/alias %+v %+v", out.Metric("bytes"), out.Metric("u64"))
	}
	if _, err := (&Payload{Metrics: []Metric{{Name: "x", DataType: Double, Value: "no"}}}).Marshal(); err == nil {
		t.Error("mismatched value encoded")
	}
}

func TestEdgeSequencing(t *testing.T) {
	e := NewEdge("G", "E", 255)
	e.now = func() time.Time { return time.UnixMilli(1000) }
	topic, b := e.Death()
	d, _ := Unmarshal(b)
	if bd, ok := d.BdSeq(); topic != "spBv1.0/G/NDEATH/E" || !ok || bd != 255 || d.Seq != nil {
		t.Fatalf("death %s %+v", topic, d)
	}
	_, b = e.Birth(Metric{Name: "m", DataType: Int32, Value: 1})
	p, _ := Unmarshal(b)
	if *p.Seq != 0 || p.Metrics[0].Name != BdSeqMetric || len(p.Metrics) != 2 {
		t.Fatalf("birth %+v", p)
	}
	for i := 1; i <= 300; i++ {
		_, b = e.Data()
		p, _ = Unmarshal(b)
		if *p.Seq != uint64(i%256) {
			t.Fatalf("data %d seq %d", i, *p.Seq)
		}
	}
	topic, _ = e.DeviceData("D")
	if topic != "spBv1.0/G/DDATA/E/D" {
		t.Fatal(topic)
	}
	e.NewSession()
	if e.BdSeq() != 0 {
		t.Fatalf("bdSeq after 255: %d", e.BdSeq())
	}
}

func TestState(t *testing.T) {
	if StateTopic("h") != "spBv1.0/STATE/h" {
		t.Fatal(StateTopic("h"))
	}
	if got := string(StatePayload(true, time.UnixMilli(5))); got != `{"online":true,"timestamp":5}` {
		t.Fatal(got)
	}
}

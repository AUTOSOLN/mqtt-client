package sparkplug

import (
	"encoding/json"
	"time"
)

// Edge emits the messages of one Sparkplug edge node, keeping its seq and
// bdSeq as the spec requires: Death is the NDEATH to register as the Will in
// CONNECT, Birth starts a session at seq 0, and every later message from the
// node takes the next seq, modulo 256. NewSession moves to the next bdSeq,
// for the next connection.
//
// An Edge is not safe for concurrent use.
type Edge struct {
	Group, Node string

	bdSeq uint64
	seq   uint64 // the next message's seq
	now   func() time.Time
}

// NewEdge returns an edge node in session bdSeq (0-255).
func NewEdge(group, node string, bdSeq uint64) *Edge {
	return &Edge{Group: group, Node: node, bdSeq: bdSeq % 256, now: time.Now}
}

// BdSeq is the current session's bdSeq.
func (e *Edge) BdSeq() uint64 { return e.bdSeq }

// NewSession moves to the next bdSeq (modulo 256). Register the new Death
// as the Will and send Birth on the new connection.
func (e *Edge) NewSession() { e.bdSeq = (e.bdSeq + 1) % 256 }

// Topic returns the edge's topic for a node-level message type.
func (e *Edge) Topic(t MessageType) string {
	return Topic{Group: e.Group, Type: t, Edge: e.Node}.String()
}

// DeviceTopic returns the edge's topic for a device-level message type.
func (e *Edge) DeviceTopic(t MessageType, device string) string {
	return Topic{Group: e.Group, Type: t, Edge: e.Node, Device: device}.String()
}

// Death returns the session's NDEATH: no seq, the bdSeq metric.
func (e *Edge) Death() (topic string, payload []byte) {
	return e.Topic(NDEATH), e.encode(&Payload{Timestamp: e.ts(), Metrics: []Metric{e.bdSeqMetric()}})
}

// Birth returns an NBIRTH at seq 0 with the bdSeq metric first, then
// metrics.
func (e *Edge) Birth(metrics ...Metric) (topic string, payload []byte) {
	e.seq = 0
	return e.Topic(NBIRTH), e.next(append([]Metric{e.bdSeqMetric()}, metrics...))
}

// Data returns an NDATA with the next seq.
func (e *Edge) Data(metrics ...Metric) (topic string, payload []byte) {
	return e.Topic(NDATA), e.next(metrics)
}

// DeviceBirth returns a DBIRTH for device with the next seq.
func (e *Edge) DeviceBirth(device string, metrics ...Metric) (topic string, payload []byte) {
	return e.DeviceTopic(DBIRTH, device), e.next(metrics)
}

// DeviceData returns a DDATA for device with the next seq.
func (e *Edge) DeviceData(device string, metrics ...Metric) (topic string, payload []byte) {
	return e.DeviceTopic(DDATA, device), e.next(metrics)
}

// DeviceDeath returns a DDEATH for device with the next seq.
func (e *Edge) DeviceDeath(device string) (topic string, payload []byte) {
	return e.DeviceTopic(DDEATH, device), e.next(nil)
}

func (e *Edge) next(metrics []Metric) []byte {
	seq := e.seq
	e.seq = (e.seq + 1) % 256
	return e.encode(&Payload{Timestamp: e.ts(), Seq: &seq, Metrics: metrics})
}

func (e *Edge) bdSeqMetric() Metric {
	return Metric{Name: BdSeqMetric, DataType: UInt64, Value: e.bdSeq}
}

func (e *Edge) ts() uint64 { return uint64(e.now().UnixMilli()) }

// encode marshals a payload built here; only metrics passed in can fail.
func (e *Edge) encode(p *Payload) []byte {
	b, err := p.Marshal()
	if err != nil {
		panic(err) // a metric whose Value doesn't match its DataType
	}
	return b
}

// StateTopic is a primary host application's STATE topic.
func StateTopic(host string) string { return Topic{Type: STATE, Host: host}.String() }

// StatePayload is a Sparkplug 3.0 STATE payload, published retained.
func StatePayload(online bool, at time.Time) []byte {
	b, _ := json.Marshal(struct {
		Online    bool  `json:"online"`
		Timestamp int64 `json:"timestamp"`
	}{online, at.UnixMilli()})
	return b
}

package sparkplug

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/AUTOSOLN/mqtt-client/sparkplug/internal/sparkplugpb"
)

// DataType is a Sparkplug B metric data type.
type DataType uint32

// The scalar data types. Values follow sparkplug_b.proto.
const (
	Int8     DataType = 1
	Int16    DataType = 2
	Int32    DataType = 3
	Int64    DataType = 4
	UInt8    DataType = 5
	UInt16   DataType = 6
	UInt32   DataType = 7
	UInt64   DataType = 8
	Float    DataType = 9
	Double   DataType = 10
	Boolean  DataType = 11
	String   DataType = 12
	DateTime DataType = 13
	Text     DataType = 14
	UUID     DataType = 15
	Bytes    DataType = 17
	File     DataType = 18
)

// BdSeqMetric is the name of the metric that carries the birth/death
// sequence number in NBIRTH and NDEATH.
const BdSeqMetric = "bdSeq"

// Metric is one metric of a payload. Value's Go type follows DataType:
// int64 for the signed integers, uint64 for the unsigned ones and DateTime,
// float32, float64, bool, string (String, Text, UUID) and []byte (Bytes,
// File). A nil Value is a null metric. Encoding also accepts any Go integer
// for the integer types.
type Metric struct {
	Name      string
	Alias     *uint64
	Timestamp uint64 // ms since the epoch; 0 leaves it out
	DataType  DataType
	Value     any
}

// Payload is a Sparkplug B payload.
type Payload struct {
	Timestamp uint64  // ms since the epoch
	Seq       *uint64 // nil when absent (NDEATH)
	Metrics   []Metric
	UUID      string
	Body      []byte
}

// Metric returns the first metric with the given name, or nil.
func (p *Payload) Metric(name string) *Metric {
	for i := range p.Metrics {
		if p.Metrics[i].Name == name {
			return &p.Metrics[i]
		}
	}
	return nil
}

// BdSeq returns the bdSeq metric's value, and whether there is one.
func (p *Payload) BdSeq() (uint64, bool) {
	m := p.Metric(BdSeqMetric)
	if m == nil {
		return 0, false
	}
	v, ok := m.Value.(uint64)
	if !ok {
		i, isInt := m.Value.(int64)
		return uint64(i), isInt
	}
	return v, true
}

// Marshal encodes the payload.
func (p *Payload) Marshal() ([]byte, error) {
	pb := &sparkplugpb.Payload{Seq: p.Seq}
	if p.Timestamp != 0 {
		pb.Timestamp = proto.Uint64(p.Timestamp)
	}
	if p.UUID != "" {
		pb.Uuid = proto.String(p.UUID)
	}
	pb.Body = p.Body
	for _, m := range p.Metrics {
		pm, err := m.toPB()
		if err != nil {
			return nil, err
		}
		pb.Metrics = append(pb.Metrics, pm)
	}
	return proto.Marshal(pb)
}

// Unmarshal decodes a payload. Metrics of non-scalar types keep their name
// and type with a nil Value.
func Unmarshal(b []byte) (*Payload, error) {
	var pb sparkplugpb.Payload
	if err := proto.Unmarshal(b, &pb); err != nil {
		return nil, fmt.Errorf("sparkplug: decode payload: %w", err)
	}
	p := &Payload{Timestamp: pb.GetTimestamp(), Seq: pb.Seq, UUID: pb.GetUuid(), Body: pb.Body}
	for _, pm := range pb.Metrics {
		p.Metrics = append(p.Metrics, metricFromPB(pm))
	}
	return p, nil
}

func (m *Metric) toPB() (*sparkplugpb.Payload_Metric, error) {
	pm := &sparkplugpb.Payload_Metric{Alias: m.Alias, Datatype: proto.Uint32(uint32(m.DataType))}
	if m.Name != "" {
		pm.Name = proto.String(m.Name)
	}
	if m.Timestamp != 0 {
		pm.Timestamp = proto.Uint64(m.Timestamp)
	}
	if m.Value == nil {
		pm.IsNull = proto.Bool(true)
		return pm, nil
	}
	bad := func() error {
		return fmt.Errorf("sparkplug: metric %q: %T value for data type %d", m.Name, m.Value, m.DataType)
	}
	switch m.DataType {
	case Int8, Int16, Int32, UInt8, UInt16, UInt32:
		n, ok := toUint64(m.Value)
		if !ok {
			return nil, bad()
		}
		pm.Value = &sparkplugpb.Payload_Metric_IntValue{IntValue: uint32(n)}
	case Int64, UInt64, DateTime:
		n, ok := toUint64(m.Value)
		if !ok {
			return nil, bad()
		}
		pm.Value = &sparkplugpb.Payload_Metric_LongValue{LongValue: n}
	case Float:
		f, ok := m.Value.(float32)
		if !ok {
			return nil, bad()
		}
		pm.Value = &sparkplugpb.Payload_Metric_FloatValue{FloatValue: f}
	case Double:
		f, ok := m.Value.(float64)
		if !ok {
			return nil, bad()
		}
		pm.Value = &sparkplugpb.Payload_Metric_DoubleValue{DoubleValue: f}
	case Boolean:
		v, ok := m.Value.(bool)
		if !ok {
			return nil, bad()
		}
		pm.Value = &sparkplugpb.Payload_Metric_BooleanValue{BooleanValue: v}
	case String, Text, UUID:
		v, ok := m.Value.(string)
		if !ok {
			return nil, bad()
		}
		pm.Value = &sparkplugpb.Payload_Metric_StringValue{StringValue: v}
	case Bytes, File:
		v, ok := m.Value.([]byte)
		if !ok {
			return nil, bad()
		}
		pm.Value = &sparkplugpb.Payload_Metric_BytesValue{BytesValue: v}
	default:
		return nil, fmt.Errorf("sparkplug: metric %q: data type %d is not supported", m.Name, m.DataType)
	}
	return pm, nil
}

func metricFromPB(pm *sparkplugpb.Payload_Metric) Metric {
	m := Metric{Name: pm.GetName(), Alias: pm.Alias, Timestamp: pm.GetTimestamp(), DataType: DataType(pm.GetDatatype())}
	if pm.GetIsNull() {
		return m
	}
	switch m.DataType {
	case Int8:
		m.Value = int64(int8(pm.GetIntValue()))
	case Int16:
		m.Value = int64(int16(pm.GetIntValue()))
	case Int32:
		m.Value = int64(int32(pm.GetIntValue()))
	case UInt8, UInt16, UInt32:
		m.Value = uint64(pm.GetIntValue())
	case Int64:
		m.Value = int64(pm.GetLongValue())
	case UInt64, DateTime:
		m.Value = pm.GetLongValue()
	case Float:
		m.Value = pm.GetFloatValue()
	case Double:
		m.Value = pm.GetDoubleValue()
	case Boolean:
		m.Value = pm.GetBooleanValue()
	case String, Text, UUID:
		m.Value = pm.GetStringValue()
	case Bytes, File:
		m.Value = pm.GetBytesValue()
	}
	return m
}

// toUint64 converts any Go integer to its two's-complement uint64.
func toUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case int:
		return uint64(n), true
	case int8:
		return uint64(n), true
	case int16:
		return uint64(n), true
	case int32:
		return uint64(n), true
	case int64:
		return uint64(n), true
	case uint:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	}
	return 0, false
}

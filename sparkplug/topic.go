// Package sparkplug builds and reads Sparkplug B messages: topics, the
// protobuf payload (Eclipse Tahu's sparkplug_b.proto) and an Edge that emits
// a node's BIRTH, DATA and DEATH messages with correct seq and bdSeq. The
// verify subpackage checks a stream of them.
//
// It is a test and tooling helper, not a full Sparkplug implementation:
// metrics carry scalar values only (no DataSet, Template or PropertySet).
package sparkplug

import (
	"fmt"
	"strings"
)

// Namespace is the Sparkplug B topic namespace.
const Namespace = "spBv1.0"

// MessageType is the message type element of a Sparkplug topic.
type MessageType string

// Message types.
const (
	NBIRTH MessageType = "NBIRTH"
	NDEATH MessageType = "NDEATH"
	NDATA  MessageType = "NDATA"
	NCMD   MessageType = "NCMD"
	DBIRTH MessageType = "DBIRTH"
	DDEATH MessageType = "DDEATH"
	DDATA  MessageType = "DDATA"
	DCMD   MessageType = "DCMD"
	STATE  MessageType = "STATE"
)

// HasSeq reports whether messages of this type carry the edge node's seq.
// NDEATH (a Will, sent by the server) and the commands don't.
func (m MessageType) HasSeq() bool {
	switch m {
	case NBIRTH, NDATA, DBIRTH, DDATA, DDEATH:
		return true
	}
	return false
}

// Device reports whether the type is a device-level message.
func (m MessageType) Device() bool {
	switch m {
	case DBIRTH, DDEATH, DDATA, DCMD:
		return true
	}
	return false
}

// Topic is a parsed Sparkplug B topic:
// spBv1.0/<group>/<type>/<edge>[/<device>], or spBv1.0/STATE/<host>.
type Topic struct {
	Group  string
	Type   MessageType
	Edge   string
	Device string // device-level types only
	Host   string // STATE only
}

// ParseTopic parses a Sparkplug B topic.
func ParseTopic(s string) (Topic, error) {
	parts := strings.Split(s, "/")
	if len(parts) < 3 || parts[0] != Namespace {
		return Topic{}, fmt.Errorf("sparkplug: %q is not a %s topic", s, Namespace)
	}
	if parts[1] == string(STATE) {
		return parseState(s, parts)
	}
	if len(parts) < 4 {
		return Topic{}, fmt.Errorf("sparkplug: %q: want %s/<group>/<type>/<edge>[/<device>]", s, Namespace)
	}
	t := Topic{Group: parts[1], Type: MessageType(parts[2]), Edge: parts[3]}
	switch t.Type {
	case NBIRTH, NDEATH, NDATA, NCMD, DBIRTH, DDEATH, DDATA, DCMD:
	default:
		return Topic{}, fmt.Errorf("sparkplug: %q: unknown message type %q", s, parts[2])
	}
	want := 4
	if t.Type.Device() {
		want = 5
	}
	if len(parts) != want {
		return Topic{}, fmt.Errorf("sparkplug: %q: wrong number of levels for %s", s, t.Type)
	}
	if t.Type.Device() {
		t.Device = parts[4]
	}
	if t.Group == "" || t.Edge == "" || (t.Type.Device() && t.Device == "") {
		return Topic{}, fmt.Errorf("sparkplug: %q: empty topic level", s)
	}
	return t, nil
}

func parseState(s string, parts []string) (Topic, error) {
	if len(parts) != 3 || parts[2] == "" {
		return Topic{}, fmt.Errorf("sparkplug: %q: want %s/STATE/<host>", s, Namespace)
	}
	return Topic{Type: STATE, Host: parts[2]}, nil
}

// String is the topic as published.
func (t Topic) String() string {
	if t.Type == STATE {
		return Namespace + "/STATE/" + t.Host
	}
	s := Namespace + "/" + t.Group + "/" + string(t.Type) + "/" + t.Edge
	if t.Device != "" {
		s += "/" + t.Device
	}
	return s
}

// EdgeID is the edge node's identity, "<group>/<edge>".
func (t Topic) EdgeID() string { return t.Group + "/" + t.Edge }

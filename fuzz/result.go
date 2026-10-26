package fuzz

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// Result is what a run did: every step, what it sent and received, and which
// expectations failed. It serializes to JSON, so a test can log it whole.
type Result struct {
	Scenario string       `json:"scenario"`
	Address  string       `json:"address"`
	Version  byte         `json:"version"` // protocol-version byte: 3, 4 or 5
	Steps    []StepResult `json:"steps"`
	// Failures lists every failed expectation as "step N (label): why".
	Failures []string `json:"failures,omitempty"`
	// Error is the setup error that stopped the run, if any (see ErrSetup).
	Error string `json:"error,omitempty"`
}

// OK reports whether the run completed and every expectation held.
func (r *Result) OK() bool { return r.Error == "" && len(r.Failures) == 0 }

// Step returns the result of the first step with the given label, or nil.
func (r *Result) Step(label string) *StepResult {
	for i := range r.Steps {
		if r.Steps[i].Label == label {
			return &r.Steps[i]
		}
	}
	return nil
}

// Received is every packet the run read, in order.
func (r *Result) Received() []Packet {
	var out []Packet
	for _, s := range r.Steps {
		out = append(out, s.Received...)
	}
	return out
}

// Closed returns the step during which the server closed the connection,
// or nil if it never did.
func (r *Result) Closed() *StepResult {
	for i := range r.Steps {
		if r.Steps[i].Closed {
			return &r.Steps[i]
		}
	}
	return nil
}

// JSON is the result as indented JSON, for logging.
func (r *Result) JSON() string {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Sprintf("{%q: %q}", "error", err.Error())
	}
	return string(b)
}

// StepResult is one step of a run.
type StepResult struct {
	Step  int    `json:"step"` // 1-based
	Type  string `json:"type"`
	Label string `json:"label,omitempty"`

	// Start is when the step began, from when the connection opened, and
	// Elapsed how long it took.
	Start   time.Duration `json:"-"`
	Elapsed time.Duration `json:"-"`

	// Sent is the number of bytes written, over all repetitions.
	Sent int `json:"sent,omitempty"`
	// Skipped is set on a sending step that did not run because the server
	// had already closed the connection.
	Skipped  bool     `json:"skipped,omitempty"`
	Received []Packet `json:"received,omitempty"`

	// Closed is set on the step during which the server closed the
	// connection. CloseReason is then the reason code of the DISCONNECT, or
	// of the refusing CONNACK, that came before the close, if any did.
	Closed      bool  `json:"closed,omitempty"`
	CloseReason *byte `json:"close_reason,omitempty"`

	// Failure says why the step's expectation failed; empty when it held or
	// the step had none.
	Failure string `json:"failure,omitempty"`
}

// MarshalJSON writes Start and Elapsed as start_ms and elapsed_ms.
func (s StepResult) MarshalJSON() ([]byte, error) {
	type plain StepResult
	return json.Marshal(struct {
		plain
		StartMs   int64 `json:"start_ms"`
		ElapsedMs int64 `json:"elapsed_ms"`
	}{plain(s), s.Start.Milliseconds(), s.Elapsed.Milliseconds()})
}

// Packet is one packet received from the server, decoded best-effort.
type Packet struct {
	Type  string `json:"type"` // Connack, Publish, ... as comqtt names them
	Flags byte   `json:"flags,omitempty"`
	Size  int    `json:"size"` // the whole frame, header included

	PacketID       uint16 `json:"packet_id,omitempty"`
	SessionPresent bool   `json:"session_present,omitempty"`
	// ReasonCode is set on CONNACK, DISCONNECT, AUTH and the publish
	// acknowledgements; ReasonCodes on SUBACK and UNSUBACK.
	ReasonCode  *byte   `json:"reason_code,omitempty"`
	ReasonCodes Codes   `json:"reason_codes,omitempty"`
	Topic       string  `json:"topic,omitempty"`
	Payload     []byte  `json:"-"`
	PayloadSize int     `json:"payload_size,omitempty"`
	Properties  *Props5 `json:"properties,omitempty"` // MQTT 5 only, nil when there are none

	// DecodeError is set when the body did not parse.
	DecodeError string `json:"decode_error,omitempty"`
	Raw         []byte `json:"-"`

	typ byte
}

// Is reports whether the packet is of the named type (case-insensitive).
func (p *Packet) Is(name string) bool { return strings.EqualFold(p.Type, name) }

// Codes are reason codes. They serialize as a list of numbers rather than
// base64.
type Codes []byte

// MarshalJSON writes the codes as numbers.
func (c Codes) MarshalJSON() ([]byte, error) {
	out := make([]int, len(c))
	for i, b := range c {
		out[i] = int(b)
	}
	return json.Marshal(out)
}

// Props5 is the MQTT 5 properties of a received packet. Properties whose
// absence matters (Session Expiry, Maximum QoS, ...) are pointers.
type Props5 struct {
	SessionExpiry     *uint32    `json:"session_expiry,omitempty"`
	ReceiveMaximum    uint16     `json:"receive_maximum,omitempty"`
	MaximumPacketSize uint32     `json:"maximum_packet_size,omitempty"`
	MaximumQoS        *byte      `json:"maximum_qos,omitempty"`
	RetainAvailable   *byte      `json:"retain_available,omitempty"`
	TopicAliasMaximum uint16     `json:"topic_alias_maximum,omitempty"`
	TopicAlias        uint16     `json:"topic_alias,omitempty"`
	AssignedClientID  string     `json:"assigned_client_id,omitempty"`
	ServerKeepAlive   *uint16    `json:"server_keep_alive,omitempty"`
	ServerReference   string     `json:"server_reference,omitempty"`
	ReasonString      string     `json:"reason_string,omitempty"`
	PayloadFormat     *byte      `json:"payload_format,omitempty"`
	MessageExpiry     uint32     `json:"message_expiry,omitempty"`
	ContentType       string     `json:"content_type,omitempty"`
	ResponseTopic     string     `json:"response_topic,omitempty"`
	SubscriptionIDs   []int      `json:"subscription_ids,omitempty"`
	AuthMethod        string     `json:"auth_method,omitempty"`
	User              []UserProp `json:"user,omitempty"`
}

// summarize converts decoded properties, or returns nil when none are set.
func summarize(p *packets.Properties) *Props5 {
	s := &Props5{
		ReceiveMaximum:    p.ReceiveMaximum,
		MaximumPacketSize: p.MaximumPacketSize,
		TopicAliasMaximum: p.TopicAliasMaximum,
		TopicAlias:        p.TopicAlias,
		AssignedClientID:  p.AssignedClientID,
		ServerReference:   p.ServerReference,
		ReasonString:      p.ReasonString,
		MessageExpiry:     p.MessageExpiryInterval,
		ContentType:       p.ContentType,
		ResponseTopic:     p.ResponseTopic,
		SubscriptionIDs:   p.SubscriptionIdentifier,
		AuthMethod:        p.AuthenticationMethod,
	}
	if p.SessionExpiryIntervalFlag {
		s.SessionExpiry = &p.SessionExpiryInterval
	}
	if p.MaximumQosFlag {
		s.MaximumQoS = &p.MaximumQos
	}
	if p.RetainAvailableFlag {
		s.RetainAvailable = &p.RetainAvailable
	}
	if p.ServerKeepAliveFlag {
		s.ServerKeepAlive = &p.ServerKeepAlive
	}
	if p.PayloadFormatFlag {
		s.PayloadFormat = &p.PayloadFormat
	}
	for _, u := range p.User {
		s.User = append(s.User, UserProp{Key: u.Key, Val: u.Val})
	}
	if reflect.ValueOf(*s).IsZero() {
		return nil
	}
	return s
}

// describe renders a packet on one line for the log.
func (p *Packet) describe() string {
	var b strings.Builder
	b.WriteString(p.Type)
	if p.Flags != 0 {
		fmt.Fprintf(&b, " flags=0x%x", p.Flags)
	}
	if p.typ == packets.Connack {
		fmt.Fprintf(&b, " session-present=%t", p.SessionPresent)
	}
	if p.PacketID != 0 {
		fmt.Fprintf(&b, " id=%d", p.PacketID)
	}
	if p.Topic != "" {
		fmt.Fprintf(&b, " topic=%q", p.Topic)
	}
	if p.typ == packets.Publish {
		fmt.Fprintf(&b, " payload=%d bytes", p.PayloadSize)
	}
	if p.ReasonCode != nil {
		fmt.Fprintf(&b, " reason=0x%02x", *p.ReasonCode)
	}
	if len(p.ReasonCodes) > 0 {
		fmt.Fprintf(&b, " reasons=%s", hexBytes(p.ReasonCodes))
	}
	if p.Properties != nil && p.Properties.ReasonString != "" {
		fmt.Fprintf(&b, " reason-string=%q", p.Properties.ReasonString)
	}
	if p.DecodeError != "" {
		fmt.Fprintf(&b, " [decode: %s]", p.DecodeError)
	}
	fmt.Fprintf(&b, " %s", hexPreview(p.Raw))
	return b.String()
}

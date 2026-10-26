// Package fuzz drives an MQTT broker with control packets in an order a YAML
// scenario chooses, for hardening and edge-case probing. Every packet goes
// out verbatim over one raw connection with no client state machine in the
// way, so a scenario can send orders the protocol forbids (DISCONNECT before
// CONNECT, two CONNECTs, UNSUBSCRIBE with no SUBSCRIBE), reserved-bit and
// malformed frames, and hand-written bytes.
//
// Each step can state what it expects back: a packet type, its reason codes,
// a connection close, or silence. Run returns a Result with every packet
// sent and received, which serializes to JSON, and an error that tells a
// broker that misbehaved (ErrExpectation) from a run that could not happen
// (ErrSetup).
//
// cmd/mqttfuzz is a command-line wrapper around this package. Scenarios
// holds the scenarios shipped with it; the format is described in
// cmd/mqttfuzz/README.md.
package fuzz

import (
	"fmt"
	"io/fs"
	"maps"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
	"gopkg.in/yaml.v3"
)

// Scenario is a parsed YAML fuzzing script. It is an ordered list of steps
// sent verbatim over one raw connection, with no client state machine in the
// way: packets may appear in any order, including orders the MQTT spec
// forbids (DISCONNECT before CONNECT, UNSUBSCRIBE with no prior SUBSCRIBE,
// two CONNECTs, a PUBLISH before CONNACK, and so on).
//
// Version, Address and TLS are the scenario's defaults; RunOptions can
// override them.
type Scenario struct {
	Version string         `yaml:"version"` // 31 | 311 | 5
	Address string         `yaml:"address"` // host:port
	TLS     bool           `yaml:"tls"`
	Vars    map[string]any `yaml:"vars"` // token defaults for ${name} substitution
	Steps   []Step         `yaml:"steps"`

	// Name identifies the scenario in logs and results: the path or name it
	// was loaded from.
	Name string `yaml:"-"`

	src  []byte
	sets map[string]string
}

// Step is one action. Type selects which of the fields below apply. The
// packet types map to the MQTT control packets; the pseudo-types (raw, recv,
// sleep, expect-close) drive the harness itself.
type Step struct {
	Type  string        `yaml:"type"`
	Label string        `yaml:"label"`
	Delay time.Duration `yaml:"delay"` // pause before this step runs

	// Repeat sends a packet or raw step this many times. A nonzero
	// packet-id goes up by one with each repetition, skipping 0.
	Repeat int `yaml:"repeat"`

	// Fixed-header flags override (low nibble, 0-15). When nil the correct
	// reserved bits for Type are used; set it to probe reserved-bit handling.
	Flags *uint8 `yaml:"flags"`

	// CONNECT. Username and Password default to RunOptions.Username and
	// Password; set either to "" to send none.
	ClientID      string  `yaml:"client-id"`
	Clean         *bool   `yaml:"clean"`
	KeepAlive     uint16  `yaml:"keepalive"`
	Username      *string `yaml:"username"`
	Password      *string `yaml:"password"`
	ProtocolName  string  `yaml:"protocol-name"`  // override, e.g. "MQIsdp" or a bad name
	ProtocolLevel *uint8  `yaml:"protocol-level"` // override the level byte only; the body is still encoded for the run's version
	ConnectFlags  *uint8  `yaml:"connect-flags"`  // override the whole connect flags byte, e.g. to set the reserved bit
	WillTopic     string  `yaml:"will-topic"`
	WillPayload   string  `yaml:"will-payload"`
	WillQoS       uint8   `yaml:"will-qos"`
	WillRetain    bool    `yaml:"will-retain"`

	// PUBLISH
	Topic   string `yaml:"topic"`
	Payload string `yaml:"payload"`
	QoS     uint8  `yaml:"qos"`
	Retain  bool   `yaml:"retain"`
	Dup     bool   `yaml:"dup"`

	// Generated payloads, for PUBLISH instead of payload, and on recv to
	// check every PUBLISH that arrives (payload-size 0 there accepts any
	// length). See internal/pattern.
	PayloadPattern string `yaml:"payload-pattern"`
	PayloadSize    int    `yaml:"payload-size"`

	// Identified packets (PUBLISH QoS>0, PUBACK/REC/REL/COMP, SUB/UNSUB).
	PacketID uint16 `yaml:"packet-id"`

	// SUBSCRIBE / UNSUBSCRIBE. Filters may be a plain list of strings or a
	// list of {filter, qos, nolocal, rap, retain-handling}.
	Filters []Filter `yaml:"filters"`

	// Reason is the reason code sent on acks, DISCONNECT and AUTH. On recv
	// it is the reason code the first packet must carry; on expect-close,
	// the reason code of the DISCONNECT (or refusing CONNACK) that must
	// come before the close.
	Reason *uint8 `yaml:"reason"`
	// Reasons, on recv, are the reason codes a SUBACK or UNSUBACK must
	// carry, in order.
	Reasons []uint8 `yaml:"reasons"`
	// ReasonOptional, on expect-close, accepts a close with no DISCONNECT
	// or refusing CONNACK before it; Reason is checked only when one came.
	ReasonOptional bool `yaml:"reason-optional"`

	// MQTT 5 properties.
	Properties *Props `yaml:"properties"`

	// raw: bytes sent exactly as given (hex, whitespace allowed).
	Hex string `yaml:"hex"`

	// recv / expect-close.
	Timeout time.Duration `yaml:"timeout"`
	Count   int           `yaml:"count"`  // recv: how many packets to read (default 1)
	Expect  string        `yaml:"expect"` // recv: a packet type name, "close" or "none"

	// sleep.
	Duration time.Duration `yaml:"duration"`
}

// Filter is one topic filter in a SUBSCRIBE or UNSUBSCRIBE. For UNSUBSCRIBE
// only Filter is used. A bare string in YAML decodes into Filter alone.
type Filter struct {
	Filter         string `yaml:"filter"`
	QoS            uint8  `yaml:"qos"`
	NoLocal        bool   `yaml:"nolocal"`
	RetainAsPub    bool   `yaml:"rap"`
	RetainHandling uint8  `yaml:"retain-handling"`
}

// UnmarshalYAML lets a filter be written either as a bare string or a map.
func (f *Filter) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&f.Filter)
	}
	type raw Filter
	return node.Decode((*raw)(f))
}

// Props is the subset of MQTT 5 properties that matter for ordering and
// edge-case probing. Unset fields are omitted from the packet.
type Props struct {
	SessionExpiry     *uint32    `yaml:"session-expiry"`
	ReceiveMaximum    uint16     `yaml:"receive-maximum"`
	MaximumPacketSize uint32     `yaml:"maximum-packet-size"`
	TopicAliasMaximum uint16     `yaml:"topic-alias-maximum"`
	TopicAlias        uint16     `yaml:"topic-alias"`
	PayloadFormat     *uint8     `yaml:"payload-format"`
	MessageExpiry     uint32     `yaml:"message-expiry"`
	ContentType       string     `yaml:"content-type"`
	ResponseTopic     string     `yaml:"response-topic"`
	SubscriptionID    int        `yaml:"subscription-identifier"`
	ReasonString      string     `yaml:"reason-string"`
	AuthMethod        string     `yaml:"auth-method"`
	AuthData          string     `yaml:"auth-data"`
	User              []UserProp `yaml:"user"`
}

// UserProp is one User Property key/value pair.
type UserProp struct {
	Key string `yaml:"key" json:"key"`
	Val string `yaml:"val" json:"val"`
}

var tokenRE = regexp.MustCompile(`\$\{([a-zA-Z0-9_.-]+)\}`)

// Load reads the scenario at path. See Parse.
func Load(path string, vars map[string]string) (*Scenario, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(path, src, vars)
}

// LoadFS reads the scenario at path in fsys, such as Scenarios or a test's
// own embedded files. See Parse.
func LoadFS(fsys fs.FS, path string, vars map[string]string) (*Scenario, error) {
	src, err := fs.ReadFile(fsys, path)
	if err != nil {
		return nil, err
	}
	return Parse(path, src, vars)
}

// Parse applies ${token} substitution to src and decodes the YAML. vars
// override the scenario's own vars:, and the builtins (rand, pid, time,
// date) are available unless overridden. An undefined token, an unknown
// field or step type, or a scenario with no steps is an error. name is used
// in logs and results.
func Parse(name string, src []byte, vars map[string]string) (*Scenario, error) {
	// Decode vars first so they can seed substitution, then substitute over
	// the whole document and decode again. vars win.
	var head struct {
		Vars map[string]any `yaml:"vars"`
	}
	if err := yaml.Unmarshal(src, &head); err != nil {
		return nil, fmt.Errorf("%s: parsing vars: %w", name, err)
	}

	vals := builtinVars()
	for k, v := range head.Vars {
		vals[k] = fmt.Sprint(v)
	}
	maps.Copy(vals, vars)
	if err := resolveVars(vals); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	substituted, err := substitute(string(src), vals)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	sc := &Scenario{Name: name, src: src, sets: maps.Clone(vars)}
	dec := yaml.NewDecoder(strings.NewReader(substituted))
	dec.KnownFields(true)
	if err := dec.Decode(sc); err != nil {
		return nil, fmt.Errorf("%s: parsing scenario: %w", name, err)
	}
	if err := sc.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return sc, nil
}

// withVars re-resolves the steps with vars added to the ones the scenario
// was loaded with. Version, Address and TLS are kept as they are.
func (sc *Scenario) withVars(vars map[string]string) (*Scenario, error) {
	sets := maps.Clone(sc.sets)
	if sets == nil {
		sets = map[string]string{}
	}
	maps.Copy(sets, vars)
	n, err := Parse(sc.Name, sc.src, sets)
	if err != nil {
		return nil, err
	}
	n.Version, n.Address, n.TLS = sc.Version, sc.Address, sc.TLS
	return n, nil
}

// validate catches scenario mistakes at load time, so that a run only fails
// on what the broker does.
func (sc *Scenario) validate() error {
	if len(sc.Steps) == 0 {
		return fmt.Errorf("scenario has no steps")
	}
	if _, err := ParseVersion(sc.Version); err != nil {
		return err
	}
	for i, s := range sc.Steps {
		if err := s.validate(); err != nil {
			return fmt.Errorf("step %d (%s): %w", i+1, s.Type, err)
		}
	}
	return nil
}

func (s *Step) validate() error {
	if err := s.validateType(); err != nil {
		return err
	}
	if s.Repeat < 0 || (s.Repeat > 1 && !s.sends()) {
		return fmt.Errorf("repeat applies to packet and raw steps only")
	}
	if len(s.Reasons) > 0 && s.Type != "recv" {
		return fmt.Errorf("reasons applies to recv only")
	}
	if s.ReasonOptional && !s.expectsClose() {
		return fmt.Errorf("reason-optional applies to expect-close only")
	}
	if (s.ProtocolLevel != nil || s.ConnectFlags != nil) && s.Type != "connect" {
		return fmt.Errorf("protocol-level and connect-flags apply to connect only")
	}
	return nil
}

func (s *Step) validateType() error {
	switch {
	case s.sends(), s.Type == "sleep", s.Type == "expect-close":
		return nil
	case s.Type == "recv":
		switch e := strings.ToLower(s.Expect); e {
		case "", "close", "none":
			return nil
		default:
			if _, ok := packetTypeByName(e); !ok {
				return fmt.Errorf("unknown expect value %q", s.Expect)
			}
			return nil
		}
	}
	return fmt.Errorf("unknown step type %q", s.Type)
}

// sends reports whether the step writes to the connection: a control
// packet or raw bytes.
func (s *Step) sends() bool {
	_, ok := packetSteps[s.Type]
	return ok || s.Type == "raw"
}

// expectsClose reports whether the step waits for the server to close the
// connection.
func (s *Step) expectsClose() bool {
	return s.Type == "expect-close" || (s.Type == "recv" && strings.EqualFold(s.Expect, "close"))
}

// resolveVars expands tokens that appear inside other vars' values, so a
// value like "fuzz-${rand}" becomes fully concrete before the document pass.
// It iterates to a fixed point and errors on a reference cycle.
func resolveVars(vals map[string]string) error {
	const maxPasses = 20
	for pass := 0; pass < maxPasses; pass++ {
		changed := false
		for k, v := range vals {
			nv := tokenRE.ReplaceAllStringFunc(v, func(m string) string {
				name := tokenRE.FindStringSubmatch(m)[1]
				if rv, ok := vals[name]; ok && rv != m {
					return rv
				}
				return m
			})
			if nv != v {
				vals[k] = nv
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	// Any token still referencing a defined var is a reference cycle: a
	// concrete value would have absorbed it above.
	for k, v := range vals {
		for _, m := range tokenRE.FindAllStringSubmatch(v, -1) {
			if _, ok := vals[m[1]]; ok {
				return fmt.Errorf("vars reference cycle involving %q", k)
			}
		}
	}
	return nil
}

// substitute replaces every ${name} with vals[name], erroring on any token
// with no value so a typo fails loudly instead of sending garbage.
func substitute(s string, vals map[string]string) (string, error) {
	var missing []string
	out := tokenRE.ReplaceAllStringFunc(s, func(m string) string {
		name := tokenRE.FindStringSubmatch(m)[1]
		if v, ok := vals[name]; ok {
			return v
		}
		missing = append(missing, name)
		return m
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("undefined token(s): %s (define in vars: or pass %s=... as a var)",
			strings.Join(dedupe(missing), ", "), missing[0])
	}
	return out, nil
}

func builtinVars() map[string]string {
	now := time.Now()
	return map[string]string{
		"rand": fmt.Sprintf("%08x", now.UnixNano()&0xffffffff),
		"pid":  fmt.Sprint(os.Getpid()),
		"time": fmt.Sprint(now.Unix()),
		"date": now.Format("20060102-150405"),
	}
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// ParseVersion maps 31/3, 311/4 and 5 to the wire protocol-version byte. An
// empty string is 3.1.1.
func ParseVersion(s string) (byte, error) {
	switch strings.TrimSpace(s) {
	case "31", "3":
		return 3, nil
	case "311", "4", "":
		return 4, nil
	case "5":
		return 5, nil
	}
	return 0, fmt.Errorf("unknown protocol version %q (use 31, 311 or 5)", s)
}

// packetTypeByName maps a case-insensitive packet name to its type byte, for
// the recv "expect" field.
func packetTypeByName(name string) (byte, bool) {
	want := strings.ToLower(strings.TrimSpace(name))
	for b, n := range packets.PacketNames {
		if strings.ToLower(n) == want {
			return b, true
		}
	}
	return 0, false
}

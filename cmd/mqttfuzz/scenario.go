package main

import (
	"fmt"
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
type Scenario struct {
	Version string         `yaml:"version"` // 31 | 311 | 5; the flag -v overrides it
	Server  string         `yaml:"server"`  // the flag -server overrides it
	Vars    map[string]any `yaml:"vars"`    // token defaults for ${name} substitution
	Steps   []Step         `yaml:"steps"`
}

// Step is one action. Type selects which of the fields below apply. The
// packet types map to the MQTT control packets; the pseudo-types (raw, recv,
// sleep, expect-close) drive the harness itself.
type Step struct {
	Type  string        `yaml:"type"`
	Label string        `yaml:"label"`
	Delay time.Duration `yaml:"delay"` // pause before this step runs

	// Fixed-header flags override (low nibble, 0-15). When nil the correct
	// reserved bits for Type are used; set it to probe reserved-bit handling.
	Flags *uint8 `yaml:"flags"`

	// CONNECT
	ClientID     string `yaml:"client-id"`
	Clean        *bool  `yaml:"clean"`
	KeepAlive    uint16 `yaml:"keepalive"`
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	ProtocolName string `yaml:"protocol-name"` // override, e.g. "MQIsdp" or a bad name
	WillTopic    string `yaml:"will-topic"`
	WillPayload  string `yaml:"will-payload"`
	WillQoS      uint8  `yaml:"will-qos"`
	WillRetain   bool   `yaml:"will-retain"`

	// PUBLISH
	Topic   string `yaml:"topic"`
	Payload string `yaml:"payload"`
	QoS     uint8  `yaml:"qos"`
	Retain  bool   `yaml:"retain"`
	Dup     bool   `yaml:"dup"`

	// Identified packets (PUBLISH QoS>0, PUBACK/REC/REL/COMP, SUB/UNSUB).
	PacketID uint16 `yaml:"packet-id"`

	// SUBSCRIBE / UNSUBSCRIBE. Filters may be a plain list of strings or a
	// list of {filter, qos, nolocal, rap, retain-handling}.
	Filters []Filter `yaml:"filters"`

	// Acks, DISCONNECT, AUTH.
	Reason uint8 `yaml:"reason"`

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
	Key string `yaml:"key"`
	Val string `yaml:"val"`
}

var tokenRE = regexp.MustCompile(`\$\{([a-zA-Z0-9_.-]+)\}`)

// loadScenario reads path, applies ${token} substitution and decodes the
// YAML. overrides come from -set flags and win over the scenario's vars.
// builtins (rand, pid, time, date) are available unless overridden.
func loadScenario(path string, overrides map[string]string) (*Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Decode vars first so they can seed substitution, then substitute over
	// the whole document and decode again. Values set with -set win.
	var head struct {
		Vars map[string]any `yaml:"vars"`
	}
	if err := yaml.Unmarshal(raw, &head); err != nil {
		return nil, fmt.Errorf("parsing vars: %w", err)
	}

	vals := builtinVars()
	for k, v := range head.Vars {
		vals[k] = fmt.Sprint(v)
	}
	for k, v := range overrides {
		vals[k] = v
	}
	if err := resolveVars(vals); err != nil {
		return nil, err
	}

	substituted, err := substitute(string(raw), vals)
	if err != nil {
		return nil, err
	}

	var sc Scenario
	dec := yaml.NewDecoder(strings.NewReader(substituted))
	dec.KnownFields(true)
	if err := dec.Decode(&sc); err != nil {
		return nil, fmt.Errorf("parsing scenario: %w", err)
	}
	if len(sc.Steps) == 0 {
		return nil, fmt.Errorf("scenario has no steps")
	}
	return &sc, nil
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
		return "", fmt.Errorf("undefined token(s): %s (define in vars: or pass -set %s=...)",
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

// parseVersion maps 31/3, 311/4 and 5 to the wire protocol-version byte.
func parseVersion(s string) (byte, error) {
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

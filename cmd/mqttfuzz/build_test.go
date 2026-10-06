package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

func TestSubstitute(t *testing.T) {
	vals := map[string]string{"a": "x", "b": "y"}
	got, err := substitute("${a}-${b}-${a}", vals)
	if err != nil {
		t.Fatal(err)
	}
	if got != "x-y-x" {
		t.Fatalf("got %q, want %q", got, "x-y-x")
	}

	if _, err := substitute("${missing}", vals); err == nil {
		t.Fatal("expected error for undefined token")
	}
}

func TestResolveVarsNested(t *testing.T) {
	vals := map[string]string{"rand": "beef", "id": "fuzz-${rand}"}
	if err := resolveVars(vals); err != nil {
		t.Fatal(err)
	}
	if vals["id"] != "fuzz-beef" {
		t.Fatalf("nested var not expanded: %q", vals["id"])
	}
}

func TestResolveVarsCycle(t *testing.T) {
	vals := map[string]string{"a": "${b}", "b": "${a}"}
	if err := resolveVars(vals); err == nil {
		t.Fatal("expected a cycle error")
	}
}

// TestEncodeReservedBits checks that SUBSCRIBE and UNSUBSCRIBE carry the
// mandatory 0010 fixed-header flags, and that a step's flags override stamps
// the low nibble without tripping the body encoder.
func TestEncodeReservedBits(t *testing.T) {
	sub := &Step{Type: "subscribe", PacketID: 1, Filters: []Filter{{Filter: "a/b", QoS: 1}}}
	b, err := encodeStep(sub, 4)
	if err != nil {
		t.Fatal(err)
	}
	if b[0] != (packets.Subscribe<<4 | 0x02) {
		t.Fatalf("SUBSCRIBE first byte = 0x%02x, want 0x%02x", b[0], packets.Subscribe<<4|0x02)
	}

	qos3 := uint8(0x06) // 0b0110 -> qos bits = 11
	pub := &Step{Type: "publish", Topic: "a", Payload: "x", Flags: &qos3}
	b, err = encodeStep(pub, 5)
	if err != nil {
		t.Fatalf("flags override should not trip the encoder: %v", err)
	}
	if b[0]&0x0F != 0x06 {
		t.Fatalf("PUBLISH low nibble = 0x%x, want 0x6", b[0]&0x0F)
	}
}

func TestEncodeRawHex(t *testing.T) {
	b, err := encodeStep(&Step{Type: "raw", Hex: "e0 00"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 2 || b[0] != 0xe0 || b[1] != 0x00 {
		t.Fatalf("raw = % x, want e0 00", b)
	}
}

// TestExampleScenariosParse makes sure the committed example scenarios stay
// loadable as the schema evolves.
func TestExampleScenariosParse(t *testing.T) {
	matches, err := filepath.Glob("scenarios/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("no example scenarios found")
	}
	for _, m := range matches {
		t.Run(filepath.Base(m), func(t *testing.T) {
			sc, err := loadScenario(m, nil)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			ver, err := parseVersion(sc.Version)
			if err != nil {
				t.Fatalf("version: %v", err)
			}
			for i, s := range sc.Steps {
				switch s.Type {
				case "recv", "sleep", "expect-close":
					continue
				}
				if _, err := encodeStep(&s, ver); err != nil {
					t.Fatalf("step %d (%s): encode: %v", i+1, s.Type, err)
				}
			}
		})
	}
}

func TestMain(m *testing.M) { os.Exit(m.Run()) }

func TestPublishPayloadPattern(t *testing.T) {
	b, err := publishPayload(&Step{PayloadSize: 200})
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 200 || b[0] != ' ' || b[94] != '~' || b[95] != ' ' {
		t.Fatalf("ascii payload = %q", b)
	}
	if _, err := publishPayload(&Step{Payload: "x", PayloadSize: 1}); err == nil {
		t.Fatal("expected an error for payload with payload-size")
	}
	if _, err := publishPayload(&Step{PayloadPattern: "nope", PayloadSize: 1}); err == nil {
		t.Fatal("expected an error for an unknown pattern")
	}
}

package cli

import (
	"fmt"
	"sort"
	"strings"
)

// Patterns are deterministic payload generators: byte i of a payload depends
// only on i, so a receiver can check every byte of any length without
// knowing what was sent, and a truncated or corrupted payload shows the
// offset where it went wrong.
var Patterns = map[string]func(i int) byte{
	// alpha is abcd...xyzabc..., what mqttpub -size always sent.
	"alpha": func(i int) byte { return 'a' + byte(i%26) },
	// ascii scrolls through the 95 printable characters, space to '~'.
	"ascii": func(i int) byte { return ' ' + byte(i%95) },
	// 01 is the text "0101...".
	"01": func(i int) byte { return '0' + byte(i%2) },
	// binary counts 00 01 ... ff 00 01 ..., not valid UTF-8, like the
	// protobuf payloads of Sparkplug B.
	"binary": func(i int) byte { return byte(i) },
}

// PatternNames lists the pattern names for flag help and errors.
func PatternNames() string {
	names := make([]string, 0, len(Patterns))
	for n := range Patterns {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// MakePattern returns size bytes of the named pattern.
func MakePattern(name string, size int) ([]byte, error) {
	f, ok := Patterns[name]
	if !ok {
		return nil, fmt.Errorf("unknown pattern %q (want %s)", name, PatternNames())
	}
	b := make([]byte, size)
	for i := range b {
		b[i] = f(i)
	}
	return b, nil
}

// PatternError is VerifyPattern's error for a payload that does not match.
// Offset is the first byte that is wrong or, for a payload of the wrong
// size, the first byte missing or extra.
type PatternError struct {
	Offset int
	msg    string
}

func (e *PatternError) Error() string { return e.msg }

// VerifyPattern checks b against the named pattern and, when size is not
// negative, its length. It returns nil when b matches and a *PatternError
// when it does not.
func VerifyPattern(name string, b []byte, size int) error {
	f, ok := Patterns[name]
	if !ok {
		return fmt.Errorf("unknown pattern %q (want %s)", name, PatternNames())
	}
	for i, c := range b {
		if want := f(i); c != want {
			return &PatternError{i, fmt.Sprintf("%s pattern mismatch at offset %d of %d: got 0x%02x, want 0x%02x", name, i, len(b), c, want)}
		}
	}
	if size >= 0 && len(b) != size {
		return &PatternError{min(len(b), size), fmt.Sprintf("%s pattern intact but %d bytes, want %d", name, len(b), size)}
	}
	return nil
}

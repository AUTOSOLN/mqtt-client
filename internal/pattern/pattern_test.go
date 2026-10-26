package pattern

import (
	"errors"
	"testing"
)

func TestPatternRoundTrip(t *testing.T) {
	for name := range Patterns {
		b, err := Make(name, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if err := Verify(name, b, 1000); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := Verify(name, b[:999], 1000); err == nil {
			t.Fatalf("%s: truncated payload verified", name)
		}
		if err := Verify(name, b[:999], -1); err != nil {
			t.Fatalf("%s: any-length check: %v", name, err)
		}
		b[500]++
		if err := Verify(name, b, 1000); err == nil {
			t.Fatalf("%s: corrupted payload verified", name)
		}
	}
}

func TestPatternAlphaUnchanged(t *testing.T) {
	b, _ := Make("alpha", 28)
	if string(b) != "abcdefghijklmnopqrstuvwxyzab" {
		t.Fatalf("alpha = %q", b)
	}
}

func TestPatternErrorOffset(t *testing.T) {
	b, _ := Make("binary", 10)
	bad := append([]byte(nil), b...)
	bad[3]++
	for _, tc := range []struct {
		b    []byte
		size int
		want int
	}{
		{bad, 10, 3},
		{b[:7], 10, 7}, // missing bytes
		{b, 8, 8},      // extra bytes
	} {
		var pe *Error
		if err := Verify("binary", tc.b, tc.size); !errors.As(err, &pe) || pe.Offset != tc.want {
			t.Errorf("%d bytes, size %d: %v, want offset %d", len(tc.b), tc.size, err, tc.want)
		}
	}
}

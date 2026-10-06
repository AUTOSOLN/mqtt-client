package cli

import "testing"

func TestPatternRoundTrip(t *testing.T) {
	for name := range Patterns {
		b, err := MakePattern(name, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if err := VerifyPattern(name, b, 1000); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := VerifyPattern(name, b[:999], 1000); err == nil {
			t.Fatalf("%s: truncated payload verified", name)
		}
		if err := VerifyPattern(name, b[:999], -1); err != nil {
			t.Fatalf("%s: any-length check: %v", name, err)
		}
		b[500]++
		if err := VerifyPattern(name, b, 1000); err == nil {
			t.Fatalf("%s: corrupted payload verified", name)
		}
	}
}

func TestPatternAlphaUnchanged(t *testing.T) {
	b, _ := MakePattern("alpha", 28)
	if string(b) != "abcdefghijklmnopqrstuvwxyzab" {
		t.Fatalf("alpha = %q", b)
	}
}

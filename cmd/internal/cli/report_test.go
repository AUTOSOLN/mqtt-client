package cli

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestLatencySummary(t *testing.T) {
	var l Latencies
	if l.Summary() != nil {
		t.Fatal("summary of nothing is not nil")
	}
	for i := 100; i >= 1; i-- {
		l.Add(time.Duration(i) * time.Millisecond)
	}
	l.Add(1500 * time.Microsecond)
	got := *l.Summary()
	if want := (Latency{P50: 50, P95: 95, P99: 99, Max: 100}); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestExitCode(t *testing.T) {
	err := errors.New("x")
	if got := ExitCode(err); got != 1 {
		t.Errorf("plain error: %d, want 1", got)
	}
	if got := ExitCode(fmt.Errorf("wrapped: %w", Setup(err))); got != 2 {
		t.Errorf("wrapped SetupError: %d, want 2", got)
	}
	if Setup(nil) != nil {
		t.Error("Setup(nil) is not nil")
	}
}

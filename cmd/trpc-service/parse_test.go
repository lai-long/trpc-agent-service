package main

import (
	"context"
	"testing"
	"time"
)

// TestParseInt pins the env fallback contract: unparsable or non-positive
// input must degrade to the documented default instead of propagating
// garbage into rate limiters and thresholds.
func TestParseInt(t *testing.T) {
	cases := []struct {
		in   string
		def  int
		want int
	}{
		{"7", 1, 7},
		{"0", 5, 5},                      // non-positive falls back
		{"-3", 5, 5},                     // negative falls back
		{"abc", 9, 9},                    // unparsable falls back
		{"", 9, 9},                       // empty falls back
		{" 7", 9, 9},                     // no implicit trimming
		{"9999999999999999999999", 9, 9}, // out of range falls back
	}
	for _, tc := range cases {
		if got := parseInt(tc.in, tc.def); got != tc.want {
			t.Errorf("parseInt(%q, %d) = %d, want %d", tc.in, tc.def, got, tc.want)
		}
	}
}

func TestParseFloat(t *testing.T) {
	cases := []struct {
		in   string
		def  float64
		want float64
	}{
		{"2.5", 1, 2.5},
		{"1e2", 1, 100},
		{"0", 5, 5},
		{"-1.5", 5, 5},
		{"abc", 9, 9},
		{"", 9, 9},
	}
	for _, tc := range cases {
		if got := parseFloat(tc.in, tc.def); got != tc.want {
			t.Errorf("parseFloat(%q, %v) = %v, want %v", tc.in, tc.def, got, tc.want)
		}
	}
}

func TestParseDuration(t *testing.T) {
	cases := []struct {
		in   string
		def  time.Duration
		want time.Duration
	}{
		{"3s", time.Second, 3 * time.Second},
		{"500ms", time.Minute, 500 * time.Millisecond},
		{"0", time.Minute, time.Minute},
		{"-5s", time.Minute, time.Minute},
		{"bogus", time.Minute, time.Minute},
		{"", time.Minute, time.Minute},
	}
	for _, tc := range cases {
		if got := parseDuration(tc.in, tc.def); got != tc.want {
			t.Errorf("parseDuration(%q, %v) = %v, want %v", tc.in, tc.def, got, tc.want)
		}
	}
}

// TestSleepFor covers all three outcomes: a done context wins immediately,
// a short wait completes, and a cancellation mid-wait returns early.
func TestSleepFor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepFor(ctx, time.Hour) {
		t.Error("sleepFor on a canceled context must return false")
	}

	start := time.Now()
	if !sleepFor(context.Background(), 10*time.Millisecond) {
		t.Error("sleepFor on a live context must return true")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("10ms sleep took %v", elapsed)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	timer := time.AfterFunc(20*time.Millisecond, cancel2)
	defer timer.Stop()
	start = time.Now()
	if sleepFor(ctx2, 10*time.Second) {
		t.Error("sleepFor must return false when the context is canceled mid-wait")
	}
	if elapsed := time.Since(start); elapsed >= 5*time.Second {
		t.Errorf("mid-wait cancellation took %v, must return early", elapsed)
	}
}

// TestIsLoopbackBind pins the dev-sentinel bind gate: only loopback IPs (and
// the "localhost" name) accept the unauthenticated Admin API.
func TestIsLoopbackBind(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:8081", true},
		{"127.9.9.9:8081", true},
		{"[::1]:8081", true},
		{"localhost:8081", true},
		{":8081", false},        // empty host binds all interfaces
		{"0.0.0.0:8081", false}, // unspecified address
		{"192.168.1.5:8081", false},
		{"example.com:8081", false}, // non-loopback name
		{"no-port", false},          // unparsable
	}
	for _, tc := range cases {
		if got := isLoopbackBind(tc.addr); got != tc.want {
			t.Errorf("isLoopbackBind(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

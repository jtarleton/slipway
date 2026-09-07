package cron

import (
	"testing"
	"time"
)

func at(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04", s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestValid(t *testing.T) {
	ok := []string{"* * * * *", "0 3 * * *", "*/15 * * * *", "0 0 1,15 * *", "30 2 * * 1-5", "0 4 * * 7"}
	for _, s := range ok {
		if !Valid(s) {
			t.Errorf("Valid(%q) = false, want true", s)
		}
	}
	bad := []string{"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "0 0 32 * *", "*/0 * * * *", "a * * * *"}
	for _, s := range bad {
		if Valid(s) {
			t.Errorf("Valid(%q) = true, want false", s)
		}
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		spec, when string
		want       bool
	}{
		{"0 3 * * *", "2026-09-07 03:00", true},
		{"0 3 * * *", "2026-09-07 03:01", false},
		{"0 3 * * *", "2026-09-07 04:00", false},
		{"*/15 * * * *", "2026-09-07 09:30", true},
		{"*/15 * * * *", "2026-09-07 09:31", false},
		{"30 2 * * 1-5", "2026-09-07 02:30", true},  // 2026-09-07 is a Monday
		{"30 2 * * 1-5", "2026-09-06 02:30", false}, // Sunday — outside Mon-Fri
		{"0 0 1,15 * *", "2026-09-15 00:00", true},
		{"0 0 1,15 * *", "2026-09-16 00:00", false},
		{"0 4 * * 0", "2026-09-06 04:00", true}, // 2026-09-06 is a Sunday
		{"0 4 * * 7", "2026-09-06 04:00", true}, // 7 == Sunday
	}
	for _, c := range cases {
		got, err := Match(c.spec, at(c.when))
		if err != nil {
			t.Fatalf("Match(%q,%q): %v", c.spec, c.when, err)
		}
		if got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.spec, c.when, got, c.want)
		}
	}
}

func TestNext(t *testing.T) {
	n, err := Next("0 3 * * *", at("2026-09-07 05:00"))
	if err != nil {
		t.Fatal(err)
	}
	if !n.Equal(at("2026-09-08 03:00")) {
		t.Errorf("Next daily-3am after 5am = %s, want next day 03:00", n)
	}

	n, _ = Next("*/15 * * * *", at("2026-09-07 09:07"))
	if !n.Equal(at("2026-09-07 09:15")) {
		t.Errorf("Next */15 after 09:07 = %s, want 09:15", n)
	}

	// Strictly after: a time that already matches gets the following one.
	n, _ = Next("0 3 * * *", at("2026-09-07 03:00"))
	if !n.Equal(at("2026-09-08 03:00")) {
		t.Errorf("Next when already matching = %s, want the next occurrence", n)
	}

	if _, err := Next("0 0 30 2 *", at("2026-01-01 00:00")); err == nil {
		t.Error("Next(Feb 30) should fail — no match within a year")
	}
}

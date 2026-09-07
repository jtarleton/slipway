package jobs

import (
	"regexp"
	"strings"
	"testing"
)

// dns1123Label is the pattern the Kubernetes API server applies to Job names.
var dns1123Label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func TestNameIsDeterministic(t *testing.T) {
	// The whole crash-recovery story rests on this: the reconciler resubmits
	// after a restart and must land on the same Job.
	first := Name("prod", 4021, 2, KindSnapshot)
	for i := 0; i < 100; i++ {
		if got := Name("prod", 4021, 2, KindSnapshot); got != first {
			t.Fatalf("Name is not deterministic: %q then %q", first, got)
		}
	}
	if want := "slipway-snapshot-prod-4021-2"; first != want {
		t.Errorf("Name() = %q, want %q", first, want)
	}
}

func TestNameDistinguishesEveryInput(t *testing.T) {
	base := Name("stage", 7, 1, KindUpdatedb)

	tests := []struct {
		what string
		got  string
	}{
		{"environment", Name("prod", 7, 1, KindUpdatedb)},
		{"deployment id", Name("stage", 8, 1, KindUpdatedb)},
		{"sequence", Name("stage", 7, 2, KindUpdatedb)},
		{"kind", Name("stage", 7, 1, KindRestore)},
	}

	for _, tt := range tests {
		if tt.got == base {
			t.Errorf("changing the %s did not change the name; both are %q", tt.what, base)
		}
	}
}

func TestNameIsAlwaysAValidLabel(t *testing.T) {
	envs := []string{
		"dev",
		"PROD",                     // uppercase
		"feature/DRUP-1123_hotfix", // branch-shaped, slashes and underscores
		"-leading-and-trailing-",   // hyphens at the edges
		"a b   c",                  // runs of whitespace
		"jamestarleton.com",        // dots
		strings.Repeat("very-long-environment-name", 6),
		"日本語", // nothing usable at all
	}

	for _, env := range envs {
		for _, kind := range Kinds() {
			name := Name(env, 999999, 12, kind)

			if len(name) > maxNameLen {
				t.Errorf("Name(%q, ...) = %q (%d chars), exceeds the %d-char limit",
					env, name, len(name), maxNameLen)
			}
			if !dns1123Label.MatchString(name) {
				t.Errorf("Name(%q, ...) = %q, not a valid DNS-1123 label", env, name)
			}
		}
	}
}

func TestNameDisambiguatesTruncatedNames(t *testing.T) {
	// Two environments sharing a long prefix must not collide once truncated —
	// a collision here would mean one environment's snapshot silently adopting
	// another's Job.
	prefix := strings.Repeat("staging-cluster-", 4)

	a := Name(prefix+"alpha", 1, 1, KindSnapshot)
	b := Name(prefix+"beta", 1, 1, KindSnapshot)

	if a == b {
		t.Fatalf("two distinct environments produced the same name: %q", a)
	}
	if len(a) != maxNameLen || len(b) != maxNameLen {
		t.Errorf("truncated names should fill the budget: got %d and %d, want %d",
			len(a), len(b), maxNameLen)
	}
}

func TestSanitize(t *testing.T) {
	tests := []struct{ in, want string }{
		{"dev", "dev"},
		{"PROD", "prod"},
		{"feature/DRUP-1123", "feature-drup-1123"},
		{"a__b__c", "a-b-c"},
		{"-leading", "leading"},
		{"trailing-", "trailing"},
		{"a   b", "a-b"},
		{"...", ""},
		{"", ""},
	}

	for _, tt := range tests {
		if got := sanitize(tt.in); got != tt.want {
			t.Errorf("sanitize(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestKindValid(t *testing.T) {
	for _, k := range Kinds() {
		if !k.Valid() {
			t.Errorf("%s.Valid() = false, want true", k)
		}
	}
	if Kind("deploy-everything").Valid() {
		t.Error("an unknown Kind reported itself valid")
	}
}

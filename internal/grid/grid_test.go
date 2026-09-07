package grid

import (
	"context"
	"strings"
	"testing"

	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/store"
)

func TestCode(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name string
		w    k8s.Workload
		want string
	}{
		{"never deployed", k8s.Workload{}, "—"},
		{"pinned", k8s.Workload{Found: true, Image: "d@" + digest, Digest: digest}, "sha256:aaaaaaaaaaaa"},
		{"tag-pinned, resolved", k8s.Workload{Found: true, Image: "reg/d:prod", RunningDigest: digest}, "sha256:aaaaaaaaaaaa (tag prod)"},
		{"untracked", k8s.Workload{Found: true, Image: "reg/d:prod"}, "reg/d:prod (untracked)"},
	}
	for _, tc := range tests {
		if got := Code(tc.w); got != tc.want {
			t.Errorf("%s: Code() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestStateAndPinned(t *testing.T) {
	if got := State(k8s.Workload{}); got != "not deployed" {
		t.Errorf("State(missing) = %q", got)
	}
	if got := State(k8s.Workload{Found: true, Desired: 2, Ready: 2}); got != "healthy" {
		t.Errorf("State(2/2) = %q", got)
	}
	if got := State(k8s.Workload{Found: true, Desired: 2, Ready: 1}); got != "degraded" {
		t.Errorf("State(1/2) = %q", got)
	}
	if Pinned(k8s.Workload{Found: true}) {
		t.Error("Pinned() true for a tag-only workload")
	}
	if !Pinned(k8s.Workload{Found: true, Digest: "sha256:x"}) {
		t.Error("Pinned() false for a digest workload")
	}
}

// stubReader hands back a fixed workload per namespace and an error for a
// namespace it is told to fail.
type stubReader struct {
	byNS map[string]k8s.Workload
	fail string
}

func (s stubReader) Workload(_ context.Context, ns, _ string) (k8s.Workload, error) {
	if ns == s.fail {
		return k8s.Workload{}, context.DeadlineExceeded
	}
	return s.byNS[ns], nil
}

func TestReadIsOrderedAndAllOrNothing(t *testing.T) {
	envs := []store.Environment{
		{Name: "dev", Namespace: "d"},
		{Name: "prod", Namespace: "p"},
	}
	r := stubReader{byNS: map[string]k8s.Workload{
		"d": {Found: true, Desired: 1, Ready: 1},
		"p": {Found: true, Desired: 3, Ready: 3},
	}}

	cells, err := Read(context.Background(), r, envs, "drupal")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(cells) != 2 || cells[0].Env.Name != "dev" || cells[1].Env.Name != "prod" {
		t.Fatalf("Read returned %+v, want dev then prod", cells)
	}

	r.fail = "p"
	if _, err := Read(context.Background(), r, envs, "drupal"); err == nil {
		t.Fatal("Read succeeded with an unreachable environment; want error")
	}
}

// Package grid is the read model behind "what is running in every environment".
//
// It is the one place that turns an environment plus its Deployment into the
// cells the CLI prints and the web UI renders, so the two never drift on what
// "pinned", "degraded" or "untracked" mean.
package grid

import (
	"context"

	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/store"
)

// Cell is one environment paired with the Deployment Slipway found in it.
type Cell struct {
	Env      store.Environment
	Workload k8s.Workload
}

// Reader is the slice of the cluster the grid needs.
type Reader interface {
	Workload(ctx context.Context, namespace, name string) (k8s.Workload, error)
}

// Read resolves every environment's workload, left to right in promotion order.
//
// One unreachable environment fails the whole read: a grid missing a column is
// more dangerous than no grid, because the column you cannot see is the one you
// then deploy over.
func Read(ctx context.Context, r Reader, envs []store.Environment, workload string) ([]Cell, error) {
	cells := make([]Cell, 0, len(envs))
	for _, env := range envs {
		wl, err := r.Workload(ctx, env.Namespace, workload)
		if err != nil {
			return nil, err
		}
		cells = append(cells, Cell{Env: env, Workload: wl})
	}
	return cells, nil
}

// Code describes what code a cell is running, in one column's width.
//
//   - a digest            the Deployment is pinned and truthful
//   - a digest (tag NAME)  pinned by tag; this is what the pods actually resolved
//   - REF (untracked)      still on a tag, nothing bypassed but nothing pinned
//   - —                    never deployed to
func Code(w k8s.Workload) string {
	switch {
	case !w.Found:
		return "—"
	case w.Digest != "":
		return short(w.Digest)
	case w.RunningDigest != "":
		return short(w.RunningDigest) + " (tag " + tagOf(w.Image) + ")"
	default:
		return w.Image + " (untracked)"
	}
}

// State is the health of a cell: not deployed, healthy, or degraded.
func State(w k8s.Workload) string {
	switch {
	case !w.Found:
		return "not deployed"
	case w.Healthy():
		return "healthy"
	default:
		return "degraded"
	}
}

// Pinned reports whether the Deployment's spec names a digest — the only state
// in which "what is deployed" does not depend on a tag that can move.
func Pinned(w k8s.Workload) bool { return w.Found && w.Digest != "" }

func short(digest string) string {
	if len(digest) > 19 {
		return digest[:19]
	}
	return digest
}

func tagOf(image string) string {
	repo := k8s.Repository(image)
	if len(image) > len(repo)+1 {
		return image[len(repo)+1:]
	}
	return "untagged"
}

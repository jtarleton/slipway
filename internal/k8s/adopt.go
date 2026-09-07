package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// argoAnnotationPrefix and argoInstanceLabel are the metadata ArgoCD stamps onto
// the resources it manages. When ArgoCD is removed but the annotations stay, a
// later reinstall silently re-adopts everything — so a namespace slipway owns
// should carry none of it.
const (
	argoAnnotationPrefix = "argocd.argoproj.io/"
	argoInstanceLabel    = "argocd.argoproj.io/instance"
)

// detachKinds is the set slipway sweeps for ArgoCD metadata: declarative config
// a person or a GitOps tool authors, not the churny resources their controllers
// generate (pods, replicasets, endpoints).
var detachKinds = []schema.GroupVersionResource{
	{Group: "apps", Version: "v1", Resource: "deployments"},
	{Group: "apps", Version: "v1", Resource: "statefulsets"},
	{Group: "apps", Version: "v1", Resource: "daemonsets"},
	{Version: "v1", Resource: "services"},
	{Version: "v1", Resource: "configmaps"},
	{Version: "v1", Resource: "secrets"},
	{Version: "v1", Resource: "persistentvolumeclaims"},
	{Version: "v1", Resource: "serviceaccounts"},
	{Group: "batch", Version: "v1", Resource: "cronjobs"},
	{Group: "batch", Version: "v1", Resource: "jobs"},
	{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
	{Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"},
	{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"},
	{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"},
}

// DetachArgo strips every argocd.argoproj.io/* annotation and the ArgoCD
// instance label from the resources in ns, so slipway is their unambiguous
// owner. onChange, if non-nil, is called for each resource altered.
//
// A resource kind the cluster does not serve is skipped, not an error — not
// every cluster has every API group.
func (c *Client) DetachArgo(ctx context.Context, ns string, onChange func(kind, name string)) (int, error) {
	if c.dyn == nil {
		return 0, fmt.Errorf("detach argo: no dynamic client")
	}

	changed := 0
	for _, gvr := range detachKinds {
		list, err := c.dyn.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			// A kind the cluster does not serve, or that this token cannot
			// read, is not a failure of the sweep.
			if apierrors.IsNotFound(err) || apierrors.IsForbidden(err) || meta.IsNoMatchError(err) {
				continue
			}
			return changed, fmt.Errorf("list %s in %s: %w", gvr.Resource, ns, err)
		}

		for i := range list.Items {
			obj := &list.Items[i]
			patch := argoStripPatch(obj.GetAnnotations(), obj.GetLabels())
			if patch == nil {
				continue
			}
			if _, err := c.dyn.Resource(gvr).Namespace(ns).Patch(
				ctx, obj.GetName(), types.MergePatchType, patch, metav1.PatchOptions{FieldManager: "slipway"},
			); err != nil {
				return changed, fmt.Errorf("patch %s/%s: %w", gvr.Resource, obj.GetName(), err)
			}
			changed++
			if onChange != nil {
				onChange(gvr.Resource, obj.GetName())
			}
		}
	}
	return changed, nil
}

// argoStripPatch builds a JSON merge patch that nulls out ArgoCD's annotations
// and instance label. It returns nil when there is nothing to strip, so callers
// can skip untouched resources.
func argoStripPatch(annotations, labels map[string]string) []byte {
	annNulls := map[string]any{}
	for k := range annotations {
		if strings.HasPrefix(k, argoAnnotationPrefix) {
			annNulls[k] = nil
		}
	}
	labelNulls := map[string]any{}
	if _, ok := labels[argoInstanceLabel]; ok {
		labelNulls[argoInstanceLabel] = nil
	}
	if len(annNulls) == 0 && len(labelNulls) == 0 {
		return nil
	}

	meta := map[string]any{}
	if len(annNulls) > 0 {
		meta["annotations"] = annNulls
	}
	if len(labelNulls) > 0 {
		meta["labels"] = labelNulls
	}
	patch, _ := json.Marshal(map[string]any{"metadata": meta})
	return patch
}

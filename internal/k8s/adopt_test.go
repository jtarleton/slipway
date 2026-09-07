package k8s

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
)

func TestArgoStripPatch(t *testing.T) {
	if p := argoStripPatch(map[string]string{"a": "b"}, map[string]string{"app": "x"}); p != nil {
		t.Errorf("argoStripPatch on clean metadata = %s, want nil", p)
	}

	p := argoStripPatch(
		map[string]string{
			"argocd.argoproj.io/tracking-id":                   "jt-drupal:apps/Deployment:jt-drupal/drupal",
			"argocd.argoproj.io/sync-wave":                     "1",
			"kubectl.kubernetes.io/last-applied-configuration": "{}",
		},
		map[string]string{"argocd.argoproj.io/instance": "jt-drupal", "app.kubernetes.io/name": "drupal"},
	)
	got := string(p)
	want := `{"metadata":{"annotations":{"argocd.argoproj.io/sync-wave":null,"argocd.argoproj.io/tracking-id":null},"labels":{"argocd.argoproj.io/instance":null}}}`
	if got != want {
		t.Errorf("argoStripPatch =\n  %s\nwant\n  %s", got, want)
	}
}

func TestDetachArgoStripsTrackingAndLeavesCleanResourcesAlone(t *testing.T) {
	tracked := unstruct("apps/v1", "Deployment", "jt-drupal", "drupal", map[string]any{
		"argocd.argoproj.io/tracking-id": "jt-drupal:apps/Deployment:jt-drupal/drupal",
	}, nil)
	trackedSvc := unstruct("v1", "Service", "jt-drupal", "drupal", map[string]any{
		"argocd.argoproj.io/tracking-id": "x",
	}, nil)
	clean := unstruct("v1", "ConfigMap", "jt-drupal", "settings", nil, nil)

	// The client-go scheme knows every built-in kind, so the fake can derive a
	// list kind for each GVR DetachArgo sweeps.
	dyn := dynamicfake.NewSimpleDynamicClient(clientscheme.Scheme, tracked, trackedSvc, clean)

	c := NewWithInterface(fake.NewClientset(), dyn)

	var touched []string
	n, err := c.DetachArgo(context.Background(), "jt-drupal", func(kind, name string) {
		touched = append(touched, kind+"/"+name)
	})
	if err != nil {
		t.Fatalf("DetachArgo: %v", err)
	}
	if n != 2 {
		t.Fatalf("changed %d resources, want 2 (the deployment and the service)", n)
	}

	dep, _ := dyn.Resource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}).
		Namespace("jt-drupal").Get(context.Background(), "drupal", metav1.GetOptions{})
	if _, ok := dep.GetAnnotations()["argocd.argoproj.io/tracking-id"]; ok {
		t.Error("deployment still carries the argo tracking annotation")
	}
}

func TestDetachArgoNeedsADynamicClient(t *testing.T) {
	c := NewWithInterface(fake.NewClientset())
	if _, err := c.DetachArgo(context.Background(), "jt-drupal", nil); err == nil {
		t.Fatal("DetachArgo ran with no dynamic client")
	}
}

func unstruct(apiVersion, kind, ns, name string, ann, labels map[string]any) *unstructured.Unstructured {
	m := map[string]any{"name": name, "namespace": ns}
	if ann != nil {
		m["annotations"] = ann
	}
	if labels != nil {
		m["labels"] = labels
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion, "kind": kind, "metadata": m,
	}}
}

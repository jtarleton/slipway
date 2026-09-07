package k8s

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPatchImageTouchesOnlyTheImage(t *testing.T) {
	dep := deployment("ghcr.io/jtarleton/d11app:sha-7374d29", 3, 3)
	dep.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
	dep.Spec.Template.Spec.Containers = append(dep.Spec.Template.Spec.Containers,
		corev1.Container{Name: "sidecar", Image: "busybox:1.36"})

	cs := fake.NewClientset(dep)
	c := NewWithInterface(cs)

	digest := "sha256:4d367e" + zeros(58)
	if err := c.PatchImage(context.Background(), prodNS, "drupal", "drupal", "ghcr.io/jtarleton/d11app@"+digest); err != nil {
		t.Fatalf("PatchImage: %v", err)
	}

	got, err := cs.AppsV1().Deployments(prodNS).Get(context.Background(), "drupal", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	var drupal, sidecar corev1.Container
	for _, ctr := range got.Spec.Template.Spec.Containers {
		switch ctr.Name {
		case "drupal":
			drupal = ctr
		case "sidecar":
			sidecar = ctr
		}
	}

	if drupal.Image != "ghcr.io/jtarleton/d11app@"+digest {
		t.Errorf("drupal image = %q, want the patched digest", drupal.Image)
	}
	// Slipway does not author these manifests; a patch that disturbs anything
	// else is a patch that will eventually delete somebody's volume mount.
	if sidecar.Image != "busybox:1.36" {
		t.Errorf("sidecar image = %q, want it untouched", sidecar.Image)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 3 {
		t.Errorf("replicas changed during an image patch: %v", got.Spec.Replicas)
	}
}

func TestPatchImageRejectsEmptyImage(t *testing.T) {
	c := NewWithInterface(fake.NewClientset(deployment("drupal:11", 1, 1)))
	if err := c.PatchImage(context.Background(), prodNS, "drupal", "drupal", ""); err == nil {
		t.Error("PatchImage accepted an empty image")
	}
}

func rollout(gen, observed int64, desired, updated, replicas, available int32, conds ...appsv1.DeploymentCondition) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "drupal", Namespace: prodNS, Generation: gen},
		Spec:       appsv1.DeploymentSpec{Replicas: &desired},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: observed,
			UpdatedReplicas:    updated,
			Replicas:           replicas,
			AvailableReplicas:  available,
			Conditions:         conds,
		},
	}
}

func TestRolloutStatus(t *testing.T) {
	stalled := appsv1.DeploymentCondition{
		Type:   appsv1.DeploymentProgressing,
		Status: corev1.ConditionFalse,
		Reason: "ProgressDeadlineExceeded",
	}

	tests := []struct {
		name    string
		dep     *appsv1.Deployment
		done    bool
		stalled bool
	}{
		{"controller has not observed the patch", rollout(2, 1, 3, 3, 3, 3), false, false},
		{"replicas still updating", rollout(2, 2, 3, 1, 3, 1), false, false},
		{"old replicas terminating", rollout(2, 2, 3, 3, 5, 3), false, false},
		{"updated but not yet available", rollout(2, 2, 3, 3, 3, 1), false, false},
		{"complete", rollout(2, 2, 3, 3, 3, 3), true, false},
		{"stalled", rollout(2, 2, 3, 1, 3, 1, stalled), false, true},

		// A stall must be reported even when the counts look complete,
		// otherwise a wedged rollout reads as a success.
		{"stalled despite healthy counts", rollout(2, 2, 3, 3, 3, 3, stalled), false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rolloutStatus(tt.dep)
			if got.Done != tt.done || got.Stalled != tt.stalled {
				t.Errorf("rolloutStatus = {done:%v stalled:%v}, want {done:%v stalled:%v} — %s",
					got.Done, got.Stalled, tt.done, tt.stalled, got.Message)
			}
			if got.Message == "" {
				t.Error("rolloutStatus returned no message; the log stream has nothing to show")
			}
		})
	}
}

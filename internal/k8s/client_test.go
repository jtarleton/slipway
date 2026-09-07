package k8s

import (
	"context"
	"testing"

	"github.com/jtarleton/slipway/internal/jobs"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const prodNS = "jt-drupal"

func deployment(image string, desired, ready int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "drupal", Namespace: prodNS},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "drupal", Image: image}}},
			},
		},
		Status: appsv1.DeploymentStatus{ReadyReplicas: ready},
	}
}

func TestWorkloadReadsTheDeployedDigest(t *testing.T) {
	digest := "sha256:9f3c1a" + zeros(58)
	c := NewWithInterface(fake.NewClientset(deployment("ghcr.io/jtarleton/d11app@"+digest, 2, 2)))

	got, err := c.Workload(context.Background(), prodNS, "drupal")
	if err != nil {
		t.Fatalf("Workload: %v", err)
	}
	if !got.Found {
		t.Fatal("Workload reported the deployment missing")
	}
	if got.Digest != digest {
		t.Errorf("Digest = %q, want %q", got.Digest, digest)
	}
	if !got.Healthy() {
		t.Errorf("Healthy() = false for %d/%d replicas", got.Ready, got.Desired)
	}
}

func TestWorkloadPartiallyRolledOutIsNotHealthy(t *testing.T) {
	c := NewWithInterface(fake.NewClientset(deployment("drupal@sha256:ab"+zeros(62), 3, 1)))

	got, err := c.Workload(context.Background(), prodNS, "drupal")
	if err != nil {
		t.Fatalf("Workload: %v", err)
	}
	if got.Healthy() {
		t.Error("Healthy() = true with 1 of 3 replicas ready")
	}
}

// A namespace that has never been deployed to is a state the grid must render,
// not an error that blanks the page.
func TestWorkloadMissingDeploymentIsNotAnError(t *testing.T) {
	c := NewWithInterface(fake.NewClientset())

	got, err := c.Workload(context.Background(), "jt-drupal-stage", "drupal")
	if err != nil {
		t.Fatalf("Workload returned an error for an absent deployment: %v", err)
	}
	if got.Found || got.Healthy() {
		t.Errorf("absent deployment reported as Found=%v Healthy=%v", got.Found, got.Healthy())
	}
}

func job(status batchv1.JobStatus) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "slipway-snapshot-prod-1-1", Namespace: prodNS},
		Status:     status,
	}
}

func TestObserveJob(t *testing.T) {
	deadline := batchv1.JobStatus{
		Failed: 1,
		Conditions: []batchv1.JobCondition{{
			Type:   batchv1.JobFailed,
			Status: corev1.ConditionTrue,
			Reason: reasonDeadlineExceeded,
		}},
	}

	tests := []struct {
		name string
		obj  *batchv1.Job
		want jobs.Observation
	}{
		{
			name: "absent",
			obj:  nil,
			want: jobs.Observation{Found: false},
		},
		{
			name: "running",
			obj:  job(batchv1.JobStatus{Active: 1}),
			want: jobs.Observation{Found: true, Active: 1},
		},
		{
			name: "succeeded",
			obj:  job(batchv1.JobStatus{Succeeded: 1}),
			want: jobs.Observation{Found: true, Succeeded: 1},
		},
		{
			name: "failed",
			obj:  job(batchv1.JobStatus{Failed: 3}),
			want: jobs.Observation{Found: true, Failed: 3},
		},
		{
			name: "deadline exceeded",
			obj:  job(deadline),
			want: jobs.Observation{Found: true, Failed: 1, DeadlineExceeded: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := fake.NewClientset()
			if tt.obj != nil {
				cs = fake.NewClientset(tt.obj)
			}
			c := NewWithInterface(cs)

			got, err := c.ObserveJob(context.Background(), prodNS, "slipway-snapshot-prod-1-1")
			if err != nil {
				t.Fatalf("ObserveJob: %v", err)
			}
			if got != tt.want {
				t.Errorf("ObserveJob = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestObservationDrivesTheEngine closes the loop: the cluster's view, flattened
// by ObserveJob, must produce the decision the state machine was designed for.
// The two halves are tested separately elsewhere; this asserts they meet.
func TestObservationDrivesTheEngine(t *testing.T) {
	tests := []struct {
		name   string
		obj    *batchv1.Job
		cur    jobs.State
		action jobs.Action
		next   jobs.State
	}{
		{"pending job never submitted", nil, jobs.Pending, jobs.Submit, jobs.Submitted},
		{"running job vanished", nil, jobs.Running, jobs.Finalize, jobs.Orphaned},
		{"submitted job started a pod", job(batchv1.JobStatus{Active: 1}), jobs.Submitted, jobs.Adopt, jobs.Running},
		{"running job finished", job(batchv1.JobStatus{Succeeded: 1}), jobs.Running, jobs.Finalize, jobs.Succeeded},
		{"running job exhausted retries", job(batchv1.JobStatus{Failed: 6}), jobs.Running, jobs.Finalize, jobs.Failed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := fake.NewClientset()
			if tt.obj != nil {
				cs = fake.NewClientset(tt.obj)
			}

			obs, err := NewWithInterface(cs).ObserveJob(context.Background(), prodNS, "slipway-snapshot-prod-1-1")
			if err != nil {
				t.Fatalf("ObserveJob: %v", err)
			}

			d := jobs.Decide(tt.cur, obs)
			if d.Action != tt.action || d.Next != tt.next {
				t.Errorf("from %s: got {%s -> %s}, want {%s -> %s} (%s)",
					tt.cur, d.Action, d.Next, tt.action, tt.next, d.Reason)
			}
		})
	}
}

func pod(image, imageID string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "drupal-abc123",
			Namespace: prodNS,
			Labels:    map[string]string{"app": "drupal"},
		},
		Status: corev1.PodStatus{
			Phase: phase,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "drupal", Image: image, ImageID: imageID},
			},
		},
	}
}

func selectedDeployment(image string) *appsv1.Deployment {
	d := deployment(image, 1, 1)
	d.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "drupal"}}
	return d
}

// A tag-pinned Deployment doesn't say what code is live — the tag can move
// without the pods restarting. The pod's imageID is the only honest answer.
func TestWorkloadResolvesTheDigestBehindATag(t *testing.T) {
	digest := "sha256:4d367e" + zeros(58)
	c := NewWithInterface(fake.NewClientset(
		selectedDeployment("ghcr.io/jtarleton/d11app:sha-7374d29"),
		pod("ghcr.io/jtarleton/d11app:sha-7374d29", "ghcr.io/jtarleton/d11app@"+digest, corev1.PodRunning),
	))

	got, err := c.Workload(context.Background(), prodNS, "drupal")
	if err != nil {
		t.Fatalf("Workload: %v", err)
	}
	if got.Digest != "" {
		t.Errorf("Digest = %q, want empty for a tag-pinned spec", got.Digest)
	}
	if got.RunningDigest != digest {
		t.Errorf("RunningDigest = %q, want %q", got.RunningDigest, digest)
	}
}

// A digest-pinned Deployment needs no pod lookup; the spec is already the truth.
func TestWorkloadSkipsPodLookupWhenPinned(t *testing.T) {
	digest := "sha256:9f3c1a" + zeros(58)
	c := NewWithInterface(fake.NewClientset(selectedDeployment("ghcr.io/jtarleton/d11app@" + digest)))

	got, err := c.Workload(context.Background(), prodNS, "drupal")
	if err != nil {
		t.Fatalf("Workload: %v", err)
	}
	if got.Digest != digest {
		t.Errorf("Digest = %q, want %q", got.Digest, digest)
	}
	if got.RunningDigest != "" {
		t.Errorf("RunningDigest = %q, want empty when the spec is already pinned", got.RunningDigest)
	}
}

// Pods that aren't running yet must not be mistaken for the live version.
func TestWorkloadIgnoresNonRunningPods(t *testing.T) {
	c := NewWithInterface(fake.NewClientset(
		selectedDeployment("ghcr.io/jtarleton/d11app:sha-7374d29"),
		pod("ghcr.io/jtarleton/d11app:sha-7374d29", "ghcr.io/jtarleton/d11app@sha256:dead"+zeros(60), corev1.PodPending),
	))

	got, err := c.Workload(context.Background(), prodNS, "drupal")
	if err != nil {
		t.Fatalf("Workload: %v", err)
	}
	if got.RunningDigest != "" {
		t.Errorf("RunningDigest = %q, want empty when no pod is running", got.RunningDigest)
	}
}

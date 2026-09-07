package k8s

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func minimalSpec() JobSpec {
	return JobSpec{Image: "mariadb:latest", Command: []string{"bash", "-c", "true"}}
}

func getJob(t *testing.T, cs *fake.Clientset, name string) *batchv1.Job {
	t.Helper()
	job, err := cs.BatchV1().Jobs(prodNS).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get job %s: %v", name, err)
	}
	return job
}

// The claim the whole crash-recovery design rests on: resubmitting a job whose
// name already exists is success, not an error, because the only thing that can
// own that name is Slipway's own earlier attempt.
func TestSubmitJobIsIdempotent(t *testing.T) {
	cs := fake.NewClientset()
	c := NewWithInterface(cs)
	name := "slipway-snapshot-prod-1-1"

	for i := 0; i < 3; i++ {
		if err := c.SubmitJob(context.Background(), prodNS, name, minimalSpec()); err != nil {
			t.Fatalf("submission %d: %v", i, err)
		}
	}

	list, err := cs.BatchV1().Jobs(prodNS).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("three submissions produced %d Jobs, want 1", len(list.Items))
	}
}

func TestSubmitJobRequiresImageAndCommand(t *testing.T) {
	c := NewWithInterface(fake.NewClientset())

	if err := c.SubmitJob(context.Background(), prodNS, "x", JobSpec{Command: []string{"true"}}); err == nil {
		t.Error("submitted a job with no image")
	}
	if err := c.SubmitJob(context.Background(), prodNS, "x", JobSpec{Image: "mariadb"}); err == nil {
		t.Error("submitted a job with no command")
	}
}

// Map iteration order is random in Go. An unsorted environment would make the
// Job spec differ between attempts, which is exactly what deterministic naming
// exists to avoid.
func TestSubmitJobSortsEnvironment(t *testing.T) {
	cs := fake.NewClientset()
	spec := minimalSpec()
	spec.Env = map[string]string{"ZULU": "3", "ALPHA": "1", "MIKE": "2"}

	if err := NewWithInterface(cs).SubmitJob(context.Background(), prodNS, "envjob", spec); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	env := getJob(t, cs, "envjob").Spec.Template.Spec.Containers[0].Env
	want := []string{"ALPHA", "MIKE", "ZULU"}
	if len(env) != len(want) {
		t.Fatalf("got %d env vars, want %d", len(env), len(want))
	}
	for i, name := range want {
		if env[i].Name != name {
			t.Errorf("env[%d] = %s, want %s", i, env[i].Name, name)
		}
	}
}

func TestSubmitJobAttachesMounts(t *testing.T) {
	cs := fake.NewClientset()
	spec := minimalSpec()
	spec.Mounts = []Mount{
		{Name: "app", PVC: "app", Path: "/data"},
		{Name: "creds", Secret: "aws-backup-credentials", Path: "/root/.aws", ReadOnly: true},
	}

	if err := NewWithInterface(cs).SubmitJob(context.Background(), prodNS, "mountjob", spec); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	pod := getJob(t, cs, "mountjob").Spec.Template.Spec
	if len(pod.Volumes) != 2 || len(pod.Containers[0].VolumeMounts) != 2 {
		t.Fatalf("got %d volumes and %d mounts, want 2 of each", len(pod.Volumes), len(pod.Containers[0].VolumeMounts))
	}
	if pod.Volumes[0].PersistentVolumeClaim == nil || pod.Volumes[0].PersistentVolumeClaim.ClaimName != "app" {
		t.Errorf("first volume is not the app PVC: %+v", pod.Volumes[0])
	}
	if pod.Volumes[1].Secret == nil || pod.Volumes[1].Secret.SecretName != "aws-backup-credentials" {
		t.Errorf("second volume is not the credentials secret: %+v", pod.Volumes[1])
	}
	if !pod.Containers[0].VolumeMounts[1].ReadOnly {
		t.Error("credentials are mounted writable")
	}
}

func TestSubmitJobWiresAnInitContainerThroughScratch(t *testing.T) {
	cs := fake.NewClientset()
	spec := minimalSpec()
	spec.Scratch = "/scratch"
	spec.Init = &InitContainer{
		Image:         "mariadb:latest",
		Command:       []string{"bash", "-c", "mariadb-dump ... > /scratch/dump.sql.gz"},
		EnvFromSecret: "db-credentials",
		Mounts:        []Mount{{Name: "creds", Secret: "aws-backup-credentials", Path: "/root/.aws", ReadOnly: true}},
	}
	spec.Mounts = []Mount{{Name: "creds", Secret: "aws-backup-credentials", Path: "/root/.aws", ReadOnly: true}}

	if err := NewWithInterface(cs).SubmitJob(context.Background(), prodNS, "snapjob", spec); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	pod := getJob(t, cs, "snapjob").Spec.Template.Spec

	if len(pod.InitContainers) != 1 {
		t.Fatalf("got %d init containers, want 1", len(pod.InitContainers))
	}
	init := pod.InitContainers[0]
	if !hasMount(init.VolumeMounts, "scratch", "/scratch") {
		t.Errorf("init container is not mounting scratch: %+v", init.VolumeMounts)
	}
	if !hasMount(pod.Containers[0].VolumeMounts, "scratch", "/scratch") {
		t.Errorf("main container is not mounting scratch: %+v", pod.Containers[0].VolumeMounts)
	}
	if init.EnvFrom == nil || init.EnvFrom[0].SecretRef.Name != "db-credentials" {
		t.Errorf("init container is missing its db secret: %+v", init.EnvFrom)
	}

	// The creds secret is requested by both containers; the pod must carry one
	// volume of that name, plus the scratch emptyDir.
	names := map[string]int{}
	for _, v := range pod.Volumes {
		names[v.Name]++
	}
	if names["creds"] != 1 || names["scratch"] != 1 {
		t.Errorf("volumes not deduped: %+v", names)
	}
	for _, v := range pod.Volumes {
		if v.Name == "scratch" && v.EmptyDir == nil {
			t.Error("scratch is not an emptyDir")
		}
	}
}

func TestSubmitJobRejectsAnInitContainerWithoutScratch(t *testing.T) {
	spec := minimalSpec()
	spec.Init = &InitContainer{Image: "x", Command: []string{"true"}}
	err := NewWithInterface(fake.NewClientset()).SubmitJob(context.Background(), prodNS, "j", spec)
	if err == nil {
		t.Fatal("SubmitJob accepted an init container with no scratch volume to hand off through")
	}
}

func hasMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, m := range mounts {
		if m.Name == name && m.MountPath == path {
			return true
		}
	}
	return false
}

func TestSubmitJobRejectsAmbiguousMounts(t *testing.T) {
	c := NewWithInterface(fake.NewClientset())

	spec := minimalSpec()
	spec.Mounts = []Mount{{Name: "both", PVC: "app", Secret: "creds", Path: "/x"}}
	if err := c.SubmitJob(context.Background(), prodNS, "j1", spec); err == nil {
		t.Error("accepted a mount that is both a PVC and a Secret")
	}

	spec.Mounts = []Mount{{Name: "neither", Path: "/x"}}
	if err := c.SubmitJob(context.Background(), prodNS, "j2", spec); err == nil {
		t.Error("accepted a mount backed by nothing")
	}
}

func TestSubmitJobCarriesLimitsAndDeadlines(t *testing.T) {
	cs := fake.NewClientset()
	spec := minimalSpec()
	spec.BackoffLimit = 0
	spec.ActiveDeadline = 30 * time.Minute
	spec.TTLAfterFinished = time.Hour
	spec.EnvFromSecret = "db-credentials"

	if err := NewWithInterface(cs).SubmitJob(context.Background(), prodNS, "limits", spec); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	job := getJob(t, cs, "limits")
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("BackoffLimit = %v, want an explicit 0", job.Spec.BackoffLimit)
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 1800 {
		t.Errorf("ActiveDeadlineSeconds = %v, want 1800", job.Spec.ActiveDeadlineSeconds)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 3600 {
		t.Errorf("TTLSecondsAfterFinished = %v, want 3600", job.Spec.TTLSecondsAfterFinished)
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("RestartPolicy = %s, want Never", job.Spec.Template.Spec.RestartPolicy)
	}
	envFrom := job.Spec.Template.Spec.Containers[0].EnvFrom
	if len(envFrom) != 1 || envFrom[0].SecretRef == nil || envFrom[0].SecretRef.Name != "db-credentials" {
		t.Errorf("EnvFrom = %+v, want the db-credentials secret", envFrom)
	}
}

func TestScaleSetsReplicas(t *testing.T) {
	cs := fake.NewClientset(deployment("drupal:11", 1, 1))
	c := NewWithInterface(cs)

	if err := c.Scale(context.Background(), prodNS, "drupal", 0); err != nil {
		t.Fatalf("Scale: %v", err)
	}
	dep, err := cs.AppsV1().Deployments(prodNS).Get(context.Background(), "drupal", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 0 {
		t.Errorf("replicas = %v, want 0", dep.Spec.Replicas)
	}
}

func TestPodsGone(t *testing.T) {
	dep := deployment("drupal:11", 1, 1)
	dep.Status.Replicas = 2
	c := NewWithInterface(fake.NewClientset(dep))

	gone, err := c.PodsGone(context.Background(), prodNS, "drupal")
	if err != nil {
		t.Fatalf("PodsGone: %v", err)
	}
	if gone {
		t.Error("PodsGone = true while 2 replicas remain; a copy would race kubelet recreating subPath directories")
	}
}

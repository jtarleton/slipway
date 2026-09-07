package k8s

import (
	"context"
	"fmt"
	"sort"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// JobSpec is everything Slipway needs to run one operation in a cluster. It is
// deliberately narrow: operations are containers running a command against an
// environment's secrets, and nothing here should grow to accommodate a case
// that isn't that.
type JobSpec struct {
	Image   string
	Command []string

	// Env carries non-secret parameters — which namespace to read from, which
	// object key to write. Secrets never travel this way.
	Env map[string]string

	// Mounts attach the volumes an operation needs: an environment's app PVC,
	// a credentials secret.
	Mounts []Mount

	// EnvFromSecret names a Secret whose keys become environment variables —
	// database credentials, object-store keys. Credentials are read at
	// submission time and never copied into the control-plane store.
	EnvFromSecret string

	ServiceAccount string

	// Init, if set, runs to completion before the main container starts,
	// sharing Scratch with it. This is the dump-then-upload shape: a mariadb
	// container writes a gzipped dump into an emptyDir, then the aws-cli main
	// container ships it to object storage — neither image carries the other's
	// tools, and the dump never touches a PersistentVolume.
	Init *InitContainer

	// Scratch, if set, mounts a job-local emptyDir at this path in both the
	// init and the main container — the handoff between a dump and its upload.
	Scratch string

	// BackoffLimit is the number of pod retries before the Job fails. Zero is a
	// meaningful value: a restore must not be retried automatically.
	BackoffLimit int32

	// ActiveDeadline bounds the whole Job. A snapshot that hangs on a lock
	// should fail visibly rather than occupy the grid forever.
	ActiveDeadline time.Duration

	// TTLAfterFinished lets Kubernetes garbage-collect the Job. Set it longer
	// than the reconcile interval — a Job swept before the engine observes it
	// is indistinguishable from one that vanished, and gets recorded as
	// orphaned.
	TTLAfterFinished time.Duration
}

// InitContainer runs before the main container of a Job. It carries what a
// prepare step needs — a dump into scratch, or a download out of object storage.
type InitContainer struct {
	Image         string
	Command       []string
	Env           map[string]string
	EnvFromSecret string
	Mounts        []Mount // the scratch mount is added automatically; this is for anything else, e.g. aws-creds
}

// Mount is one volume attached to an operation. Exactly one of PVC or Secret
// must be set.
type Mount struct {
	Name     string
	PVC      string
	Secret   string
	Path     string
	SubPath  string
	ReadOnly bool
}

func (m Mount) volume() (corev1.Volume, error) {
	switch {
	case m.PVC != "" && m.Secret != "":
		return corev1.Volume{}, fmt.Errorf("mount %s: set either PVC or Secret, not both", m.Name)
	case m.PVC != "":
		return corev1.Volume{
			Name: m.Name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: m.PVC},
			},
		}, nil
	case m.Secret != "":
		return corev1.Volume{
			Name:         m.Name,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: m.Secret}},
		}, nil
	default:
		return corev1.Volume{}, fmt.Errorf("mount %s: needs a PVC or a Secret", m.Name)
	}
}

// SubmitJob creates the Kubernetes Job for one operation.
//
// An AlreadyExists response is treated as success, and that is the point of
// deterministic naming: the only way to collide is with Slipway's own earlier
// attempt at the same job, so the work is already running and resubmitting
// after a crash is safe.
func (c *Client) SubmitJob(ctx context.Context, namespace, name string, spec JobSpec) error {
	if spec.Image == "" || len(spec.Command) == 0 {
		return fmt.Errorf("submit job %s/%s: spec needs an image and a command", namespace, name)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "slipway",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &spec.BackoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app.kubernetes.io/managed-by": "slipway"},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: spec.ServiceAccount,
					Containers: []corev1.Container{{
						Name:    "operation",
						Image:   spec.Image,
						Command: spec.Command,
					}},
				},
			},
		},
	}

	pod := &job.Spec.Template.Spec
	main := &pod.Containers[0]

	main.Env = sortedEnv(spec.Env)
	main.EnvFrom = envFrom(spec.EnvFromSecret)

	for _, m := range spec.Mounts {
		vol, err := m.volume()
		if err != nil {
			return fmt.Errorf("submit job %s/%s: %w", namespace, name, err)
		}
		pod.Volumes = append(pod.Volumes, vol)
		main.VolumeMounts = append(main.VolumeMounts,
			corev1.VolumeMount{Name: m.Name, MountPath: m.Path, SubPath: m.SubPath, ReadOnly: m.ReadOnly})
	}

	// A job-local emptyDir shared with the init container. Used for the
	// dump-then-upload shape; it never outlives the pod.
	if spec.Scratch != "" {
		pod.Volumes = append(pod.Volumes, corev1.Volume{
			Name:         "scratch",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
		main.VolumeMounts = append(main.VolumeMounts,
			corev1.VolumeMount{Name: "scratch", MountPath: spec.Scratch})
	}

	if spec.Init != nil {
		if spec.Init.Image == "" || len(spec.Init.Command) == 0 {
			return fmt.Errorf("submit job %s/%s: init container needs an image and a command", namespace, name)
		}
		if spec.Scratch == "" {
			return fmt.Errorf("submit job %s/%s: an init container needs Scratch to hand off through", namespace, name)
		}
		initc := corev1.Container{
			Name:         "prepare",
			Image:        spec.Init.Image,
			Command:      spec.Init.Command,
			Env:          sortedEnv(spec.Init.Env),
			EnvFrom:      envFrom(spec.Init.EnvFromSecret),
			VolumeMounts: []corev1.VolumeMount{{Name: "scratch", MountPath: spec.Scratch}},
		}
		for _, m := range spec.Init.Mounts {
			vol, err := m.volume()
			if err != nil {
				return fmt.Errorf("submit job %s/%s: init %w", namespace, name, err)
			}
			pod.Volumes = append(pod.Volumes, vol)
			initc.VolumeMounts = append(initc.VolumeMounts,
				corev1.VolumeMount{Name: m.Name, MountPath: m.Path, SubPath: m.SubPath, ReadOnly: m.ReadOnly})
		}
		pod.InitContainers = []corev1.Container{initc}
	}

	pod.Volumes = dedupeVolumes(pod.Volumes)

	if spec.ActiveDeadline > 0 {
		seconds := int64(spec.ActiveDeadline.Seconds())
		job.Spec.ActiveDeadlineSeconds = &seconds
	}
	if spec.TTLAfterFinished > 0 {
		seconds := int32(spec.TTLAfterFinished.Seconds())
		job.Spec.TTLSecondsAfterFinished = &seconds
	}

	_, err := c.cs.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("submit job %s/%s: %w", namespace, name, err)
	}
	return nil
}

// sortedEnv turns a map into EnvVars in name order. Map iteration is random and
// a Job spec that differs run to run defeats the idempotent resubmit.
func sortedEnv(env map[string]string) []corev1.EnvVar {
	if len(env) == 0 {
		return nil
	}
	out := make([]corev1.EnvVar, 0, len(env))
	for name, value := range env {
		out = append(out, corev1.EnvVar{Name: name, Value: value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// dedupeVolumes drops repeated volume names — the init and main container can
// legitimately ask for the same secret, and a pod spec with two volumes of one
// name is rejected.
func dedupeVolumes(vols []corev1.Volume) []corev1.Volume {
	seen := make(map[string]bool, len(vols))
	out := vols[:0]
	for _, v := range vols {
		if seen[v.Name] {
			continue
		}
		seen[v.Name] = true
		out = append(out, v)
	}
	return out
}

func envFrom(secret string) []corev1.EnvFromSource {
	if secret == "" {
		return nil
	}
	return []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: secret},
		},
	}}
}

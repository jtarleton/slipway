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

	for name, value := range spec.Env {
		job.Spec.Template.Spec.Containers[0].Env = append(
			job.Spec.Template.Spec.Containers[0].Env,
			corev1.EnvVar{Name: name, Value: value})
	}
	// Map iteration order is random and a Job spec that differs run to run
	// defeats the idempotent resubmit, so the environment is sorted.
	sort.Slice(job.Spec.Template.Spec.Containers[0].Env, func(i, j int) bool {
		return job.Spec.Template.Spec.Containers[0].Env[i].Name <
			job.Spec.Template.Spec.Containers[0].Env[j].Name
	})

	for _, m := range spec.Mounts {
		vol, err := m.volume()
		if err != nil {
			return fmt.Errorf("submit job %s/%s: %w", namespace, name, err)
		}
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, vol)
		job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			job.Spec.Template.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name: m.Name, MountPath: m.Path, SubPath: m.SubPath, ReadOnly: m.ReadOnly,
			})
	}

	if spec.EnvFromSecret != "" {
		job.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: spec.EnvFromSecret},
			},
		}}
	}
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

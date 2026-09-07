package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

// PatchImage points a Deployment's container at a new image.
//
// A strategic merge patch keyed on container name touches only the image, which
// matters because Slipway is not the author of these manifests — replicas,
// resources, probes and volumes stay exactly as whoever wrote them intended.
func (c *Client) PatchImage(ctx context.Context, namespace, deployment, container, image string) error {
	if image == "" {
		return fmt.Errorf("patch %s/%s: image must not be empty", namespace, deployment)
	}

	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []map[string]any{{"name": container, "image": image}},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("build patch: %w", err)
	}

	_, err = c.cs.AppsV1().Deployments(namespace).Patch(
		ctx, deployment, types.StrategicMergePatchType, patch, metav1.PatchOptions{
			FieldManager: "slipway",
		})
	if err != nil {
		return fmt.Errorf("patch %s/%s: %w", namespace, deployment, err)
	}
	return nil
}

// Scale sets a Deployment's replica count.
//
// Copying into an environment's volume requires its pods to be gone, not merely
// failing: kubelet recreates subPath directories whenever a pod starts, which
// would put back the very directory the copy just removed.
func (c *Client) Scale(ctx context.Context, namespace, deployment string, replicas int32) error {
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"replicas": replicas}})
	if err != nil {
		return fmt.Errorf("build scale patch: %w", err)
	}

	_, err = c.cs.AppsV1().Deployments(namespace).Patch(
		ctx, deployment, types.StrategicMergePatchType, patch, metav1.PatchOptions{
			FieldManager: "slipway",
		})
	if err != nil {
		return fmt.Errorf("scale %s/%s to %d: %w", namespace, deployment, replicas, err)
	}
	return nil
}

// PodsGone reports whether a Deployment has no pods left running.
func (c *Client) PodsGone(ctx context.Context, namespace, deployment string) (bool, error) {
	dep, err := c.cs.AppsV1().Deployments(namespace).Get(ctx, deployment, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("get deployment %s/%s: %w", namespace, deployment, err)
	}
	return dep.Status.Replicas == 0, nil
}

// RolloutStatus is a point-in-time reading of a Deployment's progress.
type RolloutStatus struct {
	Done bool

	// Stalled reports a ProgressDeadlineExceeded condition: Kubernetes has
	// given up. This is the signal to revert rather than keep waiting, and it
	// is why the wait below cannot simply poll until a timeout.
	Stalled bool

	Message string
}

// RolloutStatus reports whether a Deployment has finished rolling out. The
// checks mirror kubectl's, in the same order, so Slipway's idea of "done"
// matches what you would see at a terminal.
func (c *Client) RolloutStatus(ctx context.Context, namespace, deployment string) (RolloutStatus, error) {
	dep, err := c.cs.AppsV1().Deployments(namespace).Get(ctx, deployment, metav1.GetOptions{})
	if err != nil {
		return RolloutStatus{}, fmt.Errorf("get deployment %s/%s: %w", namespace, deployment, err)
	}
	return rolloutStatus(dep), nil
}

func rolloutStatus(dep *appsv1.Deployment) RolloutStatus {
	for _, cond := range dep.Status.Conditions {
		if cond.Type == appsv1.DeploymentProgressing &&
			cond.Status == corev1.ConditionFalse &&
			cond.Reason == "ProgressDeadlineExceeded" {
			return RolloutStatus{Stalled: true, Message: "deployment exceeded its progress deadline"}
		}
	}

	if dep.Generation > dep.Status.ObservedGeneration {
		return RolloutStatus{Message: "waiting for the controller to observe the update"}
	}

	var desired int32 = 1
	if dep.Spec.Replicas != nil {
		desired = *dep.Spec.Replicas
	}

	switch {
	case dep.Status.UpdatedReplicas < desired:
		return RolloutStatus{Message: fmt.Sprintf("%d of %d replicas updated",
			dep.Status.UpdatedReplicas, desired)}

	case dep.Status.Replicas > dep.Status.UpdatedReplicas:
		return RolloutStatus{Message: fmt.Sprintf("%d old replicas still terminating",
			dep.Status.Replicas-dep.Status.UpdatedReplicas)}

	case dep.Status.AvailableReplicas < dep.Status.UpdatedReplicas:
		return RolloutStatus{Message: fmt.Sprintf("%d of %d updated replicas available",
			dep.Status.AvailableReplicas, dep.Status.UpdatedReplicas)}
	}

	return RolloutStatus{Done: true, Message: fmt.Sprintf("%d replicas ready", desired)}
}

// WaitForRollout blocks until a Deployment finishes rolling out, stalls, or the
// context expires. Each observation is handed to onProgress, which is where the
// SSE log stream will attach.
func (c *Client) WaitForRollout(ctx context.Context, namespace, deployment string, interval time.Duration, onProgress func(RolloutStatus)) error {
	var last RolloutStatus

	err := wait.PollUntilContextCancel(ctx, interval, true, func(ctx context.Context) (bool, error) {
		status, err := c.RolloutStatus(ctx, namespace, deployment)
		if err != nil {
			return false, err
		}
		if onProgress != nil && status.Message != last.Message {
			onProgress(status)
		}
		last = status

		if status.Stalled {
			return false, fmt.Errorf("rollout of %s/%s stalled: %s", namespace, deployment, status.Message)
		}
		return status.Done, nil
	})
	if err != nil {
		return err
	}
	return nil
}

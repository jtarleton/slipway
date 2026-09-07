package k8s

import (
	"context"
	"fmt"

	"github.com/jtarleton/slipway/internal/jobs"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// reasonDeadlineExceeded is the reason Kubernetes attaches to a JobFailed
// condition when activeDeadlineSeconds elapses.
const reasonDeadlineExceeded = "DeadlineExceeded"

// ObserveJob reports the cluster's view of one Kubernetes Job, in the form the
// engine's reconciler consumes.
//
// This is the only bridge between the pure state machine and the cluster: every
// decision the engine makes is a function of what this returns, which is why it
// does no interpretation of its own beyond flattening the Job's status.
func (c *Client) ObserveJob(ctx context.Context, namespace, name string) (jobs.Observation, error) {
	job, err := c.cs.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if isNotFound(err) {
			// Not an error. A missing Job is exactly what the reconciler needs
			// to know to decide between submitting and declaring an orphan.
			return jobs.Observation{Found: false}, nil
		}
		return jobs.Observation{}, fmt.Errorf("get job %s/%s: %w", namespace, name, err)
	}

	obs := jobs.Observation{
		Found:     true,
		Active:    job.Status.Active,
		Succeeded: job.Status.Succeeded,
		Failed:    job.Status.Failed,
	}

	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed &&
			cond.Status == corev1.ConditionTrue &&
			cond.Reason == reasonDeadlineExceeded {
			obs.DeadlineExceeded = true
		}
	}
	return obs, nil
}

func isNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}

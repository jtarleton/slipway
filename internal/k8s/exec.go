package k8s

import (
	"context"
	"errors"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	uexec "k8s.io/client-go/util/exec"
)

// Exec runs command in a running pod of deployment, streaming stdout and stderr
// to out and errOut. It returns the command's exit code — a non-zero exit is a
// normal result, not a Go error, since the caller decides what a failing
// command means. A Go error means the exec itself could not be performed.
func (c *Client) Exec(ctx context.Context, namespace, deployment, container string, command []string, out, errOut io.Writer) (int, error) {
	if c.cfg == nil {
		return 0, fmt.Errorf("exec: this Client has no cluster config (built for tests)")
	}
	if len(command) == 0 {
		return 0, fmt.Errorf("exec: no command")
	}

	pod, err := c.runningPod(ctx, namespace, deployment)
	if err != nil {
		return 0, err
	}

	req := c.cs.CoreV1().RESTClient().Post().
		Resource("pods").Name(pod).Namespace(namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(c.cfg, "POST", req.URL())
	if err != nil {
		return 0, fmt.Errorf("build executor: %w", err)
	}

	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: out, Stderr: errOut})
	if err == nil {
		return 0, nil
	}
	var code uexec.CodeExitError
	if errors.As(err, &code) {
		return code.Code, nil
	}
	return 0, fmt.Errorf("exec in %s/%s: %w", namespace, pod, err)
}

// runningPod returns the name of a Running, not-terminating pod behind a
// Deployment — the one an interactive command should land in.
func (c *Client) runningPod(ctx context.Context, namespace, deployment string) (string, error) {
	dep, err := c.cs.AppsV1().Deployments(namespace).Get(ctx, deployment, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get deployment %s/%s: %w", namespace, deployment, err)
	}
	sel, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
	if err != nil || sel.Empty() {
		return "", fmt.Errorf("%s/%s has no usable pod selector", namespace, deployment)
	}
	pods, err := c.cs.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return "", fmt.Errorf("list pods for %s/%s: %w", namespace, deployment, err)
	}
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil {
			return p.Name, nil
		}
	}
	return "", fmt.Errorf("%s/%s has no running pod to exec into", namespace, deployment)
}

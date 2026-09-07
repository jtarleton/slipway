// Package k8s is Slipway's read and write path to the cluster.
//
// Everything here is scoped to what the control plane actually needs: the state
// of a Drupal Deployment, and the state of the Jobs the engine submits. It is
// deliberately not a general-purpose Kubernetes wrapper.
package k8s

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// Client is a namespaced view of one cluster.
type Client struct {
	cs kubernetes.Interface
}

// New builds a Client from a kubeconfig path. An empty path uses the standard
// loading rules, which covers both KUBECONFIG and in-cluster service accounts.
func New(kubeconfig string) (*Client, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build client: %w", err)
	}
	return &Client{cs: cs}, nil
}

// NewWithInterface wraps an existing clientset. Tests use this with the fake
// clientset; nothing in production should.
func NewWithInterface(cs kubernetes.Interface) *Client {
	return &Client{cs: cs}
}

// Workload is the observable state of one Deployment — a single cell in the
// code row of the grid.
type Workload struct {
	Found bool

	// Image is the reference as deployed; Digest is its content digest, empty
	// when the image is tag-pinned and therefore not traceable to a release.
	Image  string
	Digest string

	// RunningDigest is the digest the pods actually resolved the image to. For
	// a tag-pinned Deployment this is the only truthful answer to "what is
	// live", since the tag may have moved since the pods started.
	RunningDigest string

	Desired int32
	Ready   int32
}

// Healthy reports whether every desired replica is ready.
func (w Workload) Healthy() bool {
	return w.Found && w.Desired > 0 && w.Ready == w.Desired
}

// Workload reads one Deployment. A missing Deployment is not an error — an
// environment that has never been deployed to is a legitimate, displayable
// state, and the grid needs to render it rather than fail.
func (c *Client) Workload(ctx context.Context, namespace, name string) (Workload, error) {
	dep, err := c.cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if isNotFound(err) {
			return Workload{}, nil
		}
		return Workload{}, fmt.Errorf("get deployment %s/%s: %w", namespace, name, err)
	}

	w := Workload{Found: true, Ready: dep.Status.ReadyReplicas}
	if dep.Spec.Replicas != nil {
		w.Desired = *dep.Spec.Replicas
	}
	if containers := dep.Spec.Template.Spec.Containers; len(containers) > 0 {
		w.Image = containers[0].Image
		w.Digest = Digest(w.Image)
		if w.Digest == "" {
			w.RunningDigest = c.resolveRunningDigest(ctx, dep, containers[0].Name)
		}
	}
	return w, nil
}

// resolveRunningDigest finds the digest actually running behind a tag.
//
// A tag-pinned Deployment doesn't say what code is live — the tag can be
// repointed and the pods won't move until they restart. The pod's
// containerStatuses carry the resolved imageID, which is the only honest answer
// to "what is running right now". Best effort: a Deployment with no pods yet is
// a normal state, not an error.
func (c *Client) resolveRunningDigest(ctx context.Context, dep *appsv1.Deployment, container string) string {
	selector, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
	if err != nil || selector.Empty() {
		return ""
	}

	pods, err := c.cs.CoreV1().Pods(dep.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return ""
	}

	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != container {
				continue
			}
			// imageID is reported as repo@sha256:… once the image is pulled.
			if d := Digest(cs.ImageID); d != "" {
				return d
			}
		}
	}
	return ""
}

package k8s

import "strings"

// Digest extracts the content digest from a container image reference.
//
// Slipway pins deployments by digest, never by tag: a tag can be repointed,
// which makes "what is actually running in prod" unanswerable at exactly the
// moment you need to answer it. An image that arrives here still on a tag is a
// signal that something bypassed Slipway, so the empty return is meaningful
// rather than an error.
func Digest(image string) string {
	at := strings.LastIndex(image, "@")
	if at < 0 || at == len(image)-1 {
		return ""
	}
	digest := image[at+1:]
	if !strings.HasPrefix(digest, "sha256:") {
		return ""
	}
	return digest
}

// Repository returns the image reference with any digest or tag removed.
func Repository(image string) string {
	if at := strings.LastIndex(image, "@"); at >= 0 {
		image = image[:at]
	}
	// A colon only marks a tag if it comes after the last slash; otherwise it
	// is a registry port, as in registry.example.com:5000/drupal.
	if colon := strings.LastIndex(image, ":"); colon > strings.LastIndex(image, "/") {
		image = image[:colon]
	}
	return image
}

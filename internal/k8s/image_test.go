package k8s

import "testing"

func TestDigest(t *testing.T) {
	tests := []struct{ image, want string }{
		{"ghcr.io/jtarleton/d11app@sha256:9f3c1a" + zeros(58), "sha256:9f3c1a" + zeros(58)},
		{"registry.example.com:5000/drupal@sha256:41b7ee" + zeros(58), "sha256:41b7ee" + zeros(58)},

		// Tag-pinned images have no traceable digest. Empty is the answer, not
		// an error — it tells the grid that something bypassed Slipway.
		{"ghcr.io/jtarleton/d11app:v2.4.1", ""},
		{"drupal:11-apache", ""},
		{"drupal", ""},
		{"", ""},

		// Malformed references must not panic or half-parse.
		{"drupal@", ""},
		{"drupal@md5:abc", ""},
	}

	for _, tt := range tests {
		if got := Digest(tt.image); got != tt.want {
			t.Errorf("Digest(%q) = %q, want %q", tt.image, got, tt.want)
		}
	}
}

func TestRepository(t *testing.T) {
	tests := []struct{ image, want string }{
		{"ghcr.io/jtarleton/d11app@sha256:9f3c" + zeros(60), "ghcr.io/jtarleton/d11app"},
		{"ghcr.io/jtarleton/d11app:v2.4.1", "ghcr.io/jtarleton/d11app"},
		{"drupal:11-apache", "drupal"},
		{"drupal", "drupal"},

		// A colon before the last slash is a registry port, not a tag.
		{"registry.example.com:5000/drupal", "registry.example.com:5000/drupal"},
		{"registry.example.com:5000/drupal:v1", "registry.example.com:5000/drupal"},
	}

	for _, tt := range tests {
		if got := Repository(tt.image); got != tt.want {
			t.Errorf("Repository(%q) = %q, want %q", tt.image, got, tt.want)
		}
	}
}

func zeros(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '0'
	}
	return string(b)
}

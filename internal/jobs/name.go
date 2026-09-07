package jobs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Kind identifies what a job does. Slipway has a fixed, small set of operations
// by design. Ad-hoc commands (`slipway console`) exec into the running pod
// rather than running as a Job, so they need no Kind. Adding a Kind should be a
// deliberate act, not an extension point.
type Kind string

const (
	KindSnapshot  Kind = "snapshot"   // mysqldump to object storage
	KindPatch     Kind = "patch"      // patch a Deployment to an image digest
	KindUpdatedb  Kind = "updatedb"   // the post-deploy Drush sequence
	KindRestore   Kind = "restore"    // load a snapshot into an environment
	KindSanitize  Kind = "sanitize"   // drush sql:sanitize after a copy-down
	KindSyncFiles Kind = "sync-files" // rclone between object-store prefixes
)

// Kinds returns every operation the engine knows how to run.
func Kinds() []Kind {
	return []Kind{KindSnapshot, KindPatch, KindUpdatedb, KindRestore, KindSanitize, KindSyncFiles}
}

// Valid reports whether k is a Kind the engine recognizes.
func (k Kind) Valid() bool {
	for _, known := range Kinds() {
		if k == known {
			return true
		}
	}
	return false
}

// maxNameLen is the DNS-1123 label limit the Kubernetes API enforces on Job
// names. Exceeding it is rejected at submission time, which would strand a job
// in Pending forever, so names are truncated here instead.
const maxNameLen = 63

// hashLen is the width of the disambiguating suffix applied to truncated names.
const hashLen = 8

// Name builds the Kubernetes Job name for one step of one deployment.
//
// Determinism is the point. After a crash the reconciler resubmits, and the
// resubmission must collide with the previous attempt — an AlreadyExists error
// the caller treats as success — rather than starting a second dump against the
// same database. Every input that distinguishes two jobs must appear here.
//
// Names that would exceed the DNS-1123 limit are truncated and given a suffix
// derived from the full name, so two long environment names that share a prefix
// still produce distinct Jobs.
func Name(env string, deploymentID int64, seq int, kind Kind) string {
	full := fmt.Sprintf("slipway-%s-%s-%d-%d", sanitize(string(kind)), sanitize(env), deploymentID, seq)
	if len(full) <= maxNameLen {
		return full
	}

	sum := sha256.Sum256([]byte(full))
	suffix := "-" + hex.EncodeToString(sum[:])[:hashLen]
	return strings.TrimRight(full[:maxNameLen-len(suffix)], "-") + suffix
}

// sanitize coerces arbitrary text into the DNS-1123 label alphabet: lowercase
// alphanumerics and hyphens, no leading or trailing hyphen, no runs.
func sanitize(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	lastHyphen := true // suppresses a leading hyphen
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastHyphen = false
		default:
			if !lastHyphen {
				b.WriteByte('-')
				lastHyphen = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

// Package drupal turns Slipway's abstract job records into runnable containers.
//
// This is where every Drupal- and site-specific decision lives: which image
// runs a dump, where settings.php sits inside the app volume, what drush is
// called. The engine deliberately knows none of it — adding an operation should
// mean adding a case here, not changing how jobs are driven.
package drupal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jtarleton/slipway/internal/jobs"
	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/store"
)

// Layout describes where things live inside the shared app volume. These paths
// come from the running Deployment, not from a convention — Slipway did not
// author these manifests.
const (
	VolumeApp       = "app"
	PathSettings    = "docroot/sites/default/settings.php"
	PathFiles       = "docroot/sites/default/files"
	PathPrivate     = "private"
	SecretDB        = "db-credentials"
	SecretAWS       = "aws-backup-credentials"
	ServiceDatabase = "db"
)

// Images pins the containers operations run in.
type Images struct {
	// MariaDB supplies mysqldump and mysql. It must be at least the version of
	// the server being dumped, or mysqldump refuses newer server features.
	MariaDB string

	// AWSCLI supplies aws and tar for the files lane.
	AWSCLI string

	// Drupal is the site's own image, needed for anything that runs drush.
	// It is resolved per environment, because drush must match the code.
	Drupal string
}

// DefaultImages are sensible starting points; Drupal is filled in per operation.
func DefaultImages() Images {
	return Images{MariaDB: "mariadb:latest", AWSCLI: "amazon/aws-cli:latest"}
}

// Params are the per-job arguments carried in the job record's payload.
type Params struct {
	// SourceNamespace is the environment a copy reads from.
	SourceNamespace string `json:"source_namespace,omitempty"`

	// ObjectKey is the full s3:// destination for the files lane.
	ObjectKey string `json:"object_key,omitempty"`

	// Push sends the app volume to object storage; false pulls it back down.
	Push bool `json:"push,omitempty"`

	// Clean empties the target tree before a pull. Needed the first time an
	// environment is seeded, because a subPath mount leaves a directory where
	// its file should be; wasteful afterwards, since sync is incremental.
	Clean bool `json:"clean,omitempty"`

	// DrupalImage pins drush to the code it belongs with.
	DrupalImage string `json:"drupal_image,omitempty"`

	// SkipConfigImport omits config:import from the update sequence, for sites
	// that do not keep configuration in code.
	SkipConfigImport bool `json:"skip_config_import,omitempty"`
}

// Encode serializes params for storage on a job record.
func (p Params) Encode() ([]byte, error) { return json.Marshal(p) }

// Planner builds a runnable spec for each job.
type Planner struct {
	envs   map[int64]store.Environment
	images Images
}

// NewPlanner snapshots the environment table. Environments change rarely and a
// stale namespace is better caught at registration than mid-deploy.
func NewPlanner(envs []store.Environment, images Images) *Planner {
	byID := make(map[int64]store.Environment, len(envs))
	for _, e := range envs {
		byID[e.ID] = e
	}
	return &Planner{envs: byID, images: images}
}

// NamespaceFor resolves an environment to its namespace.
func (p *Planner) NamespaceFor(envID int64) (string, error) {
	env, ok := p.envs[envID]
	if !ok {
		return "", fmt.Errorf("no environment with id %d", envID)
	}
	return env.Namespace, nil
}

// SpecFor builds the container that performs one job.
func (p *Planner) SpecFor(job store.Job) (k8s.JobSpec, error) {
	env, ok := p.envs[job.EnvID]
	if !ok {
		return k8s.JobSpec{}, fmt.Errorf("no environment with id %d", job.EnvID)
	}

	var params Params
	if len(job.Payload) > 0 {
		if err := json.Unmarshal(job.Payload, &params); err != nil {
			return k8s.JobSpec{}, fmt.Errorf("decode payload for job %d: %w", job.ID, err)
		}
	}

	switch job.Kind {
	case jobs.KindUpdatedb:
		return p.update(env, params)
	case jobs.KindRestore:
		return p.copyDatabase(env, params)
	case jobs.KindSanitize:
		return p.sanitize(env, params)
	case jobs.KindSyncFiles:
		return p.syncFiles(env, params)
	default:
		return k8s.JobSpec{}, fmt.Errorf("no plan for job kind %q", job.Kind)
	}
}

// update runs the post-deploy sequence behind maintenance mode.
//
// The failure semantics are the whole point, and they come from `set -e` rather
// than from any handling here: if updatedb or config:import fails, the script
// exits before maintenance mode is turned back off. A half-updated database
// serving traffic is worse than a maintenance page, so the site stays down until
// somebody looks at it. Clearing maintenance mode is deliberately not a trap or
// a deferred cleanup — that would defeat it.
func (p *Planner) update(env store.Environment, params Params) (k8s.JobSpec, error) {
	image := params.DrupalImage
	if image == "" {
		image = p.images.Drupal
	}
	if image == "" {
		return k8s.JobSpec{}, fmt.Errorf("update %s: no drupal image to run drush from", env.Name)
	}

	script := `
drush() { /app/vendor/bin/drush "$@"; }

drush state:set system.maintenance_mode 1 --input-format=integer
drush cache:rebuild

drush updatedb -y
`
	if !params.SkipConfigImport {
		script += "drush config:import -y" + "\n"
	}
	script += `
drush cache:rebuild
drush state:set system.maintenance_mode 0 --input-format=integer
echo "update sequence complete"
`

	return k8s.JobSpec{
		Image:   image,
		Command: []string{"bash", "-eo", "pipefail", "-c", script},
		Mounts: []k8s.Mount{
			{Name: VolumeApp, PVC: VolumeApp, Path: "/app/" + PathSettings, SubPath: PathSettings},
			{Name: VolumeApp + "-files", PVC: VolumeApp, Path: "/app/" + PathFiles, SubPath: PathFiles},
			{Name: VolumeApp + "-private", PVC: VolumeApp, Path: "/app/" + PathPrivate, SubPath: PathPrivate},
		},
		// Update hooks are not idempotent. A retry that re-runs a hook which
		// half-applied is how you turn a failed deploy into a corrupted one.
		BackoffLimit:     0,
		ActiveDeadline:   30 * time.Minute,
		TTLAfterFinished: time.Hour,
	}, nil
}

// copyDatabase streams one environment's database into another.
//
// The dump is piped straight across rather than staged in object storage: both
// databases are in the same cluster, and an intermediate file would need a
// volume large enough to hold it. pipefail matters here — without it a failed
// mysqldump still exits zero through the pipe and the target is left with a
// half-loaded schema that looks like a success.
func (p *Planner) copyDatabase(env store.Environment, params Params) (k8s.JobSpec, error) {
	if params.SourceNamespace == "" {
		return k8s.JobSpec{}, fmt.Errorf("copy database into %s: no source namespace", env.Name)
	}
	if params.SourceNamespace == env.Namespace {
		return k8s.JobSpec{}, fmt.Errorf("copy database: source and target are both %s", env.Namespace)
	}

	// MariaDB 11 dropped the mysqldump/mysql compatibility symlinks, so the
	// binaries are resolved at run time rather than assumed. --no-tablespaces
	// is deliberately absent: it exists to avoid MySQL 8's PROCESS privilege
	// requirement and MariaDB's dumper rejects it as an unknown option.
	const script = `
DUMP="$(command -v mariadb-dump || command -v mysqldump)"
CLIENT="$(command -v mariadb || command -v mysql)"
[ -n "$DUMP" ] && [ -n "$CLIENT" ] || { echo "no mariadb client in this image" >&2; exit 1; }

"$DUMP" --single-transaction --routines --triggers --events \
  -h "$SRC_HOST" -u root -p"$MARIADB_ROOT_PASSWORD" "$MARIADB_DATABASE" \
| "$CLIENT" -h "$DST_HOST" -u root -p"$MARIADB_ROOT_PASSWORD" "$MARIADB_DATABASE"
echo "copied $SRC_HOST -> $DST_HOST"
`

	return k8s.JobSpec{
		Image:   p.images.MariaDB,
		Command: []string{"bash", "-eo", "pipefail", "-c", script},
		Env: map[string]string{
			"SRC_HOST": fmt.Sprintf("%s.%s.svc.cluster.local", ServiceDatabase, params.SourceNamespace),
			"DST_HOST": ServiceDatabase,
		},
		EnvFromSecret: SecretDB,
		// A restore is never retried automatically. Re-running one halfway
		// through leaves the target in a worse state than failing loudly.
		BackoffLimit:     0,
		ActiveDeadline:   30 * time.Minute,
		TTLAfterFinished: time.Hour,
	}, nil
}

// sanitize scrubs identifying data from a database that came down from
// production.
//
// drush sql:sanitize covers core's user and session tables and nothing else. A
// site with custom tables holding personal data needs a hook_drush_sql_sync
// implementation; this operation cannot know about them and does not pretend to.
func (p *Planner) sanitize(env store.Environment, params Params) (k8s.JobSpec, error) {
	if env.IsProduction {
		return k8s.JobSpec{}, fmt.Errorf("refusing to sanitize %s: it is production", env.Name)
	}
	image := params.DrupalImage
	if image == "" {
		image = p.images.Drupal
	}
	if image == "" {
		return k8s.JobSpec{}, fmt.Errorf("sanitize %s: no drupal image to run drush from", env.Name)
	}

	const script = `
/app/vendor/bin/drush sql:sanitize -y
/app/vendor/bin/drush cache:rebuild
echo "sanitized"
`

	return k8s.JobSpec{
		Image:   image,
		Command: []string{"bash", "-eo", "pipefail", "-c", script},
		Mounts: []k8s.Mount{
			{Name: VolumeApp, PVC: VolumeApp, Path: "/app/" + PathSettings, SubPath: PathSettings},
			{Name: VolumeApp + "-files", PVC: VolumeApp, Path: "/app/" + PathFiles, SubPath: PathFiles},
			{Name: VolumeApp + "-private", PVC: VolumeApp, Path: "/app/" + PathPrivate, SubPath: PathPrivate},
		},
		BackoffLimit:     2,
		ActiveDeadline:   15 * time.Minute,
		TTLAfterFinished: time.Hour,
	}, nil
}

// syncFiles moves the app volume through object storage.
//
// It has to go via S3 rather than a direct copy: PersistentVolumeClaims are
// namespaced, so no single pod can mount both environments' volumes however
// they are provisioned.
func (p *Planner) syncFiles(env store.Environment, params Params) (k8s.JobSpec, error) {
	if params.ObjectKey == "" {
		return k8s.JobSpec{}, fmt.Errorf("sync files for %s: no object key", env.Name)
	}

	// aws s3 sync rather than tar over a pipe: the aws-cli image carries no
	// tar, and a directory sync is incremental, which matters once the files
	// directory is measured in gigabytes.
	script := `
aws s3 sync "$OBJECT_KEY" /data --delete --only-show-errors
echo "pulled $OBJECT_KEY"
`
	if params.Push {
		script = `
aws s3 sync /data "$OBJECT_KEY" --delete --only-show-errors
echo "pushed $OBJECT_KEY"
`
	} else if params.Clean {
		// A subPath mount silently creates a *directory* where its file is
		// missing, so a pod that started against an empty volume leaves a
		// directory named settings.php that sync cannot replace with a file.
		// Only needed on first seed — sync is incremental afterwards.
		script = `
rm -rf /data/* /data/.[!.]* 2>/dev/null || true
aws s3 sync "$OBJECT_KEY" /data --delete --only-show-errors
echo "pulled $OBJECT_KEY (clean)"
`
	}

	return k8s.JobSpec{
		Image:   p.images.AWSCLI,
		Command: []string{"bash", "-eo", "pipefail", "-c", script},
		Env:     map[string]string{"OBJECT_KEY": params.ObjectKey, "AWS_DEFAULT_REGION": "us-east-1"},
		Mounts: []k8s.Mount{
			// The whole volume, not the subPaths: this lane moves settings.php,
			// files and private together.
			{Name: VolumeApp, PVC: VolumeApp, Path: "/data"},
			{Name: "aws-creds", Secret: SecretAWS, Path: "/root/.aws", ReadOnly: true},
		},
		BackoffLimit:     1,
		ActiveDeadline:   time.Hour,
		TTLAfterFinished: time.Hour,
	}, nil
}

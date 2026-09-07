# Running slipway in the cluster

The scheduler and the reconcile loop only run while `slipway serve` runs, so for
schedules to be reliable slipway needs to be a real workload, not something
started from a laptop. These manifests put it in its own `slipway` namespace
with a 1Gi PVC for the SQLite database and least-privilege RBAC into the three
Drupal namespaces.

## 1. Build and push the image

```
docker buildx build --platform linux/amd64 \
  -t ghcr.io/jtarleton/slipway:$(git rev-parse --short HEAD) \
  -t ghcr.io/jtarleton/slipway:latest \
  --push .
```

(`--platform` should match the k3s node's architecture.)

## 2. Namespace and RBAC

```
kubectl apply -f deploy/slipway/rbac.yaml
```

This creates the `slipway` namespace, its service account, and the Role /
RoleBinding pairs in `jt-drupal`, `jt-drupal-stage`, `jt-drupal-dev`.

## 3. Bootstrap secrets

```
# image pull secret — copy the one the Drupal namespaces use
kubectl -n jt-drupal get secret ghcr -o json \
  | jq '{apiVersion, kind, type, data, metadata: {name: .metadata.name}}' \
  | kubectl -n slipway create -f -

# the CI release token (optional — omit to leave POST /api/releases disabled)
kubectl -n slipway create secret generic slipway \
  --from-literal=release-token="$(openssl rand -hex 24)"
```

## 4. Deploy

```
kubectl apply -f deploy/slipway/deployment.yaml
```

Pin the image to a digest in `deployment.yaml` rather than leaving `:latest`.

## 5. Reach it

The Service is `ClusterIP`. For the web UI alone:

```
kubectl -n slipway port-forward svc/slipway 8080
```

To expose it permanently (and so CI can reach `POST /api/releases`), give the
Service `type: NodePort` and add it to whatever fronts the Drupal sites, the
same way `varnish` / `adminer` are exposed.

## What it can and cannot do

The RBAC covers `grid`, `pin`, `deploy`, `copy-down`, `snapshot`, `restore`,
`rollback`, `resume`, `console`, and the scheduler. It deliberately does **not**
grant the broad patch surface `slipway adopt` sweeps — that is a one-off
cleanup; run it from a machine with a full kubeconfig if a namespace ever needs
it again.

slipway never reads Secrets: the Jobs it creates reference `db-credentials` and
`aws-backup-credentials`, and the kubelet resolves those at run time.

## Updating slipway

Build a new image, then either edit the Deployment's image digest and
`kubectl apply`, or `kubectl -n slipway set image deployment/slipway
slipway=ghcr.io/jtarleton/slipway@sha256:…`. The PVC keeps the database across
restarts; the schema migrates itself on start.

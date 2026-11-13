# VPA Profile Operator

The VPA Profile Operator watches `apps/v1` Deployments and StatefulSets for the
`metadata.annotations["vpa-scale-profile"]` annotation. It reads named presets
from one central ConfigMap and reconciles one `VerticalPodAutoscaler` per
annotated workload. It does not modify workloads or pods, install a webhook, or
build any Cluster Autoscaler images. The VPA's configured updater may
subsequently evict pods according to the VPA policy.

## Requirements

- Kubernetes with the Vertical Pod Autoscaler CRD installed.
- A VPA build whose `autoscaling.k8s.io/v1` CRD includes
  `resourcePolicy.containerPolicies[].cpuHistogramDecayHalfLife` and
  `updatePolicy.podUpdateThreshold`.
- For the optional profile lifetime override, the VPA CRD and updater must
  also support `updatePolicy.inRecommendationBoundsEvictionLifetimeThreshold`.
- `PerVPAConfig` enabled on the VPA components that consume or validate these
  per-VPA fields (including recommender, updater, and admission controller when
  deployed). Add `--feature-gates=PerVPAConfig=true` to those component
  containers. The operator does not enable or change the VPA feature gate.

## Install

The `Publish profile operator image` GitHub Actions workflow can be run
manually from the Actions tab. It publishes
`ghcr.io/kevinkrp93/vpa-profile-operator:vpa-profile-poc` and a `sha-<commit>`
tag. Select the `main` branch when running it. Pushes do not trigger image builds.
The VPA fork's `Publish VPA profile images` workflow is also manual; select its
`vpa-profile-poc` branch when running it. Package visibility must be set to
public in GitHub Packages before unauthenticated pulls will work. The
deployment uses the `vpa-profile-poc` tag by default; change it to a specific
tag if desired.

Alternatively, build and publish your own operator image:

```sh
make test
make build
make image IMAGE=ghcr.io/OWNER/vpa-profile-operator:TAG
docker push ghcr.io/OWNER/vpa-profile-operator:TAG
```

For a custom build, set the published image in `deploy/operator.yaml`. Install
the operator and example profiles:

```sh
kubectl apply -f deploy/operator.yaml
kubectl apply -f examples/profile-configmap.yaml
```

The manifest creates the `vpa-system` namespace, service account, cluster-wide
read permissions for Deployments and StatefulSets, read access to ConfigMaps,
and permissions to manage VPAs. Configure the profile ConfigMap location with
the operator's `--profile-configmap-namespace`, `--profile-configmap-name`,
and `--profile-configmap-key` arguments.

## Profiles and opt-in

The ConfigMap's configured data key contains YAML in this format:

```yaml
profiles:
  conservative:
    cpuHistogramDecayHalfLife: 24h
    podUpdateThreshold: 0.2
    inRecommendationBoundsEvictionLifetimeThreshold: 0s
  aggressive:
    cpuHistogramDecayHalfLife: 4h
    podUpdateThreshold: 0.05
    inRecommendationBoundsEvictionLifetimeThreshold: 0s
```

Each profile must specify both values. The half-life must be a positive Go
duration. The threshold must be a non-negative number.
`inRecommendationBoundsEvictionLifetimeThreshold` is an optional non-negative
Go duration. When set, the operator writes it to the VPA update policy,
overriding the updater's global
`--in-recommendation-bounds-eviction-lifetime-threshold` for that VPA.
When omitted, the global flag still applies (its default remains 12 hours).
The example profiles explicitly use `0s` to remove only the minimum pod-age
wait for in-range updates; resource-difference, disruption and other safety
checks still apply. Changing or removing this value reconciles existing
managed VPAs. This override also requires `PerVPAConfig=true`.

Apply a profile on a
Deployment or StatefulSet's **object metadata** (not its pod template):

```yaml
metadata:
  annotations:
    vpa-scale-profile: conservative
```

`examples/workload.yaml` is a complete sample Deployment. The generated VPA
uses the `*` container policy with the selected CPU histogram half-life, an
`Recreate` update policy with the selected pod update threshold, and a target
reference to the workload. Co-named Deployments and StatefulSets use distinct
VPA names (`<workload>-deployment` and `<workload>-statefulset`); long names
are shortened deterministically.

Changes to the selected annotation or central ConfigMap are reconciled
eventually. Removing the annotation deletes that workload's VPA only when the
operator's manager label, workload kind/name identifier labels, and exact
workload UID owner reference all match. The full workload name is checked in
the owner reference; the label uses a bounded hash identifier. If the workload
is absent from the informer cache, the operator does not infer its UID or
delete a VPA; Kubernetes owner-reference garbage collection cleans up VPAs
when their workload is actually deleted. Deletes are guarded with the VPA's
UID and resource version so a VPA replaced after the ownership check is not
deleted accidentally. The operator also watches managed VPA objects and
reconciles deletion or spec drift. Existing unmanaged VPAs are never
overwritten or deleted. Ownership conflicts, invalid/missing profiles, and API
errors are logged and retried with rate limiting. Identical desired VPAs are
not updated.

Reconciliation reads workloads, profiles, and VPAs from local informer caches;
it does not issue API GETs or LISTs. One shared informer per resource maintains
the caches through watches, with no periodic resync. The ConfigMap informer
is restricted to the configured namespace and name. Status-only VPA and
workload updates do not enqueue reconciliation. Initial cache population and
watch recovery can still require API snapshots or relists.

Only necessary creates, updates, and deletes are sent during reconciliation.
Updates use the cached resource version and a deep copy of the object;
deletes use the cached UID and resource version as preconditions. Conflicts,
missing objects during updates, and create `AlreadyExists` responses are
retried through the rate-limited queue using the cache, with ownership checked
again on each attempt. A stale cache never causes an unconditional overwrite
or delete. The Kubernetes and dynamic clients share a token-bucket limiter
of 5 requests/second with a burst of 10 across API requests; watch events do
not consume individual request tokens. This limit is per operator process.

Updating a profile changes the VPA policy but does not reset or reweight CPU
histogram samples already accumulated by the recommender. Their previously
accumulated weights remain unchanged.

The corresponding VPA implementation is available at
[kevinkrp93/autoscaler, branch `vpa-profile-poc`](https://github.com/kevinkrp93/autoscaler/tree/vpa-profile-poc),
commit [`4e95bec`](https://github.com/kevinkrp93/autoscaler/commit/4e95bec).

## Development

```sh
gofmt -w api controller cmd
go test ./...
go test -race -count=1 ./...
go build ./cmd/operator
```

The unit tests cover both supported workload kinds through opt-in, profile
switching, spec-drift repair, VPA recreation, and annotation removal. They
also cover central-profile updates, namespace/kind isolation, invalid
configuration and recovery, ownership protection, stale-cache write retries,
cache-only reads, event filtering, ConfigMap watch scope, and manifest RBAC.

Tests use fake Kubernetes clients and local informer stores; no container or
cluster is required. They do not replace live-cluster validation of API-server
admission, authorization, garbage collection, or actual VPA recommendations.

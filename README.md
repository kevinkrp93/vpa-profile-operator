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
- `PerVPAConfig` enabled on the VPA components that consume or validate these
  per-VPA fields (including recommender, updater, and admission controller when
  deployed). Add `--feature-gates=PerVPAConfig=true` to those component
  containers. The operator does not enable or change the VPA feature gate.

## Install

Build and publish only this operator image:

```sh
make test
make build
make image IMAGE=ghcr.io/OWNER/vpa-profile-operator:TAG
docker push ghcr.io/OWNER/vpa-profile-operator:TAG
```

Set the published image in `deploy/operator.yaml`, then install the operator
and example profiles:

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
  aggressive:
    cpuHistogramDecayHalfLife: 4h
    podUpdateThreshold: 0.05
```

Each profile must specify both values. The half-life must be a positive Go
duration. The threshold must be a non-negative number. Apply a profile on a
Deployment or StatefulSet's **object metadata** (not its pod template):

```yaml
metadata:
  annotations:
    vpa-scale-profile: conservative
```

`examples/workload.yaml` is a complete sample Deployment. The generated VPA
uses the `*` container policy with the selected CPU histogram half-life, an
`Auto` update policy with the selected pod update threshold, and a target
reference to the workload. Co-named Deployments and StatefulSets use distinct
VPA names (`<workload>-deployment` and `<workload>-statefulset`); long names
are shortened deterministically.

Changes to the selected annotation or central ConfigMap are reconciled
eventually. Removing the annotation deletes that workload's VPA only when the
operator's manager label, workload kind/name labels, and workload UID owner
reference all match. Existing unmanaged VPAs are never overwritten or deleted.
Ownership conflicts, invalid/missing profiles, and API errors are logged and
retried with rate limiting. Identical desired VPAs are not updated.

## Development

```sh
gofmt -w api controller cmd
go test ./...
go build ./cmd/operator
```

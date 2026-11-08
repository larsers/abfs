# ABFS on GKE / Prerequisites

The cluster exists before the deployment. Config Connector runs inside it and manages Google Cloud resources only; it does not create the cluster. This page lists what must be in place before `kubectl apply -k` and `helm install`, with a gcloud command and a check for each item.

The examples use the project `example-project`, the region `europe-west4`, the cluster `abfs`, the network `example-network` and the subnet `example-subnet`. Replace them with yours.

## Tools

On the admin workstation: `gcloud`, `kubectl` (with kustomize), `helm` (the chart is checked with Helm 4), and `docker`, for the image build and for the backend check. The backend check runs the `abfs` binary of the `abfs-server` image with the admin's own credentials (`gcloud auth application-default login`); see the [runbook](runbook.md#conformance-check).

## Cluster

GKE Standard with a regional control plane. Autopilot does not allow the privileged CI pods.

| Item | Setting |
| --- | --- |
| GKE version | 1.36.4-gke.1082000 or later (the ComputeClass `maintenanceExclusion` field). HPA rightsizing, a Preview option, needs 1.36.3-gke.1630000 or later. |
| Gateway API | channel `standard` |
| Workload Identity | `workloadPool: <project>.svc.id.goog`, GKE metadata server on every node pool |
| Managed OpenTelemetry | scope `COLLECTION_AND_INSTRUMENTATION_COMPONENTS`, GKE 1.34.1-gke.2178000 or later. The chart sets the OTLP environment itself and uses no Instrumentation resource. |
| Managed Service for Prometheus | on |
| Vertical Pod Autoscaling | on (the chart creates VerticalPodAutoscaler objects) |
| Dataplane V2 | on (the chart's NetworkPolicy is inert without it) |
| Autoscaling profile | `optimize-utilization` |
| Add-ons | the defaults, HTTP load balancing and horizontal pod autoscaling, on. The Gateway class `gke-l7-rilb` needs the first, the server HPA the second. |
| Config Connector | manual operator install from the release bundle, pinned version 1.130.1 or later (the tree is checked against 1.160.0), namespaced mode. Not the GKE add-on. See [Config Connector](#config-connector). |
| Node pool auto-creation | through the ComputeClasses; no cluster-level node auto-provisioning is needed from 1.33.3-gke.1136000 |
| System pool | one small manual COS pool for Config Connector and the GKE system workloads. No chart pod runs there (see [Node service account](#node-service-account)). |

Create a cluster with these settings:

```sh
gcloud container clusters create abfs \
  --project example-project --region europe-west4 \
  --release-channel regular --cluster-version 1.36.4-gke.1082000 \
  --network example-network --subnetwork example-subnet --enable-ip-alias \
  --workload-pool example-project.svc.id.goog \
  --gateway-api standard \
  --enable-dataplane-v2 \
  --enable-managed-prometheus \
  --managed-otel-scope COLLECTION_AND_INSTRUMENTATION_COMPONENTS \
  --enable-vertical-pod-autoscaling \
  --autoscaling-profile optimize-utilization \
  --logging SYSTEM,WORKLOAD --monitoring SYSTEM \
  --machine-type e2-standard-4 --num-nodes 1 \
  --image-type COS_CONTAINERD --workload-metadata GKE_METADATA
```

`--num-nodes 1` in a regional cluster gives one system node per zone.

The command has no `--addons` flag on purpose. `--addons` enables the add-ons it names and disables all others, so `--addons ConfigConnector` alone turns off HTTP load balancing and horizontal pod autoscaling. The cluster then has no `gke-l7-*` Gateway class, the Gateway stays `Waiting for controller`, and nothing acts on the server HPA. If you need `--addons` for another add-on, list `HttpLoadBalancing,HorizontalPodAutoscaling` in it too.

For a cluster that exists, each setting except Dataplane V2 can be turned on with `gcloud container clusters update`. Dataplane V2 is set at creation only; without it, set `networkPolicy.enabled=false` in the chart and protect the server address with firewall rules.

```sh
C="abfs --project example-project --region europe-west4"
gcloud container clusters update $C --gateway-api standard
gcloud container clusters update $C --workload-pool example-project.svc.id.goog
gcloud container clusters update $C --enable-managed-prometheus
gcloud container clusters update $C --managed-otel-scope COLLECTION_AND_INSTRUMENTATION_COMPONENTS
gcloud container clusters update $C --enable-vertical-pod-autoscaling
gcloud container clusters update $C --autoscaling-profile optimize-utilization
gcloud container clusters update $C --update-addons HttpLoadBalancing=ENABLED,HorizontalPodAutoscaling=ENABLED
gcloud container node-pools update default-pool --cluster $C --workload-metadata GKE_METADATA
```

If the cluster runs the Config Connector add-on, move it to the manual install first ([Moving from the Config Connector add-on](#moving-from-the-config-connector-add-on)).

Check:

```sh
gcloud container clusters describe abfs --project example-project --region europe-west4 \
  --format 'yaml(currentMasterVersion, networkConfig.gatewayApiConfig, workloadIdentityConfig,
    networkConfig.datapathProvider, verticalPodAutoscaling, autoscaling.autoscalingProfile,
    addonsConfig.httpLoadBalancing, addonsConfig.horizontalPodAutoscaling,
    monitoringConfig.managedPrometheusConfig)'
kubectl get crd computeclasses.cloud.google.com gateways.gateway.networking.k8s.io \
  verticalpodautoscalers.autoscaling.k8s.io
end=$(( $(date +%s) + 600 ))
until kubectl get gatewayclass gke-l7-rilb; do
  [ "$(date +%s)" -ge "$end" ] && { echo "no gke-l7-rilb after 10 minutes" >&2; break; }
  sleep 15
done
```

`networkConfig.gatewayApiConfig` shows `channel: CHANNEL_STANDARD`. GKE prints nothing for `addonsConfig.httpLoadBalancing` and `addonsConfig.horizontalPodAutoscaling` when the add-ons are on; they must not show `disabled: true`.

The Gateway controller installs the `gke-l7-*` classes a few minutes after the control plane is up, so right after `clusters create` returns, `kubectl get gatewayclass gke-l7-rilb` prints `NotFound` with the add-ons on. A test cluster had the class about 4 minutes after the create started. The loop waits up to 10 minutes. Only a `NotFound` after that points at the HTTP load balancing add-on ([troubleshooting](troubleshooting.md#gateway-waits-for-its-controller)).

### Wait for cluster operations

A cluster operation that runs, such as a `clusters update`, blocks node pool creation. Node pool auto-creation for the ComputeClasses then fails with `Cluster is running incompatible operation`, and the pods stay Pending. Enabling Managed OpenTelemetry on an existing cluster takes about 10 minutes, and an add-on change about as long. Finish every `clusters update` before the KCC apply and the Helm install, and check that none runs:

```sh
gcloud container operations list --project example-project --location europe-west4 \
  --filter '(targetLink~/clusters/abfs$ OR targetLink~/clusters/abfs/) AND status!=DONE'
gcloud container operations wait <operation> --project example-project --location europe-west4
```

The list must be empty. The same holds for later updates: run them while no ABFS node needs to be created, for example outside CI hours.

A blocked creation also costs money afterwards. The autoscaler moves on to other shapes of the class after each failed attempt, and the classes set only a floor, or no size at all. When the operation ends, the shapes that have not failed yet are created, which can be a 64-vCPU highmem node for pods that need a fraction of it. A test cluster got an n4-highmem-64 node for two server pods sized for two n4-standard-8 nodes, at about five times the cost per hour. The ComputeClass has no upper size bound per family; only a `machineType` rule fixes the shape. If oversized nodes came up, cordon them and restart the workload, so the autoscaler creates right-sized nodes and removes the empty ones:

```sh
kubectl get nodes -l cloud.google.com/compute-class -L node.kubernetes.io/instance-type
kubectl cordon <node>
kubectl -n abfs rollout restart deployment/abfs-server
```

## Config Connector

Install Config Connector with its operator from the release bundle, at a pinned version, in namespaced mode. Do not use the GKE add-on (`--addons ConfigConnector`): its version follows the GKE version and lags the open-source releases by 12 months or more. The add-on of GKE 1.36.4-gke.2046003 runs 1.126.0, whose SpannerInstance has no `spec.labels`, so the `backend-new` apply fails with `strict decoding error: unknown field "spec.labels"`.

The tree needs Config Connector 1.130.1 or later; its kinds and fields are checked against 1.160.0. Use the newest published release. 1.160.0 is the newest at the time of writing; check `gs://configconnector-operator/` for newer version folders (a git tag of the project can exist before its bundle is published).

### Install the operator

Download the bundle and keep it: the uninstall needs the same files.

```sh
V=1.160.0
gcloud storage cp gs://configconnector-operator/$V/release-bundle.tar.gz release-bundle-$V.tar.gz
mkdir kcc-$V && tar -C kcc-$V -zxf release-bundle-$V.tar.gz
kubectl apply -f kcc-$V/operator-system/configconnector-operator.yaml
```

That file is for GKE Standard. Then set the operator to namespaced mode:

```sh
kubectl apply -f - <<EOF
apiVersion: core.cnrm.cloud.google.com/v1beta1
kind: ConfigConnector
metadata:
  name: configconnector.core.cnrm.cloud.google.com
spec:
  mode: namespaced
  stateIntoSpec: Absent
EOF
```

The operator then installs the resource CRDs and the Config Connector webhook. Wait for both before the first KCC apply, then check the version:

```sh
until kubectl get crd spannerinstances.spanner.cnrm.cloud.google.com >/dev/null 2>&1; do sleep 10; done
until kubectl -n cnrm-system get deployment cnrm-webhook-manager >/dev/null 2>&1; do sleep 10; done
kubectl -n cnrm-system wait --for=condition=Available deployment/cnrm-webhook-manager --timeout=10m
until kubectl get mutatingwebhookconfiguration mutating-webhook.cnrm.cloud.google.com >/dev/null 2>&1; do sleep 10; done
kubectl get ns cnrm-system -o jsonpath='{.metadata.annotations.cnrm\.cloud\.google\.com/version}{"\n"}'
```

The webhook sets the project of each object when it is created. An object created before the webhook exists never gets it, and a later update does not add it; see [troubleshooting](troubleshooting.md#kcc-objects-created-before-the-webhook).

### Service account of the namespace

The KCC tree applies its objects to the namespace `abfs`. Config Connector reconciles them as one Google service account, named in `settings.yaml` as `kccServiceAccount`. Create that account, give it the roles to create the deployment's resources and to write Config Connector's own metrics, and bind it to the namespace's controller:

```sh
P=example-project
gcloud iam service-accounts create kcc-abfs --project $P --display-name "Config Connector for abfs"
for r in roles/serviceusage.serviceUsageAdmin roles/iam.serviceAccountAdmin \
    roles/resourcemanager.projectIamAdmin roles/compute.networkAdmin \
    roles/spanner.admin roles/bigtable.admin roles/storage.admin \
    roles/monitoring.metricWriter; do
  gcloud projects add-iam-policy-binding $P \
    --member serviceAccount:kcc-abfs@$P.iam.gserviceaccount.com --role $r
done
gcloud iam service-accounts add-iam-policy-binding kcc-abfs@$P.iam.gserviceaccount.com \
  --project $P --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:$P.svc.id.goog[cnrm-system/cnrm-controller-manager-abfs]"
```

`roles/monitoring.metricWriter` on the cluster's project lets Config Connector publish its Prometheus metrics. With `backend-existing`, the account only grants roles on the backend; `roles/spanner.admin`, `roles/bigtable.admin` and `roles/storage.admin` can then be replaced by roles that allow `setIamPolicy` on the three resources. When the network is in another project, grant `roles/compute.networkAdmin` there (only needed for the proxy-subnet component).

The APIs `serviceusage.googleapis.com` and `cloudresourcemanager.googleapis.com` must be enabled by hand; the KCC base enables the others:

```sh
gcloud services enable serviceusage.googleapis.com cloudresourcemanager.googleapis.com --project $P
```

The `ConfigConnectorContext` of the namespace is part of the KCC base. After the first apply, check that the operator created the namespace's Kubernetes service account and that its controller runs:

```sh
kubectl get serviceaccount/cnrm-controller-manager-abfs -n cnrm-system
kubectl wait -n cnrm-system --for=condition=Ready pod --timeout=10m \
  -l cnrm.cloud.google.com/component=cnrm-controller-manager,cnrm.cloud.google.com/scoped-namespace=abfs
```

Upgrade and rollback are in the [runbook](runbook.md#config-connector-upgrade-and-rollback).

### Moving from the Config Connector add-on

On a cluster that runs the add-on, switch to the manual install without losing the KCC objects:

1. Disable the add-on, but do not delete the `ConfigConnector` or `ConfigConnectorContext` objects. The update fails with `CLUSTER_ALREADY_HAS_OPERATION` while another cluster or node pool operation runs; it took about 8 minutes on a regional cluster.

   ```sh
   kubectl get ns cnrm-system -o jsonpath='{.metadata.annotations.cnrm\.cloud\.google\.com/version}{"\n"}'
   gcloud container clusters update abfs --project example-project --region europe-west4 \
     --update-addons ConfigConnector=DISABLED
   ```

2. Install the operator as in [Install the operator](#install-the-operator), at the version the add-on ran or a later one. An older operator can fail CRD validation, because the add-on may have upgraded CRDs already. Apply the `ConfigConnector` object of that section too, as on a new install.

3. Check the controller as above, and that every object is still Ready:

   ```sh
   kubectl -n abfs wait --for=condition=Ready --all gcp --timeout=30m
   ```

Disabling the add-on deletes the `namespace-id` ConfigMap in `configconnector-operator-system`, so the operator recreates the `cnrm-controller-manager` StatefulSets. The `ConfigConnectorContext`, its Kubernetes service account and the Workload Identity binding stay. As a precaution, set `cnrm.cloud.google.com/deletion-policy: abandon` on KCC objects that lack it before the move, and remove it afterwards; the tree sets it on every data resource, the APIs and the Gateway address, not on the service accounts and IAM members:

```sh
kubectl -n abfs get gcp -o custom-columns='KIND:.kind,NAME:.metadata.name,POLICY:.metadata.annotations.cnrm\.cloud\.google\.com/deletion-policy'
kubectl -n abfs annotate <kind>/<name> cnrm.cloud.google.com/deletion-policy=abandon --overwrite
# after the move
kubectl -n abfs annotate <kind>/<name> cnrm.cloud.google.com/deletion-policy-
```

## Network

A plain VPC is enough; a Shared VPC is not required. The VPC needs a subnet for the cluster in the region and a proxy-only subnet for the internal Application Load Balancer of the Gateway.

### Proxy-only subnet

One per network and region, purpose `REGIONAL_MANAGED_PROXY`, role `ACTIVE`, a `/23` recommended. Check whether the network has one:

```sh
gcloud compute networks subnets list --project example-project --network example-network \
  --filter 'purpose=REGIONAL_MANAGED_PROXY AND region:europe-west4'
```

If it has none, add the component `network/proxy-subnet` to the overlay and set `proxySubnetRange` in `settings.yaml`. If it has one, leave the component out and use its range. Either way, set the chart value `networkPolicy.proxyOnlySubnetCidr` to that range.

### Firewall

In a plain VPC the Gateway controller creates its own firewall rule `gkegw1-<id>-l7-<network>-<region>` when the first Gateway is created. `<id>` is a short id of the cluster, which the controller also puts into the names of its backend services (`gkegw1-<id>-abfs-...`). The rule allows the health check ranges `35.191.0.0/16` and `130.211.0.0/22` and the proxy-only subnet, TCP 0-65535, to all instances. Check it after the install:

```sh
gcloud compute firewall-rules list --project example-project \
  --filter 'name~^gkegw1-.*-l7-example-network-europe-west4$'
```

In a Shared VPC the controller does not create that rule: the host-project owner must create the Gateway's health-check and proxy rule by hand, as Google's GKE firewall rules page describes.

### Egress for the CI nodes

The casfs loader installs the kernel headers for `uname -r`, `build-essential` and `gettext-base` from the node's apt sources on every new CI node. It runs in the node's network namespace, so it uses the node's DNS resolver and routes. The CI nodes need egress to the Ubuntu archive or to an internal mirror of it. A private cluster needs Cloud NAT for the region, or that mirror.

```sh
gcloud compute routers create abfs-nat-router --project example-project \
  --region europe-west4 --network example-network
gcloud compute routers nats create abfs-nat --project example-project \
  --region europe-west4 --router abfs-nat-router \
  --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges
```

## Node service account

The KCC base creates the account `abfs-nodes@<project>.iam.gserviceaccount.com` with `roles/container.defaultNodeServiceAccount` and `roles/artifactregistry.reader` on the project. The chart value `computeClasses.nodeServiceAccount` names it, and every ComputeClass uses it for the nodes it creates.

The system pool of the create command runs with the Compute Engine default service account, which often has no `roles/artifactregistry.reader`, so a pod there cannot pull the ABFS images (`403 Forbidden` on the registry token, then `ImagePullBackOff`). The chart therefore places every pod on a class node: the bootstrap Job and the server health test on `abfs-server` (`bootstrap.computeClass`, `tests.computeClass`), the CI pods on `abfs-ci`. If you set either value to empty, grant the system pool's node account the reader role on the repository, or create the system pool with a node account that has it.

When the Artifact Registry repository is in another project, grant the reader role there:

```sh
gcloud artifacts repositories add-iam-policy-binding abfs --project <registry project> \
  --location europe-west4 --role roles/artifactregistry.reader \
  --member serviceAccount:abfs-nodes@example-project.iam.gserviceaccount.com
```

Check after the KCC apply:

```sh
gcloud projects get-iam-policy example-project --flatten 'bindings[].members' \
  --filter 'bindings.members:abfs-nodes@' --format 'value(bindings.role)'
```

## Artifact Registry

A Docker repository for the four images, in the region of the cluster:

```sh
gcloud artifacts repositories create abfs --project example-project \
  --location europe-west4 --repository-format docker
```

## IOPS fallback node pool for CI

A ComputeClass has no field for boot disk IOPS. An auto-created CI node gets the Hyperdisk Balanced defaults for its size: at 1,000 GiB about 9,000 IOPS and 1,640 MiB/s. If builds are limited by boot disk speed, create CI node pools by hand with provisioned IOPS and list them first in the class.

GKE applies the class's `nodePoolConfig` only to the pools it creates, and a `nodepools` rule takes no other field, so the hand-made pool sets every class setting itself. Create it after the KCC apply, because it uses the node service account:

```sh
gcloud container node-pools create abfs-ci-iops \
  --project example-project --cluster abfs --region europe-west4 \
  --node-locations europe-west4-a,europe-west4-b,europe-west4-c \
  --machine-type n4-standard-32 \
  --image-type UBUNTU_CONTAINERD \
  --disk-type hyperdisk-balanced --disk-size 1000 \
  --boot-disk-provisioned-iops 40000 \
  --boot-disk-provisioned-throughput 1000 \
  --no-shielded-secure-boot \
  --service-account abfs-nodes@example-project.iam.gserviceaccount.com \
  --workload-metadata GKE_METADATA \
  --add-maintenance-exclusion-until-end-of-support \
  --enable-autoscaling --min-nodes 0 --max-nodes 10 \
  --node-labels cloud.google.com/compute-class=abfs-ci \
  --node-taints cloud.google.com/compute-class=abfs-ci:NoSchedule
```

`--min-nodes` and `--max-nodes` count per zone. The provisioned IOPS and throughput are examples; the machine type's Hyperdisk limits cap them. Then list the pool in the chart values, and the class tries it before the auto-created shapes:

```yaml
computeClasses:
  ci:
    nodepools: [abfs-ci-iops]
```

Check a node of the pool:

```sh
kubectl get nodes -l cloud.google.com/gke-nodepool=abfs-ci-iops \
  -o custom-columns=NAME:.metadata.name,KERNEL:.status.nodeInfo.kernelVersion,VERSION:.status.nodeInfo.kubeletVersion
gcloud compute disks describe <node name> --project example-project --zone <zone> \
  --format 'value(provisionedIops,provisionedThroughput)'
```

## Before the Helm install

These are part of the [runbook](runbook.md#install) and listed here so nothing is missed:

- no namespace `abfs-ci` from elsewhere: the chart creates it with Pod Security level `privileged` (the KCC base creates only `abfs`), unless `namespaces.createCi` is false;
- the Secret `gerrit-credentials` in `abfs` with the key `.netrc` (and `.gitcookies` when `pusher.gerrit.cookies` is true);
- the images pushed, with their digests from `images.txt`;
- the IP of the Gateway address for `gateway.vip`;
- no cluster operation running ([Wait for cluster operations](#wait-for-cluster-operations)).

# ABFS on GKE / Prerequisites

The cluster exists before the deployment. Config Connector runs inside it and manages Google Cloud resources only; it does not create the cluster. This page lists what must be in place before `kubectl apply -k` and `helm install`. The commands and checks, in order, are in [install](install.md).

The examples use the project `example-project`, the region `europe-west4`, the cluster `abfs`, the network `example-network` and the subnet `example-subnet`. Replace them with yours.

## Tools

On the admin workstation: `gcloud`, `kubectl` (with kustomize), `helm` (checked with Helm 4), `skopeo`, `jq`, `make`, and `docker` for image builds and the backend check. The backend check runs the `abfs` binary of the `abfs-server` image with the admin's own credentials (`gcloud auth application-default login`); see the [runbook](runbook.md#conformance-check). [Install step 1](install.md#1-tools-on-the-admin-workstation) has the checks.

## Cluster

GKE Standard with a regional control plane. Autopilot does not allow the privileged CI pods. For node counts, address ranges, quotas and the backend size of a large farm, see [sizing](sizing.md).

| Item | Setting |
| --- | --- |
| GKE version | a version whose COS image ships casfs 0.1.24 or later: COS 133 from build 44-193, first in 1.37.2-gke.1325000 on the Rapid channel ([GKE version for the CI nodes](#gke-version-for-the-ci-nodes)). With `ci.casfs.provider: daemonset`, 1.36.4-gke.1082000 or later (the ComputeClass `maintenanceExclusion` field), and no enforced `constraints/compute.requireShieldedVm` ([Secure boot on the CI nodes](#secure-boot-on-the-ci-nodes)). HPA rightsizing (Preview) needs 1.36.3-gke.1630000 or later. |
| Gateway API | channel `standard` |
| Workload Identity | `workloadPool: <project>.svc.id.goog`, GKE metadata server on every node pool |
| Managed OpenTelemetry | scope `COLLECTION_AND_INSTRUMENTATION_COMPONENTS`, GKE 1.34.1-gke.2178000 or later. The chart sets the OTLP environment itself and uses no Instrumentation resource. |
| Managed Service for Prometheus | on |
| Vertical Pod Autoscaling | on (the chart creates VerticalPodAutoscaler objects) |
| Image streaming | on, set at creation (`--enable-image-streaming`) ([Image streaming](#image-streaming)) |
| Dataplane V2 | on (the chart's NetworkPolicy is inert without it). Clusters of 1,000 to 5,000 nodes must be regional Standard with Dataplane V2 and Private Service Connect, which this install has; all three are set at creation. |
| Pod addresses | `--default-max-pods-per-node 32` (a /26 per node) and an explicit Pod range ([Pod and node ranges](#pod-and-node-ranges)); both fixed at creation |
| Autoscaling profile | `optimize-utilization` |
| Add-ons | the defaults on: HTTP load balancing (for the Gateway class `gke-l7-rilb`), horizontal pod autoscaling (the server HPA), the Compute Engine Persistent Disk CSI driver (the pusher data disks). NodeLocal DNSCache is on by default for new clusters from 1.34.1-gke.3720000; keep it ([DNS at scale](#dns-at-scale)). |
| Config Connector | manual operator install, pinned version 1.130.1 or later, namespaced mode, not the GKE add-on ([Config Connector](#config-connector)) |
| Node pool auto-creation | through the ComputeClasses, from 1.33.3-gke.1136000. Leave cluster-level node auto-provisioning off ([Cluster-level node auto-provisioning](#cluster-level-node-auto-provisioning)). |
| System pool | one small COS pool (`n4-standard-4`, Hyperdisk Balanced) for Config Connector and the GKE system workloads, with its own node service account ([Node service account](#node-service-account)) and autoscaling ([DNS at scale](#dns-at-scale)). No chart pod runs there. |
| Private nodes | on (`--enable-private-nodes`) with `--disable-default-snat`, the only supported setup ([Private nodes and egress](#private-nodes-and-egress)) |
| Control plane access | DNS-based endpoint only (`--enable-dns-access`, `--no-enable-ip-access`) ([Control plane access](#control-plane-access)) |
| Control plane to nodes | Private Service Connect ([Control plane to node traffic](#control-plane-to-node-traffic)) |
| Maintenance window | a recurring window in the quiet hours of your CI ([Node upgrades](#node-upgrades)) |
| Cloud Monitoring | `--monitoring SYSTEM` ([Monitoring packages](#monitoring-packages)) |

Before the cluster is created: the Container File System API ([install step 2](install.md#2-access-and-project-apis)), the system pool's node service account ([install step 5](install.md#5-system-node-service-account)), and the network of [Private nodes and egress](#private-nodes-and-egress). [Install step 6](install.md#6-create-the-cluster) creates the cluster and checks it.

`--num-nodes 1` in a regional cluster gives one system node per zone. N4 machines take Hyperdisk boot disks only, hence `--disk-type hyperdisk-balanced`.

The days of the maintenance window are UTC days. GKE needs at least 48 hours of maintenance availability in 92 days, in windows of 4 hours or more.

Do not pass `--addons`: it turns off every add-on it does not name. `--addons ConfigConnector` alone leaves no `gke-l7-*` Gateway class (the Gateway stays `Waiting for controller`), no server HPA and no disks for the pusher PVCs. If you need `--addons`, list `HttpLoadBalancing,HorizontalPodAutoscaling,GcePersistentDiskCsiDriver` in it too, and `NodeLocalDNS` to keep NodeLocal DNSCache.

Right after `clusters create` returns, `kubectl get gatewayclass gke-l7-rilb` prints `NotFound`. The class appears about 4 minutes after the create starts. The check of install step 6 waits up to 10 minutes. Only a `NotFound` after that points at the HTTP load balancing add-on ([troubleshooting](troubleshooting.md#gateway-waits-for-its-controller)).

For an existing cluster, each setting except Dataplane V2 can be turned on with `gcloud container clusters update`. Dataplane V2 is set at creation only; without it, set `networkPolicy.enabled=false` in the chart and protect the server address with firewall rules. Turning on image streaming later recreates nodes ([Image streaming](#image-streaming)).

```sh
C="abfs --project example-project --region europe-west4"
gcloud container clusters update $C --gateway-api standard
gcloud container clusters update $C --workload-pool example-project.svc.id.goog
gcloud container clusters update $C --enable-managed-prometheus
gcloud container clusters update $C --managed-otel-scope COLLECTION_AND_INSTRUMENTATION_COMPONENTS
gcloud container clusters update $C --enable-vertical-pod-autoscaling
gcloud container clusters update $C --enable-image-streaming
gcloud container clusters update $C --autoscaling-profile optimize-utilization
gcloud container clusters update $C \
  --update-addons HttpLoadBalancing=ENABLED,HorizontalPodAutoscaling=ENABLED,GcePersistentDiskCsiDriver=ENABLED
gcloud container node-pools update default-pool --cluster $C --workload-metadata GKE_METADATA
gcloud container clusters update $C --maintenance-window-start 2026-01-03T00:00:00Z \
  --maintenance-window-duration 8H --maintenance-window-recurrence 'FREQ=WEEKLY;BYDAY=SA,SU'
```

To move to the DNS-based endpoint, turn it on, switch `kubectl` to it, and only then turn the IP-based endpoints off. The old kubeconfig entry stops working:

```sh
gcloud container clusters update $C --enable-dns-access
gcloud container clusters get-credentials $C --dns-endpoint
gcloud container clusters update $C --no-enable-ip-access
```

A cluster created with public nodes is not supported: create a new cluster. `--disable-default-snat` can be set by `clusters update` only on a cluster created with private nodes. To move the system pool to the N4 machine type, its own node account or secure boot, create a new pool (`gcloud container node-pools create` with `--machine-type`, `--disk-type`, `--service-account`, `--workload-metadata` and `--shielded-secure-boot --shielded-integrity-monitoring`), then delete the old one. `node-pools update` cannot change secure boot. If the cluster runs the Config Connector add-on, move to the manual install first ([Moving from the Config Connector add-on](#moving-from-the-config-connector-add-on)).

### Pod and node ranges

- **Max pods per node**: 32 gives each node a /26. A CI node runs the GKE system DaemonSets, cacheman, the casfs loader and one or two builds.
- **Pod range** (`--cluster-ipv4-cidr`): a range of prefix p holds 2^(26 - p) nodes, so a /14 holds 4,096 and a /16 1,024. Pick it yourself, outside the node subnet, the proxy-only subnet, `PSC_IP` and every range of peered networks. In a Shared VPC the Pod secondary range plays this role.
- **Node subnet**: its primary range holds every node, with four addresses reserved: a /20 holds 4,092, a /22 1,020.
- **Services**: the GKE-managed default range holds 4,096 Services; keep it.

A cluster created with a smaller range or more pods per node keeps that node limit; it needs a new cluster, or an additional Pod range.

### DNS at scale

NodeLocal DNSCache answers DNS on each node; keep it on. kube-dns runs on the system pool (the class nodes are tainted), with `replicas = max(ceil(cores / 256), ceil(nodes / 16))` from its autoscaler. At 2,000 CI nodes of 32 vCPU that is 250 replicas. Set `max` in the ConfigMap `kube-system/kube-dns-autoscaler`: GKE advises at most 500 above 500 nodes, and in most cases much lower. Plan 64 for up to 2,000 CI nodes; the system pool then needs `$SYSTEM_MAX` 6 ([install step 6](install.md#6-create-the-cluster), basis in [System pool and kube-dns](sizing.md#system-pool-and-kube-dns)):

```sh
kubectl -n kube-system get configmap kube-dns-autoscaler -o jsonpath='{.data.linear}{"\n"}'
kubectl -n kube-system patch configmap kube-dns-autoscaler --type merge \
  -p '{"data":{"linear":"{\"coresPerReplica\":256,\"nodesPerReplica\":16,\"preventSinglePointFailure\":true,\"max\":<max>}"}}'
```

### Image streaming

The chart turns image streaming on for the four ComputeClasses (`computeClasses.imageStreaming`; the class field `nodePoolConfig.imageStreaming` needs 1.34.1-gke.1279000). The cluster flag covers the system pool and hand-made pools. It needs:

- the Container File System API (`containerfilesystem.googleapis.com`), enabled in install step 2 and by the KCC base;
- images in Artifact Registry;
- `roles/serviceusage.serviceUsageConsumer` for every node service account, in the project that holds the images ([Node service account](#node-service-account));
- Private Google Access on the node subnet.

Set it when the cluster is created. Turning it on or off later recreates the nodes of every pool that follows the cluster setting, and a recreated CI node loses its node cache. A change of `computeClasses.imageStreaming` applies to new node pools only. Check: a node carries the label `cloud.google.com/gke-image-streaming: "true"`, and a streamed container gets the event `ImageStreaming` with `is backed by image streaming`.

### GKE version for the CI nodes

With the default `ci.casfs.provider: image`, the CI nodes run COS and load the casfs module of the node image. The client tools need casfs 0.1.24 or later (interface 1.5.0). The loader refuses older modules, and build pods on such a node never start.

Each GKE version uses one COS image; the GKE release notes name it, for example `cos-133-19999-44-193` for 1.37.2-gke.1325000. [Install step 3](install.md#3-gke-version-for-the-ci-nodes) reads its casfs version (`ebuild_version`), which must be 0.1.24 or later. If your channel has no such version, use the Rapid channel (no GKE SLA), or `ci.casfs.provider: daemonset` with Ubuntu CI nodes, which needs [egress](#egress-for-the-ci-nodes).

### Secure boot on the CI nodes

Every class turns secure boot and integrity monitoring on; COS loads the casfs module of its own image with secure boot on. With `ci.casfs.provider: daemonset` the CI class turns secure boot off, because secure boot refuses the self-built module. The organization policy `constraints/compute.requireShieldedVm` requires secure boot, integrity monitoring and vTPM (Shielded GKE Nodes, on by default) on every new VM, so `daemonset` cannot run where it is enforced. The class field `nodePoolAutoCreation.shieldedInstanceConfig` needs GKE 1.36.3-gke.1244000 or later. A change applies only to pools GKE creates afterwards. A hand-made pool sets it at creation ([IOPS fallback node pool for CI](#iops-fallback-node-pool-for-ci)). Check a node with `gcloud compute instances describe <node> --zone <zone> --format 'value(shieldedInstanceConfig.enableSecureBoot)'`.

### Move to the Regular channel

Move from Rapid to Regular once Regular offers the cluster's minor version at a version whose COS image ships casfs 0.1.24 or later. GKE refuses the change otherwise. List the versions and check the COS image of each as above:

```sh
gcloud container get-server-config --location europe-west4 --project example-project \
  --flatten channels --filter 'channels.channel=REGULAR' \
  --format 'yaml(channels.channel, channels.validVersions)'
gcloud container clusters update abfs --project example-project --region europe-west4 \
  --release-channel regular
```

Keep the `no_minor_upgrades` exclusion. Run the [canary](runbook.md#canary-after-every-control-plane-upgrade) after the next control plane upgrade.

### Hold minor upgrades

After the create, add the maintenance exclusion `abfs-no-minor` of scope `no_minor_upgrades` until the end of support ([install step 7](install.md#7-hold-minor-upgrades)). Patch upgrades still run. Remove it (`--remove-maintenance-exclusion abfs-no-minor`) only when the COS image of the next minor version ships casfs 0.1.24 or later.

### Wait for cluster operations

A running cluster operation, such as a `clusters update`, blocks node pool creation: auto-creation fails with `Cluster is running incompatible operation`, and pods stay Pending. Enabling Managed OpenTelemetry or changing an add-on takes about 10 minutes. Finish every `clusters update` before the KCC apply and the Helm install, and check that none runs ([install step 8](install.md#8-wait-for-cluster-operations)). Run later updates while no ABFS node needs to be created, for example outside CI hours.

After a blocked creation, the autoscaler can create much larger nodes than the pods need. Cordon them and restart the workload:

```sh
kubectl get nodes -l cloud.google.com/compute-class -L node.kubernetes.io/instance-type
kubectl cordon <node>
kubectl -n abfs rollout restart deployment/abfs-server
```

### Cluster-level node auto-provisioning

ABFS does not need it. Leave it off; the console offers it at cluster creation under the advanced settings. If it is on, its CPU and memory limits count the ABFS nodes too, and pods stay Pending at the limit. Its defaults also apply to the auto-created pools wherever a ComputeClass sets no value. Raise the maximums above the peak of all nodes together: the system pool, `pusher.count` seed nodes of at least 32 vCPU and 240 GB, a node per server replica up to `server.replicas.max`, and the CI nodes of your largest build load.

The cluster check of [install step 6](install.md#6-create-the-cluster) prints `autoscaling`. `enableNodeAutoprovisioning` missing or `false` means off. `resourceLimits` lists the `minimum` and `maximum` of `cpu` (vCPUs) and `memory` (GB). Turn it off, or raise the maximums:

```sh
gcloud container clusters update abfs --project example-project --region europe-west4 \
  --no-enable-autoprovisioning
gcloud container clusters update abfs --project example-project --region europe-west4 \
  --set-nap-resource-limits resourceType=cpu,maximum=<vCPUs> \
  --set-nap-resource-limits resourceType=memory,maximum=<GB>
```

### Node upgrades

CI nodes hold version upgrades through the class maintenance exclusion (`maintenanceExclusion: UNTIL_END_OF_SUPPORT` on `abfs-ci`). Node auto-repair and some node changes still replace CI nodes in the middle of a build. The CI system must retry a build whose pod was evicted or whose node went away.

### Control plane access

The cluster has the DNS-based endpoint only. IAM decides who connects: the caller needs `container.clusters.connect`. `roles/container.clusterViewer` includes it and grants nothing inside the cluster; `roles/container.viewer`, `roles/container.developer` and `roles/container.admin` include it with rights on Kubernetes objects. GKE allows a request that either IAM or Kubernetes RBAC allows. The install creates cluster-scoped objects, so the admin needs `roles/container.admin` or an equivalent RBAC binding; `roles/container.developer` does not create cluster roles.

`get-credentials` uses the DNS-based endpoint when the IP-based endpoints are off; `--dns-endpoint` makes it explicit. Check: in `controlPlaneEndpointsConfig`, `dnsEndpointConfig` shows `allowExternalTraffic: true` and an `endpoint` under `gke.goog`, and `ipEndpointsConfig` shows `enabled: false`.

An admin host inside the VPC without an external address reaches the endpoint through Private Google Access on its subnet (the node subnet has it), Cloud NAT, or the Private Service Connect endpoint with a `gke.goog` zone. The nodes do not use this endpoint.

If a tool needs the IP-based endpoints, leave out `--no-enable-ip-access` and restrict them with `--enable-master-authorized-networks --master-authorized-networks <CIDR>,...`, and `--enable-private-endpoint` to turn the external endpoint off. `get-credentials` then picks the IP-based endpoint, and `--internal-ip` its internal address.

### CI access to the control plane

A CI system outside the cluster connects to the DNS-based endpoint with the Google credentials of a service account. Kubernetes ServiceAccount tokens and client certificates do not work. Give the account `roles/container.clusterViewer` on the project and a Role and RoleBinding in `abfs-ci`, as for [Airflow](ci-job.md#airflow-as-the-ci-system). The RBAC subject is `kind: User` with the account's email.

- **Runner in Google Cloud**: attach the account to the VM with the `userinfo-email` scope (without it GKE sees the numeric ID, not the email), or use Workload Identity Federation for GKE.
- **Runner outside Google Cloud**: impersonate the account with Workload Identity Federation. Do not use service account keys.

The runner needs `gcloud` and `gke-gcloud-auth-plugin`, and runs `gcloud container clusters get-credentials abfs --project example-project --region europe-west4 --dns-endpoint`. The endpoint has a certificate from a public Google CA, so the kubeconfig carries no cluster CA. The runner reaches the endpoint the way it reaches Google APIs. Details: [Authenticate to the Kubernetes API server](https://cloud.google.com/kubernetes-engine/docs/how-to/api-server-authentication) and [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation). [Check CI access to the control plane](install.md#check-ci-access-to-the-control-plane) tests the grants.

### Control plane to node traffic

Clusters created on GKE 1.29 or later use Private Service Connect between the control plane and the nodes. Nothing needs to be set. In the cluster check of [install step 6](install.md#6-create-the-cluster), `privateClusterConfig` must show no `peeringName`; one means the cluster uses VPC Network Peering. `privateEndpoint` is empty with `--no-enable-ip-access`. `publicEndpoint` can hold an address that accepts no connections.

### Monitoring packages

`--monitoring SYSTEM` is enough for the chart's alerts and costs nothing. The packages `POD`, `DEPLOYMENT`, `STATEFULSET`, `DAEMONSET`, `HPA`, `JOBSET`, `CADVISOR` and `KUBELET` are optional and are charged as Managed Service for Prometheus samples. `--monitoring` replaces the earlier list, so name every package:

```sh
gcloud container clusters update abfs --project example-project --region europe-west4 \
  --monitoring SYSTEM,POD,DEPLOYMENT,STATEFULSET,DAEMONSET,HPA
```

## Config Connector

Install Config Connector with its operator from the release bundle, at a pinned version, 1.130.1 or later (the tree is checked against 1.160.0), in namespaced mode. Use the newest version folder in `gs://configconnector-operator/`; a git tag can exist before its bundle is published. Do not use the GKE add-on (`--addons ConfigConnector`): its version lags, and the add-on of GKE 1.36.4-gke.2046003 (1.126.0) has no SpannerInstance `spec.labels`, so the `backend-new` apply fails with `strict decoding error: unknown field "spec.labels"`.

### Install the operator

[Install step 9](install.md#9-install-config-connector) has the commands. Keep the downloaded bundle: the uninstall needs the same files. Use `operator-system/configconnector-operator.yaml`, the file for GKE Standard. The `ConfigConnector` object sets namespaced mode with `stateIntoSpec: Absent`. Wait for the resource CRDs and the webhook before the first KCC apply; an object created before the webhook never gets its project ([troubleshooting](troubleshooting.md#kcc-objects-created-before-the-webhook)).

### Service account of the namespace

Config Connector reconciles the namespace `abfs` as the Google service account `kccServiceAccount` of `settings.yaml`. [Install step 9](install.md#9-install-config-connector) creates it (`kcc-abfs`) with `roles/serviceusage.serviceUsageAdmin`, `roles/iam.serviceAccountAdmin`, `roles/resourcemanager.projectIamAdmin`, `roles/compute.networkAdmin`, `roles/spanner.admin`, `roles/bigtable.admin`, `roles/storage.admin` and `roles/monitoring.metricWriter` (for Config Connector's own metrics), and binds it to `cnrm-system/cnrm-controller-manager-abfs` with `roles/iam.workloadIdentityUser`.

With `backend-existing`, `roles/spanner.admin`, `roles/bigtable.admin` and `roles/storage.admin` can be replaced by roles that allow `setIamPolicy` on the three resources. In a Shared VPC the account also needs `roles/compute.networkUser` on the cluster's subnet, and with `network/proxy-subnet` `roles/compute.networkAdmin` on the network project. Install step 9 grants both.

Enable `serviceusage.googleapis.com` and `cloudresourcemanager.googleapis.com` by hand ([install step 2](install.md#2-access-and-project-apis)); the KCC base enables the others. The `ConfigConnectorContext` is part of the KCC base; [install step 10](install.md#10-apply-the-kcc-overlay) checks its service account and controller. Upgrade and rollback are in the [runbook](runbook.md#config-connector-upgrade-and-rollback).

### Moving from the Config Connector add-on

1. Disable the add-on. Do not delete the `ConfigConnector` or `ConfigConnectorContext` objects. The update fails with `CLUSTER_ALREADY_HAS_OPERATION` while another operation runs.

   ```sh
   kubectl get ns cnrm-system -o jsonpath='{.metadata.annotations.cnrm\.cloud\.google\.com/version}{"\n"}'
   gcloud container clusters update abfs --project example-project --region europe-west4 \
     --update-addons ConfigConnector=DISABLED
   ```

2. Install the operator as in [install step 9](install.md#9-install-config-connector), at the version the add-on ran or later; an older operator can fail CRD validation. Apply the `ConfigConnector` object of that step too.

3. Check the controller as in [install step 10](install.md#10-apply-the-kcc-overlay), and that every object is still Ready:

   ```sh
   kubectl -n abfs wait --for=condition=Ready --all gcp --timeout=30m
   ```

Disabling the add-on deletes the `namespace-id` ConfigMap in `configconnector-operator-system`, and the operator recreates the `cnrm-controller-manager` StatefulSets. The `ConfigConnectorContext`, its Kubernetes service account and the Workload Identity binding stay. Before the move, set `cnrm.cloud.google.com/deletion-policy: abandon` on KCC objects that lack it, and remove it afterwards. The tree sets it on every data resource, the APIs and the Gateway address, not on the service accounts and IAM members:

```sh
kubectl -n abfs get gcp -o custom-columns='KIND:.kind,NAME:.metadata.name,POLICY:.metadata.annotations.cnrm\.cloud\.google\.com/deletion-policy'
kubectl -n abfs annotate <kind>/<name> cnrm.cloud.google.com/deletion-policy=abandon --overwrite
# after the move
kubectl -n abfs annotate <kind>/<name> cnrm.cloud.google.com/deletion-policy-
```

## Network

A plain VPC is enough; a Shared VPC is not required. It needs a subnet for the cluster in the region, a proxy-only subnet, a Private Service Connect endpoint for Google APIs with its DNS zones, Private Google Access on the node subnet and Cloud NAT in the region.

### Private nodes and egress

Nodes are private (`--enable-private-nodes`). This is the only supported setup; public nodes are not supported.

- Google APIs (Artifact Registry, image streaming, Spanner, Bigtable, Cloud Storage, Cloud Monitoring, Cloud Logging, the Workload Identity token services) go to the [Private Service Connect endpoint](#private-service-connect-endpoint-for-google-apis) through Cloud DNS private zones.
- Cloud NAT carries the rest: `gerrit.googlesource.com` for the patched `repo` tool ([install](install.md#check-the-patched-repo-tool)), a Gerrit outside the VPC, and with `ci.casfs.provider: daemonset` the Ubuntu archive. A Gerrit inside the VPC, or reached through Cloud VPN or Cloud Interconnect, needs no NAT.
- `--disable-default-snat`: Pod traffic keeps the Pod address, and Cloud NAT translates it once, as GKE's networking best practices advise. Cloud NAT must then serve the Pod range: `--nat-custom-subnet-ip-ranges <subnet>:ALL` covers the primary and secondary ranges. Leave the flag out only when Pod addresses must be hidden behind node addresses towards on-premises networks. On an existing cluster, `clusters update --disable-default-snat` works only if the cluster was created with private nodes; it applies once every node has been replaced.

Keep Private Google Access on for the node subnet; VMs without external addresses use the endpoint only from such a subnet. Set up the subnet flag and Cloud NAT before the cluster is created: [install step 4.1](install.md#41-private-google-access-and-cloud-nat) creates the router `abfs-nat-router` and the gateway `abfs-nat` for the cluster's subnet only (`--nat-custom-subnet-ip-ranges <subnet>:ALL`), with dynamic port allocation from 64 to 2,048 ports per VM and error logging, the values of GKE's networking best practices. Dynamic allocation does not work with endpoint-independent mapping, which is off by default.

Only one gateway can serve a subnet range. Where the region has a gateway ([install step 4.0](install.md#40-check-what-the-network-has)), reuse one that serves the cluster's subnet with all its ranges. Check after step 6: every node shows `<none>` under `EXTERNAL-IP`; the ComputeClass pools follow the cluster setting.

### Private Service Connect endpoint for Google APIs

The endpoint is a global internal address with a forwarding rule to the bundle `all-apis`. It serves `*.googleapis.com`, `pkg.dev`, `gcr.io` and `gke.goog`, not `*.rep.googleapis.com`.

- APIs: Compute Engine, Service Directory and Cloud DNS. Admin roles: `roles/compute.networkAdmin`, `roles/dns.admin`, `roles/servicedirectory.editor`.
- Address: a single IPv4 address outside every primary and secondary range of the VPC and of peered networks (example `10.100.0.5`).
- Name: 1 to 20 lower-case letters and digits, starting with a letter.
- `--service-directory-registration` sets the Service Directory region; the default is `us-central1`. All endpoints for Google APIs of one network share one region.
- Zones: a Cloud DNS private zone, visible to the VPC, for `googleapis.com` (APIs and image streaming), `pkg.dev` (Artifact Registry) and `gcr.io` (`gcr.io` and `*.gcr.io`), each with an `A` record for the zone name to the endpoint and a `CNAME` for `*.<zone>` to the zone name. Add `gke.goog` for admin hosts in the VPC that use the [DNS-based endpoint](#control-plane-access); nodes do not need it.

[Install step 4.2](install.md#42-private-service-connect-endpoint-for-google-apis) creates the address `abfs-apis-psc`, the endpoint `abfsapis` and the zones `psc-googleapis-com`, `psc-pkg-dev` and `psc-gcr-io`. The zones apply to every VM and cluster of the network. A client there that uses `*.rep.googleapis.com` then reaches the endpoint, which does not serve those names. The network can have such zones already, for example to `private.googleapis.com` or `restricted.googleapis.com` ([install step 4.0](install.md#40-check-what-the-network-has) says when to reuse them). Agree the zones with the network owner. If the VPC has a private zone for `p.googleapis.com` and no endpoint, delete that zone first, or the endpoint creation fails. In a Shared VPC, create the address, endpoint and zones in the host project; install step 4 runs there with `$NET_P`.

Check ([install step 4](install.md#check-the-network)): the forwarding rule shows `IPAddress: 10.100.0.5` and `target: all-apis`. In [install step 14](install.md#14-install-the-chart), a pod of the `abfs-client` image runs `getent hosts`, and every API name resolves to `10.100.0.5`; a public address means the zone is missing or not visible to the network. From a VM in the VPC, `curl -v 10.100.0.5/generate_204` answers HTTP `204`. The endpoint does not answer ping.

### Proxy-only subnet

One per network and region, purpose `REGIONAL_MANAGED_PROXY`, role `ACTIVE`. Every Envoy-based load balancer of the network in the region shares it, one address per proxy. A `/23` serves a small install; at thousands of CI nodes plan a `/22` ([sizing](sizing.md#gateway)). It cannot grow in place: a larger one is created with role `BACKUP` and then made `ACTIVE`. [Install step 4.3](install.md#43-proxy-only-subnet) checks for one.

If there is none, add the component `network/proxy-subnet` to the overlay and set `proxySubnetRange` in `settings.yaml`. Pick that range outside the node subnet, the cluster's Pod range (`POD_RANGE` of install step 6), `PSC_IP` and peered ranges. In a Shared VPC, pick it outside the Pod and Service secondary ranges of the host project. A free range also avoids the control-plane ranges of older private clusters on the network, often in `172.16.0.0/12`. If there is one, leave the component out and use its range. Either way, set `networkPolicy.proxyOnlySubnetCidr` to that range.

### Firewall

In a plain VPC the Gateway controller creates the rule `gkegw1-<id>-l7-<network>-<region>` with the first Gateway. `<id>` is a short cluster id. The rule allows `35.191.0.0/16`, `130.211.0.0/22` and the proxy-only subnet, TCP 0-65535, to all instances. The check of [install step 14](install.md#14-install-the-chart) lists it. In a Shared VPC the controller does not create it: the host-project owner creates the health-check and proxy rule by hand, as Google's GKE firewall rules page describes.

### Egress for the CI nodes

Only with `ci.casfs.provider: daemonset`; COS CI nodes need only the Google APIs and the store. The loader installs the kernel headers for `uname -r`, `build-essential` and `gettext-base` from the node's apt sources on every new CI node, with the node's DNS resolver and routes. The CI nodes need egress to the Ubuntu archive, through the Cloud NAT of [Private nodes and egress](#private-nodes-and-egress), or an internal mirror.

## Node service account

The KCC base creates `abfs-nodes@<project>.iam.gserviceaccount.com` with `roles/container.defaultNodeServiceAccount`, `roles/artifactregistry.reader` and `roles/serviceusage.serviceUsageConsumer` (for [image streaming](#image-streaming)). `computeClasses.nodeServiceAccount` names it for every ComputeClass.

The system pool runs as `abfs-system-nodes`, with `roles/container.defaultNodeServiceAccount` and `roles/serviceusage.serviceUsageConsumer` and no other role; without the second, it pulls its images in full. Neither account can pull the ABFS images: a pod on the system pool fails with `403 Forbidden` on the registry token, then `ImagePullBackOff`. The chart therefore places the bootstrap Job and the server health test on `abfs-server` (`bootstrap.computeClass`, `tests.computeClass`) and the CI pods on `abfs-ci`. If you set either value to empty, grant `abfs-system-nodes` `roles/artifactregistry.reader` on the repository.

When the repository is in another project, grant both roles there:

```sh
gcloud artifacts repositories add-iam-policy-binding abfs --project <registry project> \
  --location europe-west4 --role roles/artifactregistry.reader \
  --member serviceAccount:abfs-nodes@example-project.iam.gserviceaccount.com
gcloud projects add-iam-policy-binding <registry project> \
  --role roles/serviceusage.serviceUsageConsumer \
  --member serviceAccount:abfs-nodes@example-project.iam.gserviceaccount.com
```

The checks of [install step 5](install.md#5-system-node-service-account) and [install step 10](install.md#10-apply-the-kcc-overlay) list the roles of both accounts.

## Artifact Registry

A Docker repository for the four images, in the region of the cluster. The KCC tree does not create it; [install step 11](install.md#11-upload-the-images) creates it with `gcloud artifacts repositories create`.

## IOPS fallback node pool for CI

A ComputeClass sets no boot disk IOPS: an auto-created CI node gets the Hyperdisk Balanced defaults for its size. If builds are limited by boot disk speed, create CI node pools by hand with provisioned IOPS and list them first in the class. GKE applies the class's `nodePoolConfig` only to pools it creates, and a `nodepools` rule takes no other field, so the pool sets every class setting itself. Create it after the KCC apply, because it uses the node service account:

```sh
gcloud container node-pools create abfs-ci-iops \
  --project example-project --cluster abfs --region europe-west4 \
  --node-locations europe-west4-a,europe-west4-b,europe-west4-c \
  --machine-type n4-standard-32 \
  --image-type COS_CONTAINERD \
  --shielded-secure-boot --shielded-integrity-monitoring \
  --disk-type hyperdisk-balanced --disk-size 1100 \
  --boot-disk-provisioned-iops 40000 \
  --boot-disk-provisioned-throughput 1000 \
  --service-account abfs-nodes@example-project.iam.gserviceaccount.com \
  --workload-metadata GKE_METADATA \
  --add-maintenance-exclusion-until-end-of-support \
  --enable-autoscaling --min-nodes 0 --max-nodes 10 \
  --node-labels cloud.google.com/compute-class=abfs-ci \
  --node-taints cloud.google.com/compute-class=abfs-ci:NoSchedule
```

`--min-nodes` and `--max-nodes` count per zone. With `ci.casfs.provider: daemonset`, use `--image-type UBUNTU_CONTAINERD` and `--no-shielded-secure-boot --shielded-integrity-monitoring` in place of `--shielded-secure-boot --shielded-integrity-monitoring`. The IOPS and throughput are examples; the machine type caps them. Give the pool at least `computeClasses.ci.bootDiskGb`: the render check does not see hand-made pools, and on a smaller disk the cache reservation and a build do not fit ([node disk](ci-job.md#node-disk)). The pool follows the cluster's image streaming setting. List the pool in the chart values; the class tries it before the auto-created shapes:

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

These are steps of the [install](install.md):

- a GKE version whose COS image ships casfs 0.1.24 or later, and the `no_minor_upgrades` exclusion ([GKE version for the CI nodes](#gke-version-for-the-ci-nodes)), unless `ci.casfs.provider` is `daemonset`;
- no namespace `abfs-ci` from elsewhere: the chart creates it with Pod Security level `privileged` (the KCC base creates only `abfs`), unless `namespaces.createCi` is false;
- the Secret `gerrit-credentials` in `abfs` with the key `.netrc` (and `.gitcookies` when `pusher.gerrit.cookies` is true);
- the images in Artifact Registry, with their digests in `values-images.yaml` or `images.txt`;
- the IP of the Gateway address for `gateway.vip`;
- no cluster operation running ([Wait for cluster operations](#wait-for-cluster-operations));
- cluster-level node auto-provisioning off, or its limits above the peak of all nodes ([Cluster-level node auto-provisioning](#cluster-level-node-auto-provisioning));
- the Private Service Connect endpoint for Google APIs and its DNS zones, Private Google Access on the node subnet, and Cloud NAT ([Private nodes and egress](#private-nodes-and-egress));
- `kubectl` on the DNS-based endpoint, with an account that may create cluster-scoped objects ([Control plane access](#control-plane-access)).

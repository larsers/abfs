# ABFS on GKE / Install

The whole install in order, with a check after each step. Start a step only after the previous check passed; when a check fails, stop and follow its pointer. Background: [prerequisites](prerequisites.md), [runbook](runbook.md), [configuration](configuration.md), [usage](usage.md).

A **Creates** line lists the resources, IAM changes and costs of a step. Steps without one change nothing.

The install ends with [tests](#15-tests). Level 1 always runs. The quick end-to-end test and the [AAOS acceptance test](#16-aaos-acceptance-test) run only when the human chooses them.

## Before you start

Run every command from the release bundle root (or `deploy/gke` of the ABFS repository) on the admin workstation. Set these variables (example values):

```sh
P=example-project               # the project of the cluster and the backend
REGION=europe-west4
ZONE=europe-west4-b             # settings.yaml zone: the Bigtable cluster's zone, where the servers run
NET_P=$P                        # the project of the network: $P, or the host project of a Shared VPC
NET=example-network             # the VPC network
SUBNET=example-subnet           # the cluster's subnet in $REGION
CLUSTER=abfs                    # the cluster name
GKE_VERSION=1.37.2-gke.1325000  # see step 3
AR=abfs                         # the Artifact Registry repository
REG=$REGION-docker.pkg.dev/$P/$AR
PSC_IP=10.100.0.5               # a free address for the Private Service Connect endpoint
```

The other pages use the literal names `example-project`, `europe-west4` and cluster `abfs`; replace them there too. The namespace `abfs` and the release name `abfs` are fixed.

`NET_P` is `networkProject` in `settings.yaml` (step 10). In a Shared VPC it is the host project: step 4 runs there, and step 6 names the network and subnet by their path in it.

## 1. Tools on the admin workstation

| Tool | Used for | Check |
| --- | --- | --- |
| `gcloud` | Google Cloud steps | `gcloud version` |
| `kubectl` with kustomize | KCC overlay, workloads | `kubectl version --client` |
| `helm` 4 | the chart | `helm version --short` prints `v4.` |
| `skopeo` | step 11 | `skopeo --version` |
| `jq` | step 3, seed progress | `jq --version` |
| `make` | offline checks | `make --version` |
| `docker` | step 12, image builds from source | `docker version` |

```sh
sudo apt-get update && sudo apt-get -y install skopeo   # Debian 11 or later, Ubuntu 20.10 or later
sudo dnf -y install skopeo                              # Fedora, RHEL and CentOS Stream 8 or later
brew install skopeo                                     # macOS with Homebrew
```

Sign in, and check the bundle and the tree:

```sh
gcloud auth login
sha256sum -c --quiet SHA256SUMS      # bundle only; on macOS: shasum -a 256 -c --quiet SHA256SUMS
make check
```

Check: each tool prints its version, `sha256sum` prints nothing, `make check` exits 0 with no `FAIL` line.

If it fails: `FAILED` from `sha256sum` means a damaged bundle; get it again. `make check` failing on an unchanged bundle points to the wrong Helm major version.

## 2. Access and project APIs

**Creates:** enabled APIs (no charge by themselves).

The admin needs on the project (a project owner has all):

- `roles/container.admin` ([Control plane access](prerequisites.md#control-plane-access)).
- Step 4: `roles/compute.networkAdmin`, `roles/dns.admin`, `roles/servicedirectory.editor` (on `$NET_P` in a Shared VPC; the network owner may run step 4).
- Steps 5 and 9: `roles/iam.serviceAccountAdmin`, `roles/resourcemanager.projectIamAdmin`.
- Step 6: `roles/iam.serviceAccountUser` on `abfs-system-nodes` (granted in step 5).
- This step: `roles/serviceusage.serviceUsageAdmin`. Step 11: `roles/artifactregistry.admin`.
- Step 12: `roles/spanner.databaseAdmin`, `roles/bigtable.admin`, `roles/storage.objectViewer`.
- Step 14: `roles/logging.viewer`.

In a Shared VPC, step 9 also grants roles in `$NET_P`: on `$SUBNET` (needs `compute.subnetworks.setIamPolicy`, for example `roles/compute.xpnAdmin`), and with `network/proxy-subnet` on the project (needs `roles/resourcemanager.projectIamAdmin` on `$NET_P`). The network owner may grant both.

Enable the APIs needed before the KCC apply (the KCC base enables the rest, `kcc/base/services.yaml`). The Container File System API must be on before the cluster exists ([image streaming](prerequisites.md#image-streaming)):

```sh
gcloud services enable container.googleapis.com compute.googleapis.com iam.googleapis.com \
  serviceusage.googleapis.com cloudresourcemanager.googleapis.com \
  containerfilesystem.googleapis.com artifactregistry.googleapis.com \
  servicedirectory.googleapis.com dns.googleapis.com --project $P
```

In a Shared VPC, also enable `compute.googleapis.com`, `servicedirectory.googleapis.com` and `dns.googleapis.com` in `$NET_P`.

Check:

```sh
gcloud services list --enabled --project $P --format 'value(config.name)' | sort
```

It lists all nine. In a Shared VPC, run it with `--project $NET_P` too; it lists the three.

## 3. GKE version for the CI nodes

With the default `ci.casfs.provider: image`, the COS node image must ship casfs 0.1.24 or later ([GKE version for the CI nodes](prerequisites.md#gke-version-for-the-ci-nodes)). `1.37.2-gke.1325000` (Rapid) uses `cos-133-19999-44-193`, which does. For another version, find its COS image in the GKE release notes and check it, with the build number in dots:

```sh
gcloud storage cat gs://cos-tools/19999.44.193/cos-package-info.json \
  | jq -c '.installedPackages[] | select(.name == "casfs")'
gcloud container get-server-config --location $REGION --project $P \
  --flatten channels --filter 'channels.channel=RAPID' --format 'value(channels.validVersions)'
```

Check: `ebuild_version` is 0.1.24 or later; the second command lists `$GKE_VERSION`.

If it fails: pick a newer version, or use `ci.casfs.provider: daemonset` with Ubuntu CI nodes ([egress for the CI nodes](prerequisites.md#egress-for-the-ci-nodes)).

## 4. Network

**Creates:** a Cloud Router and Cloud NAT gateway (hourly, plus per GiB), a Private Service Connect endpoint (hourly), three Cloud DNS private zones (daily, plus queries). Turns on Private Google Access on the subnet. In a Shared VPC, all in `$NET_P`.

Nodes are private ([Private nodes and egress](prerequisites.md#private-nodes-and-egress)). For public nodes, skip 4.1 and 4.2 and leave out `--enable-private-nodes` in step 6. These resources affect the whole network: run 4.0 first and agree on the result with the network owner before creating anything.

### 4.0 Check what the network has

Read-only:

```sh
gcloud dns managed-zones list --project $NET_P --filter 'visibility=private' \
  --format 'table(name, dnsName, privateVisibilityConfig.networks[].networkUrl)'
gcloud compute forwarding-rules list --project $NET_P --global \
  --filter 'target=(all-apis OR vpc-sc)' --format 'table(name, IPAddress, target, network.basename())'
gcloud compute routers list --project $NET_P --regions $REGION --filter "network~/networks/$NET\$" \
  --format 'table(name, nats[].name, nats[].sourceSubnetworkIpRangesToNat)'
gcloud compute networks subnets list --project $NET_P --network $NET \
  --filter "purpose=REGIONAL_MANAGED_PROXY AND region:$REGION"
```

When all four list nothing, continue with 4.1 (`WARNING: The following filter keys were not present in any resource : visibility` is then expected). Otherwise:

- **Zones:** a private zone for `googleapis.com`, `pkg.dev` or `gcr.io` bound to `$NET` stays; do not create it in 4.2. Agree with the network owner that it covers the APIs of [Private nodes and egress](prerequisites.md#private-nodes-and-egress).
- **Forwarding rules:** an endpoint with target `all-apis` or `vpc-sc` is reused: set `PSC_IP` to its `IPAddress` and skip the address and forwarding rule of 4.2. With `vpc-sc`, the network owner confirms it serves the APIs ABFS uses.
- **Routers:** a gateway with `ALL_SUBNETWORKS_ALL_IP_RANGES` already serves `$SUBNET`: skip the router and gateway of 4.1 and check that gateway in step 4's check. With `LIST_OF_SUBNETWORKS` or `ALL_SUBNETWORKS_ALL_PRIMARY_IP_RANGES`, run `gcloud compute routers nats describe <gateway> --router <router> --region $REGION --project $NET_P`: if it serves `$SUBNET` with `ALL_IP_RANGES`, skip as above; if it serves part of it, agree the change with the network owner; if not at all, create the 4.1 gateway with `--nat-custom-subnet-ip-ranges $SUBNET:ALL` in place of `--nat-all-subnet-ip-ranges`. With no gateway, `--nat-all-subnet-ip-ranges` gives every subnet in the region egress; if `gcloud compute networks subnets list --project $NET_P --network $NET --filter "region:$REGION"` shows other subnets, agree it with the network owner or use `--nat-custom-subnet-ip-ranges $SUBNET:ALL`.
- **Subnets:** note the range of an existing proxy-only subnet for 4.3.

Zones bound from other projects and endpoints in service projects do not show here; ask the network owner.

### 4.1 Private Google Access and Cloud NAT

```sh
gcloud compute networks subnets update $SUBNET --project $NET_P \
  --region $REGION --enable-private-ip-google-access
gcloud compute routers create abfs-nat-router --project $NET_P \
  --region $REGION --network $NET
gcloud compute routers nats create abfs-nat --project $NET_P \
  --region $REGION --router abfs-nat-router \
  --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges
```

### 4.2 Private Service Connect endpoint for Google APIs

`PSC_IP` must be outside every range of the VPC and its peers; the endpoint name has 1 to 20 lower-case letters and digits ([Private Service Connect endpoint for Google APIs](prerequisites.md#private-service-connect-endpoint-for-google-apis)).

```sh
gcloud compute addresses create abfs-apis-psc --project $NET_P --global \
  --purpose PRIVATE_SERVICE_CONNECT --addresses $PSC_IP --network $NET
gcloud compute forwarding-rules create abfsapis --project $NET_P --global \
  --network $NET --address abfs-apis-psc \
  --target-google-apis-bundle all-apis \
  --service-directory-registration projects/$NET_P/locations/$REGION
for z in googleapis.com pkg.dev gcr.io; do
  n=psc-${z//./-}
  gcloud dns managed-zones create $n --project $NET_P --dns-name $z. --visibility private \
    --networks $NET --description "Google APIs through the Private Service Connect endpoint"
  gcloud dns record-sets create $z. --project $NET_P --zone $n --type A --ttl 300 --rrdatas $PSC_IP
  gcloud dns record-sets create "*.$z." --project $NET_P --zone $n --type CNAME --ttl 300 --rrdatas $z.
done
```

Add `gke.goog` to the list when admin hosts in the VPC reach the control plane through the endpoint. If the VPC has a private zone for `p.googleapis.com` and no endpoint yet, delete that zone first. Leave out zones that 4.0 found.

### 4.3 Proxy-only subnet

The Gateway needs one proxy-only subnet per network and region:

```sh
gcloud compute networks subnets list --project $NET_P --network $NET \
  --filter "purpose=REGIONAL_MANAGED_PROXY AND region:$REGION"
```

If one exists, note its range for `networkPolicy.proxyOnlySubnetCidr`. If not, step 10 creates it with `network/proxy-subnet`; pick a free `/23` for `proxySubnetRange` ([Proxy-only subnet](prerequisites.md#proxy-only-subnet)). In a plain VPC, pick it outside `10.0.0.0/8`, such as `172.16.0.0/23`: step 6 lets GKE pick the cluster's Pod range from `10.0.0.0/8`. In a Shared VPC, pick it outside the Pod and Service ranges that `gcloud container subnets list-usable` shows in step 6.

### Check the network

```sh
gcloud compute networks subnets describe $SUBNET --project $NET_P \
  --region $REGION --format 'get(privateIpGoogleAccess)'
gcloud compute routers nats describe abfs-nat --project $NET_P \
  --region $REGION --router abfs-nat-router \
  --format 'yaml(natIpAllocateOption, sourceSubnetworkIpRangesToNat, subnetworks)'
gcloud compute forwarding-rules describe abfsapis --project $NET_P --global \
  --format 'yaml(IPAddress, target, network)'
gcloud dns managed-zones list --project $NET_P --filter 'visibility=private' --format 'value(dnsName)'
```

Expected: `True`; `AUTO_ONLY` and `ALL_SUBNETWORKS_ALL_IP_RANGES` (or `LIST_OF_SUBNETWORKS` with `$SUBNET` and `ALL_IP_RANGES`); `IPAddress` `$PSC_IP` with `target: all-apis` (or `target: vpc-sc` if reused); zones `googleapis.com.`, `pkg.dev.`, `gcr.io.` and the endpoint's own `p.googleapis.com.`. For resources reused from 4.0, describe those instead.

If it fails: [Image pulls or Google API calls fail on private nodes](troubleshooting.md#image-pulls-or-google-api-calls-fail-on-private-nodes).

## 5. System node service account

**Creates:** the service account `abfs-system-nodes`, two project role bindings, and one role binding on the account.

The system pool's account ([Node service account](prerequisites.md#node-service-account)):

```sh
gcloud iam service-accounts create abfs-system-nodes --project $P \
  --display-name "ABFS system node pool"
for r in roles/container.defaultNodeServiceAccount roles/serviceusage.serviceUsageConsumer; do
  gcloud projects add-iam-policy-binding $P \
    --member serviceAccount:abfs-system-nodes@$P.iam.gserviceaccount.com --role $r
done
```

Step 6 needs `iam.serviceAccounts.actAs` on it. Grant the admin of step 6 `roles/iam.serviceAccountUser` (a project owner has it):

```sh
gcloud iam service-accounts add-iam-policy-binding abfs-system-nodes@$P.iam.gserviceaccount.com \
  --project $P --member user:<admin email> --role roles/iam.serviceAccountUser
```

Check:

```sh
gcloud projects get-iam-policy $P --flatten 'bindings[].members' \
  --filter 'bindings.members:abfs-system-nodes@' --format 'value(bindings.role)'
gcloud iam service-accounts get-iam-policy abfs-system-nodes@$P.iam.gserviceaccount.com --project $P \
  --flatten 'bindings[].members' --format 'value(bindings.role, bindings.members)'
```

The first prints only the two roles above; the second `roles/iam.serviceAccountUser` with the admin.

## 6. Create the cluster

**Creates:** a GKE Standard cluster with a regional control plane (cluster management fee) and one `n4-standard-4` system node per zone of the region.

Set the maintenance window to the quiet hours of your CI, in UTC ([Node upgrades](prerequisites.md#node-upgrades)). Do not add `--addons`: it turns off every add-on it does not name ([Cluster](prerequisites.md#cluster)).

In a Shared VPC, the host-project owner first attaches `$P` as a service project, grants the GKE service agent of `$P` its host-project roles, and creates Pod and Service secondary ranges in `$SUBNET` ([Set up clusters with Shared VPC](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-shared-vpc)). Add `--cluster-secondary-range-name <Pod range> --services-secondary-range-name <Service range>` to the create command. List the usable ranges:

```sh
gcloud container subnets list-usable --project $P --network-project $NET_P
```

Create the cluster:

```sh
gcloud container clusters create $CLUSTER \
  --project $P --region $REGION \
  --release-channel rapid --cluster-version $GKE_VERSION \
  --network projects/$NET_P/global/networks/$NET \
  --subnetwork projects/$NET_P/regions/$REGION/subnetworks/$SUBNET --enable-ip-alias \
  --enable-private-nodes \
  --enable-dns-access --no-enable-ip-access \
  --workload-pool $P.svc.id.goog \
  --gateway-api standard \
  --enable-dataplane-v2 \
  --enable-managed-prometheus \
  --managed-otel-scope COLLECTION_AND_INSTRUMENTATION_COMPONENTS \
  --enable-vertical-pod-autoscaling \
  --enable-image-streaming \
  --autoscaling-profile optimize-utilization \
  --logging SYSTEM,WORKLOAD --monitoring SYSTEM \
  --maintenance-window-start 2026-01-03T00:00:00Z --maintenance-window-duration 8H \
  --maintenance-window-recurrence 'FREQ=WEEKLY;BYDAY=SA,SU' \
  --machine-type n4-standard-4 --disk-type hyperdisk-balanced --num-nodes 1 \
  --service-account abfs-system-nodes@$P.iam.gserviceaccount.com \
  --image-type COS_CONTAINERD --workload-metadata GKE_METADATA \
  --shielded-secure-boot --shielded-integrity-monitoring
gcloud container clusters get-credentials $CLUSTER --project $P --region $REGION \
  --dns-endpoint
```

A regional create waits for capacity in every zone. On a zone stockout, wait for the operation to end, delete the cluster, and recreate it with `--node-locations` set to zones that have capacity.

Other users get `roles/container.viewer`, `roles/container.developer` or `roles/container.admin`; a CI system gets `roles/container.clusterViewer` and a Role in `abfs-ci` ([CI access to the control plane](prerequisites.md#ci-access-to-the-control-plane)). Grant them only with the project owner's approval:

```sh
gcloud projects add-iam-policy-binding $P --member user:<email> --role roles/container.viewer
```

Check:

```sh
gcloud container clusters describe $CLUSTER --project $P --region $REGION \
  --format 'yaml(currentMasterVersion, networkConfig.gatewayApiConfig, workloadIdentityConfig,
    networkConfig.datapathProvider, verticalPodAutoscaling, autoscaling,
    addonsConfig.httpLoadBalancing, addonsConfig.horizontalPodAutoscaling,
    addonsConfig.gcePersistentDiskCsiDriverConfig, monitoringConfig.managedPrometheusConfig,
    maintenancePolicy.window, controlPlaneEndpointsConfig, privateClusterConfig,
    nodePools[].config.shieldedInstanceConfig)'
kubectl get nodes -o wide
kubectl get crd computeclasses.cloud.google.com gateways.gateway.networking.k8s.io \
  verticalpodautoscalers.autoscaling.k8s.io
end=$(( $(date +%s) + 600 ))
until kubectl get gatewayclass gke-l7-rilb; do
  [ "$(date +%s)" -ge "$end" ] && { echo "no gke-l7-rilb after 10 minutes" >&2; break; }
  sleep 15
done
```

Expected:

- `currentMasterVersion` is `$GKE_VERSION`; `channel: CHANNEL_STANDARD`; `datapathProvider` is `ADVANCED_DATAPATH`.
- `httpLoadBalancing` and `horizontalPodAutoscaling` not `disabled: true`; `gcePersistentDiskCsiDriverConfig` `enabled: true`.
- `autoscalingProfile: OPTIMIZE_UTILIZATION`, no `enableNodeAutoprovisioning: true` ([Cluster-level node auto-provisioning](prerequisites.md#cluster-level-node-auto-provisioning)).
- `dnsEndpointConfig`: `allowExternalTraffic: true` and an endpoint under `gke.goog`; `ipEndpointsConfig`: `enabled: false`.
- `privateClusterConfig`: `enablePrivateNodes: true`, no `peeringName` ([Control plane to node traffic](prerequisites.md#control-plane-to-node-traffic)). A `publicEndpoint` address may show; it accepts no connections.
- `shieldedInstanceConfig`: the system pool shows `enableSecureBoot: true`, `enableIntegrityMonitoring: true`.
- Every node shows `<none>` under `EXTERNAL-IP`. The three CRDs exist; `gke-l7-rilb` appears within 10 minutes.

If it fails: [kubectl cannot reach the control plane](troubleshooting.md#kubectl-cannot-reach-the-control-plane), [Gateway waits for its controller](troubleshooting.md#gateway-waits-for-its-controller).

## 7. Hold minor upgrades

**Creates:** a maintenance exclusion on the cluster.

Keep the cluster on a minor version whose COS image ships a matching casfs ([Hold minor upgrades](prerequisites.md#hold-minor-upgrades)):

```sh
gcloud container clusters update $CLUSTER --project $P --region $REGION \
  --add-maintenance-exclusion-name abfs-no-minor \
  --add-maintenance-exclusion-scope no_minor_upgrades \
  --add-maintenance-exclusion-start "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --add-maintenance-exclusion-until-end-of-support
```

Check:

```sh
gcloud container clusters describe $CLUSTER --project $P --region $REGION \
  --format 'yaml(maintenancePolicy.window.maintenanceExclusions)'
```

It shows `abfs-no-minor`, `NO_MINOR_UPGRADES` and `UNTIL_END_OF_SUPPORT`.

## 8. Wait for cluster operations

A running cluster operation blocks node pool creation ([Wait for cluster operations](prerequisites.md#wait-for-cluster-operations)). Run this now, again before steps 10 and 14, and before any later `clusters update`:

```sh
gcloud container operations list --project $P --location $REGION \
  --filter "(targetLink~/clusters/$CLUSTER\$ OR targetLink~/clusters/$CLUSTER/) AND status!=DONE"
gcloud container operations wait <operation> --project $P --location $REGION
```

Check: the list is empty. Wait for each listed operation with the second command.

If it fails: [Pods Pending with Cluster is running incompatible operation](troubleshooting.md#pods-pending-with-cluster-is-running-incompatible-operation).

## 9. Install Config Connector

**Creates:** the Config Connector operator, the service account `kcc-abfs` with eight project roles, and a Workload Identity binding. In a Shared VPC also a binding on `$SUBNET` in the host project, and with `network/proxy-subnet` one on the host project.

Install from the release bundle, 1.130.1 or later (checked against 1.160.0), in namespaced mode, not the GKE add-on ([Config Connector](prerequisites.md#config-connector)). Keep the bundle; the uninstall needs it.

```sh
V=1.160.0
gcloud storage cp gs://configconnector-operator/$V/release-bundle.tar.gz release-bundle-$V.tar.gz
mkdir kcc-$V && tar -C kcc-$V -zxf release-bundle-$V.tar.gz
kubectl apply -f kcc-$V/operator-system/configconnector-operator.yaml
kubectl apply -f - <<EOF
apiVersion: core.cnrm.cloud.google.com/v1beta1
kind: ConfigConnector
metadata:
  name: configconnector.core.cnrm.cloud.google.com
spec:
  mode: namespaced
  stateIntoSpec: Absent
EOF
end=$(( $(date +%s) + 900 ))
until kubectl get crd spannerinstances.spanner.cnrm.cloud.google.com >/dev/null 2>&1 &&
    kubectl -n cnrm-system get deployment cnrm-webhook-manager >/dev/null 2>&1; do
  [ "$(date +%s)" -ge "$end" ] && { echo "no Config Connector CRDs or webhook after 15 minutes" >&2; break; }
  sleep 10
done
kubectl -n cnrm-system wait --for=condition=Available deployment/cnrm-webhook-manager --timeout=10m
end=$(( $(date +%s) + 600 ))
until kubectl get mutatingwebhookconfiguration mutating-webhook.cnrm.cloud.google.com >/dev/null 2>&1; do
  [ "$(date +%s)" -ge "$end" ] && { echo "no Config Connector webhook configuration after 10 minutes" >&2; break; }
  sleep 10
done
```

Create the account Config Connector uses for namespace `abfs` ([Service account of the namespace](prerequisites.md#service-account-of-the-namespace); with `backend-existing` the three backend roles can be narrower):

```sh
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

In a Shared VPC, grant the account `roles/compute.networkUser` on `$SUBNET` (the Gateway address), and `roles/compute.networkAdmin` on `$NET_P` only when the overlay lists `network/proxy-subnet`. Without them the step 10 objects stay not Ready. The network owner may run these:

```sh
gcloud compute networks subnets add-iam-policy-binding $SUBNET --project $NET_P --region $REGION \
  --member serviceAccount:kcc-abfs@$P.iam.gserviceaccount.com --role roles/compute.networkUser
gcloud projects add-iam-policy-binding $NET_P \
  --member serviceAccount:kcc-abfs@$P.iam.gserviceaccount.com --role roles/compute.networkAdmin
```

Check:

```sh
kubectl get ns cnrm-system -o jsonpath='{.metadata.annotations.cnrm\.cloud\.google\.com/version}{"\n"}'
gcloud projects get-iam-policy $P --flatten 'bindings[].members' \
  --filter 'bindings.members:kcc-abfs@' --format 'value(bindings.role)'
```

In a Shared VPC, also:

```sh
gcloud compute networks subnets get-iam-policy $SUBNET --project $NET_P --region $REGION \
  --flatten 'bindings[].members' --filter 'bindings.members:kcc-abfs@' --format 'value(bindings.role)'
gcloud projects get-iam-policy $NET_P --flatten 'bindings[].members' \
  --filter 'bindings.members:kcc-abfs@' --format 'value(bindings.role)'
```

Expected: the installed version, no timeout message from the waits, the eight roles. In a Shared VPC: `roles/compute.networkUser` on the subnet, and `roles/compute.networkAdmin` on the host project when used.

If it fails: never apply the KCC overlay before the webhook exists ([KCC objects created before the webhook](troubleshooting.md#kcc-objects-created-before-the-webhook)).

## 10. Apply the KCC overlay

**Creates:** the namespace `abfs`, the service accounts `abfs-server` and `abfs-nodes` with their roles, the Gateway address, the KCC base APIs; with `backend-new` a Spanner instance (100 processing units), a Bigtable instance with one autoscaled cluster (1 to 3 nodes by default) and a Cloud Storage bucket, billed hourly from now on; with `network/proxy-subnet` the proxy-only subnet.

Run the check of [step 8](#8-wait-for-cluster-operations) first. Copy the example overlay and set every value of its `settings.yaml`:

```sh
cp -r kcc/overlays/example kcc/overlays/<name>
```

- `project`, `region`, `zone` (`$ZONE`), `networkProject` (`$NET_P`), `network`, `subnetwork`.
- `proxySubnetRange`, only with `network/proxy-subnet`.
- `kccServiceAccount`: `kcc-abfs@<project>.iam.gserviceaccount.com`.
- `serverWorkloadIdentityMember`: always `serviceAccount:<project>.svc.id.goog[abfs/abfs-server]`.
- `spannerInstance`, `spannerDatabase`, `bigtableInstance`, `bigtableCluster`, `bucket`: backend names (bucket names are global; with `backend-existing`, the existing names).

In `kustomization.yaml`, list exactly one of `backend-new` and `backend-existing` ([Backend modes](runbook.md#backend-modes)), `network/proxy-subnet` when 4.3 found none, and `settings`. Set the Bigtable node cap with the patch in the file. Leave `gc` out ([Garbage collection](runbook.md#garbage-collection)).

```sh
kubectl kustomize kcc/overlays/<name> | less     # review
kubectl apply -k kcc/overlays/<name>
kubectl -n abfs wait --for=condition=Ready --all gcp --timeout=30m
```

Check:

```sh
kubectl -n abfs get gcp
kubectl get serviceaccount/cnrm-controller-manager-abfs -n cnrm-system
kubectl wait -n cnrm-system --for=condition=Ready pod --timeout=10m \
  -l cnrm.cloud.google.com/component=cnrm-controller-manager,cnrm.cloud.google.com/scoped-namespace=abfs
gcloud projects get-iam-policy $P --flatten 'bindings[].members' \
  --filter 'bindings.members:abfs-nodes@' --format 'value(bindings.role)'
kubectl -n abfs get computeaddress abfs-gateway -o jsonpath='{.status.observedState.address}{"\n"}'
```

Expected: every object `READY True`. `abfs-nodes` has `roles/container.defaultNodeServiceAccount`, `roles/artifactregistry.reader` and `roles/serviceusage.serviceUsageConsumer`. The last command prints the Gateway address (`gateway.vip` in step 13).

If it fails: `kubectl -n abfs describe <kind> <name>` shows why. `cannot resolve project` on every object: [KCC objects created before the webhook](troubleshooting.md#kcc-objects-created-before-the-webhook). `abfs-proxy-only` with `conflicts with`: [Proxy-only subnet fails](troubleshooting.md#proxy-only-subnet-fails-with-conflicts-with).

## 11. Upload the images

**Creates:** the Artifact Registry repository and the four images in it (storage billed per GiB and month).

The script copies the bundle's images (`images/`, listed in `images.txt`) and prints the chart's image values with the registry digests. From a source tree, build and push instead ([Bundle and source tree](runbook.md#bundle-and-source-tree)).

```sh
gcloud artifacts repositories create $AR --project $P \
  --location $REGION --repository-format docker
gcloud auth configure-docker $REGION-docker.pkg.dev
scripts/load-images.sh $REG >values-images.yaml
```

skopeo needs `gcloud` on the `PATH`; the account needs `roles/artifactregistry.writer` or more. `values-images.yaml`:

```yaml
image:
  registry: europe-west4-docker.pkg.dev/example-project/abfs
  server:
    digest: sha256:...
  pusher:
    digest: sha256:...
  client:
    digest: sha256:...
  casfsLoader:
    digest: sha256:...
```

Pass it after your values file in every `helm install`, `helm upgrade` and `helm template`.

Check:

```sh
cat values-images.yaml
gcloud artifacts docker images list $REG --include-tags
```

It lists `abfs-server`, `abfs-pusher`, `abfs-client` and `casfs-loader` with their `images.txt` tags; each `DIGEST` equals the one in `values-images.yaml`.

If the repository or the CI build image is in another project, grant `abfs-nodes` `roles/artifactregistry.reader` and `roles/serviceusage.serviceUsageConsumer` there ([Node service account](prerequisites.md#node-service-account)).

If it fails:

| Message | Fix |
| --- | --- |
| `skopeo not found` | install it (step 1) |
| `images.txt not found; pass --bundle <dir>` | run from the bundle root, or pass `--bundle <bundle root>` |
| `unknown image <name> in images.txt` | use `images.txt` and `images/` of one bundle; run `sha256sum -c SHA256SUMS` |
| `<archive> not found` | unpack the bundle again |
| `unauthorized`, `denied` or `403 Forbidden` from `skopeo copy` | run `gcloud auth configure-docker $REGION-docker.pkg.dev` as the script's user; check `gcloud auth list`; grant `roles/artifactregistry.writer` |
| `name unknown` or `Repository ... not found` | create the repository; path `<region>-docker.pkg.dev/<project>/<repository>` |
| `copied N images, not 4` | fix the earlier error, run again |

Later `ImagePullBackOff` with `403 Forbidden`: [troubleshooting](troubleshooting.md#pods-in-imagepullbackoff-with-403-forbidden).

## 12. Check the backend

Run `abfs server gcp init` from the uploaded `abfs-server` image against the backend, in both backend modes. It must change nothing but a no-op schema update. It needs `docker` (credentials from step 11) and Application Default Credentials:

```sh
gcloud auth application-default login
```

Set the image, then follow [Conformance check](runbook.md#conformance-check), with `SPANNER`, `DB`, `BT` and `BUCKET` from `settings.yaml`:

```sh
SERVER_IMG=$REG/abfs-server@<server digest from values-images.yaml>
```

Check: the comparison prints `conformant`.

If it fails: find the cause before the servers start. With `backend-existing`: [What an existing backend must have](runbook.md#what-an-existing-backend-must-have).

## 13. Chart values and the Gerrit Secret

```sh
cp chart/abfs/values-example.yaml values.yaml
```

Set in `values.yaml` ([configuration](configuration.md)):

- `backend.*`: the `settings.yaml` names ([KCC names and chart values](configuration.md#kcc-names-and-chart-values)).
- `gateway.vip`: the address from step 10.
- `computeClasses.nodeServiceAccount`: `abfs-nodes@<project>.iam.gserviceaccount.com`.
- `networkPolicy.proxyOnlySubnetCidr`: the range from step 4.3.
- `pusher.count`: fixed after the first full seed ([The pusher count is fixed](runbook.md#the-pusher-count-is-fixed)); the default 3 suits a full Android tree.
- `pusher.pool`: below.
- `alerts.enabled: true` and `alerts.clusterName: <cluster name>` (optional).

Leave `image.*` to `values-images.yaml`.

**The pusher pool.** The pool of your manifest, with `pusher.phase: seed` ([pusher](configuration.md#pusher)).

- Own Gerrit: set `pusher.pool.manifest.url`, `project` when the URL has a prefix such as `/a/`, and `branchFiles` (default: `main` with `default.xml`). For two branches:

  ```yaml
  pusher:
    pool:
      branchFiles:
        - {branch: main, file: default.xml}
        - {branch: release-1, file: default.xml}
  ```

- Upstream AOSP only: keep the example pool; the [AAOS acceptance test](#16-aaos-acceptance-test) sets a release.

**The smoke pool.** Step 14 passes `docs/examples/values-smoke.yaml` last, so no full seed starts before the tests; your manifest is seeded later ([Seed your manifest](#seed-your-manifest)). The file replaces the manifest, branch files, fetch refspec and overrides with `http://abfs-smoke-manifest.abfs.svc.cluster.local:8080/manifest`, the host of the [quick end-to-end test](#quick-end-to-end-test). Use it unchanged.

**The Gerrit Secret.** The pushers mount `gerrit-credentials` with the key `.netrc`. It must exist even for anonymous reads (then an empty file). Never print it.

```sh
kubectl -n abfs create secret generic gerrit-credentials --from-file=.netrc=<path to netrc>
```

For a cookie file, add `--from-file=.gitcookies=<path>` and set `pusher.gerrit.cookies: true`.

Check:

```sh
kubectl get namespace abfs-ci
kubectl -n abfs get secret gerrit-credentials -o jsonpath='{.data}' | jq 'keys'
helm template abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml >/dev/null && echo rendered
helm template abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml -f docs/examples/values-smoke.yaml |
  grep -m 1 'manifest-server:'
```

Expected: `NotFound` (the chart creates `abfs-ci`, [namespaces](configuration.md#namespaces)), `.netrc`, `rendered`, `manifest-server: abfs-smoke-manifest.abfs.svc.cluster.local:8080`.

If it fails: the error names the value ([troubleshooting](troubleshooting.md#render-fails-on-an-empty-digest) and the sections after it).

## 14. Install the chart

**Creates:** the ComputeClasses and their nodes (three 16-vCPU server nodes at the start, the pusher nodes, a bootstrap Job node), the pusher data disks (1 TiB Hyperdisk Balanced each by default), the Gateway with its internal load balancer, and the CI namespace.

Run the check of [step 8](#8-wait-for-cluster-operations) first. The release namespace must be `abfs`. Do not use `--wait`; it blocks until the timeout. Pass `docs/examples/values-smoke.yaml` last, here and in every `helm upgrade` until you [seed your manifest](#seed-your-manifest).

```sh
helm install abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml \
  -f docs/examples/values-smoke.yaml --timeout 30m
kubectl -n abfs logs job/abfs-bootstrap
```

`Warning: Cannot get compute class "abfs-ci"` (or another class) needs no action.

Read the Job log at once; once its node is removed, `kubectl logs job/abfs-bootstrap` prints `error: timed out waiting for the condition`. Then use Cloud Logging:

```sh
gcloud logging read "resource.type=\"k8s_container\" AND resource.labels.cluster_name=\"$CLUSTER\"
  AND resource.labels.namespace_name=\"abfs\" AND resource.labels.container_name=\"bootstrap\"" \
  --project $P --freshness 1h --order asc --format 'value(textPayload)'
```

Add `AND textPayload:"git-repo"` to the filter for the patched `repo` tool lines.

Check:

```sh
kubectl -n abfs get deploy,hpa,sts,pods
kubectl -n abfs get gateway abfs
gcloud compute firewall-rules list --project $NET_P \
  --filter "name~^gkegw1-.*-l7-$NET-$REGION\$"
CLIENT_IMG=$REG/abfs-client@<client digest from values-images.yaml>
kubectl -n abfs run psc-check --rm -i --restart=Never --pod-running-timeout=10m --image "$CLIENT_IMG" \
  --overrides '{"spec":{"nodeSelector":{"cloud.google.com/compute-class":"abfs-server"},"tolerations":[{"key":"cloud.google.com/compute-class","operator":"Equal","value":"abfs-server","effect":"NoSchedule"}]}}' \
  -- getent hosts spanner.googleapis.com containerfilesystem.googleapis.com $REGION-docker.pkg.dev
```

- The bootstrap log ends with `config:git-pusher/default written`, `config:clients/configs written`, and `git-repo stable-abfs` `is in the store` or `seeded`.
- `abfs-server` replicas ready (3 by default); pushers `1/1` Running. Until the smoke manifest host runs, each pusher logs `listing projects for repo ... failed` once a minute; that is expected.
- The Gateway shows `PROGRAMMED True` and the `gateway.vip` address.
- Plain VPC: the Gateway firewall rule exists ([Firewall](prerequisites.md#firewall)). Shared VPC: the host-project owner creates it.
- Every name resolves to `$PSC_IP` (or the address of zones kept in 4.0). A public address means a 4.2 zone is missing or not bound to the network.

If it fails:

- The hook fails with a `git-repo` line: [Check the patched repo tool](#check-the-patched-repo-tool).
- Pods Pending: [Cluster is running incompatible operation](troubleshooting.md#pods-pending-with-cluster-is-running-incompatible-operation), [max cluster cpu limit reached](troubleshooting.md#pods-pending-with-max-cluster-cpu-limit-reached), [Pusher PVCs Pending](troubleshooting.md#pusher-pvcs-pending-or-pushers-not-ready).
- Server pods restart at start: [troubleshooting](troubleshooting.md#server-pods-restart-during-their-start). Gateway not programmed or 503: [Gateway health checks fail](troubleshooting.md#gateway-health-checks-fail).

## 15. Tests

Level 1 always runs. Offer levels 2 and 3 to the human with this table; run one only when the human says so.

| Level | Proves | Time | Cost | When |
| --- | --- | --- | --- | --- |
| 1. [helm test](#helm-test), [patched repo tool](#check-the-patched-repo-tool) | gRPC health via Service and Gateway, CI tools on a CI node, repo tool in the store | minutes | the CI node `helm test` creates | always |
| 2. [Quick end-to-end test](#quick-end-to-end-test) | egress, pushers, store, mount and file read, with two small projects | about 15 minutes | the CI node for longer, maybe a server node; a few MiB that stay | optional, before the first full seed |
| 3. [AAOS acceptance test](#16-aaos-acceptance-test) | full AAOS seed, cold build, warm rebuild from the store | hours | seed and build nodes, Cloud NAT, up to about 1 TiB that stays, billed monthly | optional, only after the human approves |

**Creates (level 1):** a CI node of at least 32 vCPU and 120 GB with a 1,100 GiB boot disk, removed about 30 minutes after its last pod ends.

### helm test

```sh
helm test abfs -n abfs
```

Check: `abfs-test-server-health` and `abfs-test-ci-tools` end `Succeeded`. Do not pass `--logs`; it fails on the deleted passed pods. A failed test pod keeps its log:

```sh
kubectl -n abfs logs abfs-test-server-health
kubectl -n abfs-ci logs abfs-test-ci-tools
kubectl -n abfs-ci get pods -o wide
```

If it fails: [Build pods stuck in wait-for-casfs](troubleshooting.md#build-pods-stuck-in-wait-for-casfs), [Gateway health checks fail](troubleshooting.md#gateway-health-checks-fail).

### Check the patched repo tool

Every `abfs mount -b` needs the ref `refs/heads/stable-abfs` of `git-repo` on server `gerrit.googlesource.com` in the store. Without it a mount fails at once:

```
failed POST /get-refs: 500 500 Internal Server Error the server "gerrit.googlesource.com" not found
repo init failed: exit status 1
```

With `bootstrap.repoSetup.enabled` (the default) the bootstrap Job seeds a missing ref with `abfs repo-setup`, which needs egress to `gerrit.googlesource.com` (Cloud NAT). A failure fails `helm install` or `helm upgrade`; the Job log says why (`cannot read the git-repo ref`, `git-repo seed failed`, `not in the store after the seed`). Fix it and run `helm upgrade` again, or set `bootstrap.repoSetup.enabled=false`, upgrade, and seed by hand as below.

Check from a cacheman pod:

```sh
kubectl -n abfs-ci get pods -l app.kubernetes.io/component=cacheman
echo refs/heads/stable-abfs | kubectl -n abfs-ci exec -i <cacheman pod> -- \
  abfs -i ci test-client get-ref -s gerrit.googlesource.com -p git-repo
```

It prints a commit hash and `refs/heads/stable-abfs`. `(not exists) refs/heads/stable-abfs` or `the server "gerrit.googlesource.com" not found` means the ref is missing. Any other error means the check did not reach the store; fix that first. Seed by hand when `repoSetup` is off or the Job has no egress, with the directory and commit that `repo-setup` prints:

```sh
kubectl -n abfs-ci exec -it <cacheman pod> -- bash
abfs -i ci repo-setup
abfs -i ci --no-local-cache push -r <directory printed by repo-setup>
echo "<commit printed by repo-setup> refs/heads/stable-abfs" |
  abfs -i ci test-client put-ref -s gerrit.googlesource.com -p git-repo
```

The Job never replaces an existing ref; update the tool later with the same commands.

Level 1 is done when both checks pass. Tell the human the cost so far, and offer levels 2 and 3.

### Quick end-to-end test

Optional, about 15 minutes. The pushers push two small projects from `android.googlesource.com`, `abfs ready` resolves them, and a build pod mounts them and reads a file. Run it before the first full seed, or skip to [Seed your manifest](#seed-your-manifest).

**Creates:** the manifest host pod in `abfs` on an `abfs-server` node; GKE may add a server node (16 vCPU) while it runs. A few MiB that stay in the store ([What stays in the store](#what-stays-in-the-store)).

`docs/examples/smoke-manifest-host.yaml` holds a ConfigMap with `default.xml` (`platform/tools/repohooks` and `platform/external/jsmn` at `android-15.0.0_r36`), a Pod that serves it as a git repository with branch `main` over HTTP on port 8080, and the Service `abfs-smoke-manifest`.

The Pod needs an image with `git` and `python3` that `abfs-nodes` can pull, such as your CI build image (`abfs-client` has no `python3`):

```sh
BUILD_IMAGE=<build image>
sed "s|<build image>|$BUILD_IMAGE|" docs/examples/smoke-manifest-host.yaml >smoke-manifest-host.yaml
grep 'image:' smoke-manifest-host.yaml
```

#### Start the manifest host

```sh
kubectl apply -f smoke-manifest-host.yaml
kubectl -n abfs wait --for=condition=Ready pod/abfs-smoke-manifest --timeout 15m
kubectl -n abfs logs abfs-smoke-manifest
```

Check: the Pod is Ready; the log shows `<hash> Smoke test manifest` and `"GET /manifest/info/refs HTTP/1.1" 200` lines.

If it fails: `the image has no git` or `no python3`: use another image. `ImagePullBackOff`: `abfs-nodes` cannot read the image (step 11). Pending: `kubectl -n abfs describe pod abfs-smoke-manifest` shows the events.

#### Seed and readiness

The pushers push the two projects within a few minutes:

```sh
kubectl -n abfs get pods -l app.kubernetes.io/component=pusher -o wide
kubectl -n abfs logs abfs-pusher-0 --tail 50
kubectl -n abfs port-forward pod/abfs-pusher-0 9998:9998 >/dev/null &
sleep 2
curl -s localhost:9998/info | jq '{projects: [.Projects[] | {Name, State, LastError}], repos: [.Repos[] | {LastError}]}'
kill %1
```

Each pusher lists only its share; one may list none. A non-empty `LastError` names a failed fetch or push.

Then, from the cacheman pod:

```sh
kubectl -n abfs-ci exec <cacheman pod> -- abfs -i ci ready -b main
```

Check: `Ready: 2 of 2 projects resolve`, exit 0. `Not ready` lists each missing project with a cause.

If it fails: `manifest branch not found`: [troubleshooting](troubleshooting.md#manifest-branch-not-found-or-manifest-not-in-the-git-pusher-config). Pusher not ready: [Pusher PVCs Pending or pushers not ready](troubleshooting.md#pusher-pvcs-pending-or-pushers-not-ready). Manifest fetch error: the host is not ready or `values-smoke.yaml` was not passed. Project fetch error: no egress to `android.googlesource.com` (step 4.1).

#### Mount and read a file in a build pod

Run the chart's example Job ([build pod contract](ci-job.md#build-pod-contract)); the build image needs only `bash` here:

```sh
helm template abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml -f docs/examples/values-smoke.yaml \
  --set ci.exampleJob.enabled=true --set ci.exampleJob.image=<build image> \
  --set ci.exampleJob.branch=main \
  --set-string ci.exampleJob.command='ls && head -n 5 tools/repohooks/README.md' \
  --show-only templates/ci-example-job.yaml | kubectl create -f -
kubectl -n abfs-ci wait --for=condition=complete job/abfs-example-build --timeout 30m
kubectl -n abfs-ci logs job/abfs-example-build --all-containers
kubectl -n abfs-ci delete job abfs-example-build
```

Check: the Job completes; the log shows `Ready: 2 of 2 projects resolve`, `external` and `tools` in the `ls` output, and `# AOSP Preupload Hooks`.

If it fails: the init containers wait up to 15 minutes each ([Build pods stuck in wait-for-casfs](troubleshooting.md#build-pods-stuck-in-wait-for-casfs), ["could not detect a running cacheman"](troubleshooting.md#could-not-detect-a-running-cacheman)). Mount fails on `gerrit.googlesource.com`: [Check the patched repo tool](#check-the-patched-repo-tool). Build image pull error: step 11.

#### Remove the manifest host

```sh
kubectl delete -f smoke-manifest-host.yaml
rm smoke-manifest-host.yaml
```

Check: `kubectl -n abfs get pod,svc,configmap abfs-smoke-manifest` prints `NotFound` for all three.

The pushers log `listing projects for repo ... failed` until a `helm upgrade` without `values-smoke.yaml` ([AAOS acceptance test](#16-aaos-acceptance-test) or [Seed your manifest](#seed-your-manifest)). Never pass `values-smoke.yaml` for a real seed.

#### What stays in the store

About 5 MiB of git packs, the one-commit manifest repository, and the patched repo tool stay; garbage collection is off ([Garbage collection](runbook.md#garbage-collection)).

The quick test is done when the Job has read the file. Tell the human the cost so far.

## 16. AAOS acceptance test

Optional, level 3 of the [tests](#15-tests). It seeds an AAOS release, builds a Cuttlefish automotive target cold, rebuilds it warm in a new pod, and reads the cache hit rate.

### Time, cost and approval

| Item | Plan with |
| --- | --- |
| Time | seed about 2 hours (3 pushers); cold build 80 to 90 minutes on 64 vCPU, longer on 32; warm rebuild minutes |
| Hourly | one seed node per pusher (`abfs-pusher-seed`, 32 vCPU and 240 GB or more; `pusher.seedResources` 200Gi), the build node, Bigtable nodes up to the cap, Cloud NAT per GiB (about 400 GB of clones) |
| Store growth | up to about 1 TiB that stays (about 521 GiB Bigtable, 525 GiB Cloud Storage); garbage collection is off |
| Monthly cost of the store | about USD 105 at the lowest list prices on 2026-11-16; check [Bigtable pricing](https://cloud.google.com/bigtable/pricing) and [Cloud Storage pricing](https://cloud.google.com/storage/pricing) for your region |

Get the human's approval of the time, store growth and monthly cost, with a cost cap and a stop time, before you start; report the spend at each sub-step.

### Source of the release

- **Your Gerrit mirrors AOSP:** point `manifest.url` of `values-aaos.yaml` at the mirror's `platform/manifest` (`project: platform/manifest` when the URL has a prefix such as `/a/`). Keep the release branch, fetch refspec and override. The mirror's `platform/manifest` needs the release branch (`android-15.0.0_r36`). The pushers fetch with `gerrit-credentials`.
- **No mirror:** seed from `https://android.googlesource.com/platform/manifest`, as below.

The test replaces the pool of `values.yaml` until a `helm upgrade` without `values-aaos.yaml`.

### Release and target

| Release | Manifest branch (`ci.exampleJob.branch`) | Lunch target (`ci.exampleJob.target`) |
| --- | --- | --- |
| AAOS 15 | `android-15.0.0_r36` | `aosp_cf_x86_64_auto-bp1a-userdebug` |
| AAOS 17 | `android-17.0.0_r1` | `aosp_cf_x86_64_auto-cp2a-userdebug` |

The human may choose another release or target. The build goal is `droid`.

### Seed the release

Write `values-aaos.yaml`. The narrowed refspec also narrows the manifest project, so the override gives it the release branch ([pusher](configuration.md#pusher)). With a mirror, set `url` and `project` as in [Source of the release](#source-of-the-release):

```yaml
pusher:
  phase: seed
  pool:
    manifest:
      url: https://android.googlesource.com/platform/manifest
      project: ""
    branchFiles:
      - {branch: android-15.0.0_r36, file: default.xml}
    fetchRefSpec:
      - +refs/tags/android-15.0.0_r36:refs/tags/android-15.0.0_r36
    overrides:
      - name: platform/manifest
        fetch-ref-spec: ["+refs/heads/android-15.0.0_r36:refs/heads/android-15.0.0_r36"]
```

Run the check of [step 8](#8-wait-for-cluster-operations), then upgrade with the file last, without `values-smoke.yaml`. Pass `values-aaos.yaml` in every `helm upgrade` while the test runs:

```sh
helm upgrade abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml -f values-aaos.yaml --timeout 30m
kubectl -n abfs logs job/abfs-bootstrap
```

A CI node left from step 15 still holds the smoke manifest's server. Before the build, make sure none runs, or delete its cacheman pod while no build runs on it:

```sh
kubectl get nodes -l cloud.google.com/compute-class=abfs-ci
kubectl -n abfs-ci delete pod -l app.kubernetes.io/component=cacheman --field-selector spec.nodeName=<node>
kubectl -n abfs-ci exec <new cacheman pod> -- abfs -i ci config | grep '^manifest'
```

The last command prints `manifest-server: android.googlesource.com` (or your mirror) and `manifest-project-name: platform/manifest`.

Check: the bootstrap log shows `config:git-pusher/default written`. Watch the seed as in [Seed and readiness](#seed-and-readiness); it is done when every pusher shows an empty `LastError` and none is fetching. From a cacheman pod, `abfs -i ci ready -b android-15.0.0_r36` prints `Ready: N of N projects resolve`; without a CI node, the build Job runs this check first and waits up to 2 hours.

If it fails: `Last State: Terminated, Reason: OOMKilled` on a pusher: [A pusher OOM kill](runbook.md#a-pusher-oom-kill). `ready` names the cause of each missing project.

### Cold build

The build image needs `bash` and the Android build host tools ([Example Job](ci-job.md#example-job)). Start the Job, wait for its `build` container, and follow its log:

```sh
helm template abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml -f values-aaos.yaml \
  --set ci.exampleJob.enabled=true --set ci.exampleJob.image=<build image> \
  --set ci.exampleJob.branch=android-15.0.0_r36 \
  --set ci.exampleJob.target=aosp_cf_x86_64_auto-bp1a-userdebug \
  --show-only templates/ci-example-job.yaml | kubectl create -f -
end=$(( $(date +%s) + 2700 ))
until kubectl -n abfs-ci get pods -l job-name=abfs-example-build \
    -o jsonpath='{.items[*].status.containerStatuses[?(@.name=="build")].state}' |
    grep -q -e running -e terminated; do
  [ "$(date +%s)" -ge "$end" ] && { echo "the build container has not started after 45 minutes" >&2; break; }
  sleep 30
done
kubectl -n abfs-ci logs -f job/abfs-example-build -c build
```

If the log stream drops, the build goes on; the next block waits for the end. A failed pod is retried twice (`backoffLimit: 2`); the Job deadline is 12 hours with retries (`ci.exampleJob.deadlineSeconds`). When a build fails and the human does not want the retry, run `kubectl -n abfs-ci logs job/abfs-example-build -c build >cold-build.log`, then `kubectl -n abfs-ci delete job abfs-example-build`.

Wait for the end of the Job, keep its log, then delete it (the next run uses the same name):

```sh
ended() { kubectl -n abfs-ci get job abfs-example-build \
    -o jsonpath='{.status.conditions[?(@.status=="True")].type}' | grep -q -e Complete -e Failed; }
end=$(( $(date +%s) + 46800 ))
until ended; do
  [ "$(date +%s)" -ge "$end" ] && { echo "the Job has not ended after 13 hours; it is kept" >&2; break; }
  sleep 60
done
kubectl -n abfs-ci get job abfs-example-build
kubectl -n abfs-ci logs job/abfs-example-build -c build >cold-build.log
ended && kubectl -n abfs-ci delete job abfs-example-build
```

Check: `COMPLETIONS 1/1`; the log ends with `build completed successfully`; the `cache: hits:` ratio is close to 0.

If it fails: `OOMKilled` means `ci.exampleJob.resources` is too small ([Sizing](ci-job.md#sizing)). Mount errors: as in [Mount and read a file in a build pod](#mount-and-read-a-file-in-a-build-pod). Pending on ephemeral storage: [troubleshooting](troubleshooting.md#build-pods-pending-on-ephemeral-storage).

### Warm rebuild

Start the same Job again: a new pod at the same mount path `/work/src` ([Build cache hits](ci-job.md#build-cache-hits)):

```sh
helm template abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml -f values-aaos.yaml \
  --set ci.exampleJob.enabled=true --set ci.exampleJob.image=<build image> \
  --set ci.exampleJob.branch=android-15.0.0_r36 \
  --set ci.exampleJob.target=aosp_cf_x86_64_auto-bp1a-userdebug \
  --show-only templates/ci-example-job.yaml | kubectl create -f -
end=$(( $(date +%s) + 2700 ))
until kubectl -n abfs-ci get pods -l job-name=abfs-example-build \
    -o jsonpath='{.items[*].status.containerStatuses[?(@.name=="build")].state}' |
    grep -q -e running -e terminated; do
  [ "$(date +%s)" -ge "$end" ] && { echo "the build container has not started after 45 minutes" >&2; break; }
  sleep 30
done
kubectl -n abfs-ci logs -f job/abfs-example-build -c build
ended() { kubectl -n abfs-ci get job abfs-example-build \
    -o jsonpath='{.status.conditions[?(@.status=="True")].type}' | grep -q -e Complete -e Failed; }
end=$(( $(date +%s) + 46800 ))
until ended; do
  [ "$(date +%s)" -ge "$end" ] && { echo "the Job has not ended after 13 hours; it is kept" >&2; break; }
  sleep 60
done
kubectl -n abfs-ci get job abfs-example-build
kubectl -n abfs-ci logs job/abfs-example-build -c build >warm-build.log
ended && kubectl -n abfs-ci delete job abfs-example-build
```

Check: `COMPLETIONS 1/1`.

### Read the cache hit rate

```sh
grep -h 'cache: hits:' cold-build.log warm-build.log
```

Each line reads `Evaluation took <time>, fail: <n>, cache: hits: <n>, misses: <n>, ratio: <percent>, check errors: <n>, phony_output runs: <n>`. phony_output steps run on every build, so they count in `phony_output runs` and not in `misses`.

Check: the warm rebuild takes minutes, and each of its ratios is above 99. A few misses (build number, build host) are normal. A low ratio points to another mount path, another lunch target, or a non-deterministic step ([usage](usage.md#steady-state)).

### After the test

Report the seed time, build times, hit lines and spend. Then, as the human decides:

- Keep the release and move to the steady phase ([Seed and steady phase](runbook.md#seed-and-steady-phase)): `values-aaos.yaml` comes last, so set `phase: steady` there and upgrade:

  ```sh
  helm upgrade abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml -f values-aaos.yaml --timeout 30m
  ```

  To keep it for good, move its pool and `phase: steady` into `values.yaml` and drop `values-aaos.yaml`.
- Or return to the pool of `values.yaml`: [Seed your manifest](#seed-your-manifest). The seeded data stays and keeps costing storage.

## After the install

### Seed your manifest

A `helm upgrade` without `docs/examples/values-smoke.yaml` seeds the pool of `values.yaml`. Never seed a full tree with `values-smoke.yaml` or in `phase: steady`.

**Creates:** one seed node per pusher (`abfs-pusher-seed`, 32 vCPU and 240 GB or more) during the seed, Cloud NAT traffic for hosts outside the VPC, and store growth that stays. Estimate the cost as in [Time, cost and approval](#time-cost-and-approval); get the human's approval first.

Check the values:

```sh
helm template abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml |
  grep -E 'manifest-server:|compute-class: abfs-pusher' | sort -u
```

It prints your manifest host and `cloud.google.com/compute-class: abfs-pusher-seed`, not `abfs-smoke-manifest` or `abfs-pusher-steady`.

Run the check of [step 8](#8-wait-for-cluster-operations), then upgrade:

```sh
helm upgrade abfs chart/abfs -n abfs -f values.yaml -f values-images.yaml --timeout 30m
kubectl -n abfs logs job/abfs-bootstrap
```

A CI node left from the tests still holds the smoke manifest's server: recycle its cacheman as in [Seed the release](#seed-the-release).

Check: the bootstrap log shows `config:git-pusher/default written`. Watch the seed as in [Seed and readiness](#seed-and-readiness). Then `abfs -i ci ready -b <branch>` prints `Ready: N of N projects resolve` for each manifest branch.

### Check CI access to the control plane

Optional. Run it after the CI service account has `roles/container.clusterViewer` on `$P` and the Role and RoleBinding of [Airflow as the CI system](ci-job.md#airflow-as-the-ci-system). The admin needs `roles/iam.serviceAccountTokenCreator` on the account; on a host the account is attached to, use `gcloud auth print-access-token` without `--impersonate-service-account`. Never create a key. To leave the CI system undisturbed, use a test account with the same role and RoleBinding.

**Creates:** the Token Creator grant to the admin, if missing.

```sh
SA=<CI service account email>
DNS=$(gcloud container clusters describe $CLUSTER --project $P --region $REGION \
  --format 'value(controlPlaneEndpointsConfig.dnsEndpointConfig.endpoint)')
TOKEN=$(gcloud auth print-access-token --impersonate-service-account $SA)
ci() { KUBECONFIG=/dev/null kubectl --server "https://$DNS" --token "$TOKEN" "$@"; }
ci auth whoami
ci -n abfs-ci get pods
```

Expected: `auth whoami` shows `$SA`; `get pods` answers (`No resources found` when no build runs). The token lasts about an hour; fetch it again for a later run.

If it fails: an HTML `403. That's an error.` means the account lacks `roles/container.clusterViewer` on `$P`, or a new grant has not taken effect (wait a few minutes). A Kubernetes `Forbidden` comes from RBAC: check the RoleBinding's subject; a numeric username in `auth whoami` means the token lacks the `userinfo.email` scope. Connection error: [kubectl cannot reach the control plane](troubleshooting.md#kubectl-cannot-reach-the-control-plane).

### Then

- After the first full seed, move the pushers to `pusher.phase: steady` in `values.yaml` ([Seed and steady phase](runbook.md#seed-and-steady-phase)).
- Add branches to `pusher.pool.branchFiles` and run `helm upgrade`; for a large branch switch to `phase: seed` first ([Changes in steady state](usage.md#changes-in-steady-state)).
- Connect your CI system: [build pod contract](ci-job.md#build-pod-contract), [Airflow](ci-job.md#airflow-as-the-ci-system) or [another CI system](prerequisites.md#ci-access-to-the-control-plane), then [check its access](#check-ci-access-to-the-control-plane).
- Read [What to watch](usage.md#what-to-watch). After each control plane upgrade, run the [canary](runbook.md#canary-after-every-control-plane-upgrade).

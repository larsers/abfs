# ABFS on GKE / Troubleshooting

## Build pods stuck in wait-for-casfs

The init container `wait-for-casfs` fails after `ci.waitForCasfsTimeout` (15 minutes):

```
casfs is not loaded on this node after 15m; see the abfs-casfs-loader logs on the node
```

Read the log of the node's casfs loader (CrashLoopBackOff):

```sh
kubectl -n abfs-ci get pods -l app.kubernetes.io/component=casfs-loader -o wide
kubectl -n abfs-ci logs <loader pod> --previous
kubectl get node <node> -o jsonpath='{.status.nodeInfo.kernelVersion}{"\n"}'
```

By the last log line:

- `build failed on kernel <version>` (`provider: daemonset`): casfs at `c946f8e` builds on mainline 6.8 to 6.13 and 6.15 to 6.18, not on 6.14 or 6.19. Pin the CI pools to a GKE version whose Ubuntu kernel builds; run the runbook's canary after each control plane upgrade.
- `apt-get` errors (`provider: daemonset`): no access to the Ubuntu archive, or no `linux-headers-<version>` package. Give the CI nodes egress (Cloud NAT) or a mirror ([prerequisites](prerequisites.md#egress-for-the-ci-nodes)). `apt-get update failed 30 times` or a lock error: a node job held the apt or dpkg lock over 5 minutes.
- `modprobe casfs failed` (`provider: daemonset`): often secure boot. A hand-made pool needs `--no-shielded-secure-boot` ([Secure boot on the CI nodes](prerequisites.md#secure-boot-on-the-ci-nodes)). Check with `gcloud compute instances describe <node> --zone <zone> --format 'value(shieldedInstanceConfig.enableSecureBoot)'`.
- `provider daemonset needs Ubuntu nodes`: a hand-made pool needs `--image-type UBUNTU_CONTAINERD`.
- `module implements interface ...`, `module requires interface ...` or `module version ..., expected ...`: another casfs version is loaded. A loaded module cannot be removed; replace the node.
- `the node image ships no casfs module` or `module version ... has no interface version` (`provider: image`): CI nodes need a GKE version whose COS image ships casfs 0.1.24 or later (COS 133 build 44-193 or later). Replace the node.

Delete a failing node to stop new pods landing there.

## "could not detect a running cacheman"

Mount, remount and unmount first ping cacheman on `<cache dir>/http.socket` (`/var/lib/abfs/base/ci/cache/http.socket` by default), and fail with `ERROR: could not detect a running cacheman: ` and a reason:

- `<socket> does not exist, so cacheman is not running`: no cacheman started, or the build pod lacks the host path `/var/lib/abfs` at `/var/lib/abfs` with `ABFS_BASE_DIR=/var/lib/abfs/base`.
- `nothing listens on <socket>, so cacheman stopped or crashed`: check cacheman's restarts and its log with `--previous`.
- `permission denied on <socket>, so cacheman may run as another user`: set the build pod's `runAsUser` and `runAsGroup` explicitly to `ci.uid` and `ci.gid` (1000:1000 by default).
- `cacheman did not answer on <socket> within 2m0s` or `the listen queue of <socket> stayed full for 2m0s`: cacheman is starting or hangs; check its log, CPU and memory.

Normal: `Waiting for cacheman to answer on <socket>...` (retries up to 2 minutes), and one `could not connect to cacheman unix domain socket: ...` line before a passing mount. Checks:

```sh
kubectl -n abfs-ci get pods -l app.kubernetes.io/component=cacheman -o wide
kubectl -n abfs-ci logs <cacheman pod>
kubectl -n abfs-ci exec <cacheman pod> -- abfs -i ci cacheman status
```

## "could not determine current user"

`panic: could not determine current user: ... USER/LOGNAME/USERNAME environment variables not set`: the uid has no passwd entry. Set `USER` to the numeric uid (`USER=1000`) in the pod, or add a passwd entry. In the bootstrap Job this is a health wait that never ends.

## "manifest branch not found" or "manifest not in the git-pusher config"

`ready` or `mount -b` shows a manifest URL without a host (`https:/platform/manifest`) or with a host the pool does not use. The client's `manifest-server` and `manifest-project-name` must equal the server and project of `pusher.pool.manifest` ([configuration](configuration.md#pusher)). Check:

```sh
kubectl -n abfs-ci exec <cacheman pod> -- abfs -i ci config | grep '^manifest'
```

An override of `manifest-server` in `bootstrap.clientConfig` must equal the pool URL's host, port included. A new value reaches a node when its cacheman pod restarts. `not pushed yet: ... (no completed push)`: wait for the pushers to push the manifest.

## Mount fails with the server "gerrit.googlesource.com" not found

`ready` passes, and `mount -b` fails at once:

```
failed POST /get-refs: 500 500 Internal Server Error the server "gerrit.googlesource.com" not found
repo init failed: exit status 1
```

The store has no patched `repo` tool, which the bootstrap Job seeds with `bootstrap.repoSetup.enabled`. Read the `git-repo` lines of `kubectl -n abfs logs job/abfs-bootstrap`, or later the Cloud Logging query of [install step 14](install.md#14-install-the-chart). A failed clone needs egress to `gerrit.googlesource.com`. Check and seed by hand as in the [install](install.md#check-the-patched-repo-tool).

## KCC objects created before the webhook

Overlay objects stay not Ready with `cannot resolve project`, `no value found for annotation cnrm.cloud.google.com/project-id`, `no annotation found that matches one of the required containers` or `DependencyNotReady`. The overlay was applied before the Config Connector webhook existed, and no Google Cloud resource was created. Applying again does not repair it; delete the objects and apply again:

```sh
kubectl -n abfs delete gcp --all
kubectl apply -k kcc/overlays/<name>
```

Wait for the webhook before the first apply ([prerequisites](prerequisites.md#install-the-operator)).

## Pods in ImagePullBackOff with 403 Forbidden

Events show `failed to fetch oauth token: ... 403 Forbidden` from `<region>-docker.pkg.dev`. The node's account lacks `roles/artifactregistry.reader` on the repository, usually on the system pool (`bootstrap.computeClass` or `tests.computeClass` empty). Find the node with `kubectl -n <namespace> get pod <pod> -o wide` and grant its account the role. Class nodes use `computeClasses.nodeServiceAccount` ([prerequisites](prerequisites.md#node-service-account)).

## kubectl cannot reach the control plane

`Unable to connect to the server: dial tcp <address>:443: i/o timeout`: the kubeconfig uses an IP-based endpoint; the cluster has only the DNS-based one ([prerequisites](prerequisites.md#control-plane-access)). Fetch the entry again:

```sh
gcloud container clusters get-credentials abfs --project example-project --region europe-west4 \
  --dns-endpoint
```

- `Error from server (Forbidden):` with an HTML page `403. That's an error.`: the caller lacks `container.clusters.connect` (in `roles/container.clusterViewer`, `roles/container.viewer`, `roles/container.developer` and `roles/container.admin`). A new grant takes 2 to 7 minutes or more.
- `pods is forbidden: User ... cannot list resource`: RBAC.
- Airflow: `GKEStartPodOperator` needs `use_dns_endpoint=True` ([Airflow as the CI system](ci-job.md#airflow-as-the-ci-system)).
- An admin host without an external address needs Private Google Access on its subnet (the node subnet has it, install step 4.1), Cloud NAT, or the `gke.goog` zone pointing at the Private Service Connect endpoint.

## Image pulls or Google API calls fail on private nodes

`ErrImagePull` with a timeout towards `<region>-docker.pkg.dev`, or server timeouts towards Spanner, Bigtable or Cloud Storage. Check [Private nodes and egress](prerequisites.md#private-nodes-and-egress):

- The API names resolve to the endpoint address (the `getent hosts` check of [Private Service Connect endpoint for Google APIs](prerequisites.md#private-service-connect-endpoint-for-google-apis)). A public address means a missing private zone, or one not visible to the network (`gcloud dns managed-zones list --filter visibility=private`).
- The forwarding rule exists with `target: all-apis`, and no higher-priority egress firewall rule blocks the endpoint address.
- Private Google Access is on for the node subnet (`privateIpGoogleAccess: True`).

Hosts under `*.rep.googleapis.com` do not work through the `all-apis` endpoint.

## Gateway waits for its controller

`kubectl -n abfs get gateway abfs` stays `Accepted: Unknown` with `Waiting for controller`, and `kubectl get gatewayclass` lists no `gke-l7-*` class. On a new cluster, wait for them ([prerequisites](prerequisites.md#cluster)). Missing after 10 minutes: an `--addons` list at cluster creation turned off HTTP load balancing, horizontal pod autoscaling (the server HPA) and the Compute Engine Persistent Disk CSI driver (the pusher PVCs). Turn all three on, wait for the operation to finish, then check:

```sh
gcloud container clusters update abfs --project example-project --region europe-west4 \
  --update-addons HttpLoadBalancing=ENABLED,HorizontalPodAutoscaling=ENABLED,GcePersistentDiskCsiDriver=ENABLED
kubectl get gatewayclass gke-l7-rilb
```

## Pods Pending with "Cluster is running incompatible operation"

Autoscaler events show `NodeAutoprovisioning: attempt to create node group failed ... Cluster is running incompatible operation`. A cluster update runs, which blocks node pool creation. Wait for it ([prerequisites](prerequisites.md#wait-for-cluster-operations)), then check the node shapes: after failed attempts the autoscaler can create oversized nodes.

## Pods Pending with "max cluster cpu limit reached"

The autoscaler visibility log shows `max cluster cpu limit reached`, `max cluster memory limit reached` or `no.scale.up.nap.pod.zonal.resources.exceeded`. Cluster-level node auto-provisioning limits count the class nodes too. Check, then turn it off or raise the limits ([prerequisites](prerequisites.md#cluster-level-node-auto-provisioning)):

```sh
gcloud container clusters describe abfs --project example-project --region europe-west4 \
  --format 'yaml(autoscaling.enableNodeAutoprovisioning, autoscaling.resourceLimits)'
```

## Pusher PVCs Pending or pushers not ready

`kubectl -n abfs get pvc` shows pusher claims `Pending` with events naming `pd.csi.storage.gke.io`: the Compute Engine Persistent Disk CSI driver is off. Turn it on as in [Gateway waits for its controller](#gateway-waits-for-its-controller).

A pusher at `0/1` ready has not applied its pool config: the `/info` probe fails with `connection refused` or 503 (`config is not loaded yet`). The log names the cause, usually an unreachable store or no config entry for the pod's name. `helm upgrade` does not roll the next pusher until this one is ready. Under heavy load a pusher can miss the 10 s probe timeout and show not ready; it is not restarted.

```sh
kubectl -n abfs get pods -l app.kubernetes.io/component=pusher
kubectl -n abfs logs abfs-pusher-<n> --tail 50
```

## Server pods restart during their start

Events show `Startup probe failed: dial tcp ...:50051: connect: connection refused`. The server listens only after it reaches Spanner, Bigtable and the bucket (with `server.gc.enabled`, after its first heartbeat), and the startup probe allows 5 minutes. The log names the backend it waits on. When starts are slow but succeed, raise `server.probes.startup.failureThreshold`.

## "cannot find ...-no-cgo on PATH"

`cannot find <hook>-no-cgo on PATH (<PATH>): install the client binaries ...`: with `sync.auto-setup: true`, every mount copies seven hooks from `/var/lib/abfs/bin` into the Soong tree: `abfs-wrap-no-cgo`, `abfsbox-no-cgo`, `ninjago-no-cgo`, `abfs-cmd-map-no-cgo`, `abfs-bazel-daemon-no-cgo`, `rbetap-no-cgo` and `tap-no-cgo`.

- Put `/var/lib/abfs/bin` first on the build pod's `PATH`.
- If `ls /var/lib/abfs/bin` in the build pod lacks them, the node's cacheman `install-tools` init container failed: `kubectl -n abfs-ci logs <cacheman pod> -c install-tools`.

## Render fails on an empty digest

`image.server.digest is empty; set the digest, or image.allowTag=true for development`: set `image.server.digest`, `image.pusher.digest`, `image.client.digest` and `image.casfsLoader.digest` to the `sha256:...` values in `images.txt` of `scripts/build-images.sh --push`. Without `--push` the script prints local image IDs, not registry digests. `image.allowTag=true` is for development only.

## Render fails on the cache bounds

`server.memoryLimit ... is below the cache bounds plus 6 GiB`: lower `server.caches.contents` or `server.caches.headers`, or raise `server.memoryLimit`. The chart counts 180 B per header and 49 KB per content entry, plus 6 GiB (7 GiB with `server.gc.enabled`). The defaults (100,000,000 headers, 400,000 contents, 48Gi) fit.

## Render fails on the CI node disk

- `ci.cacheman.cacheDisk ... plus ci.buildDisk ... exceed the allocatable ephemeral storage of a CI node`: raise `computeClasses.ci.bootDiskGb`, or lower `ci.cacheman.cacheDisk` or `ci.buildDisk` ([node disk](ci-job.md#node-disk)).
- `ci.cacheman.evictThreshold was removed`: remove it from the values; the threshold is now 90 % of `ci.cacheman.cacheDisk`. With `helm upgrade --reuse-values` it comes from the release ([runbook](runbook.md#upgrades)).
- `ci.cacheman.cacheDisk ... is below 10Gi`: raise it to 10Gi or more.
- `...requests.ephemeral-storage is set`: remove the `ephemeral-storage` request from `ci.cacheman.resources` or `ci.exampleJob.resources`; set the size in `ci.cacheman.cacheDisk` or `ci.buildDisk`.

## Render fails on alerts.clusterName

`alerts.clusterName is required with alerts.enabled`: set it to the GKE cluster's name, `abfs` in the examples.

## Render fails on the server VPA mode

`server.vpa.mode Recreate next to the CPU HPA ...`: use `server.vpa.mode: Off`, or `Recreate` with `server.vpa.hpaRightsizing: true` (Preview).

## Build pods Pending on ephemeral storage

`kubectl -n abfs-ci describe pod <pod>` shows `Insufficient ephemeral-storage`, or no node group fits: the pod's `ephemeral-storage` request plus cacheman's exceed a CI node's allocatable storage. Check with `kubectl get node <node> -o jsonpath='{.status.allocatable.ephemeral-storage}'`. Causes:

- The CI system requests more than `ci.buildDisk`. Set `ci.buildDisk` to that request.
- More build pods per node than the disk holds ([node disk](ci-job.md#node-disk)).
- A hand-made pool in `computeClasses.ci.nodepools` has a smaller boot disk than `computeClasses.ci.bootDiskGb`; cacheman itself can stay Pending.

cacheman and the casfs loader (PriorityClass `<release>-ci-node`) preempt (event `Preempted`) only build pods of lower priority ([build pod contract](ci-job.md#build-pod-contract)).

## Gateway health checks fail

Requests fail with HTTP 503, or the backends are unhealthy:

```sh
kubectl -n abfs describe gateway abfs
kubectl -n abfs get healthcheckpolicy,gcpbackendpolicy,httproute
kubectl -n abfs get pods -l app.kubernetes.io/component=server
gcloud compute backend-services list --project example-project --regions europe-west4 \
  --filter 'name~abfs'
gcloud compute backend-services get-health <backend service> --project example-project \
  --region europe-west4
```

- No firewall rule for the health checks and proxies. In a plain VPC the Gateway controller creates `gkegw1-<id>-l7-<network>-<region>`; check it with the filter in the [prerequisites](prerequisites.md#firewall). In a Shared VPC the host-project owner must create it.
- `networkPolicy.proxyOnlySubnetCidr` is not the range of the proxy-only subnet: `gcloud compute networks subnets list --filter purpose=REGIONAL_MANAGED_PROXY`.
- Every server reports `NOT_SERVING`: Spanner, Bigtable or the bucket fails. The server log names it: a missing role, a `backend.*` name that differs from the KCC name, or a missing table or family.
- Pods not Ready: `kubectl -n abfs describe pod <server pod>`.

## StatefulSet update forbidden on a volumeClaimTemplate change

`helm upgrade` fails with `StatefulSet.apps "abfs-pusher" is invalid: spec: Forbidden: updates to statefulset spec for fields other than ...`: `pusher.dataDisk.size` and `pusher.dataDisk.storageClass` change the immutable `volumeClaimTemplates`. To grow the disks:

```sh
for i in 0 1 2; do
  kubectl -n abfs patch pvc data-abfs-pusher-$i -p '{"spec":{"resources":{"requests":{"storage":"2Ti"}}}}'
done
kubectl -n abfs delete sts abfs-pusher --cascade=orphan
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --timeout 30m
```

`--cascade=orphan` keeps the pods and PVCs for the new StatefulSet. A new storage class applies only to new PVCs: wipe the old ones as for a backend replacement, then seed again.

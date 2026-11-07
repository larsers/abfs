# ABFS on GKE / Troubleshooting

Each section names the symptom, the cause and the fix.

## Build pods stuck in wait-for-casfs

The init container `wait-for-casfs` loops, then fails after `ci.waitForCasfsTimeout` (15 minutes) with:

```
casfs is not loaded on this node after 15m; see the abfs-casfs-loader logs on the node
```

The casfs loader of that node did not load the module. Find it and read its log:

```sh
kubectl -n abfs-ci get pods -l app.kubernetes.io/component=casfs-loader -o wide
kubectl -n abfs-ci logs <loader pod> --previous
kubectl get node <node> -o jsonpath='{.status.nodeInfo.kernelVersion}{"\n"}'
```

A loader that cannot build or load the module exits non-zero and stays in CrashLoopBackOff. Causes, by the last log line:

- `build failed on kernel <version>`: the casfs source does not build on that kernel. casfs at `c946f8e` builds on mainline 6.8 to 6.13 and 6.15 to 6.18 and fails on 6.14 and 6.19. A new CI pool can come up on a newer kernel after a control plane upgrade (the class's maintenance exclusion covers existing pools only). Pin the CI pools to a GKE version whose Ubuntu kernel builds, and run the runbook's canary after each control plane upgrade.
- `apt-get` errors before the build: the node cannot reach the Ubuntu archive, or the archive has no `linux-headers-<version>` package for the kernel. Give the CI nodes egress (Cloud NAT) or a mirror ([prerequisites](prerequisites.md#egress-for-the-ci-nodes)). The node can run its own apt jobs at boot. The loader retries `apt-get update` every 10 seconds for up to 5 minutes while the lists lock is held, and `apt-get install` waits up to 5 minutes for the dpkg lock; `apt-get update failed 30 times`, or a lock error after the wait, means a node job held the lock longer.
- `modprobe casfs failed`: often secure boot, which refuses the self-built module. The CI class sets secure boot off; a hand-made pool needs `--no-shielded-secure-boot`. Check with `gcloud compute instances describe <node> --zone <zone> --format 'value(shieldedInstanceConfig.enableSecureBoot)'`.
- `provider daemonset needs Ubuntu nodes`: the node is not Ubuntu. A hand-made pool needs `--image-type UBUNTU_CONTAINERD`.
- `module implements interface ...` or `module requires interface ...`: a module of another casfs version is loaded. The client and the module must match. A loaded module cannot be removed; replace the node.
- `module version ..., expected ...`: as above, or set `ci.casfs.source.version` to the version the node image ships with `provider: image`.

Build pods on a node whose loader fails are retried by the CI system on another node once the timeout passes. Delete the node to stop new pods landing there.

## "could not detect a running cacheman"

Every mount, remount and unmount first pings the instance's cacheman on its HTTP socket, `<cache dir>/http.socket`; with the chart defaults that is `/var/lib/abfs/base/ci/cache/http.socket`. The ping needs no process list, so it works from a build pod without the host PID namespace. When it fails, the command stops with `ERROR: could not detect a running cacheman: ` and one of these reasons:

- `<socket> does not exist, so cacheman is not running`: no cacheman on this node has started, or the build pod mounts another host path. Check the cacheman pod of the node and its log, and that the build pod mounts the host path `/var/lib/abfs` at `/var/lib/abfs` and sets `ABFS_BASE_DIR=/var/lib/abfs/base`.
- `nothing listens on <socket>, so cacheman stopped or crashed`: the socket file is left from a cacheman that is gone. Check the cacheman pod: its restarts, and its log with `--previous`.
- `permission denied on <socket>, so cacheman may run as another user`: the build pod's uid or gid differs from cacheman's (`ci.uid`, `ci.gid`, 1000:1000 by default). Set `runAsUser` and `runAsGroup` explicitly; without `runAsGroup` the runtime takes the gid from the image's passwd entry, or 0.
- `cacheman did not answer on <socket> within 2m0s`: cacheman accepts connections but does not answer, for example while it starts or when it hangs. Check its log and its CPU and memory on the node.
- `the listen queue of <socket> stayed full for 2m0s`: cacheman takes no new connections. Check its log as above.

While cacheman accepts connections but does not answer yet, the command prints `Waiting for cacheman to answer on <socket>...` once and retries for up to 2 minutes. A mount that starts while cacheman restarts can also print one line `could not connect to cacheman unix domain socket: dial unix .../socket: connect: no such file or directory` and then pass.

Check the cacheman pod of the node:

```sh
kubectl -n abfs-ci get pods -l app.kubernetes.io/component=cacheman -o wide
kubectl -n abfs-ci logs <cacheman pod>
```

`abfs -i ci cacheman status` finds cacheman through its socket, so it works in a build pod too. There it reports that the process is in another pid namespace, because cacheman runs in its own pod. For the `ps` line with the pid and the uptime, run it in the cacheman pod: `kubectl -n abfs-ci exec <cacheman pod> -- abfs -i ci cacheman status`.

## "could not determine current user"

An `abfs` command exits with `panic: could not determine current user: ... USER/LOGNAME/USERNAME environment variables not set`. The process runs as a uid without a passwd entry in its image, and `abfs` then takes the user name from `$USER`. Every pod of the chart that runs `abfs` as `ci.uid` sets `USER` to the numeric uid; a build pod or a pod of your own needs the same (`USER=1000`), or a passwd entry for the uid. In the bootstrap Job the symptom is a health wait that never ends; the Job log prints the error every minute.

## "manifest branch not found" or "manifest not in the git-pusher config"

`ready` or `mount -b` reports the manifest at a URL such as `https:/platform/manifest` (no host) or at a host the pool does not use. The client looks the manifest up by `manifest-server` and `manifest-project-name` of its instance config, which must equal the server and project the pool registers the manifest under. Check the instance config on a CI node:

```sh
kubectl -n abfs-ci exec <cacheman pod> -- abfs -i ci config | grep '^manifest'
```

The chart writes both from `pusher.pool.manifest` (see [configuration](configuration.md#pusher)). An override of `manifest-server` in `bootstrap.clientConfig` must equal the host part of the pool URL, port included. A new value reaches a node's instance config when its cacheman pod restarts. With the right keys and no push yet, `ready` reports `not pushed yet: ... (no completed push)` until the pushers have pushed the manifest.

## Mount fails with the server "gerrit.googlesource.com" not found

`ready` passes, and `mount -b` fails at once:

```
Cloning https://gerrit.googlesource.com/git-repo branch stable-abfs into "<mount>/.repo/repo"...
failed POST /get-refs: 500 500 Internal Server Error the server "gerrit.googlesource.com" not found
repo init failed: exit status 1
```

The store has no patched `repo` tool. A new store has none until it is seeded. The bootstrap Job seeds it with `bootstrap.repoSetup.enabled`; read its log (`kubectl -n abfs logs job/abfs-bootstrap` right after the install or upgrade, later the Cloud Logging query of [runbook step 5](runbook.md#5-install-the-chart)) for the `git-repo` lines. A seed that fails on the clone needs egress from the Job's node to `gerrit.googlesource.com`. Check and seed by hand as in the [runbook](runbook.md#6-check-the-patched-repo-tool).

## Pods in ImagePullBackOff with 403 Forbidden

A pod stays in `ImagePullBackOff`, and its events show `failed to fetch oauth token: unexpected status from GET request to https://<region>-docker.pkg.dev/v2/token?... 403 Forbidden`. The pod runs on a node whose node account has no `roles/artifactregistry.reader` on the repository, usually the system pool. Check the node with `kubectl -n <namespace> get pod <pod> -o wide`. Every chart pod selects a class whose nodes use `computeClasses.nodeServiceAccount`; with `bootstrap.computeClass` or `tests.computeClass` set to empty, grant the reader role to the account of the node the pod lands on ([prerequisites](prerequisites.md#node-service-account)).

## Gateway waits for its controller

`kubectl -n abfs get gateway abfs` stays `Accepted: Unknown` with `Waiting for controller`, and `kubectl get gatewayclass` lists no `gke-l7-*` class. On a new cluster the classes come a few minutes after the create; wait for them as in [prerequisites](prerequisites.md#cluster) first. When they are still missing after 10 minutes, the cluster's HTTP load balancing add-on is off, often because the cluster was created with an `--addons` list that did not name it. The same list turns off horizontal pod autoscaling, so the server HPA does nothing. Turn both on, wait for the operation to finish, then check again:

```sh
gcloud container clusters update abfs --project example-project --region europe-west4 \
  --update-addons HttpLoadBalancing=ENABLED,HorizontalPodAutoscaling=ENABLED
kubectl get gatewayclass gke-l7-rilb
```

## Pods Pending with "Cluster is running incompatible operation"

Server, pusher or build pods stay Pending, and the autoscaler events show `NodeAutoprovisioning: attempt to create node group failed ... Cluster is running incompatible operation`. A cluster update runs, and no node pool can be created until it ends. Wait for it ([prerequisites](prerequisites.md#wait-for-cluster-operations)), then check the node shapes: after the failed attempts the autoscaler can create much larger nodes than the pods need.

## "cannot find ...-no-cgo on PATH"

The mount fails with `cannot find <hook>-no-cgo on PATH (<PATH>): install the client binaries ...`. With `sync.auto-setup: true` (the chart's client config), `abfs setup` copies seven build hooks into every Soong tree on each mount: `abfs-wrap-no-cgo`, `abfsbox-no-cgo`, `ninjago-no-cgo`, `abfs-cmd-map-no-cgo`, `abfs-bazel-daemon-no-cgo`, `rbetap-no-cgo` and `tap-no-cgo`. They are in `/var/lib/abfs/bin` on every CI node.

- Put `/var/lib/abfs/bin` first on the build pod's `PATH`; the error message prints the `PATH` the process had.
- Check that the directory holds them: `ls /var/lib/abfs/bin` in the build pod. If not, the `install-tools` init container of the node's cacheman pod failed; read its log with `kubectl -n abfs-ci logs <cacheman pod> -c install-tools`.

## Render fails on an empty digest

```
Error: execution error at (abfs/templates/...): image.server.digest is empty; set the digest, or image.allowTag=true for development
```

The chart references every image by digest. Set `image.server.digest`, `image.pusher.digest`, `image.client.digest` and `image.casfsLoader.digest` to the `sha256:...` values from `images.txt` of `scripts/build-images.sh --push`. Without `--push` the script prints local image IDs, which are not registry digests. `image.allowTag=true` uses tags instead, for development only.

## Render fails on the cache bounds

```
Error: execution error at (abfs/templates/server-deployment.yaml:...): server.memoryLimit 48Gi (51539607552 bytes) is below the cache bounds plus 6 GiB (73442450944 bytes): headers 100000000 x 180 B, contents 1000000 x 49 KB
```

Both server caches are bounded by entry count, not by bytes. The chart counts 180 B per header entry and 49 KB per content entry, adds 6 GiB, and fails when the sum exceeds `server.memoryLimit`. Lower `server.caches.contents` or `server.caches.headers`, or raise `server.memoryLimit`. The defaults (100,000,000 headers, 400,000 contents, 48Gi) fit.

## Render fails on the server VPA mode

```
Error: execution error at (abfs/templates/server-vpa.yaml:...): server.vpa.mode Recreate next to the CPU HPA makes the two autoscalers fight over the CPU request: set server.vpa.hpaRightsizing=true, or use server.vpa.mode Off
```

A VPA that applies CPU recommendations next to an HPA on CPU utilization raises the request, the utilization drops, the HPA scales in, and the loop repeats with pod recreations. Use `mode: Off` (recommendations only), or `Recreate` with `hpaRightsizing: true` (Preview), where GKE lets the two share the request.

## Gateway health checks fail

The Gateway is programmed but requests fail with HTTP 503, or the backends are unhealthy:

```sh
kubectl -n abfs describe gateway abfs
kubectl -n abfs get healthcheckpolicy,gcpbackendpolicy,httproute
kubectl -n abfs get pods -l app.kubernetes.io/component=server
gcloud compute backend-services list --project example-project --regions europe-west4 \
  --filter 'name~abfs'
gcloud compute backend-services get-health <backend service> --project example-project \
  --region europe-west4
```

Causes:

- No firewall rule for the health checks and the proxies. In a plain VPC the Gateway controller creates `gkegw1-<id>-l7-<network>-<region>`, where `<id>` is a short cluster id; check that it exists with the filter in the [prerequisites](prerequisites.md#firewall). In a Shared VPC the host-project owner must create it.
- The NetworkPolicy drops the probes or the proxied traffic: `networkPolicy.proxyOnlySubnetCidr` is not the range of the region's proxy-only subnet. Check with `gcloud compute networks subnets list --filter purpose=REGIONAL_MANAGED_PROXY`.
- Every server reports `NOT_SERVING`. The server probes Spanner, Bigtable and the bucket in each round, and a failure of any of them marks every replica not serving at once, so the load balancer answers 503 to every call. Read the server log for the backend that fails: a missing role of the server's service account, a name in `backend.*` that does not equal the KCC name, or a missing table or family.
- The pods are not Ready for another reason: `kubectl -n abfs describe pod <server pod>`.

## StatefulSet update forbidden on a volumeClaimTemplate change

`helm upgrade` fails with:

```
StatefulSet.apps "abfs-pusher" is invalid: spec: Forbidden: updates to statefulset spec for fields other than ...
```

A change of `pusher.dataDisk.size` or `pusher.dataDisk.storageClass` changes the StatefulSet's `volumeClaimTemplates`, which are immutable. To grow the disks:

```sh
for i in 0 1 2; do
  kubectl -n abfs patch pvc data-abfs-pusher-$i -p '{"spec":{"resources":{"requests":{"storage":"2Ti"}}}}'
done
kubectl -n abfs delete sts abfs-pusher --cascade=orphan
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --timeout 30m
```

The StorageClass of the chart allows volume expansion. `--cascade=orphan` keeps the pods and the PVCs; the upgrade creates the StatefulSet again, which adopts them. A new storage class applies only to new PVCs: wipe the old ones as for a backend replacement, then the seed runs again.

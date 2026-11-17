# ABFS on GKE / Runbook

Run all commands from `deploy/gke` (or the bundle root) on the admin workstation, with `kubectl` pointing at the cluster. Some need the ABFS source tree ([Bundle and source tree](#bundle-and-source-tree)). The examples use the KCC overlay `kcc/overlays/example` and the project `example-project`. Read the [prerequisites](prerequisites.md) first.

## Install

Every install command and check is in [install](install.md). Install Config Connector by hand, at a pinned version, before the KCC apply. Run no cluster operation while the KCC objects and the chart are installed ([Wait for cluster operations](prerequisites.md#wait-for-cluster-operations)). The order:

1. Apply the KCC overlay.
2. Wait until every KCC object is Ready.
3. Install the chart with `helm install --timeout 30m`, without `--wait`.

Never pass `--wait` to `helm install` or `helm upgrade`: the pushers wait for the pool config, which the post-install bootstrap Job writes, so the command blocks until it times out.

The release namespace must be `abfs`: the KCC base binds Workload Identity to `abfs/abfs-server`. The bootstrap Job runs after every install and upgrade. It writes the pool and client configs, and seeds the patched `repo` tool on a new store ([Check the patched repo tool](install.md#check-the-patched-repo-tool)). Its log is in Cloud Logging after its node is gone ([install step 14](install.md#14-install-the-chart)).

## Backend modes

The overlay lists one of:

- `backend-new`: Config Connector creates the Spanner instance and database with the schema, the Bigtable instance with one cluster, the tables `objects`, `maps` and `chunks` with their Bigtable garbage collection policies, the bucket, and the server's roles. Use it only for resources that do not exist yet. It creates no resource of the ABFS garbage collector ([Garbage collection](#garbage-collection)).
- `backend-existing`: Config Connector creates no object for the backend and only grants the server's roles on the named resources. Never put an existing backend into `backend-new`: adopting a live Bigtable instance or table can delete clusters and column families.

### What an existing backend must have

What `abfs server gcp init` creates:

| Resource | Content |
| --- | --- |
| Spanner database (`cas` by default) | sequence `ProjectIdSequence`; tables `Projects`, `Refs` (with index `RefsRefIndex`, unique index `ProjectsIndex` on `Projects`) |
| Bigtable table `objects` | families `meta`, `data` |
| Bigtable table `maps` | family `cf` |
| Bigtable table `chunks` | family `data` |
| Bigtable instance | one cluster, or an app profile with single-cluster routing (`backend.bigtableAppProfile`) |
| Cloud Storage bucket | in the region |

Every family keeps one version. With garbage collection on, the backend also has what `init --gc` adds ([Garbage collection](#garbage-collection)); run the check with `--gc` then.

### Conformance check

Run `init` from the admin workstation with your Application Default Credentials (`gcloud auth application-default login`), which need `roles/spanner.databaseAdmin`, `roles/bigtable.admin` and `roles/storage.objectViewer` on the resources. It fails in a pod with the Kubernetes service account `abfs-server`, which holds only the runtime roles.

Only the `abfs-server` image has the `server` command (`/usr/local/bin/abfs`). Run it with docker after `gcloud auth configure-docker europe-west4-docker.pkg.dev`. `P` and `SERVER_IMG` come from the install (`SERVER_IMG` is `<registry>/abfs-server@<digest from images.txt or values-images.yaml>`); set the four backend names from `settings.yaml`. Record the schema before and after:

```sh
: "${P:?set P to the project of the backend, as in the install}"
: "${SERVER_IMG:?set SERVER_IMG to the abfs-server image by digest, <registry>/abfs-server@sha256:...}"
SPANNER=abfs                        # settings.yaml spannerInstance
DB=cas                              # settings.yaml spannerDatabase
BT=abfs-objects                     # settings.yaml bigtableInstance
BUCKET=example-project-abfs-blobs   # settings.yaml bucket
abfs_admin() {
  docker run --rm --user "$(id -u):$(id -g)" -e USER=admin -e HOME=/tmp --tmpfs /tmp \
    -e GOOGLE_APPLICATION_CREDENTIALS=/adc.json \
    -v "$HOME/.config/gcloud/application_default_credentials.json:/adc.json:ro" \
    --entrypoint /usr/local/bin/abfs "$SERVER_IMG" "$@"
}
GC=                 # GC=--gc when garbage collection is on
gcloud spanner databases ddl describe $DB --instance $SPANNER --project $P >ddl-before.sql
for t in objects maps chunks ${GC:+abfsgc}; do
  gcloud bigtable instances tables describe $t --instance $BT --project $P \
    --format 'yaml(columnFamilies)'
done >families-before.yaml

abfs_admin server gcp init $GC --project $P --instance $SPANNER --db $DB \
  --bt-instance $BT --bucket $BUCKET

gcloud spanner databases ddl describe $DB --instance $SPANNER --project $P >ddl-after.sql
for t in objects maps chunks ${GC:+abfsgc}; do
  gcloud bigtable instances tables describe $t --instance $BT --project $P \
    --format 'yaml(columnFamilies)'
done >families-after.yaml
diff ddl-before.sql ddl-after.sql && diff families-before.yaml families-after.yaml && echo conformant
```

`init` prints `The GCP server resources are ready.` Any difference in the two files means the backend did not conform; find out why before the servers start.

`abfs_admin` needs rootful Docker without user namespace remapping to read the 0600 ADC file. With rootless Docker, `userns-remap`, or SELinux in enforcing mode, define `abfs_admin` with a readable copy in a directory only you can reach (`z` relabels it for SELinux), and remove it with `rm -r "$adc"` after the check:

```sh
adc=$(mktemp -d)
install -m 0644 "$HOME/.config/gcloud/application_default_credentials.json" "$adc/adc.json"
abfs_admin() {
  docker run --rm --user "$(id -u):$(id -g)" -e USER=admin -e HOME=/tmp --tmpfs /tmp \
    -e GOOGLE_APPLICATION_CREDENTIALS=/adc.json \
    -v "$adc/adc.json:/adc.json:ro,z" \
    --entrypoint /usr/local/bin/abfs "$SERVER_IMG" "$@"
}
```

## Garbage collection

Off by default (`server.gc.enabled`, `gcCollector.enabled`). Neither backend mode creates its resources:

| Resource | Created by |
| --- | --- |
| Bigtable table `abfsgc` (families `e`, `t`) and the family `abfsgc` of `objects` | `components/gc` with `backend-new`, `init --gc` with `backend-existing` |
| Bigtable app profile `abfs-gc`: single-cluster routing, single-row transactions, priority low | `components/gc` with `backend-new`, `gcloud` with `backend-existing` (below) |
| Spanner tables `AbfsGcState`, `AbfsGcLists`, `AbfsGcServers` | `init --gc`, in both modes |
| objects under `abfsgc/` in the bucket | the collector |
| `roles/storage.legacyBucketReader` of the server on the bucket | both backend components |

Never add these Spanner tables to the Config Connector `ddl` list.

### Turn it on

Steps of `doc/sysadmin/GCP_GC.md`, "Enable or disable garbage collection":

1. With `backend-new`, list `components/gc` after `backend-new`, apply, and wait until every object is Ready before `init --gc`; otherwise Config Connector deletes the family `abfsgc` that `init` creates.
2. Run `init --gc` as in [Conformance check](#conformance-check), with `GC=--gc`; its files differ (with `backend-existing` the first describe of `abfsgc` fails). Rerun it; it must print `conformant`.
3. `helm upgrade` with `server.gc.enabled=true`. A server with `--gc` stops at startup and names a missing resource.
4. `abfs_admin server gcp gc backfill` with the backend flags of `init` and `--bt-app-profile abfs-gc`; then `gc-keepalive` on for the clients (`GCP_GC.md` steps 3 and 4).
5. `helm upgrade` with `gcCollector.enabled=true` (mode `dry-run`).
6. When the dry run reports are clean, `gcCollector.mode=condemn`, for at least 7 daily cycles under real use.
7. Check the last condemn reports as `GCP_GC.md` step 8 lists.
8. `gcCollector.mode=delete` with `gcCollector.deleteTypes`, widened from the top of the graph down (`GCP_GC.md`, "Operator rules").

With `backend-existing`, create the app profile once; `<cluster>` is the cluster in `backend.zone`:

```sh
gcloud bigtable clusters list --instances <bt-instance> --project <project>
gcloud bigtable app-profiles create abfs-gc --instance <bt-instance> --project <project> \
  --route-to <cluster> --transactional-writes --priority PRIORITY_LOW \
  --description "ABFS GC collector"
```

### The collector

The CronJob `abfs-gc-collector` runs `abfs server gcp gc run --<gcCollector.mode>` daily at 02:00 UTC as the server's service account; condemn and delete pass `--min-replicas` = `server.replicas.min` - 1 ([values](configuration.md#gccollector)). Run a cycle now (never during a scheduled one: it fails and alerts until a clean cycle) and read its exit code:

```sh
kubectl -n abfs create job --from=cronjob/abfs-gc-collector gc-manual-1
kubectl -n abfs get pods -l job-name=gc-manual-1 \
  -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}'
```

| Code | Cause | Check |
| --- | --- | --- |
| 0 | Clean | |
| 1 | Cycle failed: flags, app profile, backend, lease, `condemn.not_completed` | the error line |
| 3 | `condemn.refused` | backfill, servers, pins |
| 4 | `delete.refused`; the cycle condemned | server fleet, keep-alive, app profile |
| 5 | `delete.step.error`: a worker error stopped the delete step | `error`; the next cycle retries |
| 6 | `delete.chunks.skipped` with a due record, or `unflagged` | bucket reader grant, versioning, retention, `error_sample` |
| 128+n | Signal n | `gcCollector.deadlineSeconds`, eviction |

Several causes give the lowest of 3 to 6. Each report is one log line (over 16 KiB it may split: then `kubectl -n abfs logs job/<job>`):

```sh
gcloud logging read 'resource.type="k8s_container" AND resource.labels.namespace_name="abfs" AND
  resource.labels.container_name="gc-collector" AND jsonPayload.phase:*' --project=<project> \
  --freshness=8d --format=json
```

With `alerts.enabled`, the `Rules` object `abfs-gc-collector` has `AbfsGcCycleNotClean` (the last cycle was not clean; label `result` names the cause) and `AbfsGcNoCleanCycle` (no clean cycle in 31 hours; also after the first enable). Check both in Metrics Explorer once.

For `gc undo` and `gc backfill`, `helm upgrade` with `gcCollector.suspend=true`, then wait for or delete the active Job. Undo reads the delete records (7 days), not backups.

### Upgrade and turn off

- On every upgrade, run `init --gc` of the new release before the servers. With `backend-new`, apply a Bigtable change in `components/gc` or `backend-new` first; Config Connector deletes a table or family that `init` created alone.
- Turn off: `gcCollector.enabled=false`, then `server.gc.enabled=false`, then `gc-keepalive` off. Keep `components/gc` while its resources exist, or Config Connector deletes the family `objects:abfsgc` and the five objects fail on every reconcile.
- Turning it on again needs all garbage collector state removed first. A removal procedure exists for operators and is published after it is proven.

## Seed and steady phase

The pushers start in `pusher.phase: seed` by default: the class `abfs-pusher-seed` (32 vCPU and 240 GB at least, highmem shapes) and `pusher.seedResources` (200Gi by default). The seed of a full Android tree takes hours. Watch it:

```sh
kubectl -n abfs get pods -l app.kubernetes.io/component=pusher -o wide
kubectl -n abfs logs abfs-pusher-0 --tail 50
```

When every pusher has completed its initial push, move to the steady phase:

```sh
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --set pusher.phase=steady --timeout 30m
```

Put `pusher.phase: steady` into the values file too. The StatefulSet recreates each pusher, highest ordinal first, on a node of `abfs-pusher-steady` in its PVC's zone, with `pusher.steadyResources`. The chart creates the pusher VerticalPodAutoscaler in this phase only. The pusher container is `git-pusher-seed` in the seed phase and `git-pusher` in the steady phase; the VPA controls only `git-pusher`, so seed usage never sets the steady size ([Right-sizing](configuration.md#right-sizing)). The autoscaler deletes the empty seed nodes.

Go back to `phase: seed` before a re-shard, after a lost PVC, or before adding a large new branch.

### The pusher count is fixed

`pusher.count` is fixed after the seed.

- Never `kubectl scale` the StatefulSet. With fewer pods than the pool, the missing pushers' projects go unpolled without an error. More pods than the pool fail their config lookup.
- A change of `pusher.count` is a re-shard: pushers clone the moved projects again from Gerrit (hours), and the PVCs must hold the new share. Do it in the seed phase, with `helm upgrade`, never by hand.

With `alerts.enabled=true` and `alerts.clusterName` set to the cluster's name, Managed Service for Prometheus rules fire when pool projects have no running pusher, fewer pushers report than `pushers-count`, or a pusher container (`git-pusher` or `git-pusher-seed`) restarts.

### A pusher OOM kill

An OOM-killed pusher restarts with the same limit. `kubectl -n abfs describe pod abfs-pusher-<n>` shows `Last State: Terminated, Reason: OOMKilled`. In the seed phase, raise `pusher.seedResources`. In the steady phase the VPA raises its recommendation. With `pusher.vpa.mode: Initial`, delete the pod so the StatefulSet recreates it with the new limit:

```sh
kubectl -n abfs get pods -l app.kubernetes.io/component=pusher
kubectl -n abfs get vpa abfs-pusher -o jsonpath='{.status.recommendation.containerRecommendations[?(@.containerName=="git-pusher")].target.memory}{"\n"}'
kubectl -n abfs delete pod abfs-pusher-<n>
```

The pusher resumes from its PVC. With `pusher.vpa.mode: InPlaceOrRecreate` (Preview) the VPA applies it itself. A pusher still killed at `pusher.vpa.maxMemory` needs the seed phase.

### Backend replacement

`pushed-refs` on a pusher's PVC is valid only for its backend. When the backend is replaced (a new database, instance or bucket), wipe the pusher PVCs before the pushers run against it:

```sh
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --set pusher.enabled=false --timeout 30m
kubectl -n abfs delete pvc -l app.kubernetes.io/name=abfs,app.kubernetes.io/component=pusher
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --set pusher.phase=seed --timeout 30m
```

Recycle the CI nodes too, so every node cache starts empty ([Upgrades](#upgrades)).

## CI nodes

### Canary after every control plane upgrade

The `abfs-ci` class holds back upgrades of existing CI pools with `maintenanceExclusion: UNTIL_END_OF_SUPPORT`. A new CI pool comes up on the control plane's patch version, whose COS image can ship another casfs. The cluster's `no_minor_upgrades` exclusion holds minor versions back ([prerequisites](prerequisites.md#hold-minor-upgrades)). With `ci.casfs.provider: daemonset`, a newer Ubuntu kernel can fail the casfs build (6.14 and 6.19 fail).

After every control plane upgrade, run one build pod before CI resumes, and record the versions it ran on:

```sh
helm template abfs chart/abfs -n abfs -f <values.yaml> \
  --set ci.exampleJob.enabled=true --set ci.exampleJob.image=<build image> \
  --show-only templates/ci-example-job.yaml | kubectl create -f -
ended() { kubectl -n abfs-ci get job abfs-example-build \
    -o jsonpath='{.status.conditions[?(@.status=="True")].type}' | grep -q -e Complete -e Failed; }
end=$(( $(date +%s) + 46800 ))
until ended; do
  [ "$(date +%s)" -ge "$end" ] && { echo "the Job has not ended after 13 hours; it is kept" >&2; break; }
  sleep 60
done
kubectl -n abfs-ci get job abfs-example-build
kubectl -n abfs-ci logs job/abfs-example-build --all-containers
ended && kubectl -n abfs-ci delete job abfs-example-build
kubectl get nodes -l cloud.google.com/compute-class=abfs-ci \
  -o custom-columns=NAME:.metadata.name,VERSION:.status.nodeInfo.kubeletVersion,KERNEL:.status.nodeInfo.kernelVersion
```

Any CI build pod also serves as a canary: where the loader cannot load a matching module, it stays in CrashLoopBackOff, and build pods fail after `ci.waitForCasfsTimeout` (15 minutes). Watch the loaders:

```sh
kubectl -n abfs-ci get pods -l app.kubernetes.io/component=casfs-loader -o wide
```

For a freeze, a cluster maintenance exclusion of scope `no_upgrades` stops patch upgrades too, for at most 30 days.

### Cache reset

Each CI node has one cache at `/var/lib/abfs/base/ci/cache` on its boot disk. Reset it by removing the directory and restarting the node's cacheman pod, never by emptying it. A stopped cacheman makes every mount on the node fail, so stop builds on the node first. A cacheman that starts while builds hold the node's disk preempts them (PriorityClass `<release>-ci-node`).

A cacheman that finds the cache directory skips `abfs init`, so it must never start on a half-deleted cache. The reset pod renames the directory atomically, then deletes it (about a minute per 100 GB). Do not use `kubectl debug node/... -it`, which can return before the delete ends. Wait until the reset pod succeeds, then restart cacheman:

```sh
N=<node>
CLIENT_IMG=<abfs-client image>
kubectl cordon $N
# wait until no build pod runs on the node
kubectl -n abfs-ci create -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: abfs-cache-reset
spec:
  nodeName: $N
  restartPolicy: Never
  automountServiceAccountToken: false
  tolerations: [{operator: Exists}]
  containers:
    - name: rm
      image: $CLIENT_IMG
      command: [sh, -c, 'rm -rf /ci/cache.reset && if [ -e /ci/cache ]; then mv -T /ci/cache /ci/cache.reset; fi && rm -rf /ci/cache.reset']
      volumeMounts: [{name: ci, mountPath: /ci}]
  volumes:
    - name: ci
      hostPath: {path: /var/lib/abfs/base/ci, type: Directory}
EOF
kubectl -n abfs-ci wait pod/abfs-cache-reset --for=jsonpath='{.status.phase}'=Succeeded --timeout=30m
kubectl -n abfs-ci delete pod abfs-cache-reset
kubectl -n abfs-ci delete pod --field-selector spec.nodeName=$N -l app.kubernetes.io/component=cacheman
kubectl uncordon $N
```

The pod runs as root (`privileged` level of `abfs-ci`). If the wait times out or the pod fails, read `kubectl -n abfs-ci logs abfs-cache-reset`, and do not restart cacheman until `/var/lib/abfs/base/ci/cache` is gone. A second run finishes an earlier delete. The new cacheman log shows `Initialization for instance 'ci' complete`. Deleting the node resets its cache too.

## Upgrades

Apply the KCC overlay of the new release and wait until every object is Ready, so APIs and roles exist before new pods and nodes start. Then upgrade the chart, without `--wait`:

```sh
kubectl apply -k kcc/overlays/<name>
kubectl -n abfs wait --for=condition=Ready --all gcp --timeout=30m
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --timeout 30m
```

The bootstrap Job rewrites the two configs when they changed.

- Server: a rolling update with one extra replica. Each pod drains for 120 s before SIGTERM (`server.preStopSleepSeconds`) and gets 240 s in all (`server.terminationGracePeriodSeconds`), which includes up to 105 s for the server's stop with `server.gc.enabled`.
- Pushers: a rolling update, one pod at a time; two releases run in the pool meanwhile. When the new release changes how pushers split the projects or read the pool config, stop the pool first: `helm upgrade` with `--set pusher.enabled=false`, then the upgrade with the new images and `pusher.enabled` true. The PVCs stay.
- casfs loader: a rolling update. A loaded module stays loaded, and `casfs_max_mounts` is read-only after load, so a new casfs version (node image, or loader image with `daemonset`) or `ci.casfs.maxMounts` takes effect on new nodes only.
- CI node disk: a change of `ci.cacheman.cacheDisk` reaches new CI nodes only. A change of `computeClasses.ci.bootDiskGb` or `computeClasses.imageStreaming` reaches new CI node pools only, not new nodes of existing pools. To move at once, cordon the nodes of the old CI pools, wait until their builds end, and delete the pools.
- GKE node upgrades: surge upgrades for the server and pusher nodes, version upgrades held on existing CI pools, and node auto-repair at any time ([prerequisites](prerequisites.md#node-upgrades)). The CI system retries a build whose node is replaced.
- cacheman: `updateStrategy: OnDelete`, because a stopped cacheman makes every mount on its node refuse reads and writes. Recycle the CI nodes: cordon a node, wait until its builds end and delete its cacheman pod as in [Cache reset](#cache-reset) (without the reset pod), or cordon the old nodes and let the autoscaler remove them. Each new cacheman pod copies its tool set to `/var/lib/abfs/bin`.

`ci.cacheman.evictThreshold` was removed; the threshold is now 90 % of `ci.cacheman.cacheDisk`, so remove it from your values files. With `alerts.enabled`, `alerts.clusterName` is required.

### Spanner schema changes

The `ddl` list of `kcc/components/backend-new/spanner-database-ddl.yaml` is append-only: Config Connector refuses a changed entry, and the `SpannerDatabase` reports an update error until it is restored. Never edit or remove an entry. Append a schema change by hand at the end, as a new `ALTER` or `CREATE ... IF NOT EXISTS` entry in the same `- |-` form. Check the list before applying:

```sh
make ddl-check
```

The file's `# ddl-base:` line holds the sha256 and count of the first release's entries. The release check compares the list with the GCP server schema of the ABFS commit that first created the database: `make ddl-check DDL_REPO=<abfs repo> DDL_SOURCE=<commit>`.

The check guards only the base; in review, a new migration may only add lines at the end. `scripts/gen-ddl.sh` writes the list once from `casdb/schema.sql`, for the release that first creates the database; never run it over the file again. With `backend-existing`, schema changes come from `abfs server gcp init` of the new release. With garbage collection on, `init --gc` of the new release adds the garbage collector's changes in both modes ([Garbage collection](#garbage-collection)).

## Config Connector upgrade and rollback

Upgrade with the operator file of the newer bundle. Keep each bundle; uninstalling a version needs its files.

```sh
V=<new version>
gcloud storage cp gs://configconnector-operator/$V/release-bundle.tar.gz release-bundle-$V.tar.gz
mkdir kcc-$V && tar -C kcc-$V -zxf release-bundle-$V.tar.gz
kubectl apply -f kcc-$V/operator-system/configconnector-operator.yaml
kubectl get ns cnrm-system -o jsonpath='{.metadata.annotations.cnrm\.cloud\.google\.com/version}{"\n"}'
kubectl wait -n cnrm-system --for=condition=Ready pod --timeout=10m \
  -l cnrm.cloud.google.com/component=cnrm-controller-manager,cnrm.cloud.google.com/scoped-namespace=abfs
kubectl -n abfs wait --for=condition=Ready --all gcp --timeout=30m
```

A full downgrade of the operator and its CRDs means uninstall, install, and apply the objects again. In namespaced mode (from 1.123.1), roll back the controller of a namespace with `spec.version` of its `ConfigConnectorContext`. With the CRD of 1.160.0 this goes back one minor version at most, to the latest patch of that minor:

```sh
kubectl -n abfs patch configconnectorcontext configconnectorcontext.core.cnrm.cloud.google.com \
  --type merge -p '{"spec":{"version":"<previous version>"}}'
```

A later `kubectl apply -k` keeps the patched value. Remove it with `-p '{"spec":{"version":null}}'` to run the operator's version again.

Never delete the `ConfigConnectorContext` while KCC objects exist in the namespace. Delete the objects first, as in [Teardown](#teardown).

## Teardown

1. Uninstall the release, and wait until the ABFS nodes are gone (about 5 minutes):

   ```sh
   helm uninstall abfs -n abfs --timeout 30m
   until [ -z "$(kubectl get nodes -l cloud.google.com/compute-class -o name)" ]; do sleep 30; done
   ```

   This removes the Gateway, its load balancer and the ComputeClasses. The reserved Gateway address, the pusher PVCs and the namespace `abfs-ci` stay.

2. Delete the KCC objects first, then the rest of the overlay (the namespace and its `ConfigConnectorContext`). Config Connector must still run while it deletes its objects, so never delete the `ConfigConnectorContext` first:

   ```sh
   kubectl -n abfs delete gcp --all --wait --timeout 30m
   kubectl delete -k kcc/overlays/<name> --ignore-not-found
   ```

   The namespace `abfs` then stays `Terminating` for about a minute.

   Every data resource and the Gateway address carry `cnrm.cloud.google.com/deletion-policy: abandon`: the Spanner instance and database, the Bigtable instance, tables, policies and the collector's app profile, the bucket, the proxy-only subnet, the Gateway address and the enabled APIs stay, with their data. The two IAM service accounts and every IAM grant are deleted; workloads that still run with them lose their roles at once.

3. Delete the pusher PVCs and the CI namespace:

   ```sh
   kubectl -n abfs delete pvc -l app.kubernetes.io/name=abfs,app.kubernetes.io/component=pusher
   kubectl delete namespace abfs-ci
   ```

4. To remove the data, delete the abandoned resources with gcloud. A Spanner instance cannot be deleted while it has backups (new instances get automated daily backups), so delete them first:

   ```sh
   gcloud spanner backups list --instance abfs --project example-project --format 'value(name.basename())' |
     xargs -r -n 1 gcloud spanner backups delete --instance abfs --project example-project --quiet
   gcloud spanner instances delete abfs --project example-project
   gcloud bigtable instances delete abfs-objects --project example-project
   gcloud storage rm --recursive gs://example-project-abfs-blobs
   gcloud compute addresses delete abfs-gateway --region europe-west4 --project example-project
   ```

## Bundle and source tree

The release bundle holds this folder's `AGENTS.md`, `README.md`, `Makefile`, `chart/`, `kcc/`, `docs/` and `scripts/`, the four images as OCI archives, `images.txt`, `source/` and `SHA256SUMS`. `source/` holds the ABFS and abfs-casfs trees of the images. The `abfs-client` image has the source of its static `ps` and `fuser` in `/usr/share/abfs/source`. Check the bundle with `sha256sum -c SHA256SUMS` in its root.

From the bundle root, these run as in `deploy/gke`:

- every `kubectl` and `helm` command of this runbook, and `scripts/load-images.sh`;
- `make check` (lint, template, kustomize, and `ddl-check` against the hash in the ddl file) and `scripts/check-ddl.sh` without a schema source.

These need a clone of the ABFS repository at the deployed commit:

- `scripts/build-images.sh` and `make images`: run the copy in `deploy/gke` of the clone (Dockerfiles in `images/`, plus the abfs-casfs repository); `-C` only names another ABFS repository. With a bundle, load the archives of `images/` with `scripts/load-images.sh` instead ([install step 11](install.md#11-upload-the-images)), or build from `source/` as below;
- `scripts/bundle.sh` and `make bundle` (the bundle's copy runs with `-C <clone>`);
- `scripts/gen-ddl.sh`, `make ddl`, and `make ddl-check` with `DDL_SOURCE`.

From a clone, build and push the four images (`<commit>` is the ABFS commit to deploy):

```sh
scripts/build-images.sh --casfs <abfs-casfs repo> --push \
  europe-west4-docker.pkg.dev/example-project/abfs <commit>
```

Set the four `image.<name>.digest` values of the chart to the registry digests in the printed `images.txt`; `load-images.sh` is not needed then. Without `--push` the script prints local image IDs, which are not registry digests.

The scripts take the version from a git tag, and the trees in `source/` have no git history. To build from them, make each tree a repository and tag the ABFS one with the bundle's version:

```sh
V=<version>                       # the bundle's name without abfs-gke-
tar -xzf source/abfs-$V.tar.gz && tar -xzf source/abfs-casfs-*.tar.gz
C=$(ls -d abfs-casfs-*/)
for d in abfs-$V "$C"; do
  git -C "$d" init -q && git -C "$d" add -A &&
    git -C "$d" -c user.name=build -c user.email=build@localhost commit -qm source
done
git -C abfs-$V -c user.name=build -c user.email=build@localhost tag -a -m "abfs $V" v$V
abfs-$V/deploy/gke/scripts/build-images.sh --casfs "$C" --casfs-commit HEAD --push \
  europe-west4-docker.pkg.dev/example-project/abfs HEAD
```

## Security

The OSS server has no authentication and no TLS by default: any host reaching the Gateway address can read and write the whole store. The network is the only boundary:

- The Gateway address is internal and regional, and `gateway.allowGlobalAccess` is false.
- The NetworkPolicy lets only the proxy-only subnet, the health check range, the bootstrap Job and the Helm test pod reach the server pods. It needs Dataplane V2.
- Keep the VPC's firewall rules and routes so only the CI nodes and admin hosts reach the address.

- Only the server's Google service account holds Google roles: `roles/spanner.databaseUser`, `roles/bigtable.user`, and `roles/storage.objectUser` and `roles/storage.legacyBucketReader` on the bucket. `init` runs with the admin's own credentials.
- The Gerrit credentials live in one Secret, mounted only into the pushers.
- In `abfs-ci`, the casfs loader (host root, and the host network with `ci.casfs.provider: daemonset` only) and the build pods run privileged; cacheman runs unprivileged, with a root init container for its directories. The server, the pushers and the bootstrap Job run without privileges in `abfs`; the pushers run as root, which the `baseline` level allows.
- All builds on a CI node read the same cache, and the kernel trusts cached content by name. Give each tenant its own CI class and node pools.
- The server's HTTP debug port listens on 127.0.0.1 only, and the pushers' `/info` port is not behind the Gateway.

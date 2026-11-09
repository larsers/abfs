# ABFS on GKE / Runbook

All commands run from `deploy/gke` (or the root of the release bundle) on the admin workstation, with `kubectl` pointing at the cluster. A few need the ABFS source tree; see [Bundle and source tree](#bundle-and-source-tree). The examples use the KCC overlay `kcc/overlays/example` and the project `example-project`. Read the [prerequisites](prerequisites.md) first.

## Install

Config Connector must be installed by hand at a pinned version before the KCC apply ([prerequisites](prerequisites.md#config-connector)), and no cluster operation may run while the KCC objects and the chart are installed ([Wait for cluster operations](prerequisites.md#wait-for-cluster-operations)). The order matters:

1. Apply the KCC overlay.
2. Wait until every KCC object is Ready.
3. Install the chart with `helm install --timeout 30m`, without `--wait`.

The pushers wait for the pool config, which the post-install bootstrap Job writes. `--wait` waits for the pushers to become ready before the hooks run, so with `--wait` the install blocks until it times out.

### 1. Build the images

```sh
scripts/build-images.sh --casfs <abfs-casfs repo> --push \
  europe-west4-docker.pkg.dev/example-project/abfs <commit>
```

`<commit>` is the ABFS commit to deploy. The script prints `images.txt` with the name, reference and registry digest of each image. The chart takes the digests. The build needs the ABFS and abfs-casfs repositories. With a release bundle, copy its image archives instead; the script prints the chart's image values with the digests of your registry:

```sh
gcloud auth configure-docker europe-west4-docker.pkg.dev
scripts/load-images.sh europe-west4-docker.pkg.dev/example-project/abfs >values-images.yaml
```

Pass `-f values-images.yaml` after your values file in every `helm install` and `helm upgrade`, or copy the digests into it.

### 2. Configure and apply the KCC overlay

Copy `kcc/overlays/example` to `kcc/overlays/<name>` and set every value in its `settings.yaml`. In `kustomization.yaml` pick the components:

- the backend mode: exactly one of `backend-new` and `backend-existing` (see [Backend modes](#backend-modes)). Listing both fails the build.
- `network/proxy-subnet` when the network has no proxy-only subnet in the region.
- `settings`, always.

Numbers, such as the Bigtable node cap, are set with a patch in `kustomization.yaml`; the example shows one.

```sh
kubectl kustomize kcc/overlays/<name> | less     # review
kubectl apply -k kcc/overlays/<name>
kubectl -n abfs wait --for=condition=Ready --all gcp --timeout=30m
kubectl -n abfs get gcp
```

`gcp` is the category of every Config Connector resource. An object that stays not Ready shows the reason in `kubectl -n abfs describe <kind> <name>`.

### 3. Check the backend

For both modes, run `abfs server gcp init` against the backend with the admin's own credentials, from the `abfs-server` image. It must change nothing but a no-op schema update. See [Conformance check](#conformance-check).

### 4. Prepare the chart values

Copy `chart/abfs/values-example.yaml` and set:

- the four digests from `images.txt`;
- `gateway.vip`, the address of the KCC ComputeAddress:

  ```sh
  kubectl -n abfs get computeaddress abfs-gateway -o jsonpath='{.status.observedState.address}'
  ```

- `pusher.pool.manifest` and `pusher.pool.branchFiles`: the Gerrit manifest and the branches to push. The clients get the manifest's server and project from the same value ([configuration](configuration.md#pusher));
- `pusher.count`, which is fixed after the seed;
- the names in [the KCC to chart table](configuration.md#kcc-names-and-chart-values), when they differ from the example.

The chart creates the CI namespace `abfs-ci` with Pod Security level `privileged` (`namespaces.createCi`). Create the Gerrit credentials:

```sh
kubectl -n abfs create secret generic gerrit-credentials --from-file=.netrc=<path to netrc>
```

The `.netrc` holds the HTTPS credentials of the Gerrit host. Add `--from-file=.gitcookies=<path>` and set `pusher.gerrit.cookies=true` to use a cookie file.

### 5. Install the chart

```sh
helm install abfs chart/abfs -n abfs -f <values.yaml> --timeout 30m
```

The release namespace must be `abfs`: the Workload Identity binding of the KCC base names the Kubernetes service account `abfs/abfs-server`. Helm returns after the bootstrap Job has finished. Read the Job log at once, before the other checks:

```sh
kubectl -n abfs logs job/abfs-bootstrap
kubectl -n abfs get deploy,hpa,sts,pods
kubectl -n abfs get gateway abfs
helm test abfs -n abfs
```

`helm test` checks gRPC health through the Service and through the VIP, and on a CI node the file capabilities of `casfs-mounter` and a run of it as the build uid, in a privileged pod like a build pod. The second test creates a CI node when none exists (`tests.ciTools`). The bootstrap Job and the first test run on a node of the class `abfs-server` (`bootstrap.computeClass`, `tests.computeClass`), because the system pool's node account often cannot pull the images.

The server pod fills its node, so the Job gets a node of its own. Each install and upgrade waits about 2 minutes for that node. When the Job ends, the autoscaler removes the empty node, and the Job's pod goes with it. A few minutes later `kubectl logs job/abfs-bootstrap` finds no pod and prints `error: timed out waiting for the condition`. Read the log from Cloud Logging then:

```sh
gcloud logging read 'resource.type="k8s_container" AND resource.labels.cluster_name="abfs"
  AND resource.labels.namespace_name="abfs" AND resource.labels.container_name="bootstrap"' \
  --project example-project --freshness 1h --order asc --format 'value(textPayload)'
```

Add `AND textPayload:"git-repo"` to the filter for the lines of step 6.

`helm test` deletes a test pod that passed (`hook-delete-policy: hook-succeeded`), so a passed test leaves no log, and `helm test --logs` fails with `unable to get pod ...: not found` after both tests passed. Run `helm test` without `--logs`; a failed test pod stays and keeps its log.

### 6. Check the patched repo tool

`abfs mount -b` clones the patched `repo` tool from the store: the ref `refs/heads/stable-abfs` of the project `git-repo` on the server `gerrit.googlesource.com`. A new store has none, and every mount then fails at once:

```
failed POST /get-refs: 500 500 Internal Server Error the server "gerrit.googlesource.com" not found
repo init failed: exit status 1
```

With `bootstrap.repoSetup.enabled` (the default) the bootstrap Job checks the ref and, when it is missing, runs `abfs repo-setup` and pushes the patched clone, as the "Push repo" step of the sysadmin guide does. This needs egress from the Job's node to `gerrit.googlesource.com`, through Cloud NAT on a private cluster. With an existing ref the Job changes nothing, and when the ref cannot be read for another reason the Job fails instead of seeding. After a seed the Job reads the ref again and fails when it is not there.

A failed check or seed fails the hook, so `helm install` or `helm upgrade` fails too. The Job log shows the cause on a line with `git-repo` (`cannot read the git-repo ref`, `git-repo seed failed`, or `not in the store after the seed`). Fix the cause and run the upgrade again, or set `bootstrap.repoSetup.enabled=false`, run the upgrade, and seed by hand as below. Check in both backend modes, from any cacheman pod. The first command works only right after Helm returns; later, use the Cloud Logging query of step 5:

```sh
kubectl -n abfs logs job/abfs-bootstrap | grep git-repo
kubectl -n abfs-ci get pods -l app.kubernetes.io/component=cacheman
echo refs/heads/stable-abfs | kubectl -n abfs-ci exec -i <cacheman pod> -- \
  abfs -i ci test-client get-ref -s gerrit.googlesource.com -p git-repo
```

The last command prints a commit hash and `refs/heads/stable-abfs`. `(not exists) refs/heads/stable-abfs`, or `the server "gerrit.googlesource.com" not found`, means the ref is missing. Any other error means the check did not reach the store; fix that first. Seed the ref by hand when `repoSetup` is off or the Job has no egress. `repo-setup` prints the directory of the patched clone and the two commands with its commit; the push took about 2 minutes for 10,709 objects:

```sh
kubectl -n abfs-ci exec -it <cacheman pod> -- bash
abfs -i ci repo-setup
abfs -i ci --no-local-cache push -r <directory printed by repo-setup>
echo "<commit printed by repo-setup> refs/heads/stable-abfs" |
  abfs -i ci test-client put-ref -s gerrit.googlesource.com -p git-repo
```

To update the tool later, run the same commands by hand; the Job never replaces an existing ref.

## Backend modes

The overlay lists one of two components:

- `backend-new`: Config Connector creates the Spanner instance and database (schema included), the Bigtable instance with one cluster, the tables `objects`, `maps` and `chunks` with their Bigtable garbage collection policies, the bucket, and the server's three roles. Use it only for resources that do not exist yet. It creates no resource of the ABFS garbage collector; see [Garbage collection](#garbage-collection).
- `backend-existing`: the backend exists. Config Connector creates no object for it, so nothing in the overlay can change or delete it, and only grants the server's roles on the named resources. Adopting a live Bigtable instance or table into Config Connector can delete clusters and column families, and the Spanner `ddl` list is sent again, so an existing backend is never put into `backend-new`.

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

Run `init` against the backend from the admin's workstation, with the admin's own Application Default Credentials (`gcloud auth application-default login`), which need `roles/spanner.databaseAdmin`, `roles/bigtable.admin` and `roles/storage.objectViewer` on the resources. The `abfs-server` image carries the full `abfs` binary of the deployed commit at `/usr/local/bin/abfs`; the client and pusher images have no `server` command. Run it with docker (after `gcloud auth configure-docker europe-west4-docker.pkg.dev`). `USER` is set because the image has no passwd entry for the caller's uid, and `abfs` stops without one. Record the schema before and after:

```sh
P=example-project
IMG=europe-west4-docker.pkg.dev/example-project/abfs/abfs-server@<digest from images.txt>
abfs_admin() {
  docker run --rm --user "$(id -u):$(id -g)" -e USER=admin -e HOME=/tmp --tmpfs /tmp \
    -e GOOGLE_APPLICATION_CREDENTIALS=/adc.json \
    -v "$HOME/.config/gcloud/application_default_credentials.json:/adc.json:ro" \
    --entrypoint /usr/local/bin/abfs "$IMG" "$@"
}
GC=                 # GC=--gc when garbage collection is on
gcloud spanner databases ddl describe cas --instance abfs --project $P >ddl-before.sql
for t in objects maps chunks ${GC:+gc}; do
  gcloud bigtable instances tables describe $t --instance abfs-objects --project $P \
    --format 'yaml(columnFamilies)'
done >families-before.yaml

abfs_admin server gcp init $GC --project $P --instance abfs --db cas \
  --bt-instance abfs-objects --bucket example-project-abfs-blobs

gcloud spanner databases ddl describe cas --instance abfs --project $P >ddl-after.sql
for t in objects maps chunks ${GC:+gc}; do
  gcloud bigtable instances tables describe $t --instance abfs-objects --project $P \
    --format 'yaml(columnFamilies)'
done >families-after.yaml
diff ddl-before.sql ddl-after.sql && diff families-before.yaml families-after.yaml && echo conformant
```

`abfs_admin` assumes rootful Docker without user namespace remapping, where the container runs as your own uid and can read the 0600 ADC file. It fails in these setups:

- Rootless Docker, or a daemon with `userns-remap`: the container uid maps to another host uid, which cannot read the file.
- An SELinux host in enforcing mode: the container cannot read a bind mount without a container label.

In these cases mount a readable copy instead, in a directory only you can reach. Define `abfs_admin` with this mount in place of the one above:

```sh
adc=$(mktemp -d)
install -m 0644 "$HOME/.config/gcloud/application_default_credentials.json" "$adc/adc.json"
abfs_admin() {
  docker run --rm --user "$(id -u):$(id -g)" -e USER=admin -e HOME=/tmp --tmpfs /tmp \
    -e GOOGLE_APPLICATION_CREDENTIALS=/adc.json \
    -v "$adc/adc.json:/adc.json:ro,z" \
    --entrypoint /usr/local/bin/abfs "$IMG" "$@"
}
```

The directory from `mktemp -d` is 0700, so other host users cannot reach the copy; the bind mount gives the container the file itself. `z` relabels the copy for the container and does nothing on a host without SELinux. Remove the copy with `rm -r "$adc"` after the check.

`init` prints `The GCP server resources are ready.` Its schema update consists of `CREATE ... IF NOT EXISTS` statements, a no-op on a conforming database. Any difference in the two files means the backend did not match; find out why before the servers start.

A one-off pod in the cluster with the Kubernetes service account `abfs-server` can run the same binary, but that account holds only the runtime roles (`roles/spanner.databaseUser`, `roles/bigtable.user`, `roles/storage.objectUser`). `init` needs the admin roles to update the schema and the tables, so it fails there; run it from the workstation.

## Garbage collection

Garbage collection is off by default. Neither backend mode creates its resources, and the chart starts the server without `--gc` (`server.gc.enabled: false`), so the backend has only the schema in [What an existing backend must have](#what-an-existing-backend-must-have). Turned on, garbage collection adds resources of its own, apart from the server's tables:

| Resource | Created by |
| --- | --- |
| Bigtable table `gc` (families `e`, `t`) and the family `g` of `objects` | `components/gc` with `backend-new`, `init --gc` with `backend-existing` |
| the Spanner tables of `casdb/gcschema.sql` of the deployed commit (`GcState`, `GcReplicas`, and more in later releases) | `init --gc`, in both modes |
| objects under `gc/` in the bucket: the condemned lists, and in later releases the records of deletes and removed refs | `abfs server gcp gc run` |

The Spanner tables are never entries of the Config Connector `ddl` list. That list is ordered and append-only, so garbage collector entries in it would collide with the base schema entries of later releases. Config Connector does not read the schema back from Spanner, so tables that `init` adds do not change the `SpannerDatabase`.

To turn it on, follow "Enable or disable garbage collection" in `doc/sysadmin/GCP_GC.md` of the deployed commit (in `source/` of the bundle). In this deployment, its steps 1 and 2 become these three:

1. With `backend-new`, list `components/gc` after `backend-new` in the overlay, apply it, and wait until every object is Ready. Do this before `init --gc`: Config Connector deletes a column family of `objects` that the object does not list, so a family `g` that `init` created would be removed again.
2. Run `init --gc` from the workstation as in [Conformance check](#conformance-check), with `GC=--gc`. It adds the Spanner tables, and with `backend-existing` also the Bigtable parts, so on this run the before and after files differ (with `backend-existing` the describe of the table `gc` before `init` also fails, because the table does not exist yet). Then run the check again with `GC=--gc`; that run must print `conformant`.
3. `helm upgrade` with `server.gc.enabled=true`. A server with `--gc` stops at startup when a garbage collector resource is missing and names it; it also stops when the Bigtable app profile refuses single-row transactions, and prints the `gcloud` command that allows them.

Then continue with the backfill, the client option `gc-keepalive` and the dry runs, as the server documentation describes. On every upgrade while garbage collection is on, run `init --gc` of the new release before the servers: it adds the garbage collector's Spanner tables and columns that the release needs, in both modes. With `backend-new`, Config Connector owns the Bigtable tables and their families, so a Bigtable change of a release comes in `components/gc` or `backend-new` and is applied before `init --gc`; a table or family that `init` created alone would be deleted at the next reconcile.

To turn it off, `helm upgrade` with `server.gc.enabled=false` and turn `gc-keepalive` off on the clients. The resources can stay in place: a server without `--gc` neither reads nor writes them. Keep `components/gc` in the overlay while they exist. Without it, Config Connector deletes the family `objects:g` with the condemn markers in it, and `kubectl apply -k` leaves the component's four objects in the cluster (it does not prune), where the policy of the deleted family fails on every reconcile. Removing the garbage collector resources is not part of this runbook.

## Seed and steady phase

The pushers start in `pusher.phase: seed`: the class `abfs-pusher-seed` (32 vCPU and 240 GB at least, highmem shapes) and `pusher.seedResources` (200Gi by default), sized for the largest repository. The seed of a full Android tree takes hours.

Watch the seed:

```sh
kubectl -n abfs get pods -l app.kubernetes.io/component=pusher -o wide
kubectl -n abfs logs abfs-pusher-0 --tail 50
```

When the initial push of every pusher is complete, move to the steady phase:

```sh
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --set pusher.phase=steady --timeout 30m
```

Put `pusher.phase: steady` into the values file as well, so the next upgrade keeps it. The change gives the pods a new node selector, so the StatefulSet recreates each pusher in turn, highest ordinal first, on a node of `abfs-pusher-steady` in its PVC's zone, with `pusher.steadyResources`. The chart creates the pusher VerticalPodAutoscaler in this phase only, so its history never holds the seed peak. The empty seed nodes are deleted by the autoscaler.

Go back to `phase: seed` before a re-shard, after a lost PVC, or before adding a large new branch: each of these needs the seed memory again.

### The pusher count is fixed

`pusher.count` is fixed after the seed. Each pusher's share of the projects is a rendezvous hash over its index and `pushers-count` of the pool config, which the chart renders from the same value.

- Never `kubectl scale` the StatefulSet. Fewer pods than the pool leaves the projects of the missing pushers unpolled, without an error. More pods than the pool fail their config lookup.
- A change of `pusher.count` is a re-shard: projects move between pushers, which clone them again from Gerrit (hours), and the PVCs must hold the new share. Do it in the seed phase, with `helm upgrade`, never by hand.

With `alerts.enabled=true` the chart creates Managed Service for Prometheus rules that fire when projects of the pool are assigned to no running pusher, or fewer pushers report than `pushers-count`.

### Backend replacement

`pushed-refs` on a pusher's PVC is valid only for the backend it was written against. When the backend is replaced (a new database, instance or bucket), wipe the pusher PVCs before the pushers run against the new one:

```sh
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --set pusher.enabled=false --timeout 30m
kubectl -n abfs delete pvc -l app.kubernetes.io/name=abfs,app.kubernetes.io/component=pusher
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --set pusher.phase=seed --timeout 30m
```

Recycle the CI nodes as well, so that every node cache starts empty (see [Upgrades](#upgrades)).

## CI nodes

### Canary after every control plane upgrade

The `abfs-ci` class holds back upgrades of its existing node pools with `maintenanceExclusion: UNTIL_END_OF_SUPPORT`. The exclusion covers existing CI pools only. A new CI pool, for a new shape or a stockout fallback, comes up on the control plane's newer patch version and can bring a newer kernel. casfs does not build on every kernel series (6.14 and 6.19 fail).

After every control plane upgrade, run one build pod before CI resumes, and record the versions it ran on:

```sh
helm template abfs chart/abfs -n abfs -f <values.yaml> \
  --set ci.exampleJob.enabled=true --set ci.exampleJob.image=<build image> \
  --show-only templates/ci-example-job.yaml | kubectl create -f -
kubectl -n abfs-ci logs -f job/abfs-example-build --all-containers
kubectl -n abfs-ci delete job abfs-example-build
kubectl get nodes -l cloud.google.com/compute-class=abfs-ci \
  -o custom-columns=NAME:.metadata.name,VERSION:.status.nodeInfo.kubeletVersion,KERNEL:.status.nodeInfo.kernelVersion
```

Any CI build pod in your CI system serves as the canary as well. When the loader cannot build or load the module on a node, it stays in CrashLoopBackOff and the build pods on that node fail after `ci.waitForCasfsTimeout` (15 minutes), so the CI system retries them elsewhere. Watch for it:

```sh
kubectl -n abfs-ci get pods -l app.kubernetes.io/component=casfs-loader -o wide
```

For a freeze, a cluster maintenance exclusion of scope `no_upgrades` stops patch upgrades too, for at most 30 days.

### Cache reset

Each CI node has one cache at `/var/lib/abfs/base/ci/cache` on its boot disk. Reset it by removing the cache directory and restarting the node's cacheman pod, never by emptying the directory. A stopped cacheman makes every mount on the node fail, so stop builds on the node first.

A new cacheman that still finds the cache directory skips `abfs init`, so it must never start on a half-deleted cache. The reset pod below therefore renames the directory first, in one atomic step, and deletes the renamed copy after that. A cacheman that restarts during the delete finds no cache and runs `abfs init`. On a cache of about 100 GB the delete takes about a minute. Do not run it with `kubectl debug node/... -it`: without a terminal that command returns while the delete still runs. Run the reset in a pod of its own on the node, wait until the pod has succeeded, so that the disk space is free again, then restart cacheman:

```sh
N=<node>
IMG=<abfs-client image>
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
      image: $IMG
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

`nodeName` places the pod on the cordoned node without the scheduler. It runs as root, which the `privileged` level of `abfs-ci` allows. If the wait times out or the pod fails, read `kubectl -n abfs-ci logs abfs-cache-reset`, and do not restart cacheman until `/var/lib/abfs/base/ci/cache` is gone. A second run of the pod finishes the delete of an earlier one. The new cacheman pod runs `abfs init` when the cache directory is missing; its log shows `Initialization for instance 'ci' complete`. Deleting the node resets its cache too: the cache lives on the boot disk and goes away with the node.

## Upgrades

```sh
helm upgrade abfs chart/abfs -n abfs -f <values.yaml> --timeout 30m
```

Without `--wait`, as for the install. The bootstrap Job runs again after each upgrade and rewrites the two configs when they changed.

- Server: a rolling update with one extra replica, never one fewer. Each pod drains for 120 s before SIGTERM (`server.preStopSleepSeconds`). The pod gets 240 s in all (`server.terminationGracePeriodSeconds`): the 120 s, up to 105 s for the server's own stop with `server.gc.enabled`, and 15 s of margin.
- Pushers: a rolling update, one pod at a time, so two releases run in the pool for a while. Pushers are not checked against each other. When the new release changes how pushers split the projects or read the pool config, stop the pool first: `helm upgrade` with `--set pusher.enabled=false`, then the upgrade with the new images and `pusher.enabled` true. The PVCs stay.
- casfs loader: a rolling update. A loaded module stays loaded: it cannot be removed while a casfs filesystem exists, and `casfs_max_mounts` is read-only after load. A new casfs version or a new `ci.casfs.maxMounts` takes effect on new nodes only.
- cacheman: `updateStrategy: OnDelete`. A stopped cacheman makes every mount on its node refuse reads and writes, so the DaemonSet never restarts it by itself. Drain or recycle the CI nodes: either cordon a node, wait until its builds end and delete its cacheman pod as in [Cache reset](#cache-reset) (without the reset pod), or cordon the old nodes and let the autoscaler remove them once they are empty. Each cacheman pod copies its tool set to `/var/lib/abfs/bin` when it starts, so the tools on a node always match its cacheman.

### Spanner schema changes

The `ddl` list of `kcc/components/backend-new/spanner-database-ddl.yaml` is append-only. Config Connector sends only the entries past the old list length and refuses a changed entry. Never edit or remove an entry. A later schema change is a new `ALTER` or `CREATE ... IF NOT EXISTS` entry appended at the end by hand, in the same `- |-` form. Check the list before applying:

```sh
make ddl-check
```

The file's `# ddl-base:` line holds the sha256 and the count of the entries of the first release, so the check needs no schema source. That line can be changed together with an entry, so the release check compares the list with the GCP server schema of the ABFS commit that first created the database: `make ddl-check DDL_REPO=<abfs repo> DDL_SOURCE=<commit>`.

The check guards only the base. Appended migrations are reviewed by hand: in the review of each change to the file, an entry that was already applied must show no change, and a new migration may only add lines at the end of the list. A missed edit of an applied entry does not reach the database: Config Connector refuses the changed list, and the `SpannerDatabase` reports an update error until the entry is restored. `scripts/gen-ddl.sh` writes the list once from that schema (`casdb/schema.sql` of a source tree or a commit that has the GCP server), for the release that first creates the database; never run it over the file again. With `backend-existing`, schema changes come from running `abfs server gcp init` of the new release. The garbage collector's Spanner tables are in neither: with garbage collection on, `init --gc` of the new release adds their changes in both modes ([Garbage collection](#garbage-collection)).

A `backend-new` database created from an earlier tree whose `spanner-database-ddl.yaml` has 7 entries (the base and `GcState`, `GcReplicas`) keeps that list: entries are never removed, and a shorter list makes Config Connector refuse the `SpannerDatabase` update. Copy the earlier file over `kcc/components/backend-new/spanner-database-ddl.yaml` of the new release before applying, and list `components/gc` too, because that backend's table `objects` has the family `g`, which Config Connector would otherwise delete.

## Config Connector upgrade and rollback

Upgrade with the operator file of the newer bundle. Keep each bundle: the uninstall of a version needs its files.

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

A full downgrade of the operator and its CRDs is not supported: it means uninstall, install and apply the objects again. From 1.123.1, in namespaced mode, the controller of a namespace can be rolled back by setting `spec.version` of its `ConfigConnectorContext`. Google's page allows at most 3 minor versions back, always to the latest patch of that minor. The CRD of 1.160.0 is stricter: it supports only the operator version and the previous minor version. With this operator the rollback therefore goes back one minor at most, to the latest patch of that minor:

```sh
kubectl -n abfs patch configconnectorcontext configconnectorcontext.core.cnrm.cloud.google.com \
  --type merge -p '{"spec":{"version":"<previous version>"}}'
```

The KCC base does not set `spec.version`, so a later `kubectl apply -k` keeps the patched value. Remove it with `-p '{"spec":{"version":null}}'` to run the operator's version again.

Never delete the `ConfigConnectorContext` while KCC objects exist in the namespace, for an upgrade or for any other reason. Delete the objects first, as in [Teardown](#teardown).

## Teardown

1. Uninstall the release, and wait until the ABFS nodes are gone:

   ```sh
   helm uninstall abfs -n abfs --timeout 30m
   kubectl get nodes -l cloud.google.com/compute-class
   ```

   This removes the Gateway and its load balancer, and the ComputeClasses. The Gateway address stays reserved. The pusher PVCs and the namespace `abfs-ci` stay.

2. Delete the KCC objects first, then the rest of the overlay (the namespace and its `ConfigConnectorContext`). Config Connector must still run while it deletes its objects, so never delete the `ConfigConnectorContext` while KCC objects exist in the namespace:

   ```sh
   kubectl -n abfs delete gcp --all --wait --timeout 30m
   kubectl delete -k kcc/overlays/<name> --ignore-not-found
   ```

   The first command has already removed the KCC objects of the overlay; `--ignore-not-found` keeps the second from printing a `NotFound` error for each of them and exiting 1. The namespace `abfs` then stays `Terminating` for about a minute.

   Every data resource and the Gateway address carry `cnrm.cloud.google.com/deletion-policy: abandon`, so the Spanner instance and database, the Bigtable instance, tables and policies, the bucket, the proxy-only subnet, the Gateway address and the enabled APIs stay, with their data. The address stays because its IP is in the client config of every pusher and CI node, and a new address gets another IP. The two IAM service accounts and every IAM grant are deleted: they hold no data, and the next apply creates them again. Workloads that still run with them lose their roles at once.

3. Delete the pusher PVCs and the CI namespace:

   ```sh
   kubectl -n abfs delete pvc -l app.kubernetes.io/name=abfs,app.kubernetes.io/component=pusher
   kubectl delete namespace abfs-ci
   ```

4. To remove the data too, delete the abandoned resources with gcloud. A new Spanner instance gets Spanner's default automated daily backups, and an instance cannot be deleted while it has backups, so delete them first:

   ```sh
   gcloud spanner backups list --instance abfs --project example-project --format 'value(name.basename())' |
     xargs -r -n 1 gcloud spanner backups delete --instance abfs --project example-project --quiet
   gcloud spanner instances delete abfs --project example-project
   gcloud bigtable instances delete abfs-objects --project example-project
   gcloud storage rm --recursive gs://example-project-abfs-blobs
   gcloud compute addresses delete abfs-gateway --region europe-west4 --project example-project
   ```

## Bundle and source tree

The release bundle holds this folder's `AGENTS.md`, `README.md`, `Makefile`, `chart/`, `kcc/`, `docs/` and `scripts/`, the four images as OCI archives, `images.txt`, `source/` and `SHA256SUMS`. `source/` holds the ABFS tree of the bundle's commit and the abfs-casfs tree of the loader image; unpack both to build the images yourself. The `abfs-client` image carries the source packages of the static `ps` and `fuser` it ships, in `/usr/share/abfs/source`. Check the bundle with `sha256sum -c SHA256SUMS` in its root. From the bundle root, these run as they do in `deploy/gke`:

- every `kubectl` and `helm` command of this runbook, and `scripts/load-images.sh`;
- `make check` (lint, template, kustomize, and `ddl-check` against the hash in the ddl file) and `scripts/check-ddl.sh` without a schema source.

These need the ABFS source tree, a clone of the repository at the deployed commit:

- `scripts/build-images.sh` and `make images`: they build from the repository and the abfs-casfs repository, with the Dockerfiles in `images/`, so run the copy in `deploy/gke` of the clone; `-C` only names another ABFS repository to build from. With a bundle, load the archives of `images/` with `scripts/load-images.sh` instead, or build from `source/` as below;
- `scripts/bundle.sh` and `make bundle`; the copy in the bundle runs with `-C <clone>`, which supplies the commit and its `deploy/gke`;
- `scripts/gen-ddl.sh`, `make ddl`, and `make ddl-check` with `DDL_SOURCE`: they read the GCP server schema files.

The trees in `source/` carry no git history, and the scripts take the version from a git tag. To build the images from them, make each tree a repository and tag the ABFS one with the bundle's version:

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

The images then report the same version as the bundle's.

## Security

The OSS server has no authentication and no TLS by default. Any host that reaches the Gateway address can read and write the whole store. The network is the only boundary:

- The Gateway address is internal and regional, and `gateway.allowGlobalAccess` is false.
- The NetworkPolicy lets only the proxy-only subnet, the health check range, the bootstrap Job and the Helm test pod reach the server pods. It needs Dataplane V2.
- Keep the VPC's firewall rules and routes so that only the CI nodes and the admin hosts reach the address.

Other points:

- Only the server's Google service account holds Google roles: `roles/spanner.databaseUser`, `roles/bigtable.user` and `roles/storage.objectUser` on the three resources. `init` runs with the admin's own credentials.
- The Gerrit credentials live in one Secret, mounted only into the pushers.
- The casfs loader (privileged, host root, host network) and the build pods (privileged) run in `abfs-ci`. cacheman runs there unprivileged, with a root init container for its directories. The server, the pushers and the bootstrap Job run without privileges in `abfs`; the pushers run as root, which the `baseline` level allows.
- Every build on a CI node reads the same cache, and the kernel trusts cached content by name. Give each tenant its own CI class and node pools.
- The server's HTTP debug port listens on 127.0.0.1 only, and the pushers' `/info` port is not behind the Gateway.

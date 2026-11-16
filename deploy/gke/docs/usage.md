# ABFS on GKE / How it is used

This deployment serves CI builds of an Android platform tree on GKE. Read this page first. The install is in [install](install.md), operations in the [runbook](runbook.md), the values in [configuration](configuration.md) and the build pod rules in [ci-job](ci-job.md). Developer workstations are not covered ([Not covered](#not-covered)).

## The parts

| Part | Where it runs | What it holds | Lifetime |
| --- | --- | --- | --- |
| Store | `abfs-server` replicas behind the Gateway, with Spanner, Bigtable and a Cloud Storage bucket | source, uploaded build outputs, build cache entries, the pool and client configs | durable; survives every upgrade and a `helm uninstall` |
| Git-pushers | `abfs-pusher` StatefulSet, one PersistentVolumeClaim each | clones of the pusher's projects and the refs it has pushed | durable with the PVC |
| CI node | a node of the class `abfs-ci` | the casfs kernel module, one cacheman, the ABFS tools in `/var/lib/abfs/bin`, and the node cache on the boot disk | the node; the cache goes away with it |
| Build pod | your CI system, on a CI node | one mounted workspace and the build's `out/` directory | one job |

The server replicas keep no state: refs live in Spanner, objects in Bigtable, large blobs in the bucket. A replica can stop at any time without loss.

The pushers are the only writers of source: they fetch from Gerrit and push to the store. Builds write build outputs and build cache entries, never source.

## How a build uses ABFS

A build pod runs four steps ([ci-job](ci-job.md#build-steps)):

1. **`abfs ready -b <branch> --wait -o pinned.xml`** checks that every project of the branch's manifest is in the store, and writes a pinned manifest: one fixed commit per project. It waits while the pushers catch up.
2. **`abfs mount -f pinned.xml --tmp-ws <dir>`** mounts the tree in seconds. casfs fetches a file's content only when a process reads it.
3. **The build.** `abfs setup` (run by the mount) points Soong at `ninjago`, the ABFS build of ninja. For each step `ninjago` looks up a cache key of the step's inputs and command. On a hit it puts the outputs into `out/` without running the step. On a miss it runs the step and uploads the result.
4. **`abfs unmount <dir>`.** With `--tmp-ws`, nothing of the workspace is kept. The build cache entries and outputs stay in the store.

Each read and each upload goes through the node's cacheman:

```
build pod                     CI node                          store
  read file  ---> casfs ---> cacheman  --- miss --->  Gateway ---> server ---> Bigtable / bucket
  ninjago    ----------------> cacheman  <-- content ---
                               node cache (boot disk)
```

There are two cache levels:

- **The store** is shared by every build and lasts.
- **The node cache** holds the content that builds on that node have read. A new node starts with an empty node cache, but its builds still hit the store's build cache; they only fetch more.

As a guide for a full Android platform build: against a warm store, a node with an empty node cache fetches a few GiB and finishes in minutes; a node with a warm node cache fetches almost nothing and finishes in under a minute.

Build cache keys differ per lunch target (`TARGET_PRODUCT` and variant) and include the mount path of many steps. Every build pod mounts at the same path (`/work/src` in the example Job); outputs built at another path, for example on a workstation, do not serve these builds ([ci-job](ci-job.md#build-cache-hits)).

## Lifecycle

### Initial deployment

1. **Install.** Config Connector creates the backend, the chart creates the rest, and the bootstrap Job writes the pool config and the client config into the store ([install](install.md)). No CI node exists yet.
2. **Seed.** The pushers start in the phase `seed`, on large highmem nodes, and clone and push every project of the configured manifest branches. On a full Android platform tree this takes hours, and the pusher that holds the largest repository needs close to the full seed memory (`pusher.seedResources`, 200Gi).
3. **First builds.** `abfs ready --wait` holds each build until its branch is complete, so the CI system can start before the seed ends. The first build of each lunch target runs every step and fills the cache; plan for it to take as long as a cold build, or longer. Builds of the same target that run at the same time all miss; start one build per target first when the farm is large.
4. **Steady phase for the pushers.** When every pusher has finished its first push, switch `pusher.phase` to `steady` ([runbook](runbook.md#seed-and-steady-phase)). The pushers move to smaller nodes.

After that the deployment is in steady state.

### Steady state

- **Source follows Gerrit.** Each pusher checks the manifest (`check-interval`) and fetches its projects (`push-interval`) every minute. A merged change reaches the store a minute or more after the merge; `abfs ready --wait` covers the delay.
- **Builds mostly hit.** The hit ratio is printed at the end of each ninja phase (`cache: hits: ..., misses: ..., ratio: ...`). A low ratio on a build that should be warm points to a build that is not deterministic, a new mount path, or a lunch target that nobody built yet.
- **CI nodes follow the load.** GKE creates CI nodes for pending build pods and removes nodes under-used for 30 minutes ([Build capacity](#build-capacity)). A removed node loses its node cache, so a farm quiet at night starts each day with empty node caches against a warm store.
- **The store grows.** Garbage collection is off by default (`server.gc.enabled`), so nothing is deleted. Watch the Bigtable and bucket sizes and plan storage for this.

### Changes in steady state

| Change | What to do | Cost |
| --- | --- | --- |
| A new manifest branch | add it to `pusher.pool.branchFiles` and run `helm upgrade`. For a branch that brings many new projects or a large new repository, switch to `pusher.phase: seed` first and back to `steady` after its push. | the push of the new projects; the first builds of the branch fill the cache |
| A new lunch target | nothing; the first build of the target fills its cache entries | one cold build per target |
| More or fewer pushers | change `pusher.count` in the seed phase with `helm upgrade`, never with `kubectl scale` ([runbook](runbook.md#the-pusher-count-is-fixed)) | projects move between pushers and are cloned again from Gerrit: hours |
| A larger build pod | change the request in your CI system, then check `ci.casfs.maxMounts` and the node sizes ([ci-job](ci-job.md#sizing)) | new nodes of a larger shape |
| A new backend | wipe the pusher PVCs, seed again, recycle the CI nodes ([runbook](runbook.md#backend-replacement)) | a full seed, and cold builds until the cache is filled again |

## Scaling

The defaults use generally available GKE features; [configuration](configuration.md#right-sizing) lists the values and the Preview options.

| Part | Scales on | Range by default | Up | Down |
| --- | --- | --- | --- | --- |
| CI nodes | pending build pods | no cap by default; the project's Compute Engine quota is the bound, or an opt-in `CapacityQuota` ([configuration](configuration.md#ci-node-cap)) | a node per pending pod that fits nowhere, 32 vCPU and 120 GB at least | after 30 minutes under the threshold, when no build pod runs on it |
| Server replicas | CPU, 60 % of the request | 3 to 10 | HPA adds replicas, GKE adds nodes | after 30 minutes of low load the HPA removes one replica per 5 minutes, GKE removes the empty nodes |
| Bigtable (new backend) | CPU, 60 % | 1 to 3 nodes | autoscaling | autoscaling |
| Spanner | none | 100 processing units | by hand | 100 is the smallest size |
| Pushers | none | `pusher.count`, fixed | the seed phase for large pushes | the steady phase after them |
| Gateway | managed by Google Cloud | | | |

### Build capacity

The CI system decides how many builds run. Each build pod requests CPU, memory and disk (`ephemeral-storage`, 400Gi), and GKE creates nodes of the class `abfs-ci` that fit them. The boot disk size sets how many builds fit on a node ([node disk](ci-job.md#node-disk)). With the example Job's 28 CPU and 80Gi, each pod gets a 32-vCPU node; two such pods need a 64-vCPU node ([ci-job](ci-job.md#sizing)). The class falls back over four machine families when one has no capacity in the zone.

Builds on one node share its node cache, so larger nodes with several builds fetch less but need more casfs kernel memory and a larger boot disk. Smaller nodes follow the load more finely. The `optimize-utilization` profile places new builds on busy nodes first.

Nodes go away only when no build runs on them (build pods carry `safe-to-evict: "false"`), after 30 minutes (`computeClasses.ci.consolidationDelayMinutes`). A longer delay keeps more warm nodes at the price of idle nodes. To keep a fixed number of nodes warm, add a node pool of your own with a minimum size under `computeClasses.ci.nodepools`; the class tries it first.

Use Spot nodes (`computeClasses.ci.spot`) only when the CI system retries a build whose node was preempted.

### Server and backend

As a planning figure, one 16-vCPU replica carries about 10 warm builds at once at the HPA's 60 % target, so 3 to 10 replicas cover about 30 to 100 concurrent warm builds.

Bigtable reaches its limit first: plan on its default cap of 3 nodes (`backend-new`) being reached at about 16 concurrent warm builds. Raise the cap in the KCC overlay for a larger farm; each node is a fixed hourly cost. Spanner holds only refs and stays at 100 processing units.

The servers run in the zone of the Bigtable cluster. CI nodes run in any zone of the region.

### Pushers

Each pusher owns a fixed share of the projects, chosen by a hash over its index and the pool size, and keeps clones of them on its PVC. The pushers scale in size, not in count: large nodes in the seed phase, smaller ones with a memory VPA in the steady phase ([runbook](runbook.md#seed-and-steady-phase)).

## Upgrades

### Order

Upgrade from the store outward:

1. **KCC overlay and backend schema.** Apply the new release's KCC overlay and wait until every object is Ready ([runbook](runbook.md#upgrades)). With `backend-new`, append the new release's schema entries to the KCC ddl list ([runbook](runbook.md#spanner-schema-changes)). With `backend-existing`, run the new release's `abfs server gcp init` ([runbook](runbook.md#conformance-check)). With garbage collection on, also apply the new release's `components/gc` (with `backend-new`), then run its `init --gc` in both modes ([runbook](runbook.md#garbage-collection)).
2. **Server.** `helm upgrade` with the new images. The replicas roll one at a time with one extra replica; each drains for 120 s. Running builds continue.
3. **Pushers.** They roll in the same `helm upgrade`, one pod at a time. Pushers of one pool must run the same release, and they are not checked against each other. When the new release changes how pushers split the projects or read the pool config, stop the pool first (`pusher.enabled=false`, then the upgrade with `true`), so two releases never run in one pool.
4. **CI nodes.** Each cacheman keeps its old release until its pod is deleted (`updateStrategy: OnDelete`), because a stopped cacheman stops every mount on its node. Recycle the nodes: cordon the old nodes and let them empty, or drain them one by one ([runbook](runbook.md#upgrades)). A new casfs version takes effect on new nodes only.

Upgrade the server before the clients. A server refuses a client older than it supports, so a release that raises the protocol fails builds on nodes with the old cacheman. Recycle the CI nodes soon after a server upgrade. A build pod's tools come from its node, so old and new nodes can run builds side by side.

### What a new release can change

- **The node cache format.** cacheman then empties its cache on start.
- **The build cache key format.** Builds of the new release miss until they have filled the cache again. Plan an upgrade that changes it like a [new deployment's first builds](#initial-deployment).

### GKE and Config Connector upgrades

- **GKE.** The class `abfs-ci` holds back node upgrades of existing CI pools, because a new node image can change or drop the casfs module (with `ci.casfs.provider: daemonset`, casfs does not build on every kernel series). A new CI pool comes up at the control plane's version. After every control plane upgrade, run one canary build before CI resumes ([runbook](runbook.md#canary-after-every-control-plane-upgrade)). The CI system retries builds whose node auto-repair replaced ([ci-job](ci-job.md#build-pod-contract)). Server and pusher nodes take surge upgrades in the cluster's maintenance window; the PodDisruptionBudgets keep all but one replica of each running ([prerequisites](prerequisites.md#node-upgrades)).
- **Config Connector** is installed by hand at a pinned version and upgraded with the operator file of the next version ([runbook](runbook.md#config-connector-upgrade-and-rollback)). Its upgrade does not touch the workloads.

## What to watch

| Signal | Where | What it tells you |
| --- | --- | --- |
| Cache hit ratio of each build | the end of each ninja phase in the build log | a drop without an upgrade or a new target needs a look |
| Pending build pods and CI node count | `kubectl -n abfs-ci get pods`, the cluster autoscaler events | whether nodes keep up with the CI load, and stockouts of a machine family |
| casfs loader state | `kubectl -n abfs-ci get pods -l app.kubernetes.io/component=casfs-loader` | a node without a matching casfs module; its builds fail after 15 minutes and must be retried elsewhere |
| Server CPU and replica count | the HPA, Cloud Monitoring | whether the servers sit at their maximum |
| Bigtable CPU and node count | Cloud Monitoring | whether Bigtable sits at its cap, the first limit of a growing farm |
| Store size | Bigtable storage, the bucket size | growth without garbage collection |
| Pusher pool | `alerts.enabled` rules, the pusher logs | projects that no running pusher serves, and pusher restarts, usually OOM kills |
| CI node disk | `alerts.enabled` rule `AbfsNodeDiskHigh`, or the query in [node disk](ci-job.md#node-disk) | a boot disk that fills with build outputs and cache; kubelet evicts pods at 90 % |
| cacheman memory | the VerticalPodAutoscaler `abfs-cacheman` ([configuration](configuration.md#right-sizing)) | the memory request that cacheman needs under your load |

Every workload sends OpenTelemetry metrics and traces to `otel.endpoint`.

## Not covered

- **Developer workstations.** A workstation can use the same store through the Gateway address when the network lets it in, but its builds use another mount path and share no build cache entries with CI. This deployment does not set such clients up.
- **Builds of unmerged changes.** The pool pushes branch heads and tags (`pusher.pool.fetchRefSpec`). How a presubmit build brings in a change that is not merged is not covered here.
- **Garbage collection.** It is off by default, so the store only grows.
- **Authentication and TLS.** The network is the only boundary ([runbook](runbook.md#security)).

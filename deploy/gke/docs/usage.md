# ABFS on GKE / How it is used

This page explains how ABFS is meant to be used on this deployment: what each part holds, how a build uses it, what happens in the first days and in steady state, how each part scales, and how upgrades work. The commands are in the [runbook](runbook.md), the values in [configuration](configuration.md) and the build pod rules in [ci-job](ci-job.md). Read this page first.

The deployment serves CI builds of an Android platform tree on GKE. Developer workstations are not part of it (see [Not covered](#not-covered)).

## The parts

| Part | Where it runs | What it holds | Lifetime |
| --- | --- | --- | --- |
| Store | `abfs-server` replicas behind the Gateway, with Spanner, Bigtable and a Cloud Storage bucket | the source of every pushed project, every build output that a build uploaded, the build cache entries, the configs of the pusher pool and the clients | durable; survives every upgrade and a `helm uninstall` |
| Git-pushers | `abfs-pusher` StatefulSet, one PersistentVolumeClaim each | a git clone of each project in the pusher's share, and the list of refs it has pushed | durable with the PVC |
| CI node | a node of the class `abfs-ci` | the casfs kernel module, one cacheman, the ABFS tools in `/var/lib/abfs/bin`, and the node cache on the boot disk | the node; the cache goes away with it |
| Build pod | your CI system, on a CI node | one mounted workspace and the build's `out/` directory | one job |

The server replicas keep no state of their own. Refs live in Spanner, objects in Bigtable, large blobs in the bucket. Each replica has in-memory caches of object headers and contents, which fill as it serves. Any replica can answer any call, so the Gateway spreads the load over all of them, and a replica can stop at any time without loss.

The pushers are the only writers of source. They fetch from Gerrit and push to the store. Builds write build outputs and build cache entries, never source.

## How a build uses ABFS

A build pod runs four steps ([ci-job](ci-job.md#build-steps)):

1. **`abfs ready -b <branch> --wait -o pinned.xml`** checks that every project of the branch's manifest is in the store, and writes a pinned manifest: one fixed commit per project. It waits while the pushers catch up.
2. **`abfs mount -f pinned.xml --tmp-ws <dir>`** mounts the tree. The mount takes seconds and copies nothing: casfs shows the whole tree at once, and fetches a file's content only when a process reads it.
3. **The build.** `abfs setup` (run by the mount) points Soong at `ninjago`, the ABFS build of ninja. For each build step `ninjago` looks up a cache key made of the step's inputs and command. On a hit it puts the outputs into `out/` without running the step; their content again loads on first read. On a miss it runs the step and uploads the result and its outputs, so the next build with the same inputs hits.
4. **`abfs unmount <dir>`.** With `--tmp-ws`, nothing of the workspace is kept. The build cache entries and outputs stay in the store.

Each read and each upload goes through the node's cacheman:

```
build pod                     CI node                          store
  read file  ---> casfs ---> cacheman  --- miss --->  Gateway ---> server ---> Bigtable / bucket
  ninjago    ----------------> cacheman  <-- content ---
                               node cache (boot disk)
```

So there are two cache levels:

- **The store** is shared by every build and lasts. A warm store means that most build steps hit.
- **The node cache** holds the content that builds on that node have read. It saves the network fetch for the next build on the same node. A new node starts with an empty node cache, but its builds still hit the store's build cache; they only fetch more.

As a guide for a full Android platform build: against a warm store, a node with an empty node cache fetches a few GiB and finishes in minutes; a node with a warm node cache fetches almost nothing and finishes in under a minute.

Build cache keys depend on more than the source. They differ per lunch target (`TARGET_PRODUCT` and variant) and include the mount path of many steps. Every build pod therefore mounts at the same path (`/work/src` in the example Job); outputs built at another path, for example on a workstation, do not serve these builds ([ci-job](ci-job.md#build-cache-hits)).

## Lifecycle

### Initial deployment

1. **Install.** Config Connector creates the backend, the chart creates the server, the Gateway, the pushers and the CI setup, and the bootstrap Job writes the pool config and the client config into the store ([runbook](runbook.md#install)). No CI node exists yet.
2. **Seed.** The pushers start in the phase `seed`, on large highmem nodes, and clone and push every project of the configured manifest branches. On a full Android platform tree this takes hours, and the pusher that holds the largest repository needs close to the full seed memory (`pusher.seedResources`, 200Gi).
3. **First builds.** `abfs ready --wait` holds each build until its branch is complete, so the CI system can start before the seed ends. The store has source but no build cache yet, so the first build of each lunch target runs every step and fills the cache. Plan for the first builds to take as long as a normal cold build, or longer, since they also upload. Builds of the same target that run at the same time all miss; start one build per target first when the farm is large.
4. **Steady phase for the pushers.** When every pusher has finished its first push, switch `pusher.phase` to `steady` ([runbook](runbook.md#seed-and-steady-phase)). The pushers move to smaller nodes.

After that the deployment is in steady state.

### Steady state

- **Source follows Gerrit.** Each pusher checks the manifest every minute (`check-interval`) and fetches its projects every minute (`push-interval`). A merged change reaches the store a minute or more after the merge, depending on the size of the push. `abfs ready --wait` in each job covers the delay.
- **Builds mostly hit.** A build of a commit close to an earlier build hits every step whose inputs did not change and runs the rest. The hit ratio is printed at the end of each ninja phase (`cache: hits: ..., misses: ..., ratio: ...`). A low ratio on a build that should be warm points to a build that is not deterministic, a new mount path, or a lunch target that nobody built yet.
- **CI nodes follow the load.** GKE creates CI nodes for pending build pods and removes nodes that stay under-used for 30 minutes (see [Build capacity](#build-capacity)). A node that is removed loses its node cache. A farm that is busy during the day and quiet at night therefore starts each day with new nodes and empty node caches, against a warm store.
- **The store grows.** Garbage collection is off by default (`server.gc.enabled`), so nothing is ever deleted: every pushed commit and every uploaded build output stays. Watch the Bigtable and bucket sizes and plan storage for this.

### Changes in steady state

| Change | What to do | Cost |
| --- | --- | --- |
| A new manifest branch | add it to `pusher.pool.branchFiles` and run `helm upgrade`. For a branch that brings many new projects or a large new repository, switch to `pusher.phase: seed` first and back to `steady` after its push. | the push of the new projects; the first builds of the branch fill the cache |
| A new lunch target | nothing; the first build of the target fills its cache entries | one cold build per target |
| More or fewer pushers | change `pusher.count` in the seed phase with `helm upgrade`, never with `kubectl scale` ([runbook](runbook.md#the-pusher-count-is-fixed)) | projects move between pushers and are cloned again from Gerrit: hours |
| A larger build pod | change the request in your CI system, then check `ci.casfs.maxMounts` and the node sizes ([ci-job](ci-job.md#sizing)) | new nodes of a larger shape |
| A new backend | wipe the pusher PVCs, seed again, recycle the CI nodes ([runbook](runbook.md#backend-replacement)) | a full seed, and cold builds until the cache is filled again |

## Scaling

Each part scales on its own signal. The defaults use generally available GKE features; [configuration](configuration.md#right-sizing) lists the values and the Preview options.

| Part | Scales on | Range by default | Up | Down |
| --- | --- | --- | --- | --- |
| CI nodes | pending build pods | no limit in the chart; the cluster's autoscaling limits apply | a node per pending pod that fits nowhere, 32 vCPU and 120 GB at least | after 30 minutes under the threshold, when no build pod runs on it |
| Server replicas | CPU, 60 % of the request | 3 to 10 | HPA adds replicas, GKE adds nodes | HPA removes replicas, GKE removes the empty nodes |
| Bigtable (new backend) | CPU, 60 % | 1 to 3 nodes | autoscaling | autoscaling |
| Spanner | none | 100 processing units | by hand | 100 is the smallest size |
| Pushers | none | `pusher.count`, fixed | the seed phase for large pushes | the steady phase after them |
| Gateway | managed by Google Cloud | | | |

### Build capacity

The CI system decides how many builds run. Each build pod requests CPU and memory, and GKE creates nodes of the class `abfs-ci` that fit the pods it cannot place. The node shape follows the build pod request: with the example Job's 28 CPU and 80Gi, each pod gets a 32-vCPU node of its own; two such pods need a 64-vCPU node ([ci-job](ci-job.md#sizing)). The class tries four machine families in order and falls back to the next one when a family has no capacity in the zone.

All build pods of one node share its cacheman and its node cache. This gives the choice between node sizes:

- **Larger nodes, several builds each.** Builds on one node share the content one of them fetched, so the farm fetches less. Each node needs more kernel memory for casfs (one workspace per build) and its boot disk carries all of its builds.
- **Smaller nodes, one build each.** Capacity follows the load more finely, and a removed node takes less warm cache with it. Each node fetches its own working set.

The cluster's `optimize-utilization` profile places new builds on busy nodes first, so builds pack onto warm node caches.

Nodes go away only when no build runs on them: build pods carry `safe-to-evict: "false"`. Consolidation waits 30 minutes (`computeClasses.ci.consolidationDelayMinutes`), so a node that ran a build in the last half hour usually stays for the next one. A longer delay keeps more warm nodes over quiet periods at the price of idle nodes. To keep a fixed number of nodes warm at all times, add a node pool of your own with a minimum size under `computeClasses.ci.nodepools`; the class tries it first.

Spot nodes (`computeClasses.ci.spot`) lower the cost of CI nodes. Use them only when the CI system retries a build whose node was preempted.

### Server and backend

The servers scale out on CPU. As a planning figure, one replica of 16 vCPU carries about 10 warm builds at once at the HPA's 60 % target, so the default range of 3 to 10 replicas covers about 30 to 100 concurrent warm builds. A replica can stop in the middle of a seed or a build without an error.

Bigtable reaches its limit first: plan on its default cap of 3 nodes (`backend-new`) being reached at about 16 concurrent warm builds. Raise the cap in the KCC overlay for a larger farm; each node is a fixed hourly cost. Spanner holds only refs and stays at 100 processing units.

The load on the store is highest when many new nodes start at once, for example at the start of a working day: each new node fetches its working set, and builds of new commits upload their results. The server HPA and Bigtable autoscaling follow this. A warm node cache serves its node without any fetch, so fewer new nodes means less load.

The servers run in the zone of the Bigtable cluster. CI nodes run in any zone of the region.

### Pushers

The pushers do not scale with the load. Each one owns a fixed share of the projects, chosen by a hash over its index and the pool size, and keeps clones of those projects on its PVC. Their load comes from Gerrit traffic, which is steady compared with the build load. They scale in size, not in count: large nodes in the seed phase, smaller ones with a memory VPA in the steady phase ([runbook](runbook.md#seed-and-steady-phase)).

## Upgrades

### Order

Upgrade from the store outward:

1. **Backend schema.** With `backend-new`, append the new release's schema entries to the KCC ddl list ([runbook](runbook.md#spanner-schema-changes)). With `backend-existing`, run the new release's `abfs server gcp init` ([runbook](runbook.md#conformance-check)). With garbage collection on, also apply the new release's `components/gc` (with `backend-new`), then run its `init --gc` in both modes ([runbook](runbook.md#garbage-collection)).
2. **Server.** `helm upgrade` with the new images. The server replicas roll one at a time with one extra replica, never one fewer; each drains for 120 s before it stops. Running builds continue: the client retries a call that a drain cut.
3. **Pushers.** They roll in the same `helm upgrade`, one pod at a time. Pushers of one pool must run the same release, and they are not checked against each other. When the new release changes how pushers split the projects or read the pool config, stop the pool first (`pusher.enabled=false`, then the upgrade with `true`), so two releases never run in one pool.
4. **CI nodes.** The upgrade changes the cacheman DaemonSet, but each cacheman keeps running its old release until its pod is deleted (`updateStrategy: OnDelete`), because a stopped cacheman stops every mount on its node. Recycle the nodes: cordon the old nodes and let them empty, or drain them one by one ([runbook](runbook.md#upgrades)). A new casfs version takes effect on new nodes only: the loader never removes a loaded module, which cannot be removed while a workspace is mounted anyway.

The server must be upgraded before the clients. Server and clients check each other's protocol version on every call, and a server refuses a client that is older than it supports, so a release that raises the protocol fails builds on nodes that still run the old cacheman. Recycle the CI nodes soon after a server upgrade.

The tools that a build pod uses come from its node, not from the build image. A node's tools always match its cacheman, so old and new nodes can run builds side by side during the upgrade.

### What a new release can change

- **The node cache format.** cacheman then empties its cache on start and fetches again. Recycled nodes start empty anyway.
- **The build cache key format.** Builds of the new release miss until they have filled the cache again. Plan an upgrade that changes it like a [new deployment's first builds](#initial-deployment).

### GKE and Config Connector upgrades

- **GKE.** The class `abfs-ci` holds back node upgrades of existing CI pools, because casfs does not build on every kernel series. A new CI pool still comes up at the control plane's version. After every control plane upgrade, run one canary build before CI resumes ([runbook](runbook.md#canary-after-every-control-plane-upgrade)). Server and pusher nodes take upgrades normally; the PodDisruptionBudgets keep all but one replica of each running.
- **Config Connector** is installed by hand at a pinned version and upgraded with the operator file of the next version ([runbook](runbook.md#config-connector-upgrade-and-rollback)). Its upgrade does not touch the workloads.

## What to watch

| Signal | Where | What it tells you |
| --- | --- | --- |
| Cache hit ratio of each build | the end of each ninja phase in the build log | whether builds reuse the store; a drop after an upgrade or a new target is expected, a drop without either is not |
| Pending build pods and CI node count | `kubectl -n abfs-ci get pods`, the cluster autoscaler events | whether nodes keep up with the CI load, and stockouts of a machine family |
| casfs loader state | `kubectl -n abfs-ci get pods -l app.kubernetes.io/component=casfs-loader` | a node whose kernel casfs does not build on; its builds fail after 15 minutes and must be retried elsewhere |
| Server CPU and replica count | the HPA, Cloud Monitoring | whether the servers sit at their maximum |
| Bigtable CPU and node count | Cloud Monitoring | whether Bigtable sits at its cap, the first limit of a growing farm |
| Store size | Bigtable storage, the bucket size | growth without garbage collection |
| Pusher pool | `alerts.enabled` rules, the pusher logs | projects that no running pusher serves |

Every workload sends OpenTelemetry metrics and traces to the managed collector (`otel.endpoint`).

## Not covered

- **Developer workstations.** A workstation can use the same store through the Gateway address when the network lets it in. Its builds use another mount path, so they do not share build cache entries with the CI builds. This deployment does not set such clients up.
- **Builds of unmerged changes.** The pool pushes branch heads and tags (`pusher.pool.fetchRefSpec`). How a presubmit build brings in a change that is not merged is not covered here.
- **Garbage collection.** It is off by default, so the store only grows.
- **Authentication and TLS.** The network is the only boundary ([runbook](runbook.md#security)).

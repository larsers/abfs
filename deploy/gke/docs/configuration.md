# ABFS on GKE / Configuration

The chart's defaults are in `chart/abfs/values.yaml`, and `values.schema.json` checks the types and the backend names. `chart/abfs/values-example.yaml` fits the KCC overlay `kcc/overlays/example`. This page lists every value group with its defaults, the names that must match the KCC tree, and how each component follows the load.

Values marked **Preview** turn on GKE features in Preview. Everything else uses features that are generally available.

## KCC names and chart values

Both trees name the same resources. Set each pair together; the chart cannot read the KCC objects.

| KCC setting or object | Example | Chart value |
| --- | --- | --- |
| `settings.yaml` `project` | `example-project` | `backend.project` |
| `settings.yaml` `spannerInstance` | `abfs` | `backend.spannerInstance` |
| `settings.yaml` `spannerDatabase` | `cas` | `backend.spannerDatabase` |
| `settings.yaml` `bigtableInstance` | `abfs-objects` | `backend.bigtableInstance` |
| `settings.yaml` `bigtableCluster` | `abfs-objects-c1` | none; the chart uses the instance's default app profile, or `backend.bigtableAppProfile` |
| `settings.yaml` `bucket` | `example-project-abfs-blobs` | `backend.bucket` |
| `settings.yaml` `zone` (the Bigtable cluster's) | `europe-west4-b` | `backend.zone`; the server class places its nodes there unless `computeClasses.server.zone` is set |
| `settings.yaml` `proxySubnetRange`, or the network's existing proxy-only subnet | `10.129.0.0/23` | `networkPolicy.proxyOnlySubnetCidr` |
| IAMServiceAccount `abfs-server` (base) | `abfs-server@example-project.iam.gserviceaccount.com` | `server.gcpServiceAccount`; empty means this name |
| IAMServiceAccount `abfs-nodes` (base) | `abfs-nodes@example-project.iam.gserviceaccount.com` | `computeClasses.nodeServiceAccount` |
| ComputeAddress `abfs-gateway` (base) | `abfs-gateway` | `gateway.addressName` |
| the IP of ComputeAddress `abfs-gateway` (`.status.observedState.address`) | | `gateway.vip` |
| `settings.yaml` `serverWorkloadIdentityMember` | `serviceAccount:example-project.svc.id.goog[abfs/abfs-server]` | the release namespace `abfs`; the chart's KSA is always `abfs-server` |
| Namespace `abfs` (base) | `abfs` | `helm install -n abfs` |

## image

| Value | Default | Meaning |
| --- | --- | --- |
| `image.registry` | `europe-west4-docker.pkg.dev/example-project/abfs` | Artifact Registry repository of the four images |
| `image.server.name`, `.pusher.name`, `.client.name`, `.casfsLoader.name` | `abfs-server`, `abfs-pusher`, `abfs-client`, `casfs-loader` | image names |
| `image.<name>.digest` | `""` | `sha256:...` from `images.txt`. The render fails on an empty digest. |
| `image.allowTag` | `false` | Development only: an empty digest falls back to `<registry>/<name>:<tag>` |
| `image.tag` | `""` | Tag with `allowTag`; empty means the chart's `appVersion` |
| `image.pullPolicy` | `IfNotPresent` | |

## backend

| Value | Default | Meaning |
| --- | --- | --- |
| `backend.project` | `""` (required) | Project of Spanner, Bigtable and the bucket |
| `backend.spannerInstance` | `""` (required) | Spanner instance |
| `backend.spannerDatabase` | `cas` | Spanner database |
| `backend.bigtableInstance` | `""` (required) | Bigtable instance |
| `backend.bigtableAppProfile` | `""` | Empty uses the instance's default profile, which must use single-cluster routing |
| `backend.bucket` | `""` (required) | Cloud Storage bucket |
| `backend.zone` | `""` (required) | Zone of the Bigtable cluster |

## namespaces

| Value | Default | Meaning |
| --- | --- | --- |
| `namespaces.createCi` | `true` | Create `ci.namespace` with Pod Security level `privileged` (enforce, warn and audit). Helm keeps it on uninstall. Set it to `false` when the namespace is managed elsewhere; it must then exist with that level before the install. |

The chart never creates the release namespace: the KCC base creates `abfs` with its project annotation and the level `baseline`, and the release must run there. `helm install` refuses a namespace `abfs-ci` that exists but was not created by the release; delete it first, or set `createCi=false`.

## server

| Value | Default | Meaning |
| --- | --- | --- |
| `server.enabled` | `true` | |
| `server.gcpServiceAccount` | `""` | Google service account of the KSA `abfs-server`; empty means `abfs-server@<backend.project>.iam.gserviceaccount.com` |
| `server.replicas.min`, `.max` | `3`, `10` | HPA range; without HPA the fixed count is `min` |
| `server.hpa.enabled`, `.cpuTarget` | `true`, `60` | HPA on CPU utilization of the request |
| `server.caches.headers` | `100000000` | Object header entries, about 180 B each (up to 18 GB) |
| `server.caches.contents` | `400000` | Content entries, up to about 49 KB each (up to about 20 GB). The server's own default of 1,000,000 can reach 49 GB. |
| `server.memoryLimit` | `48Gi` | Memory request and limit. `GOMEMLIMIT` is 90 % of it. The render fails when headers x 180 B + contents x 49 KB + 6 GiB exceed it. |
| `server.cpuRequest` | `14` | CPU request; no CPU limit. `GOMAXPROCS` follows the request. A pod fits a node only when its request stays below the node's vCPUs minus the CPU that GKE reserves and the system DaemonSets request, so 14 fits one replica on a 16-vCPU node, while 16 would need a 32-vCPU node. |
| `server.vpa.enabled` | `true` | Create a VerticalPodAutoscaler for the server, CPU only |
| `server.vpa.mode` | `Off` | `Off` (recommendations only) or `Recreate` (applies them by recreating pods). In-place resizing is not offered (see [Right-sizing](#right-sizing)). With `server.hpa.enabled`, `Recreate` needs `hpaRightsizing: true`: a plain VPA on CPU next to the CPU HPA makes the two fight over the request, so the render fails on that combination. |
| `server.vpa.hpaRightsizing` | `false` | **Preview**, GKE 1.36.3-gke.1630000 or later: HPA and VPA share the CPU request (annotation `gke.io/autoscaling-hpa-rightsizing-mode`). Use with `mode: Recreate`. With `mode: Off` the chart still renders the annotation, the VPA changes no pod, and the install notes print a warning. |
| `server.vpa.minCpu`, `.maxCpu` | `12`, `32` | VPA bounds |
| `server.drainDelay` | `15s` | `--drain-delay` after SIGTERM |
| `server.healthProbeInterval` | `10s` | `--health-probe-interval` of the backend probe |
| `server.shutdownTimeout` | `25s` | `--shutdown-timeout` |
| `server.logSampleRate` | `0.01` | `--log-sample-rate` |
| `server.gc.enabled` | `false` | `--gc`; needs the resources of `init --gc`, which both backend modes have |
| `server.extraArgs` | `[]` | More server flags |
| `server.preStopSleepSeconds` | `120` | Time the pod keeps serving after its endpoint is removed, at least `gateway.drainingTimeoutSec` plus about 60 s |
| `server.terminationGracePeriodSeconds` | `240` | preStop + the longest stop after SIGTERM + a margin: 120 s + 105 s + 15 s. The stop is `server.drainDelay` (15 s), `server.shutdownTimeout` (25 s), with `server.gc.enabled` up to 20 s to write the buffered GC touches and up to 30 s to delete the heartbeat row, and up to 15 s of telemetry flush. Raise it with any of these. |
| `server.minReadySeconds` | `60` | |
| `server.probes.readiness` | period 5, timeout 2, failures 2 | gRPC health on 50051 |
| `server.probes.liveness` | period 10, timeout 2, failures 6 | TCP on 50051. A backend outage marks every replica `NOT_SERVING` at once, so health does not drive liveness. |

The server listens on 50051 (gRPC, plaintext h2c) and serves its HTTP debug endpoints on 127.0.0.1:8051 only.

## gateway

| Value | Default | Meaning |
| --- | --- | --- |
| `gateway.className` | `gke-l7-rilb` | Regional internal Application Load Balancer |
| `gateway.addressName` | `abfs-gateway` | Name of the KCC ComputeAddress, the one of the KCC base |
| `gateway.vip` | `""` (required) | IP of that address. Pushers, cacheman and build pods use `<vip>:50051`. |
| `gateway.timeoutSec` | `600` | Backend timeout, for the longest single `GetObject` or `PutObject` stream |
| `gateway.drainingTimeoutSec` | `60` | Connection draining |
| `gateway.logging.enabled`, `.sampleRate` | `true`, `1000` | Access log, `sampleRate` out of 1,000,000 |
| `gateway.allowGlobalAccess` | `false` | Clients from other regions |

The health check is gRPC on port 50051 every 5 s, timeout 5 s, 2 checks to change state.

## pusher

| Value | Default | Meaning |
| --- | --- | --- |
| `pusher.enabled` | `true` | The pushers and their pool config. With `false` the clients still need a manifest, see below. |
| `pusher.count` | `3` | Pushers in the pool and in the StatefulSet. Fixed after the seed; a change is a re-shard. |
| `pusher.namePrefix` | `abfs-pusher` | StatefulSet name; pool entries are `<namePrefix>-<ordinal>` |
| `pusher.port` | `9998` | HTTP `/info` port (`defaults.port` of the pool) |
| `pusher.dataDisk.size` | `1Ti` | PVC per pusher |
| `pusher.dataDisk.storageClass` | `abfs-hyperdisk-balanced` | |
| `pusher.dataDisk.createStorageClass` | `true` | Create that StorageClass (Hyperdisk Balanced, `WaitForFirstConsumer`, expandable) |
| `pusher.phase` | `seed` | `seed`: class `abfs-pusher-seed`, `seedResources`. `steady`: class `abfs-pusher-steady`, `steadyResources` and the memory VPA. |
| `pusher.seedResources` | 28 CPU, 200Gi request and limit | Sized for the largest repository |
| `pusher.steadyResources` | 7 CPU, 32Gi request and limit | A starting value; the VPA adjusts the memory. 7 CPU fits an 8-vCPU node, for the same reason as the server request. |
| `pusher.vpa.mode` | `Initial` | Steady phase only, memory only. `Initial` applies the recommendation when a pusher restarts for another reason; `Recreate` also evicts. |
| `pusher.vpa.minMemory`, `.maxMemory` | `16Gi`, `200Gi` | VPA bounds; `maxMemory` is the seed size |
| `pusher.terminationGracePeriodSeconds` | `90` | |
| `pusher.gerrit.secretName` | `gerrit-credentials` | Secret in the release namespace with the key `.netrc` |
| `pusher.gerrit.cookies` | `false` | Also mount `.gitcookies` from the Secret |
| `pusher.pool.manifest.url` | `""` (required with pushers) | Manifest repository, for example `https://gerrit.example.com/a/platform/manifest`; without a scheme, `https`. Read for the client keys also with `pusher.enabled: false`. |
| `pusher.pool.manifest.project` | `""` | Project the pool registers the manifest under; empty means the URL path. Set it when the URL has a path prefix such as `/a/`. |
| `pusher.pool.manifest.server` | `""` | Server the pool registers the manifest under; empty means the URL host, port included |
| `pusher.pool.manifest.checkInterval` | `""` | Optional `check-interval` of the pool's manifest |
| `pusher.pool.branchFiles` | `[{branch: main, file: default.xml}]` | Manifest branches and files to push |
| `pusher.pool.fetchRefSpec` | heads and tags | `project-defaults.fetch-ref-spec` |
| `pusher.pool.projectDefaults` | `{}` | More `project-defaults` keys |
| `pusher.pool.overrides` | `[]` | Per-project overrides. A narrowed `fetch-ref-spec` also narrows the manifest project, so give the manifest project its own override with all its branches, as the example does. |
| `pusher.pool.defaults` | `{}` | Pool `defaults` keys (`max-remote-pushes`, `max-projects-syncs`, `max-memory-usage`); `port` comes from `pusher.port` |

The pushers run as root in the `baseline` namespace and reach the server through the Gateway VIP.

Clients find the manifest by server and project. The chart gives them the values the pool uses: `manifest-server` (`pusher.pool.manifest.server`, or the URL host with its port), `manifest-project-name` (`.project`, or the URL path) and `manifest-scheme` (the URL scheme), both in `config:clients/configs` and in the instance config that cacheman writes on each CI node. The chart reads the URL as the pool does:

- A URL without `://` gets `https://`, so `gerrit.example.com/platform/manifest` is valid.
- Server and project lose empty and `.` segments and their leading and trailing slashes, so `https://gerrit.example.com//platform//manifest/` gives `platform/manifest`.
- A URL without a host or without a path fails the render, because the pool rejects it. A `..` segment also fails the render.

An override in `bootstrap.clientConfig` must keep `manifest-server` equal to the host part of the pool URL, port included; otherwise `ready` and `mount -b` look for the manifest under a host no pusher writes to.

The chart reads `pusher.pool.manifest` also with `pusher.enabled: false`, so a release without pushers still gives the clients their manifest keys. When the pushers of that backend run in another release, set `pusher.pool.manifest` to the values of their pool, or set `manifest-server` and `manifest-project-name` in `bootstrap.clientConfig`. With neither, the render fails, since clients without the keys cannot run `ready` or `mount -b`.

## bootstrap

| Value | Default | Meaning |
| --- | --- | --- |
| `bootstrap.enabled` | `true` | Post-install and post-upgrade hook Job that writes `config:git-pusher/default` and `config:clients/configs` and reads them back |
| `bootstrap.deadlineSeconds` | `1500` | `activeDeadlineSeconds`; keep it below the Helm `--timeout` |
| `bootstrap.backoffLimit` | `3` | |
| `bootstrap.clientConfig` | `sync.auto-setup: true`, `force-workspaces: true`, `git-prefetching: false`, `ignore-soong-env-avail: true` | Central client config. Never put `disable-tls`, `root-ca-file` or `partial-clone` here. The chart adds `manifest-server`, `manifest-scheme` and `manifest-project-name` from `pusher.pool.manifest`; the same keys set here win. |
| `bootstrap.repoSetup.enabled` | `true` | Seed the patched `repo` tool (`refs/heads/stable-abfs` of `gerrit.googlesource.com/git-repo`) when the store has none; skipped when the ref exists. Needs egress to `gerrit.googlesource.com` ([runbook](runbook.md#6-check-the-patched-repo-tool)). |
| `bootstrap.computeClass` | `abfs-server` | ComputeClass of the Job's node; the chart adds its node selector and toleration. Needs `server.enabled`. Empty: placement only from the two values below, and the node must be able to pull the images. |
| `bootstrap.nodeSelector`, `.tolerations` | `{}`, `[]` | Further placement of the Job |

## ci

| Value | Default | Meaning |
| --- | --- | --- |
| `ci.enabled` | `true` | casfs loader, cacheman, the CI class |
| `ci.namespace` | `abfs-ci` | Namespace of the CI workloads and build pods, Pod Security `privileged` |
| `ci.uid`, `ci.gid` | `1000`, `1000` | User and group of cacheman; build pods must run with the same pair |
| `ci.hostCacheDir` | `/var/lib/abfs` | Host directory: `bin/` (tools), `base/` (`ABFS_BASE_DIR`) |
| `ci.waitForCasfsTimeout` | `15m` | Build pods fail after this when casfs is not loaded, so the CI system retries them |
| `ci.cacheman.evictThreshold` | `500GB` | 80 % of the boot disk minus the build output budget. Eviction runs from 90 % to 80 % of it. |
| `ci.cacheman.fsyncObjects` | `true` | |
| `ci.cacheman.toolDir` | `/opt/abfs/bin` | Tool set in the `abfs-client` image |
| `ci.cacheman.resources` | 2 CPU, 4Gi request, 8Gi limit | No CPU limit and no `GOMAXPROCS`: the node's one cacheman may use every core for the build pods it serves. The CPU request is a scheduling floor. |
| `ci.casfs.provider` | `daemonset` | `daemonset`: build casfs on each Ubuntu node. `image`: the node image ships casfs; load and check only. |
| `ci.casfs.maxMounts` | `4` | `casfs_max_mounts`: build pods per node plus 2. Read-only after load. |
| `ci.casfs.mountGbPerWorkspace` | `6` | Kernel memory per active workspace, for the slab budget. An active full-tree workspace holds a few GB of casfs slab ([sizing](ci-job.md#sizing)); 6 keeps a margin. |
| `ci.casfs.maxRamShare` | `0.4` | Upper bound: the loader lowers `maxMounts` until `maxMounts` x `mountGbPerWorkspace` fits this share of `MemTotal` |
| `ci.casfs.interfaceVersion` | `1.5.0` | Interface the client needs |
| `ci.casfs.source.path`, `.commit`, `.version` | `/usr/src/casfs`, `c946f8e`, `0.1.23` | Source in the loader image and the module version the loader expects (prefix match; empty skips the check) |
| `ci.casfs.resources` | 100m CPU, 256Mi request, 2Gi limit | Loader container |
| `ci.localSsd.enabled` | `false` | Not implemented; `true` fails the render |
| `ci.exampleJob.enabled` | `false` | Render the example build Job ([ci-job.md](ci-job.md)) |
| `ci.exampleJob.image` | `""` | Build image of the CI system |
| `ci.exampleJob.branch`, `.target`, `.command` | `main`, `aosp_cf_x86_64_phone-trunk_staging-userdebug`, envsetup, lunch and `m` | |
| `ci.exampleJob.resources` | 28 CPU, 80Gi request and limit | One pod per 32-vCPU, 128 GB node of the class floor ([sizing](ci-job.md#sizing)) |

## computeClasses

| Value | Default | Meaning |
| --- | --- | --- |
| `computeClasses.enabled` | `true` | |
| `computeClasses.nodeServiceAccount` | `""` (required) | `abfs-nodes@<project>.iam.gserviceaccount.com` |
| `computeClasses.ci.families` | `[n4, n4d, c4d, c4]` | Machine families in fallback order |
| `computeClasses.ci.minCores`, `.minMemoryGb` | `32`, `120` | Node floor; 120 GB keeps highcpu shapes out |
| `computeClasses.ci.bootDiskGb` | `1000` | Hyperdisk Balanced boot disk, which holds the cache |
| `computeClasses.ci.spot` | `false` | Spot rules before the on-demand ones. Only if the CI system retries builds whose node was preempted. |
| `computeClasses.ci.consolidationDelayMinutes` | `30` | |
| `computeClasses.ci.consolidationThreshold` | `""` | Empty keeps the GKE default |
| `computeClasses.ci.activeMigration` | `false` | Moving a warm fallback node to the preferred family costs a cold cache |
| `computeClasses.ci.nodepools` | `[]` | Hand-made pools tried first, for example the [IOPS fallback pool](prerequisites.md#iops-fallback-node-pool-for-ci) |
| `computeClasses.server.families` | `[n4, n4d, c4d, c4]` | |
| `computeClasses.server.zone` | `""` | Empty means `backend.zone` |
| `computeClasses.server.activeMigration` | `true` | |
| `computeClasses.pusherSeed.families`, `.minCores`, `.minMemoryGb` | `[n4, n4d, c4d, c4]`, `32`, `240` | Highmem shapes for the seed |
| `computeClasses.pusherSteady.families` | `[n4, n4d, c4d, c4]` | No size floor |

Every class sets `whenUnsatisfiable: DoNotScaleUp` and uses Hyperdisk Balanced boot disks. The CI class uses `ubuntu_containerd` with secure boot off and `maintenanceExclusion: UNTIL_END_OF_SUPPORT`; the others use `cos_containerd`. Servers and pushers never run on Spot.

## otel, pdb, networkPolicy, alerts, tests

| Value | Default | Meaning |
| --- | --- | --- |
| `otel.enabled` | `true` | OTLP environment on every workload |
| `otel.endpoint` | `http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317` | Managed OpenTelemetry collector |
| `otel.protocol` | `grpc` | `grpc` or `http/protobuf` |
| `pdb.enabled` | `true` | `maxUnavailable: 1` for the server and the pushers |
| `networkPolicy.enabled` | `true` | Server ingress only from the proxy-only subnet, the health check ranges, the bootstrap Job and the test pod. Inert without Dataplane V2. |
| `networkPolicy.proxyOnlySubnetCidr` | `""` (required when enabled) | |
| `networkPolicy.healthCheckCidrs` | `[35.191.0.0/16]` | |
| `alerts.enabled` | `false` | Managed Service for Prometheus rules for the pusher pool |
| `alerts.for` | `30m` | |
| `tests.enabled` | `true` | `helm test` pods |
| `tests.ciTools` | `true` | Also check the tools on a CI node; this can create a CI node |
| `tests.computeClass` | `abfs-server` | ComputeClass of the server health test, as for `bootstrap.computeClass`. The CI tools test always runs on `abfs-ci`. |
| `tests.nodeSelector`, `.tolerations` | `{}`, `[]` | Further placement of the server health test |
| `commonLabels` | `{}` | Extra labels on every object |

## Right-sizing

Sizes follow the load in both directions. The defaults use generally available features; the Preview ones are values.

| Component | Up | Down |
| --- | --- | --- |
| Server replicas | HPA on 60 % CPU, up to 10 | HPA down to 3; consolidation removes the emptied nodes |
| Server CPU request | 14 by default, one replica per 16-vCPU node. VPA `Off` gives recommendations. `server.vpa.hpaRightsizing: true` (**Preview**) applies them through `Recreate`. | same |
| Server memory | set by the cache flags, not by VPA: the caches fill over time, and a VPA request below their bound would end in OOM kills | smaller cache flags, measured first |
| Pushers | seed phase: class `abfs-pusher-seed`, fixed requests | steady phase: class `abfs-pusher-steady`, `steadyResources`, VPA `Initial` on memory |
| CI nodes | node auto-creation fits the pending build pods (32 vCPU floor), fallback over four families | consolidation after 30 minutes under the threshold; build pods carry `safe-to-evict: "false"` |
| Bigtable (`backend-new`) | autoscaling on 60 % CPU up to `maxNodes` (3 by default, the cost cap) | down to 1 node |
| Spanner | fixed 100 processing units | 100 is the smallest regional size |
| Gateway | managed | managed |

What none of these does is replace a large node with a smaller one when a pod's requests shrink: a resized or recreated pod usually lands on the same node, which still has room. Two cases need care:

- The server VPA offers `Off` and `Recreate` only. In-place resizing (`InPlaceOrRecreate`, Preview) is not offered for the server: `GOMAXPROCS` comes from `requests.cpu` and is fixed when the container starts, so an in-place CPU raise would leave the process at its old thread count while the HPA measures against the new request. A recreated pod reads the new request. The VPA controls CPU only.
- The pushers need large nodes for the seed and much less afterwards. `pusher.phase: steady` moves them to the steady class, which replaces the nodes. VPA would climb back to the seed size only through OOM kills, so switch back to `phase: seed` for a re-shard, a lost PVC or a large new branch. CPU stays out of the pusher VPA because it sets `GOMAXPROCS`.

On CI nodes, larger nodes let more builds share one warm cache, and smaller nodes scale more finely. The class picks shapes that fit the pending build pods, so the node size follows the build pod request your CI system sets ([ci-job.md](ci-job.md#sizing)). With `optimize-utilization` the scheduler prefers busy nodes, so builds pack onto one boot disk.

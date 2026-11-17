# ABFS on GKE / Configuration

The chart's defaults are in `chart/abfs/values.yaml`; `values.schema.json` checks the types and the backend names. `chart/abfs/values-example.yaml` fits the KCC overlay `kcc/overlays/example`. Values marked **Preview** turn on GKE features in Preview.

## KCC names and chart values

Set each pair together; the chart cannot read the KCC objects.

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
| `namespaces.createCi` | `true` | Create `ci.namespace` with Pod Security level `privileged` (enforce, warn and audit); Helm keeps it on uninstall. With `false` it must exist with that level before the install. |

The chart never creates the release namespace: the KCC base creates `abfs` (level `baseline`), and the release must run there. `helm install` refuses an existing `abfs-ci` that the release did not create; delete it first, or set `createCi=false`.

## server

| Value | Default | Meaning |
| --- | --- | --- |
| `server.enabled` | `true` | |
| `server.gcpServiceAccount` | `""` | Google service account of the KSA `abfs-server`; empty means `abfs-server@<backend.project>.iam.gserviceaccount.com` |
| `server.replicas.min`, `.max` | `3`, `10` | HPA range; without HPA the fixed count is `min` |
| `server.hpa.enabled`, `.cpuTarget` | `true`, `60` | HPA on CPU utilization of the request |
| `server.hpa.scaleDownStabilizationSeconds` | `1800` | The HPA removes replicas only after the load has stayed low this long. Scale-up keeps the HPA default. |
| `server.hpa.scaleDownPeriodSeconds` | `300` | After the window, at most one replica goes per period |
| `server.caches.headers` | `100000000` | Object header entries, about 180 B each (up to 18 GB) |
| `server.caches.contents` | `400000` | Content entries, up to about 49 KB each (up to about 20 GB; the server's own default of 1,000,000 can reach 49 GB) |
| `server.memoryLimit` | `48Gi` | Memory request and limit. `GOMEMLIMIT` is 90 % of it. The render fails when headers x 180 B + contents x 49 KB + 6 GiB exceed it, or + 7 GiB with `server.gc.enabled`. |
| `server.cpuRequest` | `14` | CPU request; no CPU limit. `GOMAXPROCS` follows the request. 14 fits one replica on a 16-vCPU node; 16 would need a 32-vCPU node. |
| `server.vpa.enabled` | `true` | Create a VerticalPodAutoscaler for the server, CPU only |
| `server.vpa.mode` | `Off` | `Off` (recommendations only) or `Recreate` (applies them by recreating pods); no in-place resizing ([Right-sizing](#right-sizing)). With `server.hpa.enabled`, `Recreate` needs `hpaRightsizing: true`, or the render fails. |
| `server.vpa.hpaRightsizing` | `false` | **Preview**, GKE 1.36.3-gke.1630000 or later: HPA and VPA share the CPU request (annotation `gke.io/autoscaling-hpa-rightsizing-mode`). Use with `mode: Recreate`; with `mode: Off` the VPA changes no pod and the install notes warn. |
| `server.vpa.minCpu`, `.maxCpu` | `12`, `32` | VPA bounds. With `mode: Recreate`, a recommendation above 14 moves a replica to a larger node with empty caches; `maxCpu: 14` keeps the replicas on 16-vCPU nodes. |
| `server.drainDelay` | `15s` | `--drain-delay` after SIGTERM |
| `server.healthProbeInterval` | `10s` | `--health-probe-interval` of the backend probe |
| `server.shutdownTimeout` | `25s` | `--shutdown-timeout` |
| `server.logSampleRate` | `0.01` | `--log-sample-rate` |
| `server.gc.enabled` | `false` | `--gc`; needs the garbage collector resources, which neither backend mode creates by default: `components/gc` with `backend-new`, then `init --gc` ([runbook](runbook.md#garbage-collection)). Adds up to about 1 GiB of server memory, which the memory check counts. |
| `server.extraArgs` | `[]` | More server flags |
| `server.preStopSleepSeconds` | `120` | Time the pod keeps serving after its endpoint is removed, at least `gateway.drainingTimeoutSec` plus about 60 s |
| `server.terminationGracePeriodSeconds` | `240` | preStop (120 s) + the longest stop after SIGTERM (105 s: `server.drainDelay`, `server.shutdownTimeout`, with `server.gc.enabled` up to 20 s of GC touches and 30 s of heartbeat delete, 15 s of telemetry flush) + 15 s margin. Raise it with any of these. |
| `server.minReadySeconds` | `60` | |
| `server.probes.startup` | period 10, timeout 2, failures 30 | TCP on 50051, open once the backend clients are set up (and, with `server.gc.enabled`, the first heartbeat written). A start may take period x failures, 5 minutes; raise `failureThreshold` when the server log shows slower starts. |
| `server.probes.readiness` | period 5, timeout 2, failures 2 | gRPC health on 50051 |
| `server.probes.liveness` | period 10, timeout 2, failures 6 | TCP on 50051; gRPC health does not drive liveness |

The server listens on 50051 (gRPC, plaintext h2c) and serves HTTP debug endpoints on 127.0.0.1:8051 only. The replicas spread over nodes (`topologySpreadConstraints` on `kubernetes.io/hostname`, `maxSkew: 1`, `whenUnsatisfiable: ScheduleAnyway`). The pod mounts no service account token (`automountServiceAccountToken: false`); Workload Identity does not need one.

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
| `pusher.enabled` | `true` | The pushers and their pool config. With `false` the clients still need the manifest keys, see below. |
| `pusher.count` | `3` | Pushers in the pool and in the StatefulSet. Fixed after the seed; a change is a re-shard. |
| `pusher.namePrefix` | `abfs-pusher` | StatefulSet name; pool entries are `<namePrefix>-<ordinal>` |
| `pusher.port` | `9998` | HTTP `/info` port (`defaults.port` of the pool), also the readiness probe |
| `pusher.dataDisk.size` | `1Ti` | PVC per pusher |
| `pusher.dataDisk.storageClass` | `abfs-hyperdisk-balanced` | |
| `pusher.dataDisk.createStorageClass` | `true` | Create that StorageClass (Hyperdisk Balanced, `WaitForFirstConsumer`, expandable) |
| `pusher.phase` | `seed` | `seed`: class `abfs-pusher-seed`, `seedResources`. `steady`: class `abfs-pusher-steady`, `steadyResources` and the memory VPA. |
| `pusher.seedResources` | 28 CPU, 200Gi request and limit | Sized for the largest repository |
| `pusher.steadyResources` | 7 CPU, 32Gi request and limit | A starting value; the VPA adjusts the memory. 7 CPU fits an 8-vCPU node. |
| `pusher.vpa.mode` | `Initial` | Steady phase only, memory only. `Initial` applies the recommendation only to a new pod, never after an OOM restart ([Right-sizing](#right-sizing)). `Recreate` also evicts a pod outside the recommendation. `InPlaceOrRecreate` (**Preview**, GKE 1.34.0-gke.2201000 or later) resizes in place and raises it after an OOM kill. |
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
| `pusher.pool.overrides` | `[]` | Per-project overrides. A narrowed `fetch-ref-spec` also narrows the manifest project: give it its own override with all its branches, as the example does. |
| `pusher.pool.defaults` | `{}` | Pool `defaults` keys (`max-remote-pushes`, `max-projects-syncs`, `max-memory-usage`); `port` comes from `pusher.port` |

The pushers run as root in the `baseline` namespace and reach the server through the Gateway VIP. A pusher is ready when `/info` answers 200 (503 until the pool config is applied); the probe runs every 30 s, and the rolling update waits for each pusher. The pushers have no liveness probe, since a restart loses a running push.

Clients find the manifest by server and project. The chart writes `manifest-server` (`pusher.pool.manifest.server`, or the URL host with its port), `manifest-project-name` (`.project`, or the URL path) and `manifest-scheme` (the URL scheme) into `config:clients/configs` and into the instance config that cacheman writes on each CI node. The chart reads the URL as the pool does:

- A URL without `://` gets `https://`, so `gerrit.example.com/platform/manifest` is valid.
- Server and project lose empty and `.` segments and their leading and trailing slashes, so `https://gerrit.example.com//platform//manifest/` gives `platform/manifest`.
- A URL without a host or without a path fails the render, because the pool rejects it. A `..` segment also fails the render.

An override in `bootstrap.clientConfig` must keep `manifest-server` equal to the host part of the pool URL, port included; otherwise `ready` and `mount -b` look under a host no pusher writes to.

With `pusher.enabled: false` (the pushers run in another release), set `pusher.pool.manifest` to the values of their pool, or set `manifest-server` and `manifest-project-name` in `bootstrap.clientConfig`. With neither, the render fails.

## bootstrap

| Value | Default | Meaning |
| --- | --- | --- |
| `bootstrap.enabled` | `true` | Post-install and post-upgrade hook Job that writes `config:git-pusher/default` and `config:clients/configs` and reads them back |
| `bootstrap.deadlineSeconds` | `1500` | `activeDeadlineSeconds`; keep it below the Helm `--timeout` |
| `bootstrap.backoffLimit` | `3` | |
| `bootstrap.clientConfig` | `sync.auto-setup: true`, `force-workspaces: true`, `git-prefetching: false`, `ignore-soong-env-avail: true` | Central client config. Never put `disable-tls`, `root-ca-file` or `partial-clone` here. The chart adds `manifest-server`, `manifest-scheme` and `manifest-project-name` from `pusher.pool.manifest`; the same keys set here win. |
| `bootstrap.repoSetup.enabled` | `true` | Seed the patched `repo` tool (`refs/heads/stable-abfs` of `gerrit.googlesource.com/git-repo`) when the store has none. Needs egress to `gerrit.googlesource.com` ([install](install.md#check-the-patched-repo-tool)). |
| `bootstrap.computeClass` | `abfs-server` | ComputeClass of the Job's node. Needs `server.enabled`. Empty: placement only from the two values below, on a node that can pull the images. |
| `bootstrap.nodeSelector`, `.tolerations` | `{}`, `[]` | Further placement of the Job |

## ci

| Value | Default | Meaning |
| --- | --- | --- |
| `ci.enabled` | `true` | casfs loader, cacheman, the CI class, and the PriorityClass `<release>-ci-node` (value 1000000000) with which they preempt build pods ([build pod contract](ci-job.md#build-pod-contract)) |
| `ci.namespace` | `abfs-ci` | Namespace of the CI workloads and build pods, Pod Security `privileged` |
| `ci.uid`, `ci.gid` | `1000`, `1000` | User and group of cacheman; build pods must run with the same pair |
| `ci.hostCacheDir` | `/var/lib/abfs` | Host directory: `bin/` (tools), `base/` (`ABFS_BASE_DIR`) |
| `ci.waitForCasfsTimeout` | `15m` | Build pods fail after this when casfs is not loaded, so the CI system retries them |
| `ci.buildDisk` | `400Gi` | `ephemeral-storage` request of one build pod, no limit; your CI system sets the same ([node disk](ci-job.md#node-disk)). The render fails when `ci.exampleJob.resources` sets its own. |
| `ci.cacheman.cacheDisk` | `400Gi` | Node cache reservation, as cacheman's `ephemeral-storage` request; no limit. Eviction threshold 90 % of it (360 GiB). The render fails when it plus `ci.buildDisk` exceed the allocatable of `computeClasses.ci.bootDiskGb`, below 10Gi, or when `ci.cacheman.resources` sets `ephemeral-storage`. Replaces `ci.cacheman.evictThreshold`, which now fails the render ([runbook](runbook.md#upgrades)). |
| `ci.cacheman.fsyncObjects` | `true` | |
| `ci.cacheman.toolDir` | `/opt/abfs/bin` | Tool set in the `abfs-client` image |
| `ci.cacheman.resources` | 2 CPU, 4Gi request, 8Gi limit | No CPU limit and no `GOMAXPROCS`. The VerticalPodAutoscaler `abfs-cacheman` recommends a memory request ([Right-sizing](#right-sizing)). |
| `ci.casfs.provider` | `image` | `image`: COS CI nodes, secure boot on, casfs from the node image (COS 133 build 44-193 or later). `daemonset`: Ubuntu CI nodes, secure boot off, the loader builds casfs on each node; not possible under `constraints/compute.requireShieldedVm`. |
| `ci.casfs.maxMounts` | `4` | `casfs_max_mounts`: build pods per node plus 2. Read-only after load. |
| `ci.casfs.mountGbPerWorkspace` | `6` | Kernel memory per active workspace, for the slab budget ([sizing](ci-job.md#sizing)) |
| `ci.casfs.maxRamShare` | `0.4` | Upper bound: the loader lowers `maxMounts` until `maxMounts` x `mountGbPerWorkspace` fits this share of `MemTotal` |
| `ci.casfs.interfaceVersion` | `1.5.0` | Interface the client needs |
| `ci.casfs.source.path`, `.commit`, `.version` | `/usr/src/casfs`, `c946f8e`, `0.1.23` | Source in the loader image; with `daemonset`, the module version the loader builds and expects (prefix match; empty skips the check). `image` checks the interface only. |
| `ci.casfs.resources` | 100m CPU, 256Mi request, 2Gi limit | Loader container |
| `ci.localSsd.enabled` | `false` | Not implemented; `true` fails the render |
| `ci.exampleJob.enabled` | `false` | Render the example build Job ([ci-job.md](ci-job.md)) |
| `ci.exampleJob.image` | `""` | Build image of the CI system |
| `ci.exampleJob.branch`, `.target`, `.command` | `main`, `aosp_cf_x86_64_phone-trunk_staging-userdebug`, envsetup, lunch and `m` | Runs in bash in the mounted tree after `set +u`, with `-e` and `pipefail` on ([Example Job](ci-job.md#example-job)) |
| `ci.exampleJob.deadlineSeconds` | `43200` | `activeDeadlineSeconds` of the example Job, retries included: 12 hours ([example Job](ci-job.md#example-job)) |
| `ci.exampleJob.resources` | 28 CPU, 80Gi request and limit | One pod per 32-vCPU, 128 GB node of the class floor ([sizing](ci-job.md#sizing)). The chart adds the `ephemeral-storage` request `ci.buildDisk`. |

## computeClasses

| Value | Default | Meaning |
| --- | --- | --- |
| `computeClasses.enabled` | `true` | |
| `computeClasses.nodeServiceAccount` | `""` (required) | `abfs-nodes@<project>.iam.gserviceaccount.com` |
| `computeClasses.imageStreaming` | `true` | Image streaming on all four classes. Needs the Container File System API and `roles/serviceusage.serviceUsageConsumer` for the node account, both in the KCC base ([prerequisites](prerequisites.md#image-streaming)). New node pools only. |
| `computeClasses.ci.families` | `[n4, n4d, c4d, c4]` | Machine families in fallback order |
| `computeClasses.ci.minCores`, `.minMemoryGb` | `32`, `120` | Node floor; 120 GB keeps highcpu shapes out |
| `computeClasses.ci.bootDiskGb` | `1100` | Hyperdisk Balanced boot disk in GiB; also sets the disk IOPS and throughput. 1,100 fits the cache and one build; two builds need 1,600 ([node disk](ci-job.md#node-disk)). New CI node pools only ([runbook](runbook.md#upgrades)). |
| `computeClasses.ci.spot` | `false` | Spot rules before the on-demand ones. Only if the CI system retries builds whose node was preempted. |
| `computeClasses.ci.consolidationDelayMinutes` | `30` | |
| `computeClasses.ci.consolidationThreshold` | `""` | Empty keeps the GKE default |
| `computeClasses.ci.activeMigration` | `false` | Moving a fallback node to the preferred family costs its warm cache |
| `computeClasses.ci.nodepools` | `[]` | Hand-made pools tried first, for example the [IOPS fallback pool](prerequisites.md#iops-fallback-node-pool-for-ci). Boot disks of at least `bootDiskGb`; the render check does not see them. |
| `computeClasses.ci.capacityQuota.enabled` | `false` | **Preview**, GKE 1.36.2-gke.2064000 or later: a `CapacityQuota` `<release>-ci` (`abfs-ci`) that caps the nodes labelled `cloud.google.com/compute-class: abfs-ci`, hand-made pools included ([CI node cap](#ci-node-cap)) |
| `computeClasses.ci.capacityQuota.nodes` | `0` | Most CI nodes; required with `enabled` |
| `computeClasses.ci.capacityQuota.cpu` | `""` | Most vCPUs over all CI nodes; empty sets no CPU limit |
| `computeClasses.server.families` | `[n4, n4d, c4d, c4]` | |
| `computeClasses.server.zone` | `""` | Empty means `backend.zone` |
| `computeClasses.server.activeMigration` | `true` | |
| `computeClasses.pusherSeed.families`, `.minCores`, `.minMemoryGb` | `[n4, n4d, c4d, c4]`, `32`, `240` | Highmem shapes for the seed |
| `computeClasses.pusherSteady.families` | `[n4, n4d, c4d, c4]` | No size floor |

Every class sets `whenUnsatisfiable: DoNotScaleUp`, Hyperdisk Balanced boot disks, `nodePoolConfig.imageStreaming` from `computeClasses.imageStreaming`, and secure boot and integrity monitoring on (`nodePoolAutoCreation.shieldedInstanceConfig`, GKE 1.36.3-gke.1244000 or later). The CI class sets `maintenanceExclusion: UNTIL_END_OF_SUPPORT` and uses `cos_containerd` (`ci.casfs.provider: image`), or `ubuntu_containerd` with secure boot off (`daemonset`). A change applies to new node pools only ([Secure boot on the CI nodes](prerequisites.md#secure-boot-on-the-ci-nodes)). The other classes use `cos_containerd`. Servers and pushers never run on Spot.

## otel, pdb, networkPolicy, alerts, tests

| Value | Default | Meaning |
| --- | --- | --- |
| `otel.enabled` | `true` | OTLP environment on every workload |
| `otel.endpoint` | `http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317` | Managed OpenTelemetry collector |
| `otel.protocol` | `grpc` | `grpc` or `http/protobuf` |
| `pdb.enabled` | `true` | `maxUnavailable: 1` for the server and the pushers, with `unhealthyPodEvictionPolicy: AlwaysAllow`, so a drain can evict pods that are not ready during a backend outage |
| `networkPolicy.enabled` | `true` | Server ingress only from the proxy-only subnet, the health check ranges, the bootstrap Job and the test pod. Inert without Dataplane V2. |
| `networkPolicy.proxyOnlySubnetCidr` | `""` (required when enabled) | |
| `networkPolicy.healthCheckCidrs` | `[35.191.0.0/16]` | |
| `alerts.enabled` | `false` | Managed Service for Prometheus rules: a `Rules` object for the pusher pool, and a `GlobalRules` object `<release>-gke-metrics` with `AbfsNodeDiskHigh` (a boot disk over 85 % for 5 minutes, any node of the cluster) and `AbfsPusherRestarted` (a pusher container, `git-pusher` or `git-pusher-seed`, restarted in the last 15 minutes). `kubectl get node <node> -L cloud.google.com/compute-class` shows the class of a node that fires. |
| `alerts.for` | `30m` | Duration of the pusher pool rules |
| `alerts.clusterName` | `""` (required with `enabled`) | Name of this GKE cluster; the `GlobalRules` rules filter on it, since they read every cluster in the metrics scope |
| `tests.enabled` | `true` | `helm test` pods |
| `tests.ciTools` | `true` | Also check the tools on a CI node; can create a CI node |
| `tests.computeClass` | `abfs-server` | ComputeClass of the server health test, as for `bootstrap.computeClass`. The CI tools test always runs on `abfs-ci`. |
| `tests.nodeSelector`, `.tolerations` | `{}`, `[]` | Further placement of the server health test |
| `commonLabels` | `{}` | Extra labels on every object |

## Right-sizing

| Component | Up | Down |
| --- | --- | --- |
| Server replicas | HPA on 60 % CPU, up to 10 | HPA down to 3, one replica per 5 minutes after 30 minutes of low load; consolidation removes the emptied nodes |
| Server CPU request | 14, one replica per 16-vCPU node; VPA `Off` recommends; `server.vpa.hpaRightsizing: true` (**Preview**) applies through `Recreate` | same |
| Server memory | set by the cache flags, not by VPA | smaller cache flags, measured first |
| Pushers | seed phase: class `abfs-pusher-seed`, fixed requests | steady phase: class `abfs-pusher-steady`, `steadyResources`, VPA on memory (`Initial` by default) |
| cacheman memory | VPA `abfs-cacheman` in mode `Off` gives a recommendation; set `ci.cacheman.resources` from it | same |
| CI nodes | node auto-creation for pending build pods (32 vCPU floor), four families; bounded by Compute Engine quota or a `CapacityQuota` | consolidation after 30 minutes under the threshold; build pods carry `safe-to-evict: "false"` |
| Bigtable (`backend-new`) | autoscaling on 60 % CPU up to `maxNodes` (3 by default, the cost cap) | down to 1 node |
| Spanner | fixed 100 processing units | 100 is the smallest regional size |
| Gateway | managed | managed |

None of these moves a pod to a smaller node when its requests shrink. Rules:

- Server VPA: `Off` and `Recreate` only, CPU only. `GOMAXPROCS` is fixed at container start, so in-place CPU resizing is not offered.
- Pushers: `pusher.phase: steady` moves them to the steady class and new nodes. The pusher container is `git-pusher-seed` in the seed phase and `git-pusher` in the steady phase, and the VPA controls only `git-pusher`: GKE's recommendation also uses usage from before the VPA existed, so a shared name would carry the seed peak into the steady size, and a seed pod created while the VPA still exists would be resized. With `mode: Initial`, an OOM-killed pusher restarts at its old limit; delete the pod (`kubectl -n abfs delete pod abfs-pusher-<n>`) to get the raised recommendation. `alerts.enabled` adds `AbfsPusherRestarted`; `kubectl -n abfs describe pod abfs-pusher-<n>` shows `Last State: Terminated, Reason: OOMKilled`. `mode: InPlaceOrRecreate` (**Preview**, GKE 1.34.0-gke.2201000 or later) needs no delete: after an OOM kill it applies the recommendation plus 20 % or 100 MB, whichever is larger. The pusher sets no `GOMEMLIMIT`, so a new limit needs no restart. Every mode stops at `maxMemory`. CPU stays out of the pusher VPA because it sets `GOMAXPROCS`. Switch back to `phase: seed` for a re-shard, a lost PVC or a large new branch.

The cacheman VPA (mode `Off`, memory only) only recommends; cacheman must not restart under running builds. Read it after a period of normal CI load, including a busy day:

```sh
kubectl -n abfs-ci get vpa abfs-cacheman -o jsonpath='{range .status.recommendation.containerRecommendations[*]}{.containerName}{" target "}{.target.memory}{" upper "}{.upperBound.memory}{"\n"}{end}'
```

Set `ci.cacheman.resources.requests.memory` near `target` and the limit at or above `upperBound`. A new value reaches a node when its cacheman pod is recreated, on new or recycled CI nodes ([runbook](runbook.md#upgrades)).

The CI node size follows the build pod request your CI system sets ([ci-job.md](ci-job.md#sizing)). With `optimize-utilization`, builds pack onto busy nodes and share one boot disk.

### CI node cap

The chart sets no cluster-level autoscaling limits (none needed from GKE 1.33.3-gke.1136000). CI nodes are bounded by the project's Compute Engine quota for the class's families, and by the cluster-wide limits of node auto-provisioning when it is on. To cap them lower, set `computeClasses.ci.capacityQuota` (**Preview**, GKE 1.36.2-gke.2064000 or later): a cluster-scoped `CapacityQuota` with a `nodes` limit and, with `cpu`, a vCPU limit. The cluster autoscaler enforces it loosely: it can exceed the limit slightly, does not remove nodes when the limit drops, and does not hold back nodes created by hand. Build pods beyond the cap stay Pending with `exceeded quota: "CapacityQuota/abfs-ci"` in `kubectl describe pod`. Near the limit it can block active migration (`computeClasses.ci.activeMigration`). Check that the quota is valid:

```sh
kubectl describe capacityquota abfs-ci
```

The condition `cluster-autoscaler.kubernetes.io/valid` must be `True`; otherwise the limits are not enforced.

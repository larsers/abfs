# ABFS on GKE / Sizing

Planning figures for a large CI farm, up to a few thousand concurrent builds on one cluster. A load test of your own trees replaces the inputs marked "measure". Settings: [configuration](configuration.md); network and cluster flags: [prerequisites](prerequisites.md) and [install](install.md).

## Load model

A build reads its map results (one lookup per build step) and objects through the node's cacheman. cacheman keeps both on the node, so a repeat build of an unchanged tree on the same node sends almost nothing to the Gateway. Load comes from cold map state:

- a new node, or a node whose cache was reset;
- steps changed by new commits;
- map entries 6 to 8 hours after they were written on the node; the client spreads each entry's expiry over that range. The expiry runs from the write, not from the last use: a node that builds all day loses the map state of its first fill over the 6 to 8 hours after it.

| Symbol | Meaning | Planning value |
| --- | --- | --- |
| A | actions per build | 155,000 to 240,000 |
| T_c | duration of a cold build | 2 to 30 minutes (all cache hits to a full content pull) |
| r_c | Gateway requests/s of one cold build, about 2A/T_c (map lookups and their EvalResponse objects) | 1,400 to 2,580 |
| r_w | map lookups/s one node adds while the entries of one fill expire, about A / 7,200 s (objects stay cached) | 22 to 33 |
| C | cold builds at the same time | from node churn and new commits; measure |
| w | Gateway requests of a warm repeat build | measure; near 0 for an unchanged tree |
| B_c | content bytes a cold build pulls | 6.8 to 20 GB |
| x | share of B_c not sent as signed URLs | 1 without `server.linksContent`; measure |
| R_rep | requests/s one server replica serves | measure |

r_c leaves out the content pull (one `GetObject` per object) and the second map lookup a missed step makes at its 30 s recheck.

**Expiry wave.** Nodes filled in one morning wave lose those map entries 6 to 8 hours later, spread over about 2 hours. Each expired entry costs one map lookup at its next use. At N = 2,000 that is at most N x r_w = 43,000 to 67,000 requests/s for 2 hours, about 48 proxies. A fill spread over the morning ramp spreads the wave further.

## Formulas and examples

N builds, b builds per node.

| Output | Formula | N = 2,000, b = 1 | N = 2,000, b = 2 |
| --- | --- | --- | --- |
| CI nodes n | ceil(N/b) + standby buffer | 2,000 | 1,000 |
| Max pods per node | 32 (a /26 per node) | 32 | 32 |
| Pod range | nodes <= 2^(26 - prefix) | /14 (4,096 nodes) | /15 (2,048) |
| Node subnet | n + system + server + pusher + buffer nodes <= 2^(32 - prefix) - 4 | /20 (4,092) | /21 (2,044) |
| Gateway req/s R | C x r_c + (N - C) x w / T_w | C = 200 at r_c 2,580: 516,000; expiry wave: 67,000 | same |
| Proxies P | max(R / 1,400, C x x x B_c / T_c / 18 MB/s) | measured cold build: 342 (bytes bind); r_c 2,580: 369 by requests, bytes: measure; expiry wave: 48 | same |
| Proxy-only subnet | addresses >= P of every Envoy-based load balancer in the region | /22 | /22 |
| System pool | kube-dns `max` 64; `$SYSTEM_MAX` = ceil(64 / 13) + 1 ([System pool and kube-dns](#system-pool-and-kube-dns)) | 6 | 6 |
| Connections per node | `gateway.connections` (below) | 4 | 4 |
| Server replicas | R / R_rep; `server.replicas.max` = that x 1.3 | measure | measure |
| Bigtable rows/s R_bt | map lookups/s + EvalResponse reads that miss the server cache (+ one header read per map result with `server.gc.enabled`) | C = 200: 2 x 200 x 1,290 = 516,000 | same |
| Bigtable nodes | ceil(R_bt / (17,000 x 0.6)) | 51 (gc off); expiry wave: 7 | 51 |
| Spanner | 100 processing units; raise by hand on CPU | 100 | 100 |
| vCPU quota | n x vCPUs per node + servers + pushers | 2,000 x 32 = 64,000 | 1,000 x 64 = 64,000 |
| Hyperdisk Balanced quota | n x `bootDiskGb` (1,100 for one build, 1,600 for two) + pusher disks | 2.2 PB | 1.6 PB |
| Primary-zone shapes | each family or machine type in the primary tier holds at most 1,000 nodes | 2 or more | 1 or more |
| Monitoring ingest | n x series per cacheman / 60 samples/s, against 100,000 ([Telemetry cost](#telemetry-cost)) | 16,700 to 130,000 | 8,300 to 65,000 |

**Proxies, worked.** The one measured cold build took T_c = 221 s and pulled B_c = 6.8 GB, so r_c = 2 x 155,000 / 221 s = 1,400. With C = 200 and x = 1: requests 200 x 1,400 / 1,400 = 200 proxies; bytes 200 x 6.8 GB / 221 s / 18 MB/s = 342 proxies. Bytes bind, so P = 342. At r_c = 2,580 (T_c = 121 s) requests give 369; the bytes of that build are not measured. Signed URLs (x below 1) lower the byte term.

The Bigtable figure counts cold builds; the expiry wave adds up to 67,000 map lookups/s for about 2 hours (7 nodes).

## Gateway

Each proxy of the internal Application Load Balancer handles about 1,400 requests/s (700 with every request logged), 18 MB/s, 3,000 active connections and 600 new HTTP connections/s, sized over 10 minutes. Every Envoy-based load balancer of the network in the region shares the proxy-only subnet. A /23 serves a small install. At thousands of CI nodes plan a /22. A subnet cannot grow in place: create a larger one with role `BACKUP`, then make it `ACTIVE` with a draining period.

**Per-connection bound.** One proxy terminates each client connection. A cold node sends up to 2,580 requests/s, above one proxy's 1,400. cacheman and the pushers list the Gateway address `gateway.connections` times and spread object keys over the connections; refs and a few calls stay on the first. Set:

```
gateway.connections = max(ceil(node peak req/s / 1,400), ceil(node peak bytes/s / 18 MB/s)) + margin
```

A cold node needs 2; the default 4 gives about 650 requests/s per connection. That the load balancer places a node's connections on different proxies is not documented; check it in the load test (proxy count and per-proxy errors during a cold wave).

**Signed URLs** (`server.linksContent`). The server returns objects above `thresholdBytes` (1 MiB) as signed Cloud Storage URLs, and the clients fetch them from Cloud Storage through the Private Service Connect endpoint, outside the Gateway. This cuts x in the byte budget, not the request count. Add the KCC component `links-content`: the server's account then holds `roles/iam.serviceAccountTokenCreator` on itself. The IAM quota for signing blobs is 60,000 per project per minute. A failed signing falls back to streaming, so the quota limits the savings, not the builds.

Keep the Gateway access log at `sampleRate: 1000` (0.1 %); full logging halves the requests a proxy serves.

## Backend

Bigtable: settings `bigtableMinNodes`, `bigtableMaxNodes`, `bigtableCpuTarget` in `settings.yaml` (`backend-new`). Bigtable requires the maximum to be at most 10 x the minimum and a CPU target from 10 to 80; `make check` refuses other values. Keep the target at 60 for latency. Set the minimum for the morning ramp: new nodes take up to 20 minutes to carry load. The default node quota is 30 SSD nodes per zone in most regions, and the configured maximum counts against it; request more before you raise `bigtableMaxNodes`.

Spanner: setting `spannerProcessingUnits` (default 100; 100 to 1,000 in steps of 100, then steps of 1,000). Spanner holds refs only. The managed autoscaler needs at least 1,000 processing units and the Enterprise or Enterprise Plus edition, so the chart does not use it; raise the setting on CPU.

## Zones

`computeClasses.zones.primary` (default `backend.zone`) is tried first for CI and server nodes; `computeClasses.zones.fallback` only when the primary zone lacks capacity, with `locationPolicy: ANY`. Servers move back to the primary zone when it has capacity again (active migration); CI nodes do not, since that would stop running builds.

- A node pool holds at most 1,000 nodes per zone, and each rule of the primary tier gets its own auto-created pool. Above 1,000 CI nodes in the primary zone, keep at least two families (or machine types) in `computeClasses.ci`, or let the rest go to fallback zones. How the autoscaler acts at that limit is not documented; check it in the load test.
- Servers in a fallback zone read Bigtable across zones.
- Install step 6 creates the cluster in the region's default zones. Check that the fallback zones are among them (`gcloud container clusters describe $CLUSTER --region $REGION --format 'value(locations)'`). If one is not, check once that a class node starts there before you widen the cluster's node locations.
- Pusher disks are zonal; a pusher stays in its disk's zone.

## Standby buffer

`ci.capacityBuffer` keeps suspended CI nodes that GKE resumes before it creates a new node. It saves VM creation, image pull and casfs load. A resumed node has an empty ABFS cache, so it does not cut the cold Gateway load.

- GKE 1.37.1-gke.3020000 or later, node pool auto-creation (the classes have it).
- No node over 208 GB of memory, no Local SSD, GPU, TPU or Confidential GKE Nodes. A machine family rule sets no upper memory bound, so the buffer needs `computeClasses.ci.machineTypes`. Check each: `gcloud compute machine-types describe <type> --zone $ZONE --format 'value(memoryMb)'` prints at most 212992. Two builds per 64-vCPU node need 256 GB on standard shapes: use one build per node, or highcpu shapes with smaller build requests.
- Suspended nodes use no vCPU or memory quota, but disk and IP quota.
- A suspended Spot node resumes as Spot. A resume needs capacity for its machine type in its zone, so a zone stockout can stop it.
- `initTime` (default 5m) is the time a new buffer node has to start the casfs loader and cacheman before GKE suspends it; raise it when the loader builds casfs (`ci.casfs.provider: daemonset`) and takes longer. `refresh` (default 24h, at most 120h) replaces old suspended nodes.

## Consolidation delay

`computeClasses.ci.consolidationDelayMinutes` is how long an empty CI node stays. A node that stays keeps its object cache. Its map entries still expire 6 to 8 hours after they were written, whatever the delay. Set the delay to the longest normal gap between builds on a node when the idle node cost is acceptable.

## System pool and kube-dns

kube-dns runs on the system pool. Its autoscaler gives max(ceil(cores / 256), ceil(nodes / 16)) replicas, 250 at both planning sizes (64,000 CI vCPU). Each replica watches the Kubernetes API, and GKE advises a `max` of at most 500 above 500 nodes, in most cases much lower. NodeLocal DNSCache answers on every node, so kube-dns sees only cache misses. Plan `max` 64 for up to 2,000 CI nodes, about one replica per 32 nodes, and raise it when kube-dns CPU or DNS errors grow in the load test.

An `n4-standard-4` system node at 32 pods per node holds about 13 kube-dns Pods: about 10 Pods per node are DaemonSets, and the CPU left after them fits about 13 kube-dns Pods of 260m. Config Connector (7 Pods), metrics-server, konnectivity and the kube-dns autoscaler need about one more node. `$SYSTEM_MAX` = ceil(`max` / 13) + 1: 6 for `max` 64. Set `max` as in [DNS at scale](prerequisites.md#dns-at-scale), then check the Pods per system node:

```sh
kubectl get pods -A -o wide --field-selector spec.nodeName=<system node> --no-headers | wc -l
```

## Telemetry cost

Prices are list prices.

- **Managed Service for Prometheus**: cacheman exports its OTLP metrics every 60 s; GKE bills them as Prometheus samples. One cacheman exports about 500 series (one build goal and target per node) to 3,900 (three goals, three targets): 43,200 samples per series per month. At 2,000 nodes: 43.2 to 337 billion samples per month, about $2,600 to $15,700 per month ($0.06 per million for the first 50 billion, $0.048 to 250 billion, $0.036 to 500 billion). This counts every histogram bucket; unchanged buckets are not billed, so the bill is lower.
- **Monitoring ingest quota**: Managed Service for Prometheus shares the Cloud Monitoring API quota "Time series ingestion requests per minute": by default 500 requests/s of up to 200 samples, 100,000 samples/s per project. Load: n x series per cacheman / 60. At 2,000 nodes that is 16,700 samples/s at 500 series and 130,000 at 3,900, above the default; at 1,000 nodes 8,300 to 65,000. Above 100,000, request a quota increase before the farm grows, or cut the series (fewer build goals and targets per node). Over quota, writes fail with `429: Quota exceeded` and the metrics have gaps.
- **Cloud Logging** (`--logging SYSTEM,WORKLOAD`): $0.50 per GiB above 50 GiB per project per month, system logs included. The volume per node is not measured: read it from the Logs Storage page after a day of CI load and multiply by n.
- **Cloud NAT**: $0.044 per hour per gateway above 32 VMs, plus $0.045 per GiB processed. With `--disable-default-snat` all Pod internet egress goes through it; Google APIs go through the Private Service Connect endpoint.

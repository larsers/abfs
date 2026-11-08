# ABFS / System Admin / GCP server

The GCP server is a CAS server that keeps its data in Google Cloud instead of a local directory, so several servers can serve the same data. Clients and git-pushers use it the same way as the file-based server in the [System Admin doc](README.md).

## Where the data goes

| Data | Service |
| --- | --- |
| Projects and refs | Spanner database (default name `cas`) |
| Object headers and objects up to 32 KiB | Bigtable table `objects` |
| Map results | Bigtable table `maps` |
| The first 1 MiB of larger objects | Bigtable table `chunks` |
| The rest of larger objects | Cloud Storage bucket, in objects of 10 MiB and then 1000 MiB |
| With `--gc`: the GC edges of each object, the objects it references | Bigtable table `gc` |
| With `--gc`: garbage collector state | Spanner tables `GcState` and `GcReplicas`, Bigtable family `objects:g` |

## Limits

* Nothing is ever deleted: there is no garbage collection yet. The server flag `--gc` and the resources of `abfs server gcp init --gc` prepare for it; without them the server behaves as one without garbage collection. Plan storage for growth.
* Some content is stored but never referenced. When an upload fails after the server tried to write the object row, the server cannot tell whether the row was written, so it keeps the content. Concurrent uploads of the same object keep the content of every upload, though only one is referenced. This content is left for a later orphan sweep, which ABFS does not have yet.
* Bigtable must be read and written through single-cluster routing: an instance with one cluster, or an app profile with single-cluster routing (`--bt-app-profile`). With multi-cluster routing a server can report objects another server has just written as missing: uploads that link them fail until the client retries, objects can be stored again, and map results can be missed. Refs are in Spanner and are not affected.
* Map results come back oldest first by the time each server added them, so with several servers the order of results added close together follows the servers' clock skew.
* `who-am-i`, `SHA256` and the `/mark` endpoint are not implemented.

## Resources

Create these with your usual tooling, in one region:

* A Spanner instance. The schema needs little capacity; the smallest instance size is enough to start.
* A Bigtable instance with one cluster (see Limits).
* A Cloud Storage bucket.

### IAM roles

| Who | Role | On |
| --- | --- | --- |
| Server service account | `roles/spanner.databaseUser` | the database |
| Server service account | `roles/bigtable.user` | the Bigtable instance |
| Server service account | `roles/storage.objectUser` | the bucket |
| Server service account, with `--links-content` | `roles/iam.serviceAccountTokenCreator` | the server service account itself, to sign URLs |
| Server service account, with `--gcp-client-metrics` | `roles/monitoring.metricWriter` | the project |
| Whoever runs `abfs server gcp init` | `roles/spanner.databaseAdmin`, `roles/bigtable.admin`, `roles/storage.objectViewer` | the instances and the bucket |

`abfs server gcp init` exports no client library metrics and needs no monitoring role.

`--links-content` also needs the IAM Service Account Credentials API (`iamcredentials.googleapis.com`) enabled in the project and, on a Compute Engine VM, the `cloud-platform` access scope, because the server signs URLs through that API with its own service account.

### Create the database and tables

`abfs server gcp init` creates the Spanner database and its schema and the Bigtable tables and column families when they are missing, and checks that the bucket exists. It keeps existing data, so run it again after each upgrade:

```sh
abfs server gcp init --project <project> --instance <spanner-instance> --bucket <bucket> [--db cas] [--bt-instance objects] [--gc]
```

With `--gc`, init also creates the garbage collector resources: the Bigtable table `gc`, the column family `g` of `objects` and the Spanner tables `GcState` and `GcReplicas`. Without it, init creates only the server's own resources. Running it again with `--gc` adds the garbage collector resources to an existing deployment.

`init` creates missing tables and does not change existing ones. A deployment that ran `init --gc` with an early development version of the garbage collector has a `GcReplicas` table with only the columns `ReplicaId`, `Version` and `LastSeen`; servers with `--gc` then fail every heartbeat and `gc run` fails. Drop that table before running `init --gc` again (for example `gcloud spanner databases ddl update <db> --instance <spanner-instance> --ddl='DROP TABLE GcReplicas'`). It holds only heartbeats, which the servers write again.

The server checks the same resources at startup and stops with an error that names the missing one; with `--gc` it also checks the garbage collector resources. With `--gc` it also applies one conditional write, a delete of the cell `g:condemned` of the absent row `abfs-setup-check`, retries it on a transient error, and stops when Bigtable refuses it or it still fails: a server rescues condemned objects with conditional writes (see [Rescue](#what-the-server-does-for-the-garbage-collector)), so its app profile must use single-cluster routing and allow single-row transactions. The default app profile of an instance created with one cluster allows them; for another profile run `gcloud bigtable app-profiles update <profile> --instance=<bt-instance> --route-to=<cluster> --transactional-writes`. Multi-cluster routing cannot allow them. The `abfs server gcp gc` commands need the garbage collector resources and refuse to run without them. Their own reads of objects leave the garbage collector state as it is: they rescue and touch nothing, so what the collector reads is not kept because it read it.

### Fill the gc table

With `--gc` on, the server writes the `gc` row of each object it adds. Objects added before that, or while it was off, have none. `abfs server gcp gc backfill` scans the `objects` table and writes the missing rows, and the rows whose edge rules are outdated for their object type; it reads every such object that is not a blob:

```sh
abfs server gcp gc backfill --project <project> --instance <spanner-instance> --bucket <bucket> [--dry-run] [--rate 2000] [--shards 16]
```

Each row it writes carries the time of the object row, so the garbage collector's `--grace` counts from the upload of the object, not from the backfill. `--dry-run` only counts the rows to write. `--rate` limits the object rows scanned per second (0 means no limit). The scan can stop at any time; it prints a key to continue from with `--start`. Running it again skips the rows that are already current.

A row is current when its version of the edge rules is at least the one its object type requires; only the rows of types whose rules changed are rewritten. Version 2 gives each deep tree an edge to every deep tree nested in it: a directory with 1,000 or more entries below it is stored as a deep tree of its own, which only deep trees link. Version 1 gave deep trees no edges, so the dry run counted those nested deep trees as unreachable although mounts read them. After upgrading from version 1, run the backfill again; it rewrites only the `deeptree` rows. A server still on version 1 keeps writing deep tree rows without these edges: the dry run counts them in `outdated_rows` (in total and per type) and warns about that server in `replicas.warnings`. Run the backfill again once the last server is upgraded.

### Garbage collection dry run

`abfs server gcp gc run --dry-run` runs one garbage collection cycle that only reports. It deletes and marks no data; its only writes are to the `GcState` row and the removal of `GcReplicas` rows, both in Spanner. `gc run` takes exactly one of `--dry-run` and `--condemn` (see [Condemn mode](#condemn-mode)); a delete mode does not exist yet.

```sh
abfs server gcp gc run --dry-run --project <project> --instance <spanner-instance> --bucket <bucket> [--report gc-report.json] [--explain <key>[,<key>]]
```

A cycle:

1. Takes the lease in `GcState`, increments the cycle number and records the start and snapshot times. While another process holds a valid lease the command fails. The lease (`--lease`, default 5 minutes) is renewed while the cycle runs, and the cycle stops itself if the lease is lost or cannot be renewed in time.
2. Reads the heartbeats in `GcReplicas` and warns about each server seen within the last 30 minutes whose settings break the timing rules: a `--gc-touch-interval` of 0 or longer than half of `--grace`, a `--cache-ttl` of 0, not shorter than `--cycle` or longer than the command's `--cache-ttl`, or GC edge rules older than the command's. The warnings are in the report and the log. The condemn mode refuses to run with any of them. The report also counts the distinct host names among these servers (`replicas.hosts`, the part of each ID before its last `-`) and their live rows per host (`replicas.rows_per_host`). A server that stops cleanly deletes its own row; rows not written for a day, each of a server process that stopped without deleting its row, are deleted.
3. Reads every ref at one snapshot time, with low priority, and sorts the refs into retention classes (below).
4. For each live mount, reads its mount details and its pinned manifest, and keeps the project revisions that are object hashes: the commits the mount reads. For each workspace ref, reads its workspace snapshot, the mount details it was taken from and their pinned manifest, and keeps their revisions the same way: the commits the workspace is based on, which the snapshot does not link. Other revisions, such as branch names, are counted and ignored.
5. Scans the `gc` table twice in key ranges (`--shards`, default 16): first keys, types and times, then the edges, and the keys of the `maps` table. It reads no object content.
6. Marks in memory everything the roots reach. It walks the roots one at a time, by class in the order of the table below (pinned revisions after the unmount refs, then builds, other `abfs-meta` refs, empty objects, recent objects and map rows), so that the report can name the root that reached an object first. Edges to objects without a `gc` row are skipped and counted. A `gc` row with an object type the binary does not know stops the run, so run the same or a newer version than the servers.
7. With `--consistency` (on by default), scans the `meta` family of the `objects` table to check that every object has a `gc` row and to sum the sizes (`meta:size`) of reachable and unreachable objects. This reads every object row once.
8. Writes a JSON report to stdout or to `--report`, and logs a one-line summary.

| Class | Roots | Retention |
| --- | --- | --- |
| Source | every ref outside the classes below, such as branches, tags and `HEAD`; `abfs/project-status`, `abfs/pushed-packs`, `config` and `acl` are counted separately | while the ref exists |
| Workspaces | `refs/abfs/ws/...` of the project `abfs-meta`, and the revisions of the pinned manifest of the mount each snapshot was taken from | while the ref exists |
| Mounts | `refs/abfs/mount/...` of `abfs-meta`, and the revisions of their pinned manifests | while the client rewrote the ref within `--mount-idle` (default `336h`) |
| Unmount records | `refs/abfs/unmount/...` of `abfs-meta`: the unmount details and the mount details they link | `--build-retention` since the ref was written |
| Builds | `refs/abfs/build/...` and `refs/abfs/build-start/...` of `abfs-meta` | `--build-retention` (default `720h`) since the ref was written |
| Eval map rows | the request and the result | `--map-retention` (default `720h`) since the row was written or used |
| Deep tree and digest map rows | the request and the result | while the tree of the request is reachable |
| LFS map rows | the request and the result | always |
| Recent objects | objects whose `gc` row was written or touched within `--grace` (default `72h`) before the snapshot | `--grace` |
| Empty objects | the empty file and the empty directory, which every server writes at start | always |

Map rows with request types not listed are kept. `--grace` must be at least `2 x --cycle + --cache-ttl + --longest-push` (defaults `24h`, `6h` and `3h`); the command refuses to run otherwise.

The report counts, per class, the refs, tombstones, roots and refs that retention would tombstone; roots whose object has no `gc` row (dangling roots); objects and edges per type, reachable and unreachable, with up to 20 unreachable keys per type (`unreachable_sample`); dangling edges with a few examples; map rows live and expired per class; the consistency result with the first objects missing a `gc` row; the time of each step; and the peak heap. Objects written within a minute of the scan are not checked for a `gc` row, and `gc` rows without an object row (uploads in flight) are counted.

The sections `live_mounts` and `workspaces` report the pins: the live mount refs or workspace refs, the mount details without a pinned manifest (and workspace snapshots without mount details), the mount details of clients without `gc-keepalive` or with one of its intervals `0` (`without_keepalive`), the pinned manifests parsed, the revisions kept, the ignored revisions, the kept revisions without a `gc` row (`dangling_revisions`), the snapshots, mount details or manifests that do not exist (`not_found`) and those that could not be read (`errors`). A missing object pins nothing; a read error may hide a pin, so only read errors stop a condemn. `samples` names up to 20 mount details per count, with the workspace snapshot, host and user, so that the clients behind them can be found.

`--explain <key>[,<key>]` adds an `explain` section with, per key: whether it has a `gc` row and its type, whether it is reachable, the root that reached it first (`first_root`: its class and object, and the project, ref and update time of a ref root, the mount details, host and user of a pinned revision, or the result of a map row), the write and touch times of its `gc` row, whether it has an `objects` row and the cycle number of its condemn marker. It reads two rows per key.

`abfs server gcp gc walk` tells whether keys are in the content of the checkouts of mounts, for a key that a client used again after a cycle found it unreachable. It reads each mount details object, such as the targets of a client's mount refs, and its pinned manifest, and walks the commit and the trees of every pinned revision from object content. It does not read the `gc` table, so it finds a key under a `gc` row that lacks an edge, which an `--explain` of the same cycle cannot. It reads trees only to list their entries, so a blob need not exist to be found. It prints, per key, `inside` with its type, the first path that reached it (project path, then the path in the project, in manifest order), the commit and the mount details, or `outside`. Its reads rescue and touch nothing; it takes no lease and writes nothing. It fails when the walk is incomplete: mount details without a pinned manifest, revisions that are not hashes, or objects it could not read; a key outside may then be inside. It reads every tree of the pinned revisions once per revision, `--workers` revisions at a time (default 16).

```
abfs server gcp gc walk --mount-details <hash>[,<hash>] --key <key>[,<key>] --project <project> --instance <spanner-instance> --bucket <bucket>
```

Memory grows with the `gc` table. The data of the mark takes about 35 bytes per object (key, type, edge index and bitmaps, with room the key arrays keep while they fill), 4 bytes per edge and 16 bytes per map row; the report shows it as `keys_bytes`, `edges_bytes` and `map_rows_bytes`. The scans read rows faster than Go's garbage collector frees them, so with the default settings the heap grows to about twice the data: on the emulators, 1.5 million objects and 30 million edges took 170 MB of data and 364 MB of heap. Set `GOMEMLIMIT` for the command to about 1.2 times the data and the heap stays near it (187 MB in the same run). A table of 10^8 objects, 2x10^9 edges and 10^7 map rows needs about 12 GB of data: give the job 16 GB with `GOMEMLIMIT=14GiB`, or about 24 GB without it.

The scans read the whole `gc` table, the `maps` keys and, with `--consistency`, the `meta` family of `objects`. Give the command a Bigtable app profile with low priority and single-cluster routing (`--bt-app-profile`).

### Condemn mode

`abfs server gcp gc run --condemn` runs the cycle of a dry run and then marks every object the mark found unreachable: it sets the cell `g:condemned` of its `objects` row to the cycle number, 8 bytes big-endian. It deletes nothing. A server with `--gc` that reads a marked object clears the marker and answers as for any other object (see [Rescue](#what-the-server-does-for-the-garbage-collector)), so a marked object that a client reads again shows that the mark called something garbage that was still in use.

```sh
abfs server gcp gc run --condemn --expected-replicas <hosts> --project <project> --instance <spanner-instance> --bucket <bucket> [--report gc-report.json] [--condemn-rate 5000] [--keep-lists 7] [--explain <key>[,<key>]]
```

The cycle condemns only when its own report is clean. It refuses, writes no marker and no list, writes the report with the reasons in `condemn.refused` and exits with an error, when any of these holds:

* a warning in `replicas.warnings`;
* the number of distinct host names among the servers seen within the last 30 minutes (`replicas.hosts`) differs from `--expected-replicas`;
* a host has more than one live row: a server there stopped without deleting its row within the last 30 minutes and was started again, or two servers share the host name. A server that stops cleanly deletes its row (see [Heartbeat](#what-the-server-does-for-the-garbage-collector)), so a restart alone does not refuse; a row left behind means a crash, a kill, a preemption or a stop that lost touches. The touches that the earlier process held in its buffer may be lost, so check its log for the stop line and wait until its row is older than 30 minutes;
* the consistency check did not run (`--consistency=false` is refused for `--condemn`), or found objects without a `gc` row;
* `gc` rows with outdated or newer edge rules (`objects.outdated_rows`, `objects.newer_rows`);
* `live_mounts.errors` or `workspaces.errors`: a pin that could not be read may hide commits a mount or workspace needs;
* a dangling root in the classes `source`, `project-status`, `pushed-packs`, `workspace`, `mount` or `permanent`. Dangling `pinned-revisions` roots, for example of projects a mount skipped with `--ignore-missing-projects`, do not refuse; check them in `dangling_revisions` and its samples.

`--expected-replicas` counts host names, not processes, because each server process writes its own row (`<host>-<random>`). VMs keep their host names over a restart. On Kubernetes, run the servers as a StatefulSet, so that a restarted pod keeps its name; a Deployment pod gets a new name, so a crashed pod's row counts as one more host for 30 minutes and the cycle refuses.

A cycle that condemns:

1. Reads the list that `GcState.ListLocation` names, the list of the last condemn cycle, and has the mark record the root that first reaches each of its keys.
2. Deletes the markers of the keys of that list that the mark found live and writes every such key to `gc/condemned/<state>/<cycle>.live` in the server bucket: gzipped JSON lines with the key, its type, the root that reached it first (`root`, as for `--explain`), its touch and write times, the cycle number of the marker it held, and `rescued` when the marker was already gone, so a server cleared it on a read (only for a list whose cycle wrote all its markers). One other case also sets `rescued` with no read: a key whose marker a later cycle cleared before it stopped short of writing its own list (that cycle's `.live` object lists it, unless the cycle stopped before writing it). A marker write that Bigtable left out of a bulk write response fails the cycle, so its list stays incomplete and the next cycle sets no `rescued` for its keys. The report section `previous_list` counts these keys per type: in the list, still unreachable, live per root class with up to 20 keys per class, and rescued. Each key of the `.live` object needs an explanation, such as a ref moved back to an old target, a touch since the condemn or a read by a client; a key without one points to a root the mark misses.
3. Writes the list of the objects it condemns to `gc/condemned/<state>/<cycle>` (the state is `default`), and records it in `GcState` (`ListLocation`, `ListSnapshotTime`, `ListComplete` false). The list is a binary object: a header with the cycle, the snapshot time, the run and the type names, then per object its 20-byte key, its type and its size, in key order, 29 bytes per object.
4. Writes the markers of the objects of the list, in key order, in batches of 2,000, at most `--condemn-rate` (default `5000`) per second (`0` means no limit). It marks only objects whose `objects` row the consistency scan read, so it never creates a row. Each write replaces any older marker, so an object that stays unreachable carries the newest cycle number. Then it sets `ListComplete`.
5. Removes the list and `.live` objects of all but the newest `--keep-lists` (default `7`) cycles.

The list is written before any marker, and the markers of the previous list are cleared before the new list replaces it, so a cycle that stops midway leaves every marker in a list that the next cycle reads: that cycle clears the markers of keys now live and rewrites the others to its own number. The report section `condemn` counts the objects marked, per type, and their bytes.

A cycle reads the previous list once and writes one list and one `.live` object; at 29 bytes per object, a list of 10 million objects is about 290 MB. Besides the memory of the dry run, the collector holds the keys of the previous list and one map entry for each.

### Enable or disable garbage collection

Garbage collection is off unless it is turned on, on the servers and on the clients. Turn it on in this order:

1. Run `abfs server gcp init --gc` to add the garbage collector resources. A server with `--gc` refuses to start without them.
2. Restart every server with `--gc`. From then on each new object gets its `gc` row.
3. Run `abfs server gcp gc backfill` for the objects added before. Start it after the last server was restarted with `--gc`, so that no object is added without a row after the scan passed its key.
4. Turn on the client option `gc-keepalive`, for all clients at once in the central client configuration (`abfs edit config:clients/configs`, see the [System Admin doc](README.md)) with `gc-keepalive: true` under `config:`, or per client with `abfs config`. Mounts started after the change rewrite their mount ref every `gc-keepalive-ref-interval` (default `15m`) and send the hashes of what they hold to the server every `gc-keepalive-touch-interval` (default `18h`), at most 10,000 hashes per second. A send that fails is tried again after 5 minutes, then after twice the previous wait, up to half the touch interval, until one works. Both intervals apply only with `gc-keepalive`; `0` turns either off.
5. Run `abfs server gcp gc run --dry-run` once per `--cycle` while seeding and builds run, and check each report: no `replicas.warnings`, no `consistency.missing_gc_rows`, no `objects.outdated_rows`, and the dangling edges and roots.
6. When the dry runs are clean, run `abfs server gcp gc run --condemn` once per `--cycle` instead (see [Condemn mode](#condemn-mode)). A delete mode does not exist yet.

Off means the state before step 1. A server without `--gc` reads and writes only what a server without garbage collection does and starts on a deployment that plain `init` created; a client without `gc-keepalive` sends nothing extra. The garbage collector resources can stay in place.

These changes that come with garbage collection apply even when it is off. Each makes a missing object a recoverable case instead of a failure:

* `GetObject` over gRPC reports an object that disappears while it is read as not found (`NotFound`) instead of an unknown error. This applies to every server type.
* A cached map result whose object is missing is skipped as a miss: `maps.MapExisting` tries the next result and stores its input again when none is left, and ninjago treats a missing eval response as a cache miss.
* The git-pusher forgets the LFS objects it pushed for a project after a push of that project fails, so the next push checks them against the server again.
* Cacheman serves a `/remote-headers` request, used only by the client keep-alive.

To turn garbage collection off, restart the servers without `--gc` and turn `gc-keepalive` off. Objects added while it is off get no `gc` row, so turning it on again needs steps 2, 3 and 4 again; until the backfill ran, the consistency pass of the dry run reports the objects without a row, and the condemn mode refuses to run while any are missing. Markers that a condemn cycle wrote stay on their rows while garbage collection is off; servers without `--gc` do not read them, and the next condemn cycle rewrites or clears them.

### Limits of the garbage collector

* A mount is kept alive only by a client with `gc-keepalive`. Mounts of clients without it, or of older clients, and mounts in remote read-only mode, which write no mount ref, are protected only by the refs they were mounted from and, for a mount that wrote its ref, for `--mount-idle` after the mount started. What they loaded can go once those refs move and `--grace` has passed.
* With `gc-keepalive`, `abfs ws mount-repo -b <branch>` (used by `abfs mount --ws` and `--tmp-ws`) pins `default.xml` of the branch when it mounts and records the pinned manifest in the mount details; `abfs ws mount` and a mount of a snapshot pass on the pin of the mount the snapshot was taken from, and mounts made from a manifest hash or a manifest file pin it. This version resolves `-b <name>` for the pin as `refs/heads/<name>`, then `refs/tags/<name>`, then as the full ref `<name>`; a version whose pin resolves only `refs/heads/<name>` records no pin for a tag or a full ref name, which the mount still checks out. No pin is recorded when a project of the manifest, in any group, does not resolve on the server, with `--smart-sync` or `--smart-tag`, or for a snapshot whose mount had none; the mount goes on and logs a warning. A mount without `-b`, which checks out the HEAD of the manifest project, gets no pin either and logs nothing. Such a mount ref keeps only the mount details, so the commits and trees the mount reads stay only while the branch reaches them or within `--grace`; a force push or a deleted branch can leave them unreachable. The report counts these mounts in `live_mounts.without_pinned_manifest`. The pin does not follow the checkouts: a branch that moves between the pin and `repo sync`, and a later `repo sync` in the workspace, check out commits that only their branch keeps.
* The servers' `--gc-touch-interval` must be at most half of the collector's `--grace`, and their `--cache-ttl` shorter than `--cycle` and at most the collector's `--cache-ttl`. The flags are set apart; the dry run warns about each server that breaks the rule.
* Only servers with `--gc` write a heartbeat. A server started without `--gc`, or with `--gc-heartbeat-interval 0`, writes no row in `GcReplicas`, so the collector cannot see it: it does not touch or rescue what its clients use, and the dry run gives no warning about it. The column `GcEnabled` is reserved for that case and is always true today. The condemn mode checks the number of server hosts against `--expected-replicas`, which is how it sees a server without `--gc`.
* `abfs ws snapshot <path>` prints the hash of a snapshot and writes no ref, so nothing keeps that snapshot, or work tree content only it holds, once `--grace` has passed after its last use. The condemn mode marks it, and a later `ws snapshot --restore` or `abfs mount -h <hash> --ws` rescues it. Use named workspaces for work that must stay.
* A condemned object read from a server's cache is not rescued, and neither are reads that never reach a server: cacheman's local content, signed URL reads within their 10 minutes, and ninjago's input checks from local attributes. Touches that a server dropped or failed to write (`abfs.gcp.gc.touches`), which include touches that Bigtable left out of a bulk write response, and touches in a server's buffer when it crashed, was killed or could not write them while stopping (its stop line, and the heartbeat row it leaves), are lost the same way. The next condemn cycle finds such objects live only if a root reaches them or a touch was written.
* The pin of a mount holds the revisions of the projects in its manifest, not the commit of the manifest project itself. When the manifest branch is force-pushed, the manifest commit the mount checked out is kept only by the manifest branch and by `--grace`.
* `abfs rerun`, `abfs file-access` and `abfs export` on a build whose eval map rows are older than `--map-retention` fail on the missing eval responses, even while the build itself is kept by `--build-retention`.

## Run the server

Both commands take the same flags. `abfs server gcp` is a subcommand of the `abfs` binary; flags of `abfs server` go before `gcp`:

```sh
abfs server --port 50051 gcp --project <project> --instance <spanner-instance> --bucket <bucket>
```

`abfs-server` is a separate binary (`make abfs-server`) with one flat list of flags, as in earlier `abfs-server` releases:

```sh
abfs-server --port 50051 --project <project> --instance <spanner-instance> --bucket <bucket>
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--project` | | Project of the Spanner and Bigtable instances (required) |
| `--instance` | | Spanner instance (required) |
| `--db` | `cas` | Spanner database |
| `--bt-instance` | `objects` | Bigtable instance |
| `--bt-app-profile` | instance default | Bigtable app profile; it must use single-cluster routing |
| `--bt-conn-pool` | `50` | Bigtable gRPC connections |
| `--bucket` | | Cloud Storage bucket (required) |
| `--headers-cache` | `100000000` | Object headers cached per server; about 180 bytes each |
| `--contents-cache` | `1000000` | Object content entries cached per server; entries of objects up to 32 KiB hold the object, so they can take up to about 49 KB each |
| `--links-content` | off | Return signed Cloud Storage URLs, valid for 10 minutes, for objects larger than `--links-content-threshold`, so that clients download them from Cloud Storage directly. Clients then need access to `storage.googleapis.com`, and the server signs one URL per Cloud Storage object on every read. Without it, the server streams every object. |
| `--links-content-threshold` | `1048576` | Size in bytes above which `--links-content` returns URLs |
| `--gcp-client-metrics` | off | Export the built-in metrics of the Spanner, Bigtable and Cloud Storage client libraries to Cloud Monitoring |
| `--gc` | off | Run the server side of the garbage collector, described in [What the server does for the garbage collector](#what-the-server-does-for-the-garbage-collector). It needs the resources of `abfs server gcp init --gc`. Off, the server reads and writes only what a server without garbage collection does, and the three flags below have no effect. |
| `--cache-ttl` | `6h` | With `--gc`, how long cached object headers and content are used before the server reads them again from Bigtable; `0` keeps them until they are evicted |
| `--gc-touch-interval` | `36h` | With `--gc`, length of the period in which the server writes at most one touch per object and map row; `0` writes none |
| `--gc-heartbeat-interval` | `5m` | With `--gc`, time between two heartbeats of the server in the Spanner table `GcReplicas`; `0` writes none |
| `--no-link-validation`, `--no-hash-validation` | off | As for the file-based server |
| `--http-host` | `localhost` | Address of the HTTP server (`--http-port`). Its endpoints, such as `/debug` and `/server/status`, have no authentication. |

The other flags (`--port`, TLS, `--auth-enabled`, `--acl-enabled`, logging) are the same as for the file-based server.

In builds that export metrics with OpenTelemetry, `OTEL_METRICS_EXPORTER` chooses where the server's metrics go, as described under Metrics in the [System Admin doc](README.md). Builds whose System Admin doc has no such section serve Prometheus metrics on `/metrics` of the HTTP server.

Several servers can share the same database, tables and bucket. Put them behind one address, for example a load balancer, and give clients that address in `--remote-servers`.

## What the server does for the garbage collector

The garbage collector runs as a separate job. With `--gc`, the server records for it which objects are used, and keeps objects that the collector marked for deletion when they are used again. Nothing in the server deletes data. Without `--gc` none of the following happens.

* **GC edges.** `PutObject` writes the `gc` row of each new object, with the GC edges, before its object row. This adds one Bigtable write to each upload. A failed `gc` row write fails the upload.

* **Rescue.** The collector marks an object it plans to delete with the cell `g:condemned` of its `objects` row. The server reads that cell with every object row it reads: in `Headers`, the existence check of `PutObject`, the link checks of `PutObject`, `PutRefs`, `XchgRefs` and `AddMapResult`, `GetObject` and `MapResults`. On a marked row it first writes a touch, then clears the marker, but only while the marker still holds the value it read, and answers as for any other object. A rescue whose touch fails fails the call and leaves the marker, so that the client's retry rescues it again: a condemn write that is retried or lands late can set the marker again after a clear, and then only the touch keeps the next cycle from finding the object unreachable. When the collector deleted the row first, the object is missing, and a client that writes it stores it again; the touch stays behind as a `gc` row with only the touch. The conditional writes need an app profile that allows single-row transactions, which the server checks at start. Rescue has no switch of its own: with `--gc` every header read includes the family `g`. Only `gc run --condemn` writes markers; until it runs, this costs only the wider read. Up to 32 rescues of one call run in parallel. A failed rescue does not stop the others; the call fails with the first error once all have run, so a retry finds only the objects whose rescue failed still marked.
* **Rescue log.** Each call that rescued an object, or failed to, logs one line, up to 100 lines a minute per server: `Rescued <n> condemned objects of <m> read in <call>, <f> failed: request <id>, peer <address>, x-forwarded-for "<addresses>", client-version "<version>"`. `GetObject` reads one object per call, so each condemned file a client reads counts as one call. Behind a load balancer the peer is the proxy; an internal Application Load Balancer appends the client's address and then its own to `x-forwarded-for`. Up to 100 lines a minute per server then name each rescued object: `Rescued <type> <key> in <call>, condemned by GC cycle <n>: request <id>`. The object lines and the call line of one call share the request id, a random one per call; a traced call adds `, trace <trace ID>` after it, which the calls of one client operation can share. An object the collector deleted before the call could rescue it logs `Condemned object <key> was deleted before it could be rescued in <call>: request <id>, peer <address>, x-forwarded-for "<addresses>", client-version "<version>"`, with no limit. When a limit left lines out, the next minute that logs a rescue, and the stop, log `Left out the lines of <n> calls with rescues in the minute from <time>` or `Left out the lines of <n> rescued objects in the minute from <time>`. The `.live` object of the next condemn cycle lists every such key.
* **Touches.** When a call finds an object (`Headers`, `GetObject`, the existence check of `PutObject`, the link checks, the results of `MapResults`), the server writes the cell `t:touch` of the object's row in the table `gc`, with a Bigtable server timestamp. Each server writes at most one touch per object in each period of `--gc-touch-interval`, counted from the server start, so an object in use has a touch younger than the interval. `MapResults` also writes the cell of each map row it returns again, with the same limit, so the timestamp of a map row is the time it was last used. Map results therefore come back in the order of their last use, oldest first, instead of the order they were added. ninjago tries the newest result first, so builds are not affected. Touches wait in a buffer of 100,000 and four workers write them in batches of up to 1,000, at least once a second. When the buffer is full, `Headers` and `MapResults`, the calls of the client keep-alive and of builds, wait up to half a second per touch for room, so a burst, such as the keep-alive of a large mount, is slowed to the rate the workers write instead of losing touches. When a wait times out, touches are dropped and counted, without waiting, until the buffer has room again. So with `--gc` the latency of `Headers` and `MapResults` depends on the table `gc`: while the buffer is full they run at the rate the workers write, and when the writes stall, the first of them waits half a second. `GetObject`, the existence check of `PutObject` and the link checks never wait for room; they drop the touch and count it. The next use of a dropped object writes its touch.
* **Cache TTL.** The header and content caches keep an entry for `--cache-ttl` and then read it again from Bigtable, so that every server sees a marker long before the collector deletes the object. The collector needs its cycle to be longer than the TTL. A shorter TTL costs more Bigtable reads on directory listings.
* **Missing map results.** `MapResults` leaves out results whose object does not exist, with one header lookup for all results, from the cache where it can. So `MapResults` can now fail with a Bigtable error from the table `objects`. Each server remembers the last touch of 262,144 map rows; a server that returns more distinct rows per touch period touches a row again once it was forgotten, so its map row touches follow the rate of `MapResults` calls rather than the period.
* **Missing chunks.** When a chunk of an object is missing while the server streams it, the server drops the object from its caches and reads the object row again. If the row is gone, the read fails as a missing object; if the row still lists the chunk, the content is lost and the read fails with an error that is not "missing".
* **Heartbeat.** Each server writes its row in the Spanner table `GcReplicas` at start and every `--gc-heartbeat-interval`: an ID made of the host name and a random suffix, logged at start, the version of the GC edge rules it writes into the table `gc`, `GcEnabled` (reserved, always true, since only servers with `--gc` write a heartbeat), its `--gc-touch-interval` and `--cache-ttl`, and the commit time. The collector uses it to see which server versions and settings are running. A failed write is logged and counted; the server keeps running. When the server stops, it writes the touches left in its buffer, for up to 10 seconds, and logs one line `GC touches at stop: ...` with the touches written, dropped and failed during the stop and since the start, because its metrics are no longer exported by then. Then it deletes its row, unless a touch was dropped or failed during the stop. A row that stays after its server stopped marks an unclean stop (a crash, a kill, a preemption or touches lost at the stop) whose buffered touches may be lost.

Cost: one Bigtable write per object and map row used, per server and touch period, batched; one touch and one conditional write per rescue.

Metrics, as OpenTelemetry counters. Their names keep the dots when they are exported with OTLP. Cloud Monitoring stores them as they arrive, for example `prometheus.googleapis.com/abfs.gcp.gc.rescues/counter` through the Ops Agent OTLP receiver, so a PromQL query names them with `__name__`: `sum by (call, type) ({__name__="abfs.gcp.gc.rescues"})`. On a Prometheus `/metrics` endpoint the dots become underscores and the names end in `_total`, for example `abfs_gcp_gc_rescues_total`.

| Metric | Labels | Counts |
| --- | --- | --- |
| `abfs.gcp.gc.rescues` | `call`, `type` | Marked objects rescued, by call: `headers`, `validation`, `put_object`, `get_object`, `map_results`, and by object type, `other` for a type the server does not know |
| `abfs.gcp.gc.rescues_lost` | `call` | Marked objects that the collector deleted before the call could rescue them |
| `abfs.gcp.gc.rescue_failures` | `call` | Rescues that failed, including those whose touch failed; each failed its call |
| `abfs.gcp.gc.touches` | `result` | Touches `written`, `dropped` because the buffer was full, or `failed` |
| `abfs.gcp.cache.ttl_expiries` | `cache` | Cache lookups of `headers` or `content` that found an entry older than `--cache-ttl` |
| `abfs.gcp.gc.map_results_dropped` | | Map results left out because their object is missing |
| `abfs.gcp.gc.heartbeat_failures` | | Failed heartbeat writes |

## Try it locally with emulators

This runs the server against the Spanner and Bigtable emulators and the Cloud Storage testbench in Docker, with no Google Cloud project.

Start the emulators:

```sh
docker run -d --rm --name abfs-spanner -p 127.0.0.1:9010:9010 -p 127.0.0.1:9020:9020 \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud emulators spanner start --host-port=0.0.0.0:9010 --rest-port=9020
docker run -d --rm --name abfs-bigtable -p 127.0.0.1:8086:8086 \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators bigtable start --host-port=0.0.0.0:8086
docker run -d --rm --name abfs-gcs -p 127.0.0.1:9000:9000 -p 127.0.0.1:8888:8888 \
  gcr.io/cloud-devrel-public-resources/storage-testbench:latest
```

When they are up, create the Spanner instance and the bucket, start the testbench's gRPC port and point ABFS at the emulators:

```sh
curl -s -X POST localhost:9020/v1/projects/local-proj/instances \
  -d '{"instanceId": "abfs", "instance": {"config": "emulator-config", "displayName": "abfs", "nodeCount": 1}}'
curl -s -X POST -H "Content-Type: application/json" "localhost:9000/storage/v1/b?project=local-proj" -d '{"name": "abfs-chunks"}'
curl -s "localhost:9000/start_grpc?port=8888"
export SPANNER_EMULATOR_HOST=localhost:9010
export BIGTABLE_EMULATOR_HOST=localhost:8086
export STORAGE_EMULATOR_HOST_GRPC=localhost:8888
```

Create the database and tables and start the server:

```sh
abfs server gcp init --project local-proj --instance abfs --bucket abfs-chunks
abfs server --port 50051 gcp --project local-proj --instance abfs --bucket abfs-chunks
```

In another shell, check it:

```sh
abfs --remote-servers localhost:50051 --disable-tls --auth-type none test-client health-check
abfs --remote-servers localhost:50051 --disable-tls --auth-type none test-client headers 4b825dc642cb6eb9a060e54bf8d69288fbee4904
```

The second command prints `4b825dc642cb6eb9a060e54bf8d69288fbee4904 tree 0`. From here, follow the [System Admin doc](README.md) from "Configuration" on. Stop the emulators with `docker stop abfs-spanner abfs-bigtable abfs-gcs`; they keep no data.

# ABFS / System Admin / GCP server garbage collection

Garbage collection removes objects that nothing needs any more from the tables and the bucket of a [GCP server](GCP_SERVER.md). This page explains how it works, how to turn it on step by step, and then each command and server behaviour in detail.

## How garbage collection works

Garbage collection is off by default. Three parts take part when it is on:

* the collector, `abfs server gcp gc run`, a job that runs one cycle at a time on a schedule, such as once a day (see [Timing bounds](#timing-bounds));
* the servers, started with `--gc`, which record for the collector which objects are used;
* the clients, with the client option `gc-keepalive`, which keep their mounts and the content they wrote visible to the collector.

**Roots.** The collector keeps everything that a root reaches:

* every ref of every project, such as branches, tags and `HEAD`;
* the refs that clients write in the project `abfs-meta`: mount refs within `--mount-idle` and workspace refs, snapshot refs for `--snapshot-retention` after their last write, and build and unmount refs within their retention; the mount refs of live mounts, workspace refs and snapshot refs also keep the commits that the pinned manifest of their mount names. The mount ref of a mount that `abfs unmount` ended keeps only its mount details, until it goes with its unmount ref after `--build-retention` or at `--mount-idle`, whichever comes first;
* map rows, such as cached build results, within their retention;
* every object written or used within the grace period (`--grace`, default `72h`).

**Mark.** Each server writes, for every object it stores, a row in the Bigtable table `gc` with the objects it links (its GC edges). The collector reads every ref at one snapshot time, scans the `gc` table and marks in memory everything the roots reach. What stays unmarked is unreachable. It reads no object content.

**Grace.** A new object is unreachable for a while: an upload stores the children of a commit before the ref that links it, clients reuse hashes they hold, and a server records a use of an object at most once per `--gc-touch-interval`. So an object counts as garbage only when it was neither written nor used within `--grace`; [Root classes](#root-classes) gives the smallest value the command accepts.

**Condemn, rescue, delete.** Nothing is deleted in the cycle that finds it unreachable:

```
cycle N     mark; condemn: write list N to the bucket and set marker N on each
            unreachable object
            a server that reads a marked object clears the marker (rescue)
cycle N+1   mark again; delete each object of list N that is still unreachable
            and still holds marker N; record its chunks; condemn again (list N+1)
cycle N+2   delete the chunks recorded in cycle N+1; delete from list N+1; ...
```

A rescue shows that an object was still in use, so the delete of cycle N+1, a conditional write on the marker, does not match. Large objects keep chunks in the table `chunks` and in the bucket; these go one cycle after their object, so that a reader that already started still finds them.

**Client content.** A mount of a client with `gc-keepalive` rewrites its mount ref every 15 minutes and sends the hashes of what it holds to the server every 18 hours, which keeps them used. Cacheman with `gc-keepalive` keeps content a client wrote out of eviction until a server confirmed it holds the object. `abfs ws snapshot` writes a ref for the snapshot, which keeps it for `--snapshot-retention` after its last `ws snapshot` or `--restore`.

**Servers.** Each server with `--gc` writes a heartbeat row to the Spanner table `GcServers` with its settings, whether it lost a record of a use (a touch) of an object or of a map row, and its stop. A use the collector cannot see may be the last use of an object it deletes, so the collector refuses to condemn when the number of server hosts does not match, and refuses to delete when any server that ran within the grace crashed, lost an object touch, paused or ran with settings that break the timing rules. A lost map row touch makes the keep-alive limit of the live mounts shorter (see [Client caches](#client-caches)).

**Staged path.** Turn it on in three modes, each after the one before ran clean: `--dry-run` only reports, `--condemn` also marks, and `--delete` also deletes. Delete mode needs backups of the tables between cycles, which `gc undo` restores from.

## Commands

| Command | What it does | Writes | When to run it |
| --- | --- | --- | --- |
| `abfs server gcp init --gc` | Creates the garbage collector tables and columns | Spanner and Bigtable schema | Once, and with each new version before its servers start |
| `abfs server gcp --gc` (server flag) | The server side: `gc` rows, touches, rescues, heartbeats | `gc` rows, touches, heartbeat rows; deletes nothing | On every server while garbage collection is on |
| `gc backfill` | Writes the `gc` rows that objects lack or that have outdated edge rules | `gc` rows | After every server runs with `--gc`, and after an upgrade of the edge rules |
| `gc run --dry-run` | Runs one cycle that only reports | `GcState`, removal of old heartbeat rows | Before condemn mode, and any time to look |
| `gc run --condemn` | A dry run that also lists and marks the unreachable objects | Lists in the bucket, markers | On the job's schedule, after clean dry runs |
| `gc run --delete` | A condemn cycle that first deletes from the previous list, deletes chunks and removes expired refs | Deletes rows, chunks and refs; writes records | On the job's schedule, after clean condemn cycles, with backups between cycles |
| `gc undo` | Writes back what delete cycles deleted, from tables restored from backups | `objects`, `gc`, `maps` and `chunks` rows, bucket metadata | After a wrong delete, with the delete job stopped |
| `gc walk` | Tells whether keys are in the checkouts of mounts, from object content | Nothing | To explain a key that a client used after it was condemned |

All `gc` commands are subcommands of `abfs server gcp gc` and take `--project`, `--instance`, `--bucket`, `--db`, `--bt-instance` and `--bt-app-profile` as the server does.

## Enable or disable garbage collection

Garbage collection is off unless it is turned on, on the servers and on the clients. Turn it on in this order:

1. Run `abfs server gcp init --gc` to add the garbage collector resources. A server with `--gc` refuses to start without them.
2. Restart every server with `--gc`. From then on each new object gets its `gc` row.
3. Run `abfs server gcp gc backfill` for the objects added before. Start it after the last server was restarted with `--gc`, so that no object is added without a row after the scan passed its key.
4. Turn on the client option `gc-keepalive`, for all clients at once in the central client configuration (`abfs edit config:clients/configs`, see the [System Admin doc](README.md)) with `gc-keepalive: true` under `config:`, or per client with `abfs config`. Set it in the configuration cacheman starts from as well as in the one the mounts start from. Mounts started after the change rewrite their mount ref every 15 minutes, a fixed interval that their mount details record (a configuration that still sets `gc-keepalive-ref-interval` loads, and the key is ignored), and send the hashes of what they hold to the server every `gc-keepalive-touch-interval` (default `18h`), at most 10,000 hashes per second. A send that fails is tried again after 5 minutes, then after twice the previous wait, up to half the touch interval, until one works. The touch interval applies only with `gc-keepalive`; `0` turns the sends off.
5. Run `abfs server gcp gc run --dry-run` on the job's schedule while seeding and builds run, and check each report: no `replicas.warnings`, no `consistency.missing_gc_rows`, no `objects.outdated_rows`, and the dangling edges and roots.
6. When the dry runs are clean, run `abfs server gcp gc run --condemn` on the same schedule instead (see [Condemn mode](#condemn-mode)).
7. Prepare delete mode: a Bigtable instance with one cluster (see [Delete checks](#delete-checks)), an [app profile](#app-profile-of-the-collector) and the [permissions](#permissions-of-the-collector) of the collector, the bucket's [soft delete](#soft-delete-of-the-bucket), versioning off and no retention policy, and a job that takes [backups](#backups) between cycles.
8. When the condemn cycles have run clean for a week or more under real use, run `abfs server gcp gc run --delete` on the same schedule instead (see [Delete mode](#delete-mode)). Before the first one, check the last condemn reports:
   * `delete.refused` names no reason you cannot explain;
   * the `delete.plan` counts per type and their samples are explained;
   * `live_mounts.without_keepalive` and `live_mounts.without_local_write_check` are 0;
   * `previous_list` shows no rescued or live key without an explanation (see [The .live object](#the-live-object)).

   Snapshots taken before `gc-keepalive` was on have no ref, so the first delete cycles delete each one that no named workspace holds, with the work tree content only it holds. Before the first delete cycle, have users restore every such snapshot they want to keep into a spare workspace, such as a new `--tmp-ws` mount, with `abfs ws snapshot --restore <hash> <path>`, which writes its ref, or keep the work in a named workspace. A restore replaces the work tree at `<path>`, so never restore into a workspace with work in it. Widen `--delete-types` step by step, as the [operator rules](#operator-rules) say.

The first `--delete` cycles after an upgrade only condemn: a list written by a collector of an older version has no completion time, and the window of a delete cycle must start after a cycle first found no server of an older version. Deletes start once both hold.

Off means the state before step 1. A server without `--gc` reads and writes only what a server without garbage collection does and starts on a deployment that plain `init` created; a client without `gc-keepalive` sends nothing extra. The garbage collector resources can stay in place.

These changes that come with garbage collection apply even when it is off. Each makes a missing object a recoverable case instead of a failure:

* `GetObject` over gRPC reports an object that disappears while it is read as not found (`NotFound`) instead of an unknown error. This applies to every server type.
* A cached map result whose object is missing is skipped as a miss: `maps.MapExisting` tries the next result and stores its input again when none is left, and ninjago treats a missing eval response as a cache miss.
* The git-pusher forgets the LFS objects it pushed for a project after a push of that project fails, so the next push checks them against the server again. It also forgets a cached deep tree that the server reports missing, and computes it again.
* Cacheman serves a `/remote-headers` request, used only by the client keep-alive.

To turn garbage collection off, stop the collector job, restart the servers without `--gc` and turn `gc-keepalive` off. Objects added while it is off get no `gc` row, so turning it on again needs steps 2, 3 and 4 again; until the backfill ran, the consistency pass of the dry run reports the objects without a row, and the condemn and delete modes refuse to run while any are missing. Markers that a condemn cycle wrote stay on their rows while garbage collection is off; servers without `--gc` do not read them, and the next condemn cycle rewrites or clears them.

### Upgrades

* Run `init --gc` with the new version first, then upgrade the servers and the collector together. A collector of an older version sees no heartbeat of servers that write `GcServers`, so its condemn cycles refuse on the host count.
* After the upgrade, no server of an older version may run against the store, not even briefly. The collector sees such a server only while it runs at one of its reads of `GcReplicas`; one that starts and stops between two reads leaves no trace, and it ran without the timing bounds and loss records the delete checks rely on. A server of an older version started with `--gc-heartbeat-interval 0` writes no row at all.
* Use the same or a newer version for the collector than for the servers: a `gc` row with an object type the collector does not know stops the run, and a collector of an older version does not read `MapRowLossTime`, so it misses the map row touches that servers of this version lost, which older servers recorded in `TouchLossTime`.

## Where the data goes

| Data | Service |
| --- | --- |
| The GC edges of each object, the objects it references, and its last touch | Bigtable table `gc` |
| The condemn marker of an object | Bigtable family `objects:g` |
| The lease and cycle state of the collector | Spanner table `GcState` |
| Server heartbeats | Spanner table `GcServers`; `GcReplicas` holds those of servers of older versions |
| Condemned lists and their `.live` objects | bucket, `gc/condemned/<state>/` |
| Delete records | bucket, `gc/deleted/<state>/` |
| Ref records | bucket, `gc/refs/<state>/` |

The state is `default`.

## Resources and permissions

### Create the resources

With `--gc`, `abfs server gcp init` also creates the garbage collector resources: the Bigtable table `gc`, the column family `g` of `objects` and the Spanner tables `GcState`, `GcServers` and `GcReplicas`. Without it, init creates only the server's own resources. Running it again with `--gc` adds the garbage collector resources to an existing deployment, and the columns that newer versions added to `GcState` and `GcServers`.

Run it with the new version before you start servers of that version: a server with `--gc` checks these tables and columns at start and refuses to start without them. The statements are in `shared/remote/servers/gcp/casdb/gcschema.sql`, and each can be applied again without effect. A deployment that applies the schema with its own tooling as an append-only list of statements appends the new ones (`CREATE TABLE IF NOT EXISTS GcServers`, the four `ALTER TABLE GcState ADD COLUMN IF NOT EXISTS` statements of `ListCompleteTime`, `CondemnCycle`, `LegacyServersGoneTime` and `MaxGraceSeconds`, and `ALTER TABLE GcServers ADD COLUMN IF NOT EXISTS MapRowLossTime`) before it upgrades the servers.

`init` creates missing tables and does not change existing ones.

At start, a server with `--gc` checks the garbage collector resources, then applies one conditional write: a delete of the cell `g:condemned` of the absent row `abfs-setup-check`. It retries it on a transient error and stops when Bigtable refuses it or it still fails. A server rescues condemned objects with conditional writes (see [Rescue](#rescue)), so its app profile must use single-cluster routing and allow single-row transactions. The default app profile of an instance created with one cluster allows them; for another profile run `gcloud bigtable app-profiles update <profile> --instance=<bt-instance> --route-to=<cluster> --transactional-writes`. Multi-cluster routing cannot allow them.

The `gc` commands need the garbage collector resources and refuse to run without them. Their own reads of objects leave the garbage collector state as it is: they rescue and touch nothing, so what the collector reads is not kept because it read it.

### App profile of the collector

The scans and writes of the collector are batch work. Give the `gc` commands their own Bigtable app profile with single-cluster routing, single-row transactions and priority low, and pass it with `--bt-app-profile`; the servers keep their own profile, at high priority:

```sh
gcloud bigtable app-profiles create abfs-gc --instance=<bt-instance> --route-to=<cluster> --transactional-writes --priority=PRIORITY_LOW --description="ABFS GC collector"
```

`gc backfill`, `gc run` and `gc undo` read the profile at start; `gc run --delete` fails before it scans when the profile cannot be read or lacks one of these settings, and the other modes and commands only warn. Every report records the profile in `app_profile`. With `--gcp-client-metrics`, off by default as on the server, the `gc` commands export the metrics of the client libraries, so the attempt latencies and retries of the collector show per app profile in Cloud Monitoring.

### Permissions of the collector

The service account that runs the `gc` commands needs:

| Permission | Granted by | Needed for |
| --- | --- | --- |
| Read and write of `GcState`, `GcServers`, `GcReplicas` and `Refs`, read of `Projects`, DML deletes | `roles/spanner.databaseUser` on the database | the lease, the heartbeats, ref removal |
| `bigtable.tables.readRows`, `bigtable.tables.mutateRows` | `roles/bigtable.user` | scans, markers, conditional deletes and the conditional write probe |
| `bigtable.appProfiles.get` | `roles/bigtable.user` | the app profile check at start; delete mode fails without it |
| `bigtable.clusters.list` | `roles/bigtable.user` | the cluster count |
| `bigtable.backups.list` | `roles/bigtable.user` | the backup checks of the delete and chunk steps |
| `bigtable.tables.get` | `roles/bigtable.user` | the check of the restored tables in `gc undo` |
| `monitoring.timeSeries.create` | `roles/bigtable.user`, also in `roles/storage.objectUser` | client library metrics (`--gcp-client-metrics`) |
| `storage.objects.get`, `list`, `create`, `delete`, `update` | `roles/storage.objectUser` on the bucket | lists and records, generation reads and chunk deletes, the `chunks-done` flag and the `abfs-undo` stamp |
| `storage.buckets.get` | not in `roles/storage.objectUser`; `roles/storage.legacyBucketReader` on the bucket, or a custom role | the soft delete, versioning, retention and hold checks of the chunk step, and the retention of records |

Role contents: [Bigtable roles](https://docs.cloud.google.com/iam/docs/roles-permissions/bigtable) and [Cloud Storage roles](https://docs.cloud.google.com/iam/docs/roles-permissions/storage). A missing permission on a delete check refuses the deletes of the cycle and names the permission. Without `storage.buckets.get` the chunk step deletes nothing and no record is removed.

Operators need more for the runbooks: creating Bigtable backups needs `bigtable.backups.create`, and restoring them `bigtable.backups.restore` and `bigtable.tables.create` (for example `roles/bigtable.admin`), which `roles/bigtable.user` does not have ([backups](https://docs.cloud.google.com/bigtable/docs/backups)). Restoring soft-deleted chunks needs `storage.objects.list`, `storage.objects.restore` and `storage.objects.create`, all in `roles/storage.objectUser`.

## Fill the gc table

With `--gc` on, the server writes the `gc` row of each object it adds. Objects added before that, or while it was off, have none. `abfs server gcp gc backfill` scans the `objects` table and writes the missing rows, and the rows whose edge rules are outdated for their object type; it reads every such object that is not a blob:

```sh
abfs server gcp gc backfill --project <project> --instance <spanner-instance> --bucket <bucket> [--dry-run] [--rate 2000] [--shards 16]
```

* Each row it writes carries the time of the object row, so the garbage collector's `--grace` counts from the upload of the object, not from the backfill.
* `--dry-run` only counts the rows to write.
* `--rate` limits the object rows scanned per second (0 means no limit).
* The scan can stop at any time; it prints a key to continue from with `--start`. Running it again skips the rows that are already current.
* Do not run it while delete cycles run: stop the collector job, or run it without `--delete`, until the backfill has finished. A delete cycle checks that each object written during its consistency scan has a `gc` row, which only a server with `--gc` writes; a backfill that writes the row of such an object in between hides the server without `--gc` that wrote it.

A row is current when its version of the edge rules is at least the one its object type requires; only the rows of types whose rules changed are rewritten. Version 2 gives each deep tree an edge to every deep tree nested in it: a directory with 1,000 or more entries below it is stored as a deep tree of its own, which only deep trees link. Version 1 gave deep trees no edges, so the dry run counted those nested deep trees as unreachable although mounts read them.

After upgrading from version 1, run the backfill again; it rewrites only the `deeptree` rows. A server still on version 1 keeps writing deep tree rows without these edges: the dry run counts them in `outdated_rows` (in total and per type) and warns about that server in `replicas.warnings`. Run the backfill again once the last server is upgraded.

## Dry run

`abfs server gcp gc run --dry-run` runs one garbage collection cycle that only reports. It deletes and marks no data; its only writes are to the `GcState` row and the removal of old heartbeat rows of `GcServers` and `GcReplicas`, all in Spanner. `gc run` takes exactly one of `--dry-run`, `--condemn` (see [Condemn mode](#condemn-mode)) and `--delete` (see [Delete mode](#delete-mode)).

```sh
abfs server gcp gc run --dry-run --project <project> --instance <spanner-instance> --bucket <bucket> [--report gc-report.json] [--explain <key>[,<key>]]
```

### A cycle

1. Takes the lease in `GcState`, increments the cycle number and records the start and snapshot times. While another process holds a valid lease the command fails. The lease (5 minutes) is renewed while the cycle runs, and the cycle stops itself if the lease is lost or cannot be renewed in time.
2. Reads the server heartbeats (see [Heartbeat](#heartbeat)) in `GcServers`, and in `GcReplicas` those of servers of older versions. It warns about each live server, one seen within the last 30 minutes, whose settings break the timing rules:
   * a `--gc-touch-interval` of 0 or longer than half of `--grace`;
   * a `--cache-ttl` of 0;
   * GC edge rules older than the command's.

   It also warns when `--grace` is shorter than the largest `--cache-ttl` of the servers that may have run within the grace plus `--longest-push`, and when no such server wrote a heartbeat; `settings.cache_ttl_hours` reports that largest TTL. The warnings are in the report and the log; the condemn and delete modes refuse to run with any of them. The report counts the distinct host names among the live servers (`replicas.hosts`, the part of each ID before its last `-`) and their live rows per host (`replicas.rows_per_host`). A server that stops cleanly records the stop in its row, which then no longer counts as live unless the stop recorded lost object touches (`replicas.stopped` counts the others).

   The command deletes the `GcServers` rows whose last heartbeat and stop are older than the longest of a day, the largest `--grace` of any condemn or delete cycle plus 10 hours, and its own `--grace` plus 10 hours (`replicas.servers_removed`), and the `GcReplicas` rows not written for a day (`replicas.removed`); the 10 hours are an hour and the 9 hours before the window in which a lost map row touch still counts. It records in `GcState` since when every read found `GcReplicas` empty (`replicas.legacy_servers_gone_time`), and the latest lost map row touch that shortens the keep-alive limit (`replicas.map_row_loss_time`, see [Client caches](#client-caches)). For a server that crashed, that time is 30 minutes after its last heartbeat.
3. Reads every ref at one snapshot time, with low priority, and sorts the refs into retention classes (below).
4. For each live mount (see [Mounts and unmounts](#mounts-and-unmounts)), reads its mount details and its pinned manifest, and keeps the project revisions that are object hashes: the commits the mount reads. For each workspace ref and each snapshot ref, reads its workspace snapshot, the mount details it was taken from and their pinned manifest, and keeps their revisions the same way: the commits the workspace is based on, which the snapshot does not link. Other revisions, such as branch names, are counted and ignored.
5. Scans the `gc` table twice in 16 key ranges: first keys, types and times, then the edges, and the keys of the `maps` table. It reads no object content.
6. Marks in memory everything the roots reach. It walks the roots one at a time in a fixed order: the refs of the classes source, workspaces, snapshots, mounts and unmount records, then the pinned revisions, builds, other `abfs-meta` refs, empty objects, recent objects and map rows, so that the report can name the root that reached an object first. Edges to objects without a `gc` row are skipped and counted. A `gc` row with an object type the binary does not know stops the run.
7. Scans the `meta` family of the `objects` table to check that every object has a `gc` row and to sum the sizes (`meta:size`) of reachable and unreachable objects. This reads every object row once. In a delete cycle the scan also checks each object written during it (see [Delete checks](#delete-checks)).
8. Writes a JSON report to stdout or to `--report`, and logs a one-line summary.

### Root classes

| Class | Roots | Retention |
| --- | --- | --- |
| Source | every ref outside the classes below, such as branches, tags and `HEAD`; `abfs/project-status`, `abfs/pushed-packs`, `config` and `acl` are counted separately | while the ref exists |
| Workspaces | `refs/abfs/ws/...` of the project `abfs-meta`, and the revisions of the pinned manifest of the mount each snapshot was taken from | while the ref exists |
| Snapshots | `refs/abfs/snapshot/...` of `abfs-meta` (see [Snapshot refs](#snapshot-refs)), and the revisions of the pinned manifest of the mount each snapshot was taken from | `--snapshot-retention` (default `720h`) since the ref was written |
| Mounts | `refs/abfs/mount/...` of `abfs-meta`, and for live mounts the revisions of their pinned manifests (see [Mounts and unmounts](#mounts-and-unmounts)) | while the client rewrote the ref within `--mount-idle` (default `336h`); the ref of an ended mount, which keeps only its mount details, until it goes with its unmount ref after `--build-retention` or at `--mount-idle`, whichever comes first |
| Unmount records | `refs/abfs/unmount/...` of `abfs-meta`: the unmount details and the mount details they link | `--build-retention` since the ref was written |
| Builds | `refs/abfs/build/...` and `refs/abfs/build-start/...` of `abfs-meta` | `--build-retention` (default `720h`) since the ref was written |
| Eval map rows | the request and the result | `--map-retention` (default `720h`) since the row was written or used |
| Deep tree and digest map rows | the request and the result | while the tree of the request is reachable |
| LFS map rows | the request and the result | always |
| Recent objects | objects whose `gc` row was written or touched within `--grace` (default `72h`) before the snapshot | `--grace` |
| Empty objects | the empty file and the empty directory, which every server writes at start | always |

Map rows with request types not listed are kept. A ref past its retention is no root; delete mode removes it (see [Ref removal](#ref-removal)). `--grace` must be longer than `--longest-push` (default `3h`, the longest git-pusher push or build upload), or the command refuses to run. It must also cover the largest `--cache-ttl` of the servers that may have run within it plus `--longest-push`, or condemn and delete cycles refuse (step 2 of [A cycle](#a-cycle)). Each server records its cache TTL in its heartbeat row, and the collector reads it there, as the [timing bounds](#timing-bounds) of delete mode do. `--mount-idle` must be at least 1 hour, four times the interval at which clients rewrite their mount ref.

#### Mounts and unmounts

A mount writes the ref `refs/abfs/mount/<prefix>/<start>` when it starts, where `<prefix>` is its `--ref-prefix` (default `user/<user>`) and `<start>` its start time in nanoseconds. `abfs unmount` writes `refs/abfs/unmount/<prefix>/<start>` after it stopped the processes that used the mount point, the mount daemon among them, unmounted it and, with `--rm`, removed the workspace. It sees and stops only the processes of the user who runs it: a copy of the mount in another mount namespace that processes of another user use, such as a container running as root with a bind mount of the workspace, does not keep the unmount from succeeding and outlives it (see [Limits of the garbage collector](#limits-of-the-garbage-collector)). A failed or refused unmount writes none, nor does an unmount that cannot read the mount details at the mount point or runs with `--no-mount-details`.

* A mount ref without an unmount ref of the same name in the same project is a live mount: the revisions of its pinned manifest are roots, the [client cache checks](#client-caches) count it, and it stays live while the client rewrote the ref within `--mount-idle`. A mount that ended without `abfs unmount`, such as a crashed client, a killed daemon or a deleted pod, stays live that long.
* A mount ref with such an unmount ref is an ended mount. It stays a root of its mount details, but its revisions are no roots and it counts in none of the `live_mounts` fields, so it refuses no deletes in the [client cache checks](#client-caches). The report counts these refs in `refs.mount.ended`. Delete mode removes the mount ref together with its unmount ref once that ref is past `--build-retention`, or when the mount ref is past `--mount-idle`, whichever comes first.
* A workspace keeps the revisions of its mount through its workspace ref, not through the mount ref, so ending the mount does not affect it. A later mount of the same workspace or revision writes its own mount ref.

### Report

The report counts:

* per class, the refs, tombstones, roots and refs past their retention (`refs_to_tombstone` in total), and for mounts the roots that belong to ended mounts (`ended`);
* roots whose object has no `gc` row (dangling roots);
* objects and edges per type, reachable and unreachable, with up to 20 unreachable keys per type (`unreachable_sample`), and dangling edges with a few examples;
* map rows live and expired per class;
* the consistency result with the first objects missing a `gc` row. Objects without a `gc` row that were written after the scan of the `gc` table started, less a minute, are counted apart (`too_new`): an upload during the scans lands there when its `gc` row was written after the scan of the `gc` table passed its key, so the count grows with the upload rate and is not an error. `gc` rows without an object row (uploads in flight) are counted too;
* the time of each step and the peak heap;
* while `GcState` names the list of a condemn or delete cycle, `delete.plan`: what a delete cycle would remove from it now, the candidates and `would_delete` per type with their bytes, the levels and the map rows (see [Delete mode](#delete-mode)). `delete.plan_error` says why the list could not be read.

The sections `live_mounts`, `workspaces` and `snapshots` report the pins:

| Field | Counts |
| --- | --- |
| `live` | the mount refs of live mounts, the workspace refs or the snapshot refs |
| `without_pinned_manifest` | mount details without a pinned manifest, and workspace snapshots without mount details |
| `without_keepalive` | mount details of clients without `gc-keepalive` or with one of its intervals `0` |
| `without_local_write_check` | mount details with `gc-keepalive` whose cacheman does not keep locally written content until a server confirms it |
| `touch_interval_too_long` | mount details whose keep-alive touch interval plus a tenth is not shorter than `--grace` less the longest touch interval and cache TTL of the servers and a minute, and also less 9 hours when `replicas.map_row_loss_time` is set |
| `pinned_manifests`, `revisions`, `ignored_revisions` | the pinned manifests parsed, the revisions kept, and the revisions that are not hashes |
| `dangling_revisions` | kept revisions without a `gc` row |
| `not_found` | snapshots, mount details or manifests that do not exist; they pin nothing |
| `errors` | those that could not be read; a read error may hide a pin, so only read errors stop a condemn |

`samples` names up to 20 mount details per count, with the workspace snapshot, host and user, so that the clients behind them can be found.

### Explain

`--explain <key>[,<key>]` adds an `explain` section with, per key: whether it has a `gc` row and its type, whether it is reachable, the root that reached it first, the write and touch times of its `gc` row, whether it has an `objects` row and the cycle number of its condemn marker. The first root (`first_root`) gives its class and object, and the project, ref and update time of a ref root, the mount details, host and user of a pinned revision, or the result of a map row. It reads two rows per key.

### Memory and load

Memory grows with the `gc` table. The data of the mark takes about 35 bytes per object (key, type, edge index and bitmaps, with room the key arrays keep while they fill), 4 bytes per edge and 16 bytes per map row; the report shows it as `keys_bytes`, `edges_bytes` and `map_rows_bytes`.

The scans read rows faster than Go's garbage collector frees them, so with the default settings the heap grows to about twice the data: 1.5 million objects and 30 million edges take about 170 MB of data and 360 MB of heap. Set `GOMEMLIMIT` for the command to about 1.2 times the data and the heap stays near it (about 190 MB for the same table). A table of 10^8 objects, 2x10^9 edges and 10^7 map rows needs about 12 GB of data: give the job 16 GB with `GOMEMLIMIT=14GiB`, or about 24 GB without it.

The scans read the whole `gc` table, the `maps` keys and the `meta` family of `objects`. Run the command with the [app profile of the collector](#app-profile-of-the-collector).

## Walk the checkouts of mounts

`abfs server gcp gc walk` tells whether keys are in the content of the checkouts of mounts, for a key that a client used again after a cycle found it unreachable:

```
abfs server gcp gc walk --mount-details <hash>[,<hash>] --key <key>[,<key>] --project <project> --instance <spanner-instance> --bucket <bucket>
```

* It reads each mount details object, such as the targets of a client's mount refs, and its pinned manifest, and walks the commit and the trees of every pinned revision from object content.
* A chroot mount pins the tree of its parent workspace for a project in the annotation `abfs.dir.hash.tag` and mounts that tree. The walk follows it as well and reports what it found there with the type prefixed `dir-hash-`. That tree holds the parent workspace's files, so pass the details of the parent mount too.
* A manifest can pin a project at an annotated tag, as release manifests do. The walk follows the tag, and a tag of a tag, up to 10 deep, to the commit, and reports a key it found as a tag with the type `tag`. A pinned revision that cannot be read or is neither a commit nor a tag is reported with the type `revision`, and such a target of a tag with the type `tag-target`.
* It does not read the `gc` table, so it finds a key under a `gc` row that lacks an edge, which an `--explain` of the same cycle cannot.
* It reads trees only to list their entries, so a blob need not exist to be found.
* It prints, per key, `inside` with its type, the first path that reached it (project path, then the path in the project, in manifest order), the pinned revision in the column `COMMIT` (a tag when the manifest pins one) and the mount details, or `outside`.
* Its reads rescue and touch nothing; it takes no lease and writes nothing.
* It fails when the walk is incomplete: mount details without a pinned manifest, revisions or annotated trees that are not hashes, objects it could not read, objects of an unexpected type, such as a tag of a tree, or more than 10 nested tags. A key outside may then be inside.
* It reads every tree of the pinned revisions once per revision, 16 revisions at a time.

## Condemn mode

`abfs server gcp gc run --condemn` runs the cycle of a dry run and then marks every object the mark found unreachable: it sets the cell `g:condemned` of its `objects` row to the cycle number, 8 bytes big-endian. It deletes nothing. A server with `--gc` that reads a marked object clears the marker and answers as for any other object (see [Rescue](#rescue)), so a marked object that a client reads again shows that the mark called something garbage that was still in use.

```sh
abfs server gcp gc run --condemn --expected-replicas <hosts> --project <project> --instance <spanner-instance> --bucket <bucket> [--report gc-report.json] [--explain <key>[,<key>]]
```

### Condemn refusals

The cycle condemns only when its own report is clean. It refuses, writes no marker and no list, writes the report with the reasons in `condemn.refused` and exits with an error, when any of these holds:

* a warning in `replicas.warnings`;
* the number of distinct host names among the servers seen within the last 30 minutes (`replicas.hosts`) differs from `--expected-replicas`, or is outside `--min-replicas` and `--max-replicas`;
* a host has more than one live row: a server there crashed, or stopped with lost object touches, within the last 30 minutes and was started again, or two servers share the host name. A server that stops cleanly records its stop in its row (see [Heartbeat](#heartbeat)), so a restart alone does not refuse. A row without a stop means a crash, a kill or a preemption, and a stop that lost touches records the loss. The touches that the earlier process held in its buffer may be lost, so check its log for the stop line and wait until its row is older than 30 minutes;
* the consistency check did not run, or found objects without a `gc` row;
* `gc` rows with outdated or newer edge rules (`objects.outdated_rows`, `objects.newer_rows`);
* `live_mounts.errors`, `workspaces.errors` or `snapshots.errors`: a pin that could not be read may hide commits a mount, workspace or snapshot needs;
* a dangling root in the classes `source`, `project-status`, `pushed-packs`, `workspace`, `snapshot`, `mount` or `permanent`. Dangling `pinned-revisions` roots, for example of projects a mount skipped with `--ignore-missing-projects`, do not refuse; check them in `dangling_revisions` and its samples.

### Replica counts

`--expected-replicas` counts host names, not processes, because each server process writes its own row (`<host>-<random>`). VMs keep their host names over a restart. On Kubernetes, run the servers as a StatefulSet, so that a restarted pod keeps its name; a Deployment pod gets a new name, so a crashed pod's row counts as one more host for 30 minutes and the cycle refuses. The live rows of `GcServers` and `GcReplicas` count together, so servers of an older version count during an upgrade.

A fleet whose size changes, such as an autoscaled one, passes `--min-replicas` instead, and `--max-replicas` when its size has an upper bound. The evidence of each server comes from its own heartbeat row, so the count only notices servers that write none. Set `--min-replicas` to the autoscaler minimum less the servers a disruption budget lets stop at once, and leave `--max-replicas` unset: a stopping server keeps its heartbeat until it stopped, so a rollout can show more live hosts than the autoscaler maximum.

Give each server enough time to stop: the time the platform allows before it kills a stopping server must cover the server's own drain plus 10 seconds to write its buffered touches and 30 seconds to record its stop. A server killed before that leaves a crash row, which stops deletes (see [Delete checks](#delete-checks)).

### A cycle that condemns

1. Reads the list that `GcState.ListLocation` names, the list of the last condemn cycle, and has the mark record the root that first reaches each of its keys.
2. Deletes the markers of the keys of that list that the mark found live and writes every such key to `gc/condemned/<state>/<cycle>.live` in the server bucket (see [The .live object](#the-live-object)).
3. Writes the list of the objects it condemns to `gc/condemned/<state>/<cycle>` (the state is `default`), and records it in `GcState` (`ListLocation`, `ListSnapshotTime`, `ListComplete` false). The list is a binary object: a header with the cycle, the snapshot time, the run and the type names, then per object its 20-byte key, its type and its size, in key order, 29 bytes per object.
4. Writes the markers of the objects of the list, in key order, in batches of 2,000, at most 5,000 per second. It marks only objects whose `objects` row the consistency scan read, so it never creates a row. Each write replaces any older marker, so an object that stays unreachable carries the newest cycle number. Then it sets `ListComplete` and the completion time.
5. Removes the list and `.live` objects of all but the newest 7 cycles.

The list is written before any marker, and the markers of the previous list are cleared before the new list replaces it. So a cycle that stops midway leaves every marker in a list that the next cycle reads: that cycle clears the markers of keys now live and rewrites the others to its own number. The report section `condemn` counts the objects marked, per type, and their bytes.

A cycle reads the previous list once and writes one list and one `.live` object; at 29 bytes per object, a list of 10 million objects is about 290 MB. Besides the memory of the dry run, the collector holds the keys of the previous list and one map entry for each.

### The .live object

The `.live` object holds gzipped JSON lines, one per key of the previous list that the mark found live, with:

* the key and its type;
* the root that reached it first (`root`, as for `--explain`);
* its touch and write times, and the cycle number of the marker it held;
* `rescued` when the marker was already gone, so a server cleared it on a read. This is set only for a list whose cycle wrote all its markers.

One other case also sets `rescued` with no read: a key whose marker a later cycle cleared before it stopped short of writing its own list (that cycle's `.live` object lists it, unless the cycle stopped before writing it). A marker write that Bigtable left out of a bulk write response fails the cycle, so its list stays incomplete and the next cycle sets no `rescued` for its keys.

The report section `previous_list` counts these keys per type: in the list, still unreachable, live per root class with up to 20 keys per class, and rescued. Each key of the `.live` object needs an explanation, such as a ref moved back to an old target, a touch since the condemn or a read by a client. A key without one points to a root the mark misses; `gc walk` checks whether it is in a client's checkouts.

## Delete mode

`abfs server gcp gc run --delete` runs a condemn cycle that first deletes what the previous condemn or delete cycle condemned and is still garbage. Cycle N+1 deletes an object only when all of these hold:

* it is in the list of cycle N, the list `GcState` names;
* the mark of cycle N+1 finds it unreachable and not recent, and the consistency scan finds its `objects` row;
* every map row that names it was deleted first (see [Map rows](#map-rows));
* every parent of it in the mark's graph that still has an `objects` row was deleted earlier in the same step;
* its marker still holds N on the newest cell. The delete itself checks this: it is a conditional write on the `objects` row. A server that used the object since the condemn cleared the marker, so the delete does not match;
* every [delete check](#delete-checks) passed.

```sh
abfs server gcp gc run --delete --expected-replicas <hosts> --project <project> --instance <spanner-instance> --bucket <bucket> --bt-app-profile abfs-gc [--report gc-report.json] [--delete-types <type>[,<type>]]
```

A delete cycle:

1. Runs the steps of a condemn cycle up to its checks. A condemn refusal stops the whole cycle, as in condemn mode.
2. Removes the refs past their retention (see [Ref removal](#ref-removal)).
3. Runs the delete checks. When one fails, the cycle lists the reason in `delete.refused`, logs `GC cycle <n> does not delete and only condemns: <reason>` and goes on as a condemn cycle: it skips step 6 and still condemns and completes its list.
4. Clears the markers of the keys of the previous list that are live, as a condemn cycle does.
5. Runs the [chunk step](#chunk-step) for the delete records of earlier cycles, also in a cycle whose deletes were refused.
6. Reads the heartbeats again and waits until every live server wrote one whose touch counts it read after the snapshot, at most its heartbeat interval plus a minute; the checks of the servers must hold again on this read. Then it deletes: the map rows first, then the objects level by level, each object after its parents. Before the deletes of each batch of up to 2,000 keys it writes a [delete record](#delete-records) to the bucket. The `gc` row of each deleted object goes too, unless the object was written again since the start of the grace. A key whose delete had no clear outcome is read again at the end of the step; one still present stays listed.
7. Condemns what is still unreachable and was not deleted, as a condemn cycle does, and completes its list.
8. Removes the lists, delete records and ref records of old cycles.

So an object is deleted at the earliest in the cycle after the one that condemned it, which needs it unreachable and older than `--grace` at both marks; its chunks go one cycle later.

`--delete` fails at start, before it scans, when its Bigtable app profile does not fit (see [App profile of the collector](#app-profile-of-the-collector)).

### Flags of delete mode

Only `--delete` takes the flags from `--delete-types` to `--allow-no-soft-delete`; the other modes reject them. The flags of the dry run and the condemn mode apply as well.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--expected-replicas`, `--min-replicas`, `--max-replicas` | | As in condemn mode; one of the first two is required |
| `--delete-types` | every type | Object types, separated by commas, that the cycle may delete. Others are kept, with every object they link |
| `--accept-mounts-without-keepalive` | off | Delete blobs, trees and eval objects while live mounts run without `gc-keepalive`; the check then only warns, except after a lost map row touch (see [Client caches](#client-caches)) |
| `--no-backup-check` | off | Delete without the [backups](#backups) of the four tables |
| `--allow-no-soft-delete` | off | Delete bucket chunks although the bucket keeps deleted objects for less than 7 days (see [Soft delete](#soft-delete-of-the-bucket)) |
| `--mount-idle`, `--build-retention` | `336h`, `720h` | As in the dry run; delete mode removes the refs past them |
| `--snapshot-retention` | `720h` | Keep what a snapshot ref reaches, with the commits its mount was pinned to, for this time after the ref was written; delete mode removes older snapshot refs. All modes read it |
| `--gcp-client-metrics` | off | Export the client library metrics to Cloud Monitoring |

These are fixed: the delete step deletes at most 2,000 objects per second, with 32 conditional deletes in flight over the 16 key ranges; the chunk step deletes at most 500 chunks per second, and only those of records at least 15 minutes old, at the earliest in the next cycle, which is longer than the 10 minute lifetime of signed chunk URLs plus a minute of clock skew on each side; one attempt of a Bigtable write has a deadline of 30 seconds, a list object that plus its size at 8 MiB/s.

### Timing bounds

Each delete cycle checks its snapshot time against the list it deletes from:

* Lower bound: the completion time of the list plus the largest `--cache-ttl` of the servers in the window, plus 251 seconds, plus a minute of clock skew. Before that, a server may still answer from a cache entry or a read that started before the markers landed. 251 seconds is the longer of two server bounds: the writes that record a new reference end within 2 minutes of the start of their link validation, and a touch is written or counted as lost within 251 seconds of the use.
* Upper bound: before the snapshot of the list plus `--grace` less 3 minutes (the minute of clock skew and the 2 minute write bound). Later than that, an object that was first used after the list was taken may no longer be recent.

The window of a cycle starts `--grace` plus a minute before its snapshot; the checks of the servers cover every server process that may have run in it.

At the defaults (`--cache-ttl 6h`, `--grace 72h`) and one cycle a day, a delete cycle must start at least about 6.1 hours after the previous cycle completed its list and less than 71 hours 57 minutes after that cycle's snapshot: one missed daily cycle still deletes, two do not. A cycle outside the bounds only condemns, and the cycle after it can delete again.

The collector does not take the schedule as a setting: both bounds use the recorded times of the list. Choose the time between two cycles longer than the largest `--cache-ttl` of the servers plus the time a cycle takes to complete its list plus about 5 minutes, and shorter than half of `--grace` so that one missed cycle still deletes.

For a trial on a small store, servers at `--cache-ttl 5m --gc-touch-interval 30m` and the collector at `--grace 90m --longest-push 20m`, run every 30 minutes, fit these rules. The lower bound is then the list completion plus about 10.2 minutes and the upper bound the list snapshot plus 87 minutes.

### Delete checks

Each reason in `delete.refused` stops the deletes of that cycle only; the cycle still condemns and completes its list, so the next cycle can delete from it.

| Reason | What to do |
| --- | --- |
| No list, a list that cannot be read or is not complete, a list without a completion time after its snapshot (written by a collector of an older version), or a list of another cycle than the last condemn or delete cycle (that cycle stopped before it completed its own list) | Run the next cycle: it writes a complete list, and the cycle after it deletes from that list |
| The snapshot is before the lower bound | Run the cycle later; the reason names the earliest time |
| The snapshot is not before the upper bound | Nothing; this cycle condemns again, and the next one on time deletes |
| A server last wrote its heartbeat at a time and did not record a stop | It crashed, was killed or preempted. Deletes wait until its last heartbeat is about `--grace` plus 31 minutes old |
| A server lost touches | Deletes wait until the loss is `--grace` plus a minute old. Find the cause in the server's log and `abfs.gcp.gc.touches` with `kind` `object` |
| Live mounts touch their objects less often than the grace allows, less the 8 hours for which clients reuse map results and the hour a keep-alive pass may take, since a server lost map row touches, or crashed and may have lost them | Deletes wait until the loss is `--grace` plus about 9 hours old, or until those mounts end or touch more often. Find the cause in `abfs.gcp.gc.touches` with `kind` `map_row`, or in the log of the server that crashed |
| Live mounts run without the GC keep-alive, and a server lost map row touches or crashed, which `--accept-mounts-without-keepalive` does not accept | Deletes wait until the loss is `--grace` plus about 9 hours old, or until those mounts end or run with `gc-keepalive` |
| A server runs without link validation | Restart it without `--no-link-validation` |
| A server's settings break the timing rules | Fix the setting; deletes wait until the server with the old setting stopped `--grace` plus a minute ago |
| `--grace` is shorter than the largest cache TTL of the servers in the window plus `--longest-push` | Lower the servers' `--cache-ttl` or raise `--grace`; deletes wait until the server with the longer TTL stopped `--grace` plus a minute ago |
| `GcReplicas` holds rows, no cycle recorded that it is empty, or it was first found empty after the window start | Servers of an older version ran. Stop them; deletes wait until `--grace` plus a minute after a cycle first found `GcReplicas` empty |
| The Bigtable instance has more than one cluster, or its clusters cannot be listed | Delete mode needs an instance with one cluster: conditional writes sent to different clusters can both succeed |
| The app profile refused a conditional write, or the probe failed | Use a profile with single-cluster routing and single-row transactions; check the [permissions](#permissions-of-the-collector) |
| The backups cannot be listed | A backup that is still being created fails the listing; let backups finish before the cycle starts |
| No ready backup of a table started after the list was completed | Take the [backups](#backups) between cycles, or pass `--no-backup-check` |
| The removal of expired refs failed | The next cycle tries again |
| Live mounts run without `gc-keepalive`; mounts with it run with a cacheman that does not check local writes; mounts touch their objects less often than the grace allows | See [Client caches](#client-caches) |
| Objects written during the consistency scan have no `gc` row; the reason names up to 10 of them | A server without `--gc` writes to the store; find and stop it |
| The `gc` rows of the objects written during the consistency scan cannot be read | Check the collector's access to Bigtable and the load of its app profile; the next cycle checks again |
| Only some of the objects written during the consistency scan were checked: the checks fell behind the writes, and the scan stopped waiting for them | Nothing; deletes resume in a later cycle with fewer writes during its scan (see [Planning figures](#planning-figures)) |
| A server wrote no heartbeat with touch counts read after the snapshot in time | Check that server's heartbeat writes (`abfs.gcp.gc.heartbeat_failures`) |

A crash, an out-of-memory kill, a preemption, a pause of a VM, a stop killed before it recorded itself, or one dropped or failed object touch on any server stops deletes for about `--grace` plus 31 minutes, which is three daily cycles at the defaults. A dropped or failed map row touch stops them for about `--grace` plus 9 hours only while a live mount touches its objects too seldom for the shorter limit, or a live mount without `gc-keepalive` runs under `--accept-mounts-without-keepalive`; with the trial settings above that is any live mount with `gc-keepalive`. A crash counts as such a loss too, for about `--grace` plus 9.5 hours after the server's last heartbeat.

### Client caches

A client cache can hold the only copy of content a client wrote, such as build outputs equal to an old, unreachable blob. A cacheman with `gc-keepalive` on in its own configuration keeps such content out of eviction until a server confirmed the object after the write, and pushes it when the server lacks it; the mount records this in its mount details. When `--delete-types` allows blobs, trees, eval requests or eval responses, the cycle refuses its deletes while:

* a live mount runs without `gc-keepalive` or with one of its intervals `0` (`live_mounts.without_keepalive`). Turn `gc-keepalive` on as step 4 of [Enable or disable garbage collection](#enable-or-disable-garbage-collection) says. `--accept-mounts-without-keepalive` turns this check into a warning: content such mounts wrote and their cache evicted is then lost once deleted. It does not do so after a lost map row touch (`replicas.map_row_loss_time`, see the next items): a mount without keep-alive never touches the outputs of a map result it applies, so without the map row only the touch of the server's answer keeps them, and these deletes wait until the loss is `--grace` plus about 9 hours old. A mount without keep-alive writes its mount ref only when it starts, so it counts until `abfs unmount` ends it or for `--mount-idle` after it started, whichever comes first. After such a mount ended without `abfs unmount`, these deletes wait up to 14 days;
* a live mount with `gc-keepalive` runs with a cacheman that does not check local writes (`live_mounts.without_local_write_check`): an older cacheman, or one started with `gc-keepalive` off. A cacheman restarted with `gc-keepalive` off under a live mount turns that mount's field false for good; restart the mount;
* a live mount touches its objects less often than the grace allows (`live_mounts.touch_interval_too_long`): its `gc-keepalive-touch-interval` plus a tenth must be shorter than `--grace` less the longest touch interval and cache TTL of the servers and a minute. When a server recorded a lost map row touch since the window start less 9 hours, or a server crashed whose last heartbeat plus 30 minutes is in that time (`replicas.map_row_loss_time`), the limit is 9 hours shorter. A server that crashed may have lost map row touches it did not record. Cacheman answers map lookups from its own copy of the results a server returned for up to 8 hours, so a build can put the outputs of a result in the mount that long after a server returned it. The eval map row keeps those outputs, but a lost map row touch can let it expire, and then only the touch of the server's answer keeps them until the next keep-alive pass names them. The other hour allows for that pass: it sends at most 10,000 hashes a second, about 17 minutes for a mount of 10 million, and a failed pass is retried after 5 minutes. At the defaults the limit is about 30 hours, or about 21 hours after a map row loss, and the default `gc-keepalive-touch-interval` of `18h` fits both.

At each start, cacheman runs a start pass: it goes through every entry it may evict, oldest first and at most 10,000 entries per second, and asks the server about the blobs, trees and objects among them. A cache of 10 million entries takes about 17 minutes. Until the pass ends, eviction takes only the entries the pass has checked; a cache at its eviction threshold with none of those left evicts unchecked entries. ARI entries, RBE CAS blobs and entries whose file is gone are not checked; eviction takes them as it does without `gc-keepalive`.

Cacheman reports its checks in `remote_checks` of its stats. `skipped` counts the entries the checks left out, and `restored` counts such entries that an older cacheman kept out of eviction and that a start returned to it; the start logs up to 10 of their hashes. Both include blobs, trees and objects whose file is gone, which cacheman cannot tell from RBE CAS blobs.

### Map rows

Before the objects, the step deletes the map rows that name a candidate, and the eval rows past `--map-retention` whose two ends stay live.

* Each delete is conditional: a row written or used since the start of the grace (for an expired eval row, since the start of the map retention) stays, and a row that stays keeps both its candidate ends.
* A row goes only when both its ends are of types `--delete-types` allows; an end without a `gc` row counts as allowed. Otherwise the row stays and keeps its candidate ends, so a cycle limited to some types drops no cached result of the others.
* Readers that miss a deleted eval row compute the result again.

### Delete records

Each batch of the delete step writes the object `gc/deleted/<state>/<cycle>/<level>-<range>-<batch>` before any of its deletes. It holds a header with the cycle, the run, the start of the grace and the completion time of the list the cycle deleted from, then per object its 20-byte key and its `data:content` descriptor when a chunk is outside the row, and per map row the keys of its two ends. The chunk step and `gc undo` read these records.

When every key of a record is done, the chunk step sets the custom metadata `chunks-done=<cycle>` on it. The cycle keeps the records of the newest 7 cycles that have records, and every record flagged within the longer of 7 days and the bucket's soft delete retention. Before it removes the records of a cycle it writes `gc/deleted/<state>/<cycle>/pruned`, so that a `gc undo` of that cycle fails instead of restoring nothing. A record without the flag is never removed; `delete.chunks.unflagged` counts them and gives the age of the oldest.

### Chunk step

In each delete cycle, also one whose deletes were refused, the chunk step reads the delete records without the flag that were created at least 15 minutes ago. For each object of a record:

| Object row | Outcome | Count |
| --- | --- | --- |
| present with the recorded descriptor | the delete did not happen; the chunks stay | `kept_row_present` |
| present with another descriptor | a client wrote the object again after the delete; the recorded chunks go | `reput_after_delete` |
| absent | the recorded chunks go, and the `gc` row, unless the object was written again since | `row_absent` |

A chunk row goes only when it has no cell newer than the record. A bucket object goes only under the generation and metageneration read just before the delete, so an object restored from soft delete or stamped by `gc undo` stays (`restored_after_record`). A delete the bucket refuses (`delete_refused`) or an error leaves the record without the flag, for the next cycle.

The step deletes nothing, and lists why in `delete.chunks.skipped`, when the bucket settings cannot be read or the bucket has any of:

* Object Versioning on: a delete would keep a noncurrent version;
* a retention policy, a default event-based hold or object retention: each can refuse a delete;
* a soft delete retention under 7 days, without `--allow-no-soft-delete`.

Unless `--no-backup-check`, a record also waits (`records_without_backup`) until a ready, unexpired backup of the `chunks` table exists that started after the list of its cycle was completed: chunk rows have no soft delete, so that backup is their only copy.

### Soft delete of the bucket

Cloud Storage soft delete keeps a deleted object restorable for its retention, 7 days by default and at most 90 ([soft delete](https://docs.cloud.google.com/storage/docs/soft-delete)). Check a bucket with `gcloud storage buckets describe gs://<bucket> --format="default(soft_delete_policy)"`. By default the chunk step refuses to delete on a bucket whose retention is under 7 days; `--allow-no-soft-delete` deletes anyway. Both are valid choices:

* With soft delete, `gc undo` can bring deleted bucket chunks back: restore the names it prints with `gcloud storage restore` and run it again. The deleted bytes are billed during the retention, and a bucket deleted while soft delete is on stays restorable, and cannot be removed for good, until its retention ends.
* Without soft delete, a deleted bucket chunk is gone at once. `gc undo` restores only the objects whose chunks still exist; it prints the `gs://` names of the missing chunks, leaves the objects that need them and every object that links those unrestored, and exits non-zero. Objects of up to 1 MiB have no bucket chunk and come back from the backups.

Delete records are kept for at least the soft delete retention, so that an undo finds them while it can restore what they name.

### Ref removal

Each delete cycle that passes the condemn checks removes the refs that were past their retention at its snapshot:

| Class | Removed when not written within |
| --- | --- |
| mount refs | `--mount-idle`; the ref of an ended mount also goes with its unmount ref, and is removed before it |
| build, build-start and unmount refs | `--build-retention` |
| snapshot refs | `--snapshot-retention` |

* It first writes them to `gc/refs/<state>/<cycle>` in the bucket, as JSON with the project ID, host, project, ref, update time and target of each.
* Then it removes them in Spanner transactions of up to 500 refs, each ref only while its update time and target are still the ones of the snapshot; a ref written since stays a root.
* The report counts per class in `delete.refs.classes` the refs recorded, `removed`, `changed` (written since) and `gone`.
* A failed removal refuses the deletes of the cycle, and the next cycle tries again.
* Ref records are kept like delete records, counted from their write.

The mark of the cycle does not count these refs as roots, so the cycle condemns what only they reached and the next cycle deletes it. To put a ref back while its target exists, send its line again; this writes a new update time, so the ref is a root for its whole retention. With ACLs, the operator needs write access to the project of the ref, `abfs-meta` for the refs of clients:

```sh
echo "<target> <ref>" | abfs test-client put-ref -s <host> -p <project>
```

### Snapshot refs

Only clients with `gc-keepalive` write a snapshot ref: `refs/abfs/snapshot/user/<user>/<hash>` in `abfs-meta`, on `abfs ws snapshot` and again after a successful `abfs ws snapshot --restore <hash>`, which moves its update time. Local-only clients and cacheman in remote read-only mode write none. The name falls under the ACL rule `refs/abfs/*/user/<user>/*`: `<user>` is the local user name, and with ACLs it must equal the part of the user's email before `@`, or the server refuses the write.

Cacheman writes the ref only after the snapshot and the content it links are on the server. The command waits for this at most 30 seconds. When the write fails or is not confirmed in that time, the command logs a warning and still succeeds: the snapshot is stored, and a write still queued in cacheman usually lands later.

* The ref keeps the snapshot, its mount details, their pinned manifest and the pinned commits for `--snapshot-retention` (default `720h`) after its last write.
* Delete mode then removes the ref, records it in `gc/refs/<state>/<cycle>`, and one cycle later deletes what only the ref reached.
* A snapshot taken without `gc-keepalive`, or by an older client, has no ref and is deleted once it is garbage. A `--restore` of it before the delete creates its ref.
* The report has a section `snapshots` with the fields of `live_mounts`, counts the refs in `refs.snapshot`, and records `settings.snapshot_retention_hours`.
* A read error on a snapshot's pins, or a snapshot ref whose target has no `gc` row, stops a condemn.
* A collector of an older version counts these refs as `other-meta` and keeps the snapshots for good, but not the commits their mounts were pinned to; run a collector of this version.

### Report of a delete cycle

The section `delete` holds:

| Field | Content |
| --- | --- |
| `refused`, `warnings` | why the cycle did not delete, and the checks that options turned into warnings |
| `too_new` | the check of the objects the consistency scan found without a `gc` row because they were written during it: `keys` checked, which equals `consistency.too_new` unless the scan stopped waiting for the checks; `not_hashes`; `gone`, deleted again before the check; `without_gc_row`, with up to 10 `samples`, which are a sample and not the lowest keys. When the check failed, it counts the objects checked before, and the reason is in `refused` |
| `plan` | what the cycle planned to delete |
| `step` | the record prefix; the objects deleted per type and their bytes; `not_matched` with samples, each of which should have a rescue log line of a server; `gone`; `blocked` (below a key that was not deleted); `type_not_allowed`; keys of unknown outcome; `gc` rows and map rows deleted and kept; the time of the first 100 levels per key range |
| `chunks` | the chunk step |
| `refs` | the ref removal |
| `records_removed` | the delete records removed |

### Planning figures

* A delete step removes about 24,000 objects in about 19 seconds on one Bigtable node, about 0.8 ms per object. Its rate of at most 2,000 objects per second also bounds it: 10^6 candidates take at least about 8 minutes.
* A condemn or delete cycle over about 71 million objects takes about 7 to 14 minutes on one node with autoscaling to 3.
* Writing the markers of about 27,000 objects takes about 6 seconds at the rate of at most 5,000 per second.
* A delete cycle checks each object written during its consistency scan while the scan runs: it reads the object's `gc` row, and its `objects` row only when the `gc` row is missing, in batches of 100 keys with 8 batches in flight, and keeps only counts and the batches in flight in memory. That is one row read per object. Bigtable rates one SSD node at up to 17,000 row reads per second ([performance](https://docs.cloud.google.com/bigtable/docs/performance)), so on a node with no other load than the collector 1 million objects written during a scan take about 1 minute of reads and 10 million about 10 minutes, less with more nodes. The collector reads at low priority, so a node busy with a seed checks slower.
* When the checks fall behind, the scan waits for them, at most 15 minutes summed over its key ranges. The wait makes a delete cycle up to about 15 minutes longer than a condemn cycle, about the length of one more cycle at 71 million objects, and about 1 minute longer when all 16 key ranges wait at once. The checks share the node with the scan, so they also slow it. Past that wait the scan stops checking and cancels the reads in flight, finishes at full speed, and the cycle refuses its deletes. A cycle during a seed or a large catch-up of a git-pusher can refuse this way; deletes resume in a later cycle with fewer writes during its scan.
* The step runs one level per link in a chain of unreachable objects: for a dropped branch of 10,000 commits, expect about 10,000 levels and an estimated 13 to 20 minutes of delete step and tens of thousands of delete records, which the next chunk step reads.
* Deleted data is billed until it is gone: in backups for their retention, and soft-deleted chunks for the soft delete retention.

## Backups and undo

### Backups

Each delete cycle needs a ready backup of each of the tables `objects`, `gc`, `maps` and `chunks` that started after the list it deletes from was completed, that is after the previous condemn or delete cycle ended. Without them the cycle refuses its deletes, and still condemns and runs its chunk step.

So a scheduled delete job takes a set of four backups between cycles: after each cycle completed its list, and ready before the next cycle starts. A backup that is still being created when the collector lists the backups fails the listing and refuses the deletes of that cycle. Creating a backup usually takes less than a minute and can take up to an hour ([backups](https://docs.cloud.google.com/bigtable/docs/backups)).

```sh
for t in objects gc maps chunks; do
  gcloud bigtable backups create gc-<n>-$t --instance=<bt-instance> --cluster=<cluster> --table=$t --retention-period=7d
done
gcloud bigtable backups list --instance=<bt-instance> --cluster=<cluster>
```

The set taken before cycle N is also the source of an undo of cycle N and later cycles, and the chunk step of cycle N+1 needs its `chunks` backup. Keep each set for as long as an undo of the cycles after it may be needed; 7 days (`--retention-period=7d`) is a good default. The delete records are kept at least 7 days after the chunk step flagged them, so they outlive a 7 day set by at least a cycle, and an undo whose backups expired cannot start. Backups are incremental: they share storage with the table.

### Undo

`abfs server gcp gc undo` writes back what delete cycles deleted, from the delete records and from the four tables restored from the backups taken before the first of those cycles. It restores every object and map row those cycles deleted, not only the wrong ones; what is still garbage is condemned and deleted again later.

1. Stop the delete job.
2. Restore the four backups of the set taken before the first cycle to undo, each once it is an hour old (Bigtable advises to wait that long before a restore of a standard backup), to tables with a suffix:

   ```sh
   gcloud bigtable instances tables restore --source=projects/<project>/instances/<bt-instance>/clusters/<cluster>/backups/gc-<n>-<table> --destination=<table>-r --destination-instance=<bt-instance>
   ```

3. Run the undo for the first wrong cycle through the last cycle that ran. A later cycle can delete the children of an object an earlier cycle deleted, so an undo of one cycle alone can miss them:

   ```sh
   abfs server gcp gc undo --cycle <n> [--through <m>] --from-suffix -r --project <project> --instance <spanner-instance> --bucket <bucket> --bt-app-profile abfs-gc
   ```

4. When it prints bucket chunks it waits for, restore them from soft delete and run the same undo again:

   ```sh
   abfs server gcp gc undo ... | awk '/^  gs:\/\//{print $1}' | gcloud storage restore --read-paths-from-stdin
   ```

   Without soft delete these chunks are gone, and the objects that need them stay deleted. The command fails while anything was not restored. Run a failed undo again: chunks it kept for an object it did not finish stay in the bucket for good otherwise.
5. Undo restores no refs. It names the ref records of the cycle before `--cycle` through `--through`; put back the refs that were removed by mistake with `test-client put-ref` (see [Ref removal](#ref-removal)).
6. Fix the cause of the wrong delete before the delete job runs again, and delete the restored tables.

What the undo does:

* It checks that each restored table was restored from a backup of its live table that ended before the first delete record of cycle n was written; `--no-source-check` skips this. It fails when a cycle in the range has pruned records.
* It takes the lease in the phase `undo`, which does not count as a condemn cycle. Before its first write it waits until a collector that lost the lease has stopped writing: the expiry of that lease, or the time of its own lease when that lease was released, plus two write attempts of 30 seconds each and a minute.
* It restores the newest cycle first and in each cycle the deepest level first, so children come back before their parents.
* For each object whose row is absent: its bucket chunks must exist and get the custom metadata `abfs-undo=<cycle>`, so that a chunk step that read them earlier cannot delete them; its chunk rows and its `gc` row are written with a new time and a touch, so it is recent for `--grace`; its `objects` row comes last, without the marker.
* An object is not restored, nor anything that links it, when a bucket chunk is missing, its restored row holds another descriptor than its record, or it links an object deleted since the backup that this run did not restore.
* Map rows come back with a new time. What is present stays as it is.

## Operator rules

* Raise `--grace` only with care. The collector keeps a server row for the largest grace of any earlier condemn or delete cycle plus an hour, and a delete cycle with a larger grace needs rows from further back, which earlier cycles may have removed. After raising `--grace`, run condemn cycles instead of delete cycles until the increase has passed since the last cycle with the old grace. Lowering it needs no wait.
* No server of an older version may run against the store after an upgrade, not even briefly (see [Upgrades](#upgrades)).
* A map row is deleted only when every end that has a `gc` row is of a type `--delete-types` allows.
* Widen `--delete-types` from the top of the graph down: commits, `project-status` and build objects first, then trees and deep trees, then blobs. Blobs, trees and eval objects need every live mount on `gc-keepalive` with a cacheman that checks local writes.
* The servers' `--gc-touch-interval` must be at most half of the collector's `--grace`, and their largest `--cache-ttl` plus the collector's `--longest-push` at most `--grace`. A server with `--gc` refuses to start with either at `0`. The collector reads both from the heartbeat rows; the dry run warns when they break the rule.
* Schedule the cycles as [Timing bounds](#timing-bounds) says: further apart than the largest `--cache-ttl` of the servers plus the time a cycle takes, and less than half of `--grace` apart.

## What the server does for the garbage collector

The collector runs as a separate job. With `--gc`, the server records for it which objects are used, and keeps objects that the collector marked for deletion when they are used again. Nothing in the server deletes data. Without `--gc` none of the following happens.

### Server flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--gc` | off | Run the server side of the garbage collector. It needs the resources of `abfs server gcp init --gc`. Off, the server reads and writes only what a server without garbage collection does, and the flags below have no effect |
| `--cache-ttl` | `6h` | With `--gc`, how long cached object headers and content are used before the server reads them again from Bigtable; above 0 |
| `--gc-touch-interval` | `36h` | With `--gc`, length of the period in which the server writes at most one touch per object and map row; above 0 |

A server with `--gc` refuses to start with `--cache-ttl 0` or `--gc-touch-interval 0`. It writes its [heartbeat](#heartbeat) every 5 minutes, a fixed interval.

### GC edges

`PutObject` writes the `gc` row of each new object, with the GC edges, before its object row. This adds one Bigtable write to each upload. A failed `gc` row write fails the upload.

### Rescue

The collector marks an object it plans to delete with the cell `g:condemned` of its `objects` row. The server reads that cell with every object row it reads: in `Headers`, the existence check of `PutObject`, the link checks of `PutObject`, `PutRefs`, `XchgRefs` and `AddMapResult`, `GetObject` and `MapResults`. On a marked row it:

1. writes a touch;
2. clears the marker, but only while the marker still holds the value it read;
3. answers as for any other object.

A rescue whose touch fails fails the call and leaves the marker, so that the client's retry rescues it again. A condemn write that is retried or lands late can set the marker again after a clear, and then only the touch keeps the next cycle from finding the object unreachable.

When the collector deleted the row first, the object is missing, and a client that writes it stores it again. The touch stays behind as a `gc` row with only the touch.

The conditional writes need an app profile that allows single-row transactions, which the server checks at start. Rescue has no switch of its own: with `--gc` every header read includes the family `g`. Only condemn and delete cycles write markers; until one runs, this costs only the wider read.

Up to 32 rescues of one call run in parallel. A failed rescue does not stop the others; the call fails with the first error once all have run, so a retry finds only the objects whose rescue failed still marked.

### Rescue log

Each call that rescued an object, or failed to, logs lines that name the call and the client:

| Line | Limit |
| --- | --- |
| `Rescued <n> condemned objects of <m> read in <call>, <f> failed: request <id>, peer <address>, x-forwarded-for "<addresses>", client-version "<version>"` | 100 a minute per server |
| `Rescued <type> <key> in <call>, condemned by GC cycle <n>: request <id>` | 100 a minute per server |
| `Condemned object <key> was deleted before it could be rescued in <call>: request <id>, peer <address>, x-forwarded-for "<addresses>", client-version "<version>"` | none |
| `Left out the lines of <n> calls with rescues in the minute from <time>`, `Left out the lines of <n> rescued objects in the minute from <time>` | logged in the next minute that logs a rescue, and at the stop, when a limit left lines out |

* `GetObject` reads one object per call, so each condemned file a client reads counts as one call.
* The object lines and the call line of one call share the request id, a random one per call. A traced call adds `, trace <trace ID>` after it, which the calls of one client operation can share.
* Behind a load balancer the peer is the proxy; an internal Application Load Balancer appends the client's address and then its own to `x-forwarded-for`.
* The `.live` object of the next condemn cycle lists every rescued key.

### Touches

When a call finds an object (`Headers`, `GetObject`, the existence check of `PutObject`, the link checks, the results of `MapResults`), the server writes the cell `t:touch` of the object's row in the table `gc`, with a Bigtable server timestamp.

* Each server writes at most one touch per object in each period of `--gc-touch-interval`, so an object in use has a touch younger than the interval. The periods count from the server start, shifted for each object by an offset taken from its key, so the periods of different objects end at different times and their next touches spread over the interval.
* `MapResults` also writes the cell of each map row it returns again, with the same limit, so the timestamp of a map row is the time it was last used. Map results therefore come back in the order of their last use, oldest first, instead of the order they were added. ninjago tries the newest result first, so builds are not affected.
* Each server remembers the last touch of up to 4,000,000 map rows, so that it rewrites each at most once per touch interval. A server that returns more distinct rows per touch period touches a row again once it was forgotten, so its map row touches follow the rate of `MapResults` calls rather than the period. The memory grows with the rows returned, about 225 bytes per row and 900 MB when full.

Touches wait in a buffer of 100,000, and four workers write them in batches of up to 1,000, at least once a second:

* When the buffer is full, `Headers` and `MapResults`, the calls of the client keep-alive and of builds, wait up to half a second per object touch for room. A burst, such as the keep-alive of a large mount, is slowed to the rate the workers write instead of losing touches.
* When a wait times out, touches are dropped and counted, without waiting, until the buffer has room again. So with `--gc` the latency of `Headers` and `MapResults` depends on the table `gc`: while the buffer is full they run at the rate the workers write, and when the writes stall, the first of them waits half a second.
* `GetObject`, the existence check of `PutObject` and the link checks never wait for room; they drop the touch and count it. The next use of a dropped object writes its touch.
* Map row touches never wait either, and are queued only while the buffer is less than half full, so they take at most about half of it and leave the rest to object touches. The next use of a row whose touch was dropped writes its touch.
* A touch is written or counted as lost within 251 seconds of the use. A touch whose write starts later, because the process was frozen or starved, is written and also counted as failed.

Every dropped or failed touch of an object is recorded in the server's heartbeat row and refuses deletes for about `--grace`. Map row touches are counted and recorded apart: a map row touch keeps an eval map row, and through it the outputs of its result, within `--map-retention`. A lost one refuses deletes only while a live mount touches its objects too seldom to keep such outputs without the map row (see [Client caches](#client-caches)).

### Cache TTL and write bound

The header and content caches keep an entry for `--cache-ttl` and then read it again from Bigtable, so that every server sees a marker long before the collector deletes the object. The TTL counts from the start of the Bigtable read that loaded the entry, on a clock that keeps running while the VM is suspended. A read that itself took `--cache-ttl` or longer is not used: the call fails with `Unavailable` and the client retries. A delete cycle waits the largest TTL of the servers after the previous list was complete (see [Timing bounds](#timing-bounds)), and the collector's `--grace` must cover the TTL plus its `--longest-push`. A shorter TTL costs more Bigtable reads on directory listings.

After a call validated the links of a new object, ref or map result, the writes that record it must end within 2 minutes of the start of the validation. A write later than that fails the call with `Unavailable`, and the client sends it again, which validates again. The timing bounds of delete mode count on this.

### Missing map results and chunks

* `MapResults` leaves out results whose object does not exist, with one header lookup for all results, from the cache where it can. So `MapResults` can fail with a Bigtable error from the table `objects`.
* When a chunk of an object is missing while the server streams it, the server drops the object from its caches and reads the object row again. If the row is gone, the read fails as a missing object; if the row still lists the chunk, the content is lost and the read fails with an error that is not "missing".

### Heartbeat

Each server writes its row in the Spanner table `GcServers` before it serves and then every 5 minutes. The row holds:

* an ID made of the host name and a random suffix, logged at start;
* the version of the GC edge rules the server writes into the table `gc`, and `GcEnabled`, always true;
* its `--gc-touch-interval`, `--cache-ttl` and heartbeat interval;
* `LinkValidation`, false with `--no-link-validation`;
* `LastSeen`, the commit time, and `StatsTime`, a Spanner read time taken just before the server read its touch counts;
* `TouchLossTime`, set by a heartbeat or the stop that records a loss: object touches dropped or failed since the last heartbeat, a pause of the process (the wall clock ran more than 10 seconds ahead of the monotonic clock, as in a VM suspend), or a gap between heartbeat attempts longer than the interval plus 251 seconds;
* `MapRowLossTime`, set the same way when map row touches were dropped or failed since the last heartbeat. Servers of older versions leave it empty and record those losses in `TouchLossTime`;
* `StoppedAt`, set by a clean stop.

The collector uses the rows to see which server versions and settings run and which servers crashed or lost touches. A server that cannot write its first heartbeat within a minute fails to start. A later failed write is logged and counted; once the last successful one is older than two intervals plus 30 seconds (10 minutes 30 seconds), every call answers `Unavailable` until the next one succeeds.

When the server stops, it writes the touches left in its buffer, for up to 10 seconds, and logs one line `GC touches at stop: ...` with the object touches written, dropped and failed during the stop and since the start, and the map row touches written, dropped and failed since the start, because its metrics are no longer exported by then. Then it records the stop in its row, with `TouchLossTime` when it lost object touches and `MapRowLossTime` when it lost map row touches since its last heartbeat. The row stays. A row whose last heartbeat is older than 30 minutes and that has no `StoppedAt` marks a crash, a kill or a preemption, whose buffered touches may be lost; the collector counts it as a loss of object and map row touches. So does a stop whose record failed.

`GcReplicas` stays empty for servers of this version. Servers of older versions write their heartbeat there and delete it at a clean stop; the collector reads it only to see them.

### Cost

One Bigtable write per object and map row used, per server and touch period, batched; one touch and one conditional write per rescue; one Spanner read and write per heartbeat.

### Metrics

Metrics, as OpenTelemetry counters. Their names keep the dots when they are exported with OTLP. Cloud Monitoring stores them as they arrive, for example `prometheus.googleapis.com/abfs.gcp.gc.rescues/counter` through the Ops Agent OTLP receiver, so a PromQL query names them with `__name__`: `sum by (call, type) ({__name__="abfs.gcp.gc.rescues"})`. On a Prometheus `/metrics` endpoint the dots become underscores and the names end in `_total`, for example `abfs_gcp_gc_rescues_total`.

| Metric | Labels | Counts |
| --- | --- | --- |
| `abfs.gcp.gc.rescues` | `call`, `type` | Marked objects rescued, by call: `headers`, `validation`, `put_object`, `get_object`, `map_results`, and by object type, `other` for a type the server does not know |
| `abfs.gcp.gc.rescues_lost` | `call` | Marked objects that the collector deleted before the call could rescue them |
| `abfs.gcp.gc.rescue_failures` | `call` | Rescues that failed, including those whose touch failed; each failed its call |
| `abfs.gcp.gc.touches` | `kind`, `result` | Touches of an `object` or a `map_row`, `written`, `dropped` because the buffer was full (about half full for map rows), or `failed` |
| `abfs.gcp.cache.ttl_expiries` | `cache` | Cache lookups of `headers` or `content` that found an entry older than `--cache-ttl` |
| `abfs.gcp.gc.map_results_dropped` | | Map results left out because their object is missing |
| `abfs.gcp.gc.heartbeat_failures` | | Failed heartbeat writes |

## Limits of the garbage collector

Mounts and pins:

* A mount is kept alive only by a client with `gc-keepalive`. Mounts of clients without it, or of older clients, and mounts in remote read-only mode, which write no mount ref, are protected only by the refs they were mounted from and, for a mount that wrote its ref, until it was unmounted or for `--mount-idle` after it started, whichever comes first. What they loaded can go once those refs move and `--grace` has passed.
* With `gc-keepalive`, `abfs ws mount-repo -b <branch>` (used by `abfs mount --ws` and `--tmp-ws`) pins `default.xml` of the branch when it mounts and records the pinned manifest in the mount details. `abfs ws mount` and a mount of a snapshot pass on the pin of the mount the snapshot was taken from, and mounts made from a manifest hash or a manifest file pin it.
* This version resolves `-b <name>` for the pin as `refs/heads/<name>`, then `refs/tags/<name>`, then as the full ref `<name>`. A version whose pin resolves only `refs/heads/<name>` records no pin for a tag or a full ref name, which the mount still checks out.
* No pin is recorded when a project of the manifest, in any group, does not resolve on the server, with `--smart-sync` or `--smart-tag`, or for a snapshot whose mount had none; the mount goes on and logs a warning. A mount without `-b`, which checks out the HEAD of the manifest project, gets no pin either and logs nothing. Such a mount ref keeps only the mount details, so the commits and trees the mount reads stay only while the branch reaches them or within `--grace`; a force push or a deleted branch can leave them unreachable. The report counts these mounts in `live_mounts.without_pinned_manifest`.
* The pin does not follow the checkouts: a branch that moves between the pin and `repo sync`, and a later `repo sync` in the workspace, check out commits that only their branch keeps.
* The pin of a mount holds the revisions of the projects in its manifest, not the commit of the manifest project itself. When the manifest branch is force-pushed, the manifest commit the mount checked out is kept only by the manifest branch and by `--grace`.
* A workspace snapshot without a snapshot ref (taken without `gc-keepalive` or by an older client) is kept, with work tree content only it holds, only for `--grace` after its last use, and one with a ref for `--snapshot-retention` after its last `ws snapshot` or `--restore`. For a snapshot without a ref, the commits its mount was pinned to stay pinned by the mount ref only until `abfs unmount` ends the mount, not for `--mount-idle` as in earlier versions. Use named workspaces for work that must stay.
* A snapshot restored while a delete cycle runs, after its ref passed `--snapshot-retention`, can lose the commits its mount was pinned to; restore well within the retention.
* An ended mount pins nothing from the time its unmount ref is written. `abfs unmount` sees and stops only the processes of the user who runs it, so a copy of the mount in another mount namespace that only processes of other users use, such as a container running as root with a bind mount of the workspace, outlives it. Those processes go on reading through cacheman. What they read is then protected only by the other roots and `--grace`, and each read touches what it reads; without the unmount ref, the pins of the mount kept it until `--mount-idle`. Stop containers and other users' processes that use the workspace before `abfs unmount`.
* The collector matches a mount ref and an unmount ref by name: two mounts with the same `--ref-prefix` started in the same nanosecond share one name, and the unmount of either ends both.

Servers:

* Only servers with `--gc` write a heartbeat. A server started without `--gc` writes no row, so the collector cannot see it: it does not touch or rescue what its clients use, and the dry run gives no warning about it. The condemn and delete modes check the number of server hosts, which is how they see such a server. Delete mode also refuses when an object written during its consistency scan has no `gc` row; objects written before show as missing `gc` rows. A server without `--gc` that only reads, or writes objects that exist, leaves no trace.
* A condemned object read from a server's cache is not rescued, and neither are reads that never reach a server: cacheman's local content, signed URL reads within their 10 minutes, and ninjago's input checks from local attributes. Object touches that a server dropped or failed to write, and touches in its buffer when it crashed or was killed, are lost the same way. A lost object touch refuses deletes for about `--grace`; condemn mode does not refuse on it, and the next condemn cycle finds such objects live only if a root reaches them or a touch was written.
* A pause of a server process under 10 seconds is not recorded.

Clients:

* A client that holds only a hash, not the content, and uses it more than `--grace` plus two cycles after its last server read gets not found once the object is deleted.
* With `--accept-mounts-without-keepalive`, content that a mount without keep-alive wrote and its cache evicted is lost once deleted.
* A mount whose keep-alive did not touch for longer than `--grace` less the server touch interval, such as a workspace unmounted for days or a stopped cacheman, loses content it wrote that its cache evicted.
* Cacheman can evict a blob between casfs storing it and cacheman indexing it; it counts this in `remote_checks.lost_before_index` with the hash. A write whose notification casfs dropped is never indexed and is not protected.
* Cacheman cannot push back a cached tree or eval response the server lacks when one of its children, at any depth, is in no local store, such as a child a lazy mount never read. It drops such a push at the first retry of that child once the child has been missing for 30 minutes, up to about two minutes after that, and counts this once per entry in `remote_checks.dropped`; the entry stays in the cache, and the next keep-alive touch or cacheman start pushes it again. It does not drop the push while an upload, a map result, a wait or a ref update needs the same child. While a mount is connected and the child stays missing, each keep-alive cycle can queue the push again, which then waits and is dropped again.
* The keep-alive does not touch files under `.git` and `.ctrl` directories.
* A cacheman restarted with `gc-keepalive` off under a live mount checks no local writes until the mount's next keep-alive tick, up to 15 minutes later, turns the mount's field false.
* A cache that reaches its eviction threshold during the start pass evicts entries the pass has not checked when it fills faster than the pass goes through its entries, or when the server cannot be reached; cacheman counts them in `remote_checks.evicted_unchecked`.
* After a server lost a map row touch, the outputs of a map result that a build applied up to 8 hours after a server returned it are kept only by the touch of that answer until the next keep-alive pass names them. They survive a keep-alive pass that reaches them up to an hour after it was due, plus what the shorter limit leaves over the touch interval and its tenth. A longer keep-alive outage, such as a stopped cacheman or a server that cannot be reached, can lose them. Without a lost map row touch, the eval map row keeps them for `--map-retention`.

Builds and records:

* `abfs rerun`, `abfs file-access` and `abfs export` on a build whose eval map rows are older than `--map-retention` fail on the missing eval responses, even while the build itself is kept by `--build-retention`.
* A map row touch that a server dropped or failed to write leaves the row's last use earlier, so an eval map row in use can expire earlier than `--map-retention` after its last use. A build then runs the step again.

Leaks, left for a later orphan sweep:

* A collector that lost its lease and deletes late can leave a row with only a condemn marker, or a `gc` row without an object row.
* A map touch that a server wrote after the delete of its row writes the row again.
* An undo that failed after it kept the chunks of an object it did not finish leaves those chunks, unless it is run again.

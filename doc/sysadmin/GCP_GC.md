# ABFS / System Admin / GCP server garbage collection

Garbage collection removes objects that nothing needs any more from the tables and the bucket of a [GCP server](GCP_SERVER.md). This page explains how it works, how to turn it on step by step, and then each command and server behaviour in detail.

## How garbage collection works

Garbage collection is off by default. Three parts take part when it is on:

* the collector, `abfs server gcp gc run`, a job that runs one cycle at a time on a schedule, such as once a day (see [The hold](#the-hold));
* the servers, started with `--gc`, which record for the collector which objects are used;
* the clients, with the client option `gc-keepalive`, which keep their mounts and the content they wrote visible to the collector.

**Roots.** The collector keeps everything that a root reaches:

* every ref of every project, such as branches, tags and `HEAD`;
* the refs that clients write in the project `abfs-meta`: mount refs within `--mount-idle` and workspace refs, snapshot refs for `--snapshot-retention` after their last write, and build and unmount refs within their retention; the mount refs of live mounts, workspace refs and snapshot refs also keep the commits that the pinned manifest of their mount names. The mount ref of a mount that `abfs unmount` ended keeps only its mount details, until it goes with its unmount ref after `--build-retention` or at `--mount-idle`, whichever comes first;
* map rows, such as cached build results, within their retention;
* every object written or used within the grace period (`--grace`, default `72h`).

**Mark.** Each server writes, for every object it stores, a row in the Bigtable table `abfsgc` with the objects it links (its GC edges). The collector reads every ref at one snapshot time, scans the `abfsgc` table and marks in memory everything the roots reach. What stays unmarked is unreachable. It reads no object content.

**Grace.** A new object is unreachable for a while: an upload stores the children of a commit before the ref that links it, clients reuse hashes they hold, and a server records a use of an object at most once per `--gc-touch-interval`. So an object counts as garbage only when it was neither written nor used within `--grace`; [Root classes](#root-classes) gives the smallest value the command accepts.

**Condemn, rescue, delete.** Nothing is deleted in the cycle that finds it unreachable, nor for a hold after that:

```
cycle N     mark; condemn: write list N to the bucket and to AbfsGcLists, set marker N
            on each unreachable object without one, then complete list N
            a server that reads a marked object clears the marker (rescue)
cycle D     mark again; once the hold of list N has ended (--grace plus 16h after
            its completion, 4 daily cycles at the defaults), delete each object
            that is still unreachable and still holds marker N; record its rows
cycle D+8   delete the chunks recorded in cycle D, in the first cycle whose chunk
            step starts at least 7 days (the undo window) after the record
```

A rescue shows that an object was still in use, so the delete, a conditional write on the marker, does not match. A marker of a complete list is never written again, so an object that stays unreachable keeps the number of the cycle that first condemned it. Large objects keep chunks in the table `chunks` and in the bucket; these go 7 days or more after the first delete record that names their object, so that a reader that already started still finds them and `gc undo` can bring the object back.

**Client content.** A mount of a client with `gc-keepalive` rewrites its mount ref every 15 minutes and sends the hashes of what it holds to the server every 18 hours, which keeps them used. Cacheman with `gc-keepalive` keeps content a client wrote out of eviction until a server confirmed it holds the object. At a start it checks its cache without touching it, and keeps what the server lacks out of eviction for 96 hours without pushing it, so that a mount that holds it can name it. `abfs ws snapshot` writes a ref for the snapshot, which keeps it for `--snapshot-retention` after its last `ws snapshot` or `--restore`.

**Servers.** Each server with `--gc` writes a heartbeat row to the Spanner table `AbfsGcServers` with the version of the GC rules it follows, its settings, whether it lost a record of a use (a touch) of an object or of a map row, and its stop. The collector refuses to condemn when the number of server hosts does not match, and refuses to delete when any server that ran within the grace followed an older version of the GC rules or ran with settings that break the timing rules. The hold of the markers rests on no touch that a server buffers, so a crash, a lost touch or a pause of a server refuses nothing; lost touches are reported in the heartbeat row and the metrics.

**Staged path.** Turn it on in three modes, each after the one before ran clean: `--dry-run` only reports, `--condemn` also marks, and `--delete` also deletes. Each delete record holds the rows it deletes, so `gc undo` can write them back.

## Commands

| Command | What it does | Writes | When to run it |
| --- | --- | --- | --- |
| `abfs server gcp init --gc` | Creates the garbage collector tables and columns | Spanner and Bigtable schema | Once, and with each new version before its servers start |
| `abfs server gcp --gc` (server flag) | The server side: `abfsgc` rows, touches, rescues, heartbeats | `abfsgc` rows, touches, heartbeat rows; deletes nothing | On every server while garbage collection is on |
| `gc backfill` | Writes the `abfsgc` rows that objects lack or that have outdated edge rules | `abfsgc` rows | After every server runs with `--gc`, and after an upgrade of the edge rules |
| `gc run --dry-run` | Runs one cycle that only reports | `AbfsGcState`, removal of old heartbeat rows | Before condemn mode, and any time to look |
| `gc run --condemn` | A dry run that also lists and marks the unreachable objects | Lists in the bucket, `AbfsGcLists` rows, markers | On the job's schedule, after clean dry runs |
| `gc run --delete` | A condemn cycle that first deletes the objects whose marker has held long enough, deletes chunks and removes expired refs | Deletes rows, chunks and refs; writes records | On the job's schedule, after clean condemn cycles |
| `gc undo` | Writes back what delete cycles deleted, from their delete records | `objects`, `abfsgc` and `maps` rows, a cell of `chunks` rows, bucket metadata | After a wrong delete, with the delete job stopped |
| `gc walk` | Tells whether keys are in the checkouts of mounts, from object content | Nothing | To explain a key that a client used after it was condemned |

All `gc` commands are subcommands of `abfs server gcp gc` and take `--project`, `--instance`, `--bucket`, `--db`, `--bt-instance` and `--bt-app-profile` as the server does.

## Enable or disable garbage collection

Garbage collection is off unless it is turned on, on the servers and on the clients. Turn it on in this order:

1. Run `abfs server gcp init --gc` to add the garbage collector resources. A server with `--gc` refuses to start without them.
2. Restart every server with `--gc`. From then on each new object gets its `abfsgc` row.
3. Run `abfs server gcp gc backfill` for the objects added before. Start it after the last server was restarted with `--gc`, so that no object is added without a row after the scan passed its key.
4. Turn on the client option `gc-keepalive`, for all clients at once in the central client configuration (`abfs edit config:clients/configs`, see the [System Admin doc](README.md)) with `gc-keepalive: true` under `config:`, or per client with `abfs config`. Set it in the configuration cacheman starts from as well as in the one the mounts start from. Mounts started after the change rewrite their mount ref every 15 minutes, a fixed interval that their mount details record (a configuration that still sets `gc-keepalive-ref-interval` loads, and the key is ignored), and send the hashes of what they hold to the server every `gc-keepalive-touch-interval` (default `18h`), at most 10,000 hashes per second. A send that fails is tried again after 5 minutes, then after twice the previous wait, up to half the touch interval, until one works. The touch interval applies only with `gc-keepalive`; `0` turns the sends off.
5. Run `abfs server gcp gc run --dry-run` on the job's schedule while seeding and builds run, and check each report: no `replicas.warnings`, no `consistency.missing_gc_rows`, no `objects.outdated_rows`, and the dangling edges and roots.
6. When the dry runs are clean, run `abfs server gcp gc run --condemn` on the same schedule instead (see [Condemn mode](#condemn-mode)).
7. Prepare delete mode: a Bigtable instance with one cluster (see [Delete checks](#delete-checks)), an [app profile](#app-profile-of-the-collector) and the [permissions](#permissions-of-the-collector) of the collector, and a bucket with versioning off and no retention policy.
8. When the condemn cycles have run clean for a week or more under real use, run `abfs server gcp gc run --delete` on the same schedule instead (see [Delete mode](#delete-mode)). Before the first one, check the last condemn reports:
   * `app_profile.problems` is empty; the other [delete checks](#delete-checks), such as the cluster count, the conditional write probe and the link validation and versions of the servers, run only in delete mode, so a failed one shows first in the first delete cycle, which then lists it in `delete.refused`, only condemns and exits 4;
   * the `delete.plan` counts per type and their samples are explained;
   * `live_mounts.without_keepalive` and `live_mounts.without_local_write_check` are 0;
   * `condemn.cleared` and `previous_list` show no rescued or live key without an explanation (see [The .live object](#the-live-object)).

   The first deletes come a hold after the first complete list: at daily cycles 4 cycles later, or 5 when a cycle takes more than about 8 hours to complete its list (see [The hold](#the-hold)).

   Snapshots taken before `gc-keepalive` was on have no ref, so the first delete cycles delete each one that no named workspace holds, with the work tree content only it holds. Before the first delete cycle, have users restore every such snapshot they want to keep into a spare workspace, such as a new `--tmp-ws` mount, with `abfs ws snapshot --restore <hash> <path>`, which writes its ref, or keep the work in a named workspace. A restore replaces the work tree at `<path>`, so never restore into a workspace with work in it. Widen `--delete-types` step by step, as the [operator rules](#operator-rules) say.

A store on which an earlier version of the collector ran is not upgraded in place: remove its garbage collector state first, so that no earlier marker survives (see [Remove garbage collection](#remove-garbage-collection)), then follow the steps above from step 1. A store of a version before these names holds instead the Spanner tables `GcState`, `GcServers`, `GcLists` and, if present, `GcReplicas`, the Bigtable table `gc` and the column family `g` of `objects`, and the bucket objects under `gc/`.

Off means the state before step 1. A server without `--gc` reads and writes only what a server without garbage collection does and starts on a deployment that plain `init` created; a client without `gc-keepalive` sends nothing extra. The garbage collector resources can stay in place.

These changes that come with garbage collection apply even when it is off. Each makes a missing object a recoverable case instead of a failure:

* `GetObject` over gRPC reports an object that disappears while it is read as not found (`NotFound`) instead of an unknown error. This applies to every server type.
* A cached map result whose object is missing is skipped as a miss: `maps.MapExisting` tries the next result and stores its input again when none is left, and ninjago treats a missing eval response as a cache miss.
* The git-pusher forgets the LFS objects it pushed for a project after a push of that project fails, so the next push checks them against the server again. It also forgets a cached deep tree that the server reports missing, and computes it again.
* Cacheman serves a `/remote-headers` request, used only by the client keep-alive.

To turn garbage collection off, stop the collector job, restart the servers without `--gc` and turn `gc-keepalive` off. Objects added while it is off get no `abfsgc` row, so turning it on again needs steps 2, 3 and 4 again; until the backfill ran, the consistency pass of the dry run reports the objects without a row, and the condemn and delete modes refuse to run while any are missing. Markers that a condemn cycle wrote stay on their rows while garbage collection is off, and servers without `--gc` do not rescue them, so a marked object used while it was off keeps a marker whose hold may have ended. Before turning it on again, remove the garbage collector state (see [Remove garbage collection](#remove-garbage-collection)). If the state stays, the collector covers the case in which objects were added while it was off: a condemn or delete cycle that finds an object without an `abfsgc` row, or one written during its scan by a server without `--gc`, deletes every row of `AbfsGcLists` of its state before it writes any marker (`condemn.records_cleared`), so no old marker counts for a delete and each is written again with a full hold. For that, run one condemn cycle after step 2 and before the backfill; it refuses because of the missing rows. It cannot tell an off period in which no object was added.

### Remove garbage collection

Removal deletes the garbage collector resources and all their state, the delete records included, so no `gc undo` is possible after it. The chunks of delete records without the flag `chunks-done` stay in the bucket and the `chunks` table for good. Refs and snapshot refs stay; without garbage collection a snapshot ref is an ordinary ref.

1. Stop the collector job and let a running `gc backfill` finish. `SELECT LeaseExpiry FROM AbfsGcState` must return no time in the future.
2. Restart every server without `--gc`. Wait until no server with `--gc` is live, 11 minutes 30 seconds at the default heartbeat; this query must return 0:

   ```sh
   gcloud spanner databases execute-sql <db> --instance=<spanner-instance> --sql="SELECT COUNT(*) FROM AbfsGcServers WHERE StoppedAt IS NULL AND LastSeen > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 MINUTE)"
   ```

3. Turn `gc-keepalive` off in the configurations step 4 of [Enable or disable garbage collection](#enable-or-disable-garbage-collection) turned it on in. A mount keeps it until it is mounted again, and cacheman keeps its local write check until it restarts.
4. Delete the column family `abfsgc` of `objects`, the table `abfsgc` and the app profile of the collector. gcloud cannot delete a column family, so use `cbt`. When `objects` has deletion protection, turn it off for the family delete and on again after.

   ```sh
   cbt -project <project> -instance <bt-instance> deletefamily objects abfsgc
   gcloud bigtable instances tables delete abfsgc --instance=<bt-instance> --quiet
   gcloud bigtable app-profiles delete abfs-gc --instance=<bt-instance> --force --quiet
   ```

   A deployment that manages its Bigtable tables with a tool such as Config Connector or Terraform removes the family, the table and the profile there instead, or the tool creates them again.
5. Drop the Spanner tables. Spanner backups keep them until the backups expire.

   ```sh
   gcloud spanner databases ddl update <db> --instance=<spanner-instance> --ddl='DROP TABLE IF EXISTS AbfsGcLists; DROP TABLE IF EXISTS AbfsGcServers; DROP TABLE IF EXISTS AbfsGcState'
   ```

6. Delete the bucket objects. On a store where no condemn or delete cycle ran, the prefix is empty and the command exits 1 with `matched no objects`, which means nothing was left to remove.

   ```sh
   gcloud storage rm --all-versions "gs://<bucket>/abfsgc/**"
   ```

7. Remove the roles of a separate collector account (see [Permissions of the collector](#permissions-of-the-collector)); keep those of the server account.

After the removal, `objects` has the families `data` and `meta`, the instance has the tables `chunks`, `maps` and `objects`, the database has no `AbfsGc` table, and a server without `--gc` starts and reads the store. Do not undelete the table `abfsgc` to turn garbage collection on again, since its edges are stale. Turn it on with the steps of [Enable or disable garbage collection](#enable-or-disable-garbage-collection) from step 1; `init --gc` creates the table again at once.

### Upgrades

* Each server writes the version of the GC rules it follows into its heartbeat row (version 3 in this release). A collector refuses to delete while a row of a lower version may have run within its grace, and does not complete its list while a row of a lower version may have run within the grace before its snapshot; the markers of such a list are written again by a later cycle. So after an upgrade of the servers, deletes start once no server of an older version ran within the grace and a list completed after that has held.
* A server change that the delete rule depends on raises the version, so the rule above covers later upgrades too.
* Use the same or a newer version for the collector than for the servers: an `abfsgc` row with an object type the collector does not know, or of newer edge rules, stops the condemn.

## Where the data goes

| Data | Service |
| --- | --- |
| The GC edges of each object, the objects it references, and its last touch | Bigtable table `abfsgc` |
| The condemn marker of an object | Bigtable family `objects:abfsgc` |
| The lease and cycle state of the collector | Spanner table `AbfsGcState` |
| The record of each condemned list: its completion time, the largest cache TTL of the servers and the number of markers | Spanner table `AbfsGcLists` |
| Server heartbeats | Spanner table `AbfsGcServers` |
| Condemned lists and their `.live` objects | bucket, `abfsgc/condemned/<state>/` |
| Delete records | bucket, `abfsgc/deleted/<state>/` |
| Ref records | bucket, `abfsgc/refs/<state>/` |

The state is `default`.

## Resources and permissions

### Create the resources

With `--gc`, `abfs server gcp init` also creates the garbage collector resources: the Bigtable table `abfsgc`, the column family `abfsgc` of `objects` and the Spanner tables `AbfsGcState`, `AbfsGcServers` and `AbfsGcLists`. Without it, init creates only the server's own resources. Running it again with `--gc` adds the garbage collector resources to an existing deployment.

Run it with the new version before you start servers of that version: a server with `--gc` checks `AbfsGcState` and `AbfsGcServers` at start and refuses to start without them. Servers do not use `AbfsGcLists`; the collector reads it in every cycle. When it is missing, the condemn and delete modes fail with an error that names `init --gc`, and a dry run reports that error in `delete.plan_error`. The statements are in `shared/remote/servers/gcp/casdb/gcschema.sql`, and each can be applied again without effect.

`init` creates missing tables and does not change existing ones.

At start, a server with `--gc` checks the garbage collector resources, then applies one conditional write: a delete of the cell `abfsgc:condemned` of the absent row `abfs-setup-check`. It retries it on a transient error and stops when Bigtable refuses it or it still fails. A server rescues condemned objects with conditional writes (see [Rescue](#rescue)), so its app profile must use single-cluster routing and allow single-row transactions. The default app profile of an instance created with one cluster allows them; for another profile run `gcloud bigtable app-profiles update <profile> --instance=<bt-instance> --route-to=<cluster> --transactional-writes`. Multi-cluster routing cannot allow them.

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
| Read and write of `AbfsGcState`, `AbfsGcServers`, `AbfsGcLists` and `Refs`, read of `Projects`, DML deletes | `roles/spanner.databaseUser` on the database | the lease, the heartbeats, the list records, ref removal |
| `bigtable.tables.readRows`, `bigtable.tables.mutateRows` | `roles/bigtable.user` | scans, markers, conditional deletes and the conditional write probe |
| `bigtable.appProfiles.get` | `roles/bigtable.user` | the app profile check at start; delete mode fails without it |
| `bigtable.clusters.list` | `roles/bigtable.user` | the cluster count |
| `monitoring.timeSeries.create` | `roles/bigtable.user`, also in `roles/storage.objectUser` | client library metrics (`--gcp-client-metrics`) |
| `storage.objects.get`, `list`, `create`, `delete`, `update` | `roles/storage.objectUser` on the bucket | lists and records, generation reads and chunk deletes, the `chunks-done` flag and the `abfs-undo` stamp |
| `storage.buckets.get` | not in `roles/storage.objectUser`; `roles/storage.legacyBucketReader` on the bucket, or a custom role | the versioning, retention and hold checks of the chunk step |

Role contents: [Bigtable roles](https://docs.cloud.google.com/iam/docs/roles-permissions/bigtable) and [Cloud Storage roles](https://docs.cloud.google.com/iam/docs/roles-permissions/storage). A missing permission on a delete check refuses the deletes of the cycle and names the permission. Without `storage.buckets.get` the chunk step deletes no chunks.

## Fill the abfsgc table

With `--gc` on, the server writes the `abfsgc` row of each object it adds. Objects added before that, or while it was off, have none. `abfs server gcp gc backfill` scans the `objects` table and writes the missing rows, and the rows whose edge rules are outdated for their object type; it reads every such object that is not a blob:

```sh
abfs server gcp gc backfill --project <project> --instance <spanner-instance> --bucket <bucket> [--dry-run] [--rate 2000] [--shards 16]
```

* Each row it writes carries the time of the object row, so the garbage collector's `--grace` counts from the upload of the object, not from the backfill.
* `--dry-run` only counts the rows to write.
* `--rate` limits the object rows scanned per second (0 means no limit).
* The scan can stop at any time; it prints a key to continue from with `--start`. Running it again skips the rows that are already current.
* Do not run it while delete cycles run: stop the collector job, or run it without `--delete`, until the backfill has finished. A delete cycle checks that each object written during its consistency scan has an `abfsgc` row, which only a server with `--gc` writes; a backfill that writes the row of such an object in between hides the server without `--gc` that wrote it.

A row is current when its version of the edge rules is at least the one its object type requires; only the rows of types whose rules changed are rewritten. Version 2 gives each deep tree an edge to every deep tree nested in it: a directory with 1,000 or more entries below it is stored as a deep tree of its own, which only deep trees link. Version 1 gave deep trees no edges, so the dry run counted those nested deep trees as unreachable although mounts read them.

After upgrading from version 1, run the backfill again; it rewrites only the `deeptree` rows. A server still on version 1 keeps writing deep tree rows without these edges: the dry run counts them in `outdated_rows` (in total and per type) and warns about that server in `replicas.warnings`. Run the backfill again once the last server is upgraded.

## Dry run

`abfs server gcp gc run --dry-run` runs one garbage collection cycle that only reports. It deletes and marks no data; its only writes are to the `AbfsGcState` row and the removal of old heartbeat rows of `AbfsGcServers`, both in Spanner. `gc run` takes exactly one of `--dry-run`, `--condemn` (see [Condemn mode](#condemn-mode)) and `--delete` (see [Delete mode](#delete-mode)).

```sh
abfs server gcp gc run --dry-run --project <project> --instance <spanner-instance> --bucket <bucket> [--report gc-report.json] [--explain <key>[,<key>]]
```

### A cycle

1. Takes the lease in `AbfsGcState`, increments the cycle number and records the start and snapshot times. While another process holds a valid lease the command fails. The lease (5 minutes) is renewed while the cycle runs, and the cycle stops itself if the lease is lost or cannot be renewed in time.
2. Reads the list records in `AbfsGcLists` and the server heartbeats (see [Heartbeat](#heartbeat)) in `AbfsGcServers`. It warns about each live server whose settings break the timing rules:
   * a `--gc-touch-interval` of 0 or longer than half of `--grace`;
   * a `--cache-ttl` of 0;
   * GC rules older than the command's.

   A live server is one that did not record a stop and whose last heartbeat is within its serve limit: two heartbeat intervals, 30 seconds and a minute of clock skew, 11 minutes 30 seconds at the default interval. A server stops serving 10 minutes 30 seconds after its last successful heartbeat, so a crashed server stops counting as live soon after. It also warns when `--grace` is shorter than the largest `--cache-ttl` of the servers that may have run within the grace plus `--longest-push`, and when no such server wrote a heartbeat; `settings.cache_ttl_hours` reports that largest TTL. A server may have run within the grace while it did not record a stop and its last heartbeat is less than 30 minutes before the start of the grace, or it stopped after that start. The warnings are in the report and the log; the condemn and delete modes refuse to run with any of them. The report counts the distinct host names among the live servers (`replicas.hosts`, the part of each ID before its last `-`) and their live rows per host (`replicas.rows_per_host`). A server that stops cleanly records the stop in its row, which then no longer counts as live (`replicas.stopped`).

   The command deletes the `AbfsGcServers` rows whose last heartbeat and stop are older than the longest of a day, the largest `--grace` of any condemn or delete cycle plus 10 hours, and its own `--grace` plus 10 hours (`replicas.servers_removed`); the 10 hours are an hour and the 9 hours before the window in which a lost map row touch is still reported (`replicas.map_row_loss_time`). Lost touches are reported, not checked.
3. Reads every ref at one snapshot time, with low priority, and sorts the refs into retention classes (below).
4. For each live mount (see [Mounts and unmounts](#mounts-and-unmounts)), reads its mount details and its pinned manifest, and keeps the project revisions that are object hashes: the commits the mount reads, and the workspace trees that a mount pins in the project annotation `abfs.dir.hash.tag` and uses in place of a commit's tree. For each workspace ref and each snapshot ref, reads its workspace snapshot, the mount details it was taken from and their pinned manifest, and keeps their revisions the same way: the commits the workspace is based on, which the snapshot does not link. Other revisions, such as branch names, are counted and ignored.
5. Scans the `abfsgc` table twice in 16 key ranges: first keys, types and times, then the edges, and the keys of the `maps` table. It reads no object content.
6. Marks in memory everything the roots reach. It walks the roots one at a time in a fixed order: the refs of the classes source, workspaces, snapshots, mounts and unmount records, then the pinned revisions, builds, other `abfs-meta` refs, empty objects, recent objects and map rows, so that the report can name the root that reached an object first. Edges to objects without an `abfsgc` row are skipped and counted. An `abfsgc` row with an object type the binary does not know stops the run.
7. Scans the `meta` family and the newest condemn marker of the `objects` table to check that every object has an `abfsgc` row, to sum the sizes (`meta:size`) of reachable and unreachable objects and to read the marker of each. This reads every object row once. `consistency.marked` counts the objects with a marker and `consistency.marked_live` those the mark found live. In a delete cycle the scan also checks each object written during it (see [Delete checks](#delete-checks)).
8. Writes the JSON report as one line to stdout or to `--report`, and logs a one-line summary.

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
| Recent objects | objects whose `abfsgc` row was written or touched within `--grace` (default `72h`) before the snapshot | `--grace` |
| Empty objects | the empty file and the empty directory, which every server writes at start | always |

Map rows with request types not listed are kept. A ref past its retention is no root; delete mode removes it (see [Ref removal](#ref-removal)). `--grace` must be longer than `--longest-push` (default `3h`, the longest git-pusher push or build upload), or the command refuses to run. It must also cover the largest `--cache-ttl` of the servers that may have run within it plus `--longest-push`, or condemn and delete cycles refuse (step 2 of [A cycle](#a-cycle)). Each server records its cache TTL in its heartbeat row, and the collector reads it there, as the [hold](#the-hold) of delete mode does. `--mount-idle` must be at least 1 hour, four times the interval at which clients rewrite their mount ref.

#### Mounts and unmounts

A mount writes the ref `refs/abfs/mount/<prefix>/<start>` when it starts, where `<prefix>` is its `--ref-prefix` (default `user/<user>`) and `<start>` its start time in nanoseconds. `abfs unmount` writes `refs/abfs/unmount/<prefix>/<start>` after it stopped the processes that used the mount point, the mount daemon among them, unmounted it and, with `--rm`, removed the workspace. It sees and stops only the processes of the user who runs it: a copy of the mount in another mount namespace that processes of another user use, such as a container running as root with a bind mount of the workspace, does not keep the unmount from succeeding and outlives it (see [Limits of the garbage collector](#limits-of-the-garbage-collector)). A failed or refused unmount writes none, nor does an unmount that cannot read the mount details at the mount point or runs with `--no-mount-details`.

* A mount ref without an unmount ref of the same name in the same project is a live mount: the revisions of its pinned manifest are roots, the [client cache checks](#client-caches) count it, and it stays live while the client rewrote the ref within `--mount-idle`. A mount that ended without `abfs unmount`, such as a crashed client, a killed daemon or a deleted pod, stays live that long.
* A mount ref with such an unmount ref is an ended mount. It stays a root of its mount details, but its revisions are no roots and it counts in none of the `live_mounts` fields, so it refuses no deletes in the [client cache checks](#client-caches). The report counts these refs in `refs.mount.ended`. Delete mode removes the mount ref together with its unmount ref once that ref is past `--build-retention`, or when the mount ref is past `--mount-idle`, whichever comes first.
* A workspace keeps the revisions of its mount through its workspace ref, not through the mount ref, so ending the mount does not affect it. A later mount of the same workspace or revision writes its own mount ref.

### Report

The report counts:

* per class, the refs, tombstones, roots and refs past their retention (`refs_to_tombstone` in total), and for mounts the roots that belong to ended mounts (`ended`);
* roots whose object has no `abfsgc` row (dangling roots);
* objects and edges per type, reachable and unreachable, with up to 20 unreachable keys per type (`unreachable_sample`), and dangling edges with a few examples;
* map rows live and expired per class;
* the consistency result with the first objects missing an `abfsgc` row. Objects without an `abfsgc` row that were written after the scan of the `abfsgc` table started, less a minute, are counted apart (`too_new`): an upload during the scans lands there when its `abfsgc` row was written after the scan of the `abfsgc` table passed its key, so the count grows with the upload rate and is not an error. `abfsgc` rows without an object row (uploads in flight) are counted too;
* the time of each step and the peak heap;
* `delete.plan`: what a delete cycle would remove now, from the markers the scan read and the list records. `lists` gives per list whose marker an unreachable object holds its completion time, the cache TTL recorded with it, its hold, when the hold ends (`eligible_time`) and whether it has ended; `held` counts the unreachable objects whose list is still held, `unrecorded` those whose marker has no complete record, which the next condemn writes again; then the candidates and `would_delete` per type with their bytes, the levels and the map rows (see [Delete mode](#delete-mode)). `delete.plan_error` says why the list records could not be read.

The sections `live_mounts`, `workspaces` and `snapshots` report the pins:

| Field | Counts |
| --- | --- |
| `live` | the mount refs of live mounts, the workspace refs or the snapshot refs |
| `without_pinned_manifest` | mount details without a pinned manifest, and workspace snapshots without mount details |
| `without_keepalive` | mount details of clients without `gc-keepalive` or with one of its intervals `0` |
| `without_local_write_check` | mount details with `gc-keepalive` whose cacheman does not keep locally written content until a server confirms it |
| `touch_interval_too_long` | mount details whose keep-alive touch interval plus a tenth is not shorter than `--grace` less the longest touch interval and cache TTL of the servers and a minute |
| `pinned_manifests`, `revisions`, `ignored_revisions` | the pinned manifests parsed, the revisions kept, and the revisions that are not hashes |
| `dangling_revisions` | kept revisions without an `abfsgc` row |
| `not_found` | snapshots, mount details or manifests that do not exist; they pin nothing |
| `errors` | those that could not be read; a read error may hide a pin, so only read errors stop a condemn |

`samples` names up to 20 mount details per count, with the workspace snapshot, host and user, so that the clients behind them can be found.

### Exit status

`gc run` writes the report, then exits with the status of the first cause in this table. A dry run exits 0 or 1.

| Status | Cause | Modes |
| --- | --- | --- |
| 0 | Nothing refused, stopped or skipped | all |
| 1 | The cycle failed: flags, app profile, backend, lease, a list not completed (`condemn.not_completed`), or the report could not be written | all |
| 3 | The cycle refused to condemn (`condemn.refused`) | condemn, delete |
| 4 | The cycle refused to delete and only condemned (`delete.refused`) | delete |
| 5 | A worker error stopped the delete step (`delete.step.error`); the next cycle deletes the keys it did not reach | delete |
| 6 | The chunk step deleted no chunks of a delete record past the undo window (`delete.chunks.skipped`, `records_skipped`), could not list the records (`list_failed`), or left a record without its flag a day past the undo window (`unflagged`; the cause is in `errors`, `error_sample`, `record_errors`, `delete_refused` or `unknown_depot`) | delete |

A chunk step that skips while every record is younger than the undo window, as in the first 7 days of delete mode, exits 0.

### Explain

`--explain <key>[,<key>]` adds an `explain` section with, per key: whether it has an `abfsgc` row and its type, whether it is reachable, the root that reached it first, the write and touch times of its `abfsgc` row, whether it has an `objects` row and the cycle number of its condemn marker. The first root (`first_root`) gives its class and object, and the project, ref and update time of a ref root, the mount details, host and user of a pinned revision, or the result of a map row. It reads two rows per key.

### Memory and load

Memory grows with the `abfsgc` table. The data of the mark takes about 35 bytes per object (key, type, edge index and bitmaps, with room the key arrays keep while they fill), 4 bytes per edge and 16 bytes per map row; the report shows it as `keys_bytes`, `edges_bytes` and `map_rows_bytes`.

The scans read rows faster than Go's garbage collector frees them, so with the default settings the heap grows to about twice the data: 1.5 million objects and 30 million edges take about 170 MB of data and 360 MB of heap. Set `GOMEMLIMIT` for the command to about 1.2 times the data and the heap stays near it (about 190 MB for the same table). A table of 10^8 objects, 2x10^9 edges and 10^7 map rows needs about 12 GB of data: give the job 16 GB with `GOMEMLIMIT=14GiB`, or about 24 GB without it.

The scans read the whole `abfsgc` table, the `maps` keys and the `meta` family of `objects`. Run the command with the [app profile of the collector](#app-profile-of-the-collector).

## Walk the checkouts of mounts

`abfs server gcp gc walk` tells whether keys are in the content of the checkouts of mounts, for a key that a client used again after a cycle found it unreachable:

```
abfs server gcp gc walk --mount-details <hash>[,<hash>] --key <key>[,<key>] --project <project> --instance <spanner-instance> --bucket <bucket>
```

* It reads each mount details object, such as the targets of a client's mount refs, and its pinned manifest, and walks the commit and the trees of every pinned revision from object content.
* A chroot mount pins the tree of its parent workspace for a project in the annotation `abfs.dir.hash.tag` and mounts that tree. The walk follows it as well and reports what it found there with the type prefixed `dir-hash-`. That tree holds the parent workspace's files, so pass the details of the parent mount too.
* A manifest can pin a project at an annotated tag, as release manifests do. The walk follows the tag, and a tag of a tag, up to 10 deep, to the commit, and reports a key it found as a tag with the type `tag`. A pinned revision that cannot be read or is neither a commit nor a tag is reported with the type `revision`, and such a target of a tag with the type `tag-target`.
* It does not read the `abfsgc` table, so it finds a key under an `abfsgc` row that lacks an edge, which an `--explain` of the same cycle cannot.
* It reads trees only to list their entries, so a blob need not exist to be found.
* It prints, per key, `inside` with its type, the first path that reached it (project path, then the path in the project, in manifest order), the pinned revision in the column `COMMIT` (a tag when the manifest pins one) and the mount details, or `outside`.
* Its reads rescue and touch nothing; it takes no lease and writes nothing.
* It fails when the walk is incomplete: mount details without a pinned manifest, revisions or annotated trees that are not hashes, objects it could not read, objects of an unexpected type, such as a tag of a tree, or more than 10 nested tags. A key outside may then be inside.
* It reads every tree of the pinned revisions once per revision, 16 revisions at a time.

## Condemn mode

`abfs server gcp gc run --condemn` runs the cycle of a dry run and then marks every object the mark found unreachable that holds no marker of a complete list: it sets the cell `abfsgc:condemned` of its `objects` row to the cycle number, 8 bytes big-endian. It deletes nothing. A server with `--gc` that reads a marked object clears the marker and answers as for any other object (see [Rescue](#rescue)), so a marked object that a client reads again shows that the mark called something garbage that was still in use.

```sh
abfs server gcp gc run --condemn --expected-replicas <hosts> --project <project> --instance <spanner-instance> --bucket <bucket> [--report gc-report.json] [--explain <key>[,<key>]]
```

### Condemn refusals

The cycle condemns only when its own report is clean. It refuses, writes no marker and no list, writes the report with the reasons in `condemn.refused` and exits with status 3 (see [Exit status](#exit-status)), when any of these holds:

* a warning in `replicas.warnings`;
* the number of distinct host names among the live servers (`replicas.hosts`) differs from `--expected-replicas`, or is outside `--min-replicas` and `--max-replicas`. The count notices servers that write no heartbeat row; two live rows of one host, as after a crash and a restart under the same name, count once and refuse nothing;
* the consistency check did not run, or found objects without an `abfsgc` row. Such objects show that servers ran without `--gc`, so the cycle first deletes every row of `AbfsGcLists` of its state (`condemn.records_cleared`; see [Enable or disable garbage collection](#enable-or-disable-garbage-collection));
* `abfsgc` rows with outdated or newer edge rules (`objects.outdated_rows`, `objects.newer_rows`);
* `live_mounts.errors`, `workspaces.errors` or `snapshots.errors`: a pin that could not be read may hide commits a mount, workspace or snapshot needs;
* a dangling root in the classes `source`, `project-status`, `pushed-packs`, `config`, `acl`, `workspace`, `snapshot`, `mount` or `permanent`. Dangling `pinned-revisions` roots, for example of projects a mount skipped with `--ignore-missing-projects`, do not refuse; check them in `dangling_revisions` and its samples.

### Replica counts

`--expected-replicas` counts host names, not processes, because each server process writes its own row (`<host>-<random>`). VMs keep their host names over a restart. On Kubernetes, run the servers as a StatefulSet, so that a restarted pod keeps its name; a Deployment pod gets a new name, so a crashed pod's row counts as one more host until its serve limit has passed, about 11 minutes at the default heartbeat, and a cycle that reads the rows in that time refuses to condemn and to delete. With `--max-replicas` the same holds at its bound. The holds of earlier lists run on.

A fleet whose size changes, such as an autoscaled one, passes `--min-replicas` instead, and `--max-replicas` when its size has an upper bound. The evidence of each server comes from its own heartbeat row, so the count only notices servers that write none. Set `--min-replicas` to the autoscaler minimum less the servers a disruption budget lets stop at once, and leave `--max-replicas` unset: a stopping server keeps its heartbeat until it stopped, so a rollout can show more live hosts than the autoscaler maximum.

Give each server enough time to stop: the time the platform allows before it kills a stopping server must cover the server's own drain plus 10 seconds to write its buffered touches and 30 seconds to record its stop. A server killed before that leaves a row without a stop, which refuses nothing, but its lost touches can make the next condemn mark objects that are still used; their reads then rescue them.

### A cycle that condemns

1. Reads the list that `AbfsGcState.ListLocation` names, the list of the last condemn cycle, for the report only, and has the mark record the root that first reaches each of its keys. A list that is gone or cannot be read is reported in `previous_list.error`.
2. Deletes the markers of the objects that the consistency scan read with a marker and the mark found live, and writes every such key to `abfsgc/condemned/<state>/<cycle>.live` in the server bucket (see [The .live object](#the-live-object)). So an object that a root reaches again, or that was used, gets a full hold when it is condemned again. `condemn.cleared` counts them and `condemn.live_list` names the object.
3. Deletes the rows of `AbfsGcLists` older than the cycle before whose marker no object holds (`condemn.records_removed`).
4. Writes the list of the objects it condemns to `abfsgc/condemned/<state>/<cycle>` (the state is `default`): the unreachable objects without a marker, and those whose marker is of a cycle without a complete record, such as a cycle that stopped midway. An object whose marker has a complete record keeps it (`condemn.kept`): that value is what a later delete of the object matches. It records the list in `AbfsGcState` (`ListLocation`, `ListSnapshotTime`, `ListComplete` false) and inserts its row in `AbfsGcLists` without a completion time. The list is a binary object: a header with the cycle, the snapshot time, the run and the type names, then per object its 20-byte key, its type and its size, in key order, 29 bytes per object.
5. Writes the markers of the objects of the list, in key order, in batches of 2,000, at most 5,000 per second. It marks only objects whose `objects` row the consistency scan read, so it never creates a row.
6. Reads `AbfsGcServers` again after the last marker batch returned and takes the largest `--cache-ttl` of the servers that may have run within the grace before its snapshot. When one of them runs without the garbage collector hooks, has a `--cache-ttl` of 0 or follows an older version of the GC rules, or none wrote a row, the cycle does not complete its list: it lists the reasons in `condemn.not_completed` and fails, and a later cycle writes those markers again. Otherwise it sets `ListComplete` and the completion time in `AbfsGcState`, and the completion time (a commit time), the TTL and the number of markers in `AbfsGcLists`; `condemn.cache_ttl_hours` reports the TTL.
7. Removes the list and `.live` objects of all but the newest 7 cycles.

The list and its row in `AbfsGcLists` are written before any marker, and only a complete row makes a marker count for a delete. So a cycle that stops midway leaves markers without a complete record, which the next cycle writes again with its own number. The report section `condemn` counts the objects marked, per type, and their bytes.

A cycle reads the previous list once and writes one list and one `.live` object; at 29 bytes per object, a list of 10 million objects is about 290 MB. Besides the memory of the dry run, the collector holds the keys of the previous list and one map entry for each, and the marker of each unreachable object, which fits the room of the existing entry.

### The .live object

The `.live` object holds gzipped JSON lines, one per object that the scan read with a marker and the mark found live, with:

* the key and its type;
* the root that reached it first (`root`, as for `--explain`), for the keys of the previous list;
* the cycle number of the marker it held (`marker`).

A key that a server read since its condemn has no marker left and is not in the `.live` object. The report section `previous_list` counts the keys of the previous list per type: in the list, still unreachable, live per root class with up to 20 keys per class, and rescued, the live keys without a marker. `rescued` is counted only for a list whose cycle wrote all its markers. Each key of the `.live` object needs an explanation, such as a ref moved back to an old target, a touch since the condemn or a read by a client. A key without one points to a root the mark misses; `gc walk` checks whether it is in a client's checkouts.

## Delete mode

`abfs server gcp gc run --delete` runs a condemn cycle that first deletes what earlier cycles condemned, held long enough and is still garbage. Cycle D deletes an object only when all of these hold:

* the consistency scan of cycle D read its `objects` row with the marker of a cycle N, as the newest cell;
* the list of cycle N has a complete record in `AbfsGcLists`, and the snapshot of cycle D is at least the [hold](#the-hold) of that list after its completion;
* the mark of cycle D finds it unreachable and not recent;
* every map row that names it was deleted first (see [Map rows](#map-rows));
* every parent of it in the mark's graph whose `objects` row the scan read was deleted earlier in the same step. A parent that is not live and not deleted, such as one written or used after the marker of the object, keeps it until the parent's own marker has held;
* its marker still holds N on the newest cell. The delete itself checks this: it is a conditional write on the `objects` row with the value the scan read for that object, so one cycle deletes the objects of several lists. A server that used the object since the scan cleared the marker, so the delete does not match;
* every [delete check](#delete-checks) passed.

```sh
abfs server gcp gc run --delete --expected-replicas <hosts> --project <project> --instance <spanner-instance> --bucket <bucket> --bt-app-profile abfs-gc [--report gc-report.json] [--delete-types <type>[,<type>]]
```

A delete cycle:

1. Runs the steps of a condemn cycle up to its checks. A condemn refusal stops the whole cycle, as in condemn mode.
2. Removes the refs past their retention (see [Ref removal](#ref-removal)).
3. Runs the delete checks. When one fails, the cycle lists the reason in `delete.refused`, logs `GC cycle <n> does not delete and only condemns: <reason>` and goes on as a condemn cycle: it skips step 6 and still condemns and completes its list.
4. Clears the markers of the live objects, as a condemn cycle does.
5. Runs the [chunk step](#chunk-step) for the delete records of earlier cycles, also in a cycle whose deletes were refused.
6. Reads the heartbeats again; the checks of the servers must hold again on this read. Then it deletes: the map rows first, then the objects level by level, each object after its parents. Before the deletes of each batch of up to 2,000 keys it writes a [delete record](#delete-records) to the bucket. The `abfsgc` row of each deleted object goes too, unless the object was written again since the start of the grace. A key whose delete had no clear outcome is read again at the end of the step; one still present keeps its marker. A read that fails stops the cycle before its list; the next cycle plans from the markers and deletes.
7. Condemns what is still unreachable, was not deleted and holds no marker of a complete list, as a condemn cycle does, and completes its list.
8. Removes the lists, delete records and ref records of old cycles.

So an object is deleted at the earliest a hold after the list that condemned it was completed, unreachable and older than `--grace` at the condemn and at the delete; its chunks go 7 days or more after the first delete record that names it (see [Undo](#undo)). A delete step that the collector left, by a crash or a lost lease, needs no special care: the next cycle plans again from the markers, and a late delete of the old run matches only an object that still holds the marker that run read.

`--delete` fails at start, before it scans, when its Bigtable app profile does not fit (see [App profile of the collector](#app-profile-of-the-collector)).

### Flags of delete mode

Only `--delete` takes `--delete-types` and `--accept-mounts-without-keepalive`; the other modes reject them. The flags of the dry run and the condemn mode apply as well.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--expected-replicas`, `--min-replicas`, `--max-replicas` | | As in condemn mode; one of the first two is required |
| `--delete-types` | every type | Object types, separated by commas, that the cycle may delete. Others are kept, with every object they link |
| `--accept-mounts-without-keepalive` | off | Delete blobs, trees and eval objects while live mounts run without `gc-keepalive`; the check then only warns (see [Client caches](#client-caches)) |
| `--mount-idle`, `--build-retention` | `336h`, `720h` | As in the dry run; delete mode removes the refs past them |
| `--snapshot-retention` | `720h` | Keep what a snapshot ref reaches, with the commits its mount was pinned to, for this time after the ref was written; delete mode removes older snapshot refs. All modes read it |
| `--gcp-client-metrics` | off | Export the client library metrics to Cloud Monitoring |

These are fixed: the delete step deletes at most 2,000 objects per second, with 32 conditional deletes in flight over the 16 key ranges; the chunk step deletes at most 500 chunks per second, and only those of records at least 7 days old, far longer than the 10 minute lifetime of signed chunk URLs, and repeats the `abfsgc` row deletes of a record from 15 minutes after it; one attempt of a Bigtable write has a deadline of 30 seconds, a list object that plus its size at 8 MiB/s.

### The hold

A delete cycle deletes the objects that hold the marker of a list only once its snapshot is at least the hold of that list after the list was completed. The hold is the longer of:

* `--grace` plus 16 hours;
* the cache TTL recorded with the list, plus 2 minutes, plus the longest time a client relies on an answer that an object is present, plus a minute of clock skew. That time is `--longest-push`, or, when longer, 8 hours (cacheman reuses map results that long), plus the longest `gc-keepalive-touch-interval` of the live mounts of the delete cycle and a tenth of it, plus an hour for the keep-alive pass.

A server answers present from information read before a marker landed for at most its cache TTL and 2 minutes after the list was completed, and a client relies on such an answer for at most the time above, so no client relies on an object once its hold has ended. The hold rests on no touch that a server buffers. The cache TTL recorded with a list is the largest of the servers that may have run within the grace before its snapshot, read after its last marker was written. The 16 hours keep the outputs of a map result that a mount without `gc-keepalive` applies, under `--accept-mounts-without-keepalive`, for as long as earlier versions did at a cache TTL of up to 6 hours 39 minutes.

At the defaults (`--cache-ttl 6h`, `--grace 72h`, `gc-keepalive-touch-interval 18h`) the second term is about 34.9 hours and the hold is 88 hours. `delete.plan.lists` reports the hold of each list and when it ends.

The collector does not take the schedule as a setting, and a late or missed cycle costs nothing but its own deletes: any cycle after the hold deletes. At one cycle a day, the objects of list N are deleted in cycle N+4 when the list was completed within about 8 hours of its snapshot, else in cycle N+5.

The window of a cycle starts `--grace` plus a minute before its snapshot; the checks of the servers cover every server process that may have run in it.

For a trial on a small store, servers at `--cache-ttl 5m --gc-touch-interval 30m` and the collector at `--grace 90m --longest-push 20m` fit these rules. The margin of 16 hours is fixed, so the first deletes come 17.5 hours after the first complete list.

### Delete checks

Each reason in `delete.refused` stops the deletes of that cycle only; the cycle still condemns and completes its list, and the holds of earlier lists run on, so the next cycle can delete. Which objects a cycle may delete, those whose list has held, is not a check but the plan: a cycle with no list past its hold deletes nothing and refuses nothing.

| Reason | What to do |
| --- | --- |
| The list records in `AbfsGcLists` cannot be read | Check the collector's access to Spanner, and that `init --gc` created `AbfsGcLists` |
| A server of an older version of the GC rules may have run within the grace | Stop it; deletes wait until it stopped `--grace` plus a minute ago |
| A server runs without link validation | Restart it without `--no-link-validation` |
| A server's settings break the timing rules | Fix the setting; deletes wait until the server with the old setting stopped `--grace` plus a minute ago |
| `--grace` is shorter than the largest cache TTL of the servers in the window plus `--longest-push` | Lower the servers' `--cache-ttl` or raise `--grace`; deletes wait until the server with the longer TTL stopped `--grace` plus a minute ago |
| The number of live server hosts does not match, on the second read of the servers | As for a [condemn refusal](#condemn-refusals); a crashed pod replaced under a new name counts for about 11 minutes |
| The Bigtable instance has more than one cluster, or its clusters cannot be listed | Delete mode needs an instance with one cluster: conditional writes sent to different clusters can both succeed |
| The app profile refused a conditional write, or the probe failed | Use a profile with single-cluster routing and single-row transactions; check the [permissions](#permissions-of-the-collector) |
| The removal of expired refs failed | The next cycle tries again |
| Live mounts run without `gc-keepalive`; mounts with it run with a cacheman that does not check local writes; mounts touch their objects less often than the grace allows | See [Client caches](#client-caches) |
| Objects written during the consistency scan have no `abfsgc` row; the reason names up to 10 of them | A server without `--gc` writes to the store; find and stop it |
| The `abfsgc` rows of the objects written during the consistency scan cannot be read | Check the collector's access to Bigtable and the load of its app profile; the next cycle checks again |
| Only some of the objects written during the consistency scan were checked: the checks fell behind the writes, and the scan stopped waiting for them | Nothing; deletes resume in a later cycle with fewer writes during its scan (see [Planning figures](#planning-figures)) |

A crash, an out-of-memory kill, a preemption, a pause of a VM or a lost touch of a server refuses no deletes. A server records lost touches in its heartbeat row and counts them in `abfs.gcp.gc.touches`; alert on them, since a lost touch can make a condemn mark an object that is still used, which a read then rescues.

### Client caches

A client cache can hold the only copy of content a client wrote, such as build outputs equal to an old, unreachable blob. A cacheman with `gc-keepalive` on in its own configuration keeps such content out of eviction until a server confirmed the object after the write, and pushes it when the server lacks it and a write or the keep-alive of a mount names it; the mount records this in its mount details. When `--delete-types` allows blobs, trees, eval requests or eval responses, the cycle refuses its deletes while:

* a live mount runs without `gc-keepalive` or with one of its intervals `0` (`live_mounts.without_keepalive`). Turn `gc-keepalive` on as step 4 of [Enable or disable garbage collection](#enable-or-disable-garbage-collection) says. `--accept-mounts-without-keepalive` turns this check into a warning: content such mounts wrote and their cache evicted is then lost once deleted. The outputs of a map result such a mount applies are kept by the eval map row, or, when a server lost the touch of that row, by the 16 hours of the [hold](#the-hold) beyond the grace. A mount without keep-alive writes its mount ref only when it starts, so it counts until `abfs unmount` ends it or for `--mount-idle` after it started, whichever comes first. After such a mount ended without `abfs unmount`, these deletes wait up to 14 days;
* a live mount with `gc-keepalive` runs with a cacheman that does not check local writes (`live_mounts.without_local_write_check`): an older cacheman, or one started with `gc-keepalive` off. A cacheman restarted with `gc-keepalive` off under a live mount turns that mount's field false for good; restart the mount;
* a live mount touches its objects less often than the grace allows (`live_mounts.touch_interval_too_long`): its `gc-keepalive-touch-interval` plus a tenth must be shorter than `--grace` less the longest touch interval and cache TTL of the servers and a minute. A mount within this limit keeps what it holds recent, so the condemn does not mark it and no read needs a rescue. At the defaults the limit is about 30 hours, and the default `gc-keepalive-touch-interval` of `18h` fits it.

The longest touch interval of the live mounts also counts in the [hold](#the-hold): cacheman answers map lookups from its own copy of the results a server returned for up to 8 hours, so a build can put the outputs of a result in the mount that long after a server returned it, and the next keep-alive pass names them up to the touch interval and a tenth later, plus an hour for the pass itself. A pass sends at most 10,000 hashes a second, about 17 minutes for a mount of 10 million, and a failed pass is retried after 5 minutes.

At each start, cacheman runs a start pass: it goes through every entry it may evict, oldest first and at most 10,000 entries per second, and asks the server about the blobs, trees and objects among them. The pass asks the server to record no use: the server writes no touch, rescues nothing and answers a condemned object it reads as missing. A header the server cached before the condemn still answers present until its cache TTL ends. A start does not use the content, so it keeps nothing alive. Older servers ignore the request and touch and rescue as for any other call. A cache of 10 million entries takes about 17 minutes. Until the pass ends, eviction takes only the entries the pass has checked; a cache at its eviction threshold with none of those left evicts unchecked entries. ARI entries, RBE CAS blobs and entries whose file is gone are not checked; eviction takes them as it does without `gc-keepalive`.

The start pass parks each entry the server lacks: the entry leaves the eviction order and is not pushed. Most such entries are garbage the collector deleted; a few can be content a client wrote that cacheman never indexed. A mount that holds the content names it in its next keep-alive pass. When the server lacks it, cacheman pushes it; when the server has it, the entry goes back to the eviction order as used now. A local write of the content ends the park the same way. A maintenance run of cacheman, at each start after the start pass has read the eviction order and every 3 hours, returns entries parked for 96 hours to their place in the eviction order before the park, so that eviction takes them first. Until such an entry is used, a later start pass that finds it missing does not park it again; it moves it to the young end of the eviction order, as a push does, so that a mount can still name it. Parked entries count toward the eviction threshold. Once no other entry is left, eviction takes parked entries, oldest park first, until the cache is down to 80 % of its threshold, where each eviction run stops. The cacheman calls `/wait-for` and `/is-object-remote` treat a parked entry as missing on the server, so a build that waits for a parked output pushes it, or finds it on the server and returns it to the eviction order as used now.

Cacheman reports its checks in `remote_checks` of its stats. `skipped` counts the entries the checks left out, and `restored` counts such entries that an older cacheman kept out of eviction and that a start returned to it; the start logs up to 10 of their hashes. Both include blobs, trees and objects whose file is gone, which cacheman cannot tell from RBE CAS blobs. `parked` counts the entries parked, `parked_size` gives the bytes parked now, `released` counts the entries returned to the eviction order after 96 hours, and `evicted_parked` those evicted.

### Map rows

Before the objects, the step deletes the map rows that name a candidate, and the eval rows past `--map-retention` whose two ends stay live.

* Each delete is conditional: a row written or used since the start of the grace (for an expired eval row, since the start of the map retention) stays, and a row that stays keeps both its candidate ends.
* A row goes only when both its ends are of types `--delete-types` allows; an end without an `abfsgc` row counts as allowed. Otherwise the row stays and keeps its candidate ends, so a cycle limited to some types drops no cached result of the others.
* Readers that miss a deleted eval row compute the result again.

### Delete records

Each batch of the delete step writes the object `abfsgc/deleted/<state>/<cycle>/<level>-<range>-<batch>` before any of its deletes. It holds a header with the cycle, the run and the start of the grace, then the 20-byte key of each object and the keys of the two ends of each map row, then per object the newest cell of each column of the families `meta` and `data` of its `objects` row, with its time, and the values of the family `e` of its `abfsgc` row. A record holds at most 16 MiB; a larger batch goes into several records, and the report counts their size in `delete.step.record_bytes`. The chunk step and `gc undo` read these records.

When every key of a record is done, the chunk step sets the custom metadata `chunks-done=<cycle>` on it. The cycle keeps the records of the newest 7 cycles that have records, and every record flagged within 7 days, so a record lives about 14 days. Before it removes the records of a cycle it writes `abfsgc/deleted/<state>/<cycle>/pruned`, so that a `gc undo` of that cycle fails instead of restoring nothing. A record without the flag is never removed; `delete.chunks.unflagged` counts those older than 8 days, past the 7 days every record waits, and gives the age of the oldest; while it is above 0 the cycle exits 6.

### Chunk step

In each delete cycle, also one whose deletes were refused, the chunk step reads the delete records without the flag. It runs before the delete step of its cycle, so the records a cycle writes show first in the chunk step of the next cycle. From 15 minutes after a record was created, it repeats the `abfsgc` row deletes of its objects without a row, which a delete step that stopped can leave, and sets `gc-rows-done=<cycle>` on it (`records_gc_rows_done`). From 7 days after, the undo window, it deletes the chunks, and repeats the `abfsgc` row deletes of a record without `gc-rows-done`. Younger records count in `records_waiting`. For each object of a record:

| Object row | Outcome | Count |
| --- | --- | --- |
| present with the recorded descriptor | the delete did not happen; the chunks stay | `kept_row_present` |
| present with another descriptor | a client wrote the object again after the delete; the recorded chunks go | `reput_after_delete` |
| absent | the recorded chunks go, and the `abfsgc` row, unless the object was written again since | `row_absent` |

A chunk row goes only when it has no cell newer than the record, such as the cell `gc undo` writes. A bucket object goes only under the generation and metageneration read just before the delete, so an object written again or stamped by `gc undo` stays (`restored_after_record`). A delete the bucket refuses (`delete_refused`) or an error leaves the record without the flag, for the next cycle.

The step deletes no chunks, and lists why in `delete.chunks.skipped`, when the bucket settings cannot be read or the bucket has any of the settings below. The `abfsgc` row deletes run either way. `records_skipped` counts the records past the undo window whose chunks it skipped. When the records cannot be listed, the step does nothing and reports `list_failed`. Either makes the cycle exit with status 6, as does a record that `unflagged` counts.

* Object Versioning on: a delete would keep a noncurrent version;
* a retention policy or a default event-based hold: each can refuse a delete.

The gRPC API the servers use does not report object retention, so the step cannot check it; a delete that the retention of an object refuses counts in `delete_refused`.

Bucket soft delete plays no part: a bucket with it on works as one without it.

### Ref removal

Each delete cycle that passes the condemn checks removes the refs that were past their retention at its snapshot:

| Class | Removed when not written within |
| --- | --- |
| mount refs | `--mount-idle`; the ref of an ended mount also goes with its unmount ref, and is removed before it |
| build, build-start and unmount refs | `--build-retention` |
| snapshot refs | `--snapshot-retention` |

* It first writes them to `abfsgc/refs/<state>/<cycle>` in the bucket, as JSON with the project ID, host, project, ref, update time and target of each.
* Then it removes them in Spanner transactions of up to 500 refs, each ref only while its update time and target are still the ones of the snapshot; a ref written since stays a root.
* The report counts per class in `delete.refs.classes` the refs recorded, `removed`, `changed` (written since) and `gone`.
* A failed removal refuses the deletes of the cycle, and the next cycle tries again.
* Ref records are kept like delete records, counted from their write.

The mark of the cycle does not count these refs as roots, so the cycle condemns what only they reached, and a cycle after its hold deletes it. To put a ref back while its target exists, send its line again; this writes a new update time, so the ref is a root for its whole retention. With ACLs, the operator needs write access to the project of the ref, `abfs-meta` for the refs of clients:

```sh
echo "<target> <ref>" | abfs test-client put-ref -s <host> -p <project>
```

### Snapshot refs

Only clients with `gc-keepalive` write a snapshot ref: `refs/abfs/snapshot/user/<user>/<hash>` in `abfs-meta`, on `abfs ws snapshot` and again after a successful `abfs ws snapshot --restore <hash>`, which moves its update time. Local-only clients and cacheman in remote read-only mode write none. The name falls under the ACL rule `refs/abfs/*/user/<user>/*`: `<user>` is the local user name, and with ACLs it must equal the part of the user's email before `@`, or the server refuses the write.

Cacheman writes the ref only after the snapshot and the content it links are on the server. The command waits for this at most 30 seconds. When the write fails or is not confirmed in that time, the command logs a warning and still succeeds: the snapshot is stored, and a write still queued in cacheman usually lands later.

* The ref keeps the snapshot, its mount details, their pinned manifest and the pinned commits for `--snapshot-retention` (default `720h`) after its last write.
* Delete mode then removes the ref, records it in `abfsgc/refs/<state>/<cycle>`, and a hold later deletes what only the ref reached.
* A snapshot taken without `gc-keepalive`, or by an older client, has no ref and is deleted once it is garbage. A `--restore` of it before the delete creates its ref.
* The report has a section `snapshots` with the fields of `live_mounts`, counts the refs in `refs.snapshot`, and records `settings.snapshot_retention_hours`.
* A read error on a snapshot's pins, or a snapshot ref whose target has no `abfsgc` row, stops a condemn.
* A collector of an older version counts these refs as `other-meta` and keeps the snapshots for good, but not the commits their mounts were pinned to; run a collector of this version.

### Report of a delete cycle

The section `delete` holds:

| Field | Content |
| --- | --- |
| `refused`, `warnings` | why the cycle did not delete, and the checks that options turned into warnings |
| `too_new` | the check of the objects the consistency scan found without an `abfsgc` row because they were written during it: `keys` checked, which equals `consistency.too_new` unless the scan stopped waiting for the checks; `not_hashes`; `gone`, deleted again before the check; `without_gc_row`, with up to 10 `samples`, which are a sample and not the lowest keys. When the check failed, it counts the objects checked before, and the reason is in `refused` |
| `plan` | what the cycle planned to delete |
| `step` | the record prefix; the objects deleted per type and their bytes; `not_matched` with samples, each of which should have a rescue log line of a server; `gone`; `blocked` (below a key that was not deleted); `type_not_allowed`; keys of unknown outcome; `abfsgc` rows and map rows deleted and kept; the time of the first 100 levels per key range |
| `chunks` | the chunk step |
| `refs` | the ref removal |
| `records_removed` | the delete records removed |

### Planning figures

* A delete step removes about 24,000 objects in about 19 seconds on one Bigtable node, about 0.8 ms per object. Its rate of at most 2,000 objects per second also bounds it: 10^6 candidates take at least about 8 minutes.
* A condemn or delete cycle over about 71 million objects takes about 7 to 14 minutes on one node with autoscaling to 3.
* Writing the markers of about 27,000 objects takes about 6 seconds at the rate of at most 5,000 per second.
* A delete cycle checks each object written during its consistency scan while the scan runs: it reads the object's `abfsgc` row, and its `objects` row only when the `abfsgc` row is missing, in batches of 100 keys with 8 batches in flight, and keeps only counts and the batches in flight in memory. That is one row read per object. Bigtable rates one SSD node at up to 17,000 row reads per second ([performance](https://docs.cloud.google.com/bigtable/docs/performance)), so on a node with no other load than the collector 1 million objects written during a scan take about 1 minute of reads and 10 million about 10 minutes, less with more nodes. The collector reads at low priority, so a node busy with a seed checks slower.
* When the checks fall behind, the scan waits for them, at most 15 minutes summed over its key ranges. The wait makes a delete cycle up to about 15 minutes longer than a condemn cycle, about the length of one more cycle at 71 million objects, and about 1 minute longer when all 16 key ranges wait at once. The checks share the node with the scan, so they also slow it. Past that wait the scan stops checking and cancels the reads in flight, finishes at full speed, and the cycle refuses its deletes. A cycle during a seed or a large catch-up of a git-pusher can refuse this way; deletes resume in a later cycle with fewer writes during its scan.
* The step runs one level per link in a chain of unreachable objects: for a dropped branch of 10,000 commits, expect about 10,000 levels and an estimated 13 to 20 minutes of delete step and tens of thousands of delete records, which the next chunk step reads.
* Deleted chunks are billed for the 7 days of the undo window. The delete records hold the deleted rows, about 3 KB per object on average plus 20 bytes per edge, for about 14 days: at 5 million deleted objects a day, about 15 GB a day and 210 GB in all. `delete.step.record_bytes` gives the size per cycle.

## Backups and undo

### Backups

The collector needs no backups, and no delete cycle checks for them. Bigtable backups of the tables are disaster recovery, on a schedule of their own; `gc undo` does not read them. Restoring a backup brings back a table as it was at the backup, with every write since lost, and is for the loss of a table, not for a wrong delete.

### Undo

`abfs server gcp gc undo` writes back what delete cycles deleted, from their delete records, which hold the rows they deleted, and the live tables. It restores every object and map row those cycles deleted, not only the wrong ones; what is still garbage is condemned again after `--grace`, and deleted a hold later.

The undo window is 7 days. For each key it counts from the first delete record that names it. Within the window, the chunks of the deleted object still exist and an undo restores it. After that, the chunk step deletes the chunks: an undo then restores the objects whose content is inline (up to 32 KiB, most trees and commits) and the map rows, while the records exist, about 14 days.

A batch writes its record before its deletes, so a record can name a key that its cycle did not delete: the delete step stopped, the marker of the key changed, or the outcome of the delete stayed unknown. A later cycle deletes that key. An undo of that later cycle can lose up to one cycle of the window for the key, and then reports its chunks as missing (`missing_chunks`).

1. Stop the delete job.
2. Run the undo for the first wrong cycle through the last cycle that ran. A later cycle can delete the children of an object an earlier cycle deleted, so an undo of one cycle alone can miss them:

   ```sh
   abfs server gcp gc undo --cycle <n> [--through <m>] --project <project> --instance <spanner-instance> --bucket <bucket> --bt-app-profile abfs-gc
   ```

3. When it reports objects blocked by a child, follow the reason: undo the cycle it names too, or run the same undo again. The command fails while anything was not restored. Run a failed undo again: chunks it kept for an object it did not finish stay in the store for good otherwise.
4. Undo restores no refs. It names the ref records of the cycle before `--cycle` through `--through`; put back the refs that were removed by mistake with `test-client put-ref` (see [Ref removal](#ref-removal)).
5. Fix the cause of the wrong delete before the delete job runs again.

What the undo does:

* It fails when a cycle in the range has pruned records, or a record of the range is of format version 1. When a restored object links an absent object, it reads the keys of the records of every cycle, and then any record of format version 1 under `abfsgc/deleted/<state>/` fails it. Delete those records first (see [Upgrades](#upgrades)).
* It takes the lease in the phase `undo`, which does not raise the largest grace, and lists the records again then, so that no collector removes them meanwhile. A range it refuses fails before the lease and leaves the GC state as it was. Before its first write it waits until a collector that lost the lease has stopped writing: the expiry of that lease, or the time of its own lease when that lease was released, plus two write attempts of 30 seconds each and a minute.
* It restores the newest cycle first and in each cycle the deepest level first, so children come back before their parents.
* For each object whose row is absent: its chunks must exist; its bucket chunks get the custom metadata `abfs-undo=<cycle>` and its chunk rows a cell `data:undo` with a new time, so that a chunk step that read them earlier cannot delete them; its `abfsgc` row is written from the record with a new time and a touch, so it is recent for `--grace`; its `objects` row comes last, with the cells and times of the record, without the marker.
* An object is not restored, nor anything that links it, when a chunk is gone (`missing_chunks`; the log names the chunk), it had no `abfsgc` row when it was deleted (`without_gc_row`), or it links an absent object that this run did not restore and that a delete record of another cycle names, or, when the records of a cycle were pruned, any absent object (`blocked_by_child`).
* Map rows come back with a new time. A map row is not restored when an end was not restored, or is absent under the same rules as a linked object (`map_rows_blocked`; the log names the end). What is present stays as it is.

## Operator rules

* Raise `--grace` only with care. The collector keeps a server row for the largest grace of any earlier condemn or delete cycle plus an hour, and a delete cycle with a larger grace needs rows from further back, which earlier cycles may have removed. After raising `--grace`, run condemn cycles instead of delete cycles until the increase has passed since the last cycle with the old grace. Lowering it needs no wait.
* A server of an older version of the GC rules refuses the deletes of every cycle whose grace it may have run in, and the completion of every such list (see [Upgrades](#upgrades)). A server that writes no heartbeat row is seen only by the host count.
* A map row is deleted only when every end that has an `abfsgc` row is of a type `--delete-types` allows.
* Widen `--delete-types` from the top of the graph down: commits, `project-status` and build objects first, then trees and deep trees, then blobs. Blobs, trees and eval objects need every live mount on `gc-keepalive` with a cacheman that checks local writes.
* The servers' `--gc-touch-interval` must be at most half of the collector's `--grace`, and their largest `--cache-ttl` plus the collector's `--longest-push` at most `--grace`. A server with `--gc` refuses to start with either at `0`. The collector reads both from the heartbeat rows; the dry run warns when they break the rule.
* Deletes come a hold after each list, whatever the schedule of the cycles (see [The hold](#the-hold)).

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

`PutObject` writes the `abfsgc` row of each new object, with the GC edges, before its object row. This adds one Bigtable write to each upload. A failed `abfsgc` row write fails the upload.

### Rescue

The collector marks an object it plans to delete with the cell `abfsgc:condemned` of its `objects` row. The server reads that cell with every object row it reads: in `Headers`, the existence check of `PutObject`, the link checks of `PutObject`, `PutRefs`, `XchgRefs` and `AddMapResult`, `GetObject` and `MapResults`. On a marked row it:

1. writes a touch;
2. clears the marker, but only while the marker still holds the value it read;
3. answers as for any other object.

A rescue whose touch fails fails the call and leaves the marker, so that the client's retry rescues it again. A condemn write that is retried or lands late can set the marker again after a clear, and then only the touch keeps the next cycle from finding the object unreachable.

When the collector deleted the row first, the object is missing, and a client that writes it stores it again. The touch stays behind as an `abfsgc` row with only the touch.

The conditional writes need an app profile that allows single-row transactions, which the server checks at start. Rescue has no switch of its own: with `--gc` every header read includes the family `abfsgc`. Only condemn and delete cycles write markers; until one runs, this costs only the wider read.

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
* The report of the next condemn cycle counts the rescued keys of the previous list in `previous_list`.

### Touches

When a call finds an object (`Headers`, `GetObject`, the existence check of `PutObject`, the link checks, the results of `MapResults`), the server writes the cell `t:touch` of the object's row in the table `abfsgc`, with a Bigtable server timestamp.

* Each server writes at most one touch per object in each period of `--gc-touch-interval`, so an object in use has a touch younger than the interval. The `abfsgc` row of an upload and the touch of a rescue, both written before the call answers, count as the touch of their period, so the next hit in that period writes none. The periods count from the server start, shifted for each object by an offset taken from its key, so the periods of different objects end at different times and their next touches spread over the interval.
* `MapResults` also writes the cell of each map row it returns again, with the same limit, so the timestamp of a map row is the time it was last used. Map results therefore come back in the order of their last use, oldest first, instead of the order they were added. ninjago tries the newest result first, so builds are not affected.
* Each server remembers the last touch of up to 4,000,000 map rows, so that it rewrites each at most once per touch interval. A server that returns more distinct rows per touch period touches a row again once it was forgotten, so its map row touches follow the rate of `MapResults` calls rather than the period. The memory grows with the rows returned, about 225 bytes per row and 900 MB when full.

Touches wait in a buffer of 100,000, and four workers write them in batches of up to 1,000, at least once a second:

* When the buffer is 80% full, `Headers` and `MapResults`, the calls of the client keep-alive and of builds, wait up to half a second per object touch for room below that mark. A burst, such as the keep-alive of a large mount, is slowed to the rate the workers write instead of losing touches.
* When a wait times out, touches are dropped and counted, without waiting, until the buffer is below 80% again. So with `--gc` the latency of `Headers` and `MapResults` depends on the table `abfsgc`: while the buffer is 80% full they run at the rate the workers write, and when the writes stall, the first of them waits half a second.
* `GetObject`, the existence check of `PutObject` and the link checks never wait for room. They may use the whole buffer, so they find room during a burst of `Headers` or `MapResults`. When the buffer is full they drop the touch and count it. The next use of a dropped object writes its touch.
* Map row touches never wait either, and are queued only while the buffer is less than half full, so they take at most about half of it and leave the rest to object touches. The next use of a row whose touch was dropped writes its touch.
* A touch is written or counted as lost within 251 seconds of the use. A touch whose write starts later, because the process was frozen or starved, is written and also counted as failed.

Every dropped or failed touch of an object is recorded in the server's heartbeat row and counted in the metrics; it refuses no delete. Touches decide only what a condemn marks: an object whose touch was lost can be marked while it is still used, and its next read rescues it. Map row touches are counted and recorded apart: a map row touch keeps an eval map row, and through it the outputs of its result, within `--map-retention`.

### Cache TTL and write bound

The header and content caches keep an entry for `--cache-ttl` and then read it again from Bigtable, so that every server sees a marker well before the collector deletes the object. The TTL counts from the start of the Bigtable read that loaded the entry, on a clock that keeps running while the VM is suspended. A read that itself took `--cache-ttl` or longer is not used: the call fails with `Unavailable` and the client retries. The hold of a list counts the largest TTL of the servers (see [The hold](#the-hold)), and the collector's `--grace` must cover the TTL plus its `--longest-push`. A shorter TTL costs more Bigtable reads on directory listings.

After a call validated the links of a new object, ref or map result, the writes that record it must end within 2 minutes of the start of the validation. An interceptor on the server's Bigtable and Spanner connections checks this before each attempt, the retries of the client libraries included, on the clock that keeps running while the VM is suspended. A write attempt later than that is not sent; the call fails with `Unavailable`, and the client sends it again, which validates again.

A call that answers that objects are present, `Headers`, `GetObject` before it returns the content, `MapResults` and `PutObject` for an object that exists, fails with `Unavailable` when 2 minutes or more passed on that clock since the call started, as after a suspend between the lookup and the answer. The [hold](#the-hold) counts on both bounds. `abfs.gcp.gc.answer_seconds` records the time of each such call; a `Headers` call of many keys with many rescues that runs out of time is retried and finds the markers cleared.

### Missing map results and chunks

* `MapResults` leaves out results whose object does not exist, with one header lookup for all results, from the cache where it can. So `MapResults` can fail with a Bigtable error from the table `objects`.
* When a chunk of an object is missing while the server streams it, the server drops the object from its caches and reads the object row again. If the row is gone, the read fails as a missing object; if the row still lists the chunk, the content is lost and the read fails with an error that is not "missing".

### Heartbeat

Each server writes its row in the Spanner table `AbfsGcServers` before it serves and then every 5 minutes. The row holds:

* an ID made of the host name and a random suffix, logged at start;
* the version of the GC rules the server follows, which it also writes into the table `abfsgc`, and `GcEnabled`, always true;
* its `--gc-touch-interval`, `--cache-ttl` and heartbeat interval;
* `LinkValidation`, false with `--no-link-validation`;
* `LastSeen`, the commit time;
* `TouchLossTime`, set by a heartbeat or the stop that records a loss: object touches dropped or failed since the last heartbeat, a pause of the process (the wall clock ran more than 10 seconds ahead of the monotonic clock, as in a VM suspend), or a gap between heartbeat attempts longer than the interval plus 251 seconds;
* `MapRowLossTime`, set the same way when map row touches were dropped or failed since the last heartbeat;
* `StoppedAt`, set by a clean stop.

The collector uses the rows to see which servers ran in the window of a cycle, with which version of the GC rules and settings, and to take the cache TTL it records with a list. Losses are reported, not refusals. A server that cannot write its first heartbeat within a minute fails to start. A later failed write is logged and counted; once the last successful one is older than two intervals plus 30 seconds (10 minutes 30 seconds), every call answers `Unavailable` until the next one succeeds, so that the collector sees every server that serves.

When the server stops, it writes the touches left in its buffer, for up to 10 seconds, and logs one line `GC touches at stop: ...` with the object touches written, dropped and failed during the stop and since the start, and the map row touches written, dropped and failed since the start, because its metrics are no longer exported by then. Then it records the stop in its row, with `TouchLossTime` when it lost object touches and `MapRowLossTime` when it lost map row touches since its last heartbeat. The row stays. A row without `StoppedAt` whose last heartbeat is past its serve limit marks a crash, a kill, a preemption or a stop whose record failed. It no longer counts as a live host, and it counts for the window of a cycle, its cache TTL and its version until its last heartbeat is 30 minutes before the start of the grace.

### Cost

One Bigtable write per object and map row used, per server and touch period, batched; one touch and one conditional write per rescue; one Spanner read and write per heartbeat.

### Metrics

Metrics, as OpenTelemetry counters and one histogram. Their names keep the dots when they are exported with OTLP. Cloud Monitoring stores them as they arrive, for example `prometheus.googleapis.com/abfs.gcp.gc.rescues/counter` through the Ops Agent OTLP receiver, so a PromQL query names them with `__name__`: `sum by (call, type) ({__name__="abfs.gcp.gc.rescues"})`. On a Prometheus `/metrics` endpoint the dots become underscores and the names end in `_total`, for example `abfs_gcp_gc_rescues_total`.

| Metric | Labels | Counts |
| --- | --- | --- |
| `abfs.gcp.gc.rescues` | `call`, `type` | Marked objects rescued, by call: `headers`, `validation`, `put_object`, `get_object`, `map_results`, and by object type, `other` for a type the server does not know |
| `abfs.gcp.gc.rescues_lost` | `call` | Marked objects that the collector deleted before the call could rescue them |
| `abfs.gcp.gc.rescue_failures` | `call` | Rescues that failed, including those whose touch failed; each failed its call |
| `abfs.gcp.gc.touches` | `kind`, `result` | Touches of an `object` or a `map_row`, `written`, `dropped` because the buffer was full (80% full for the touches of `Headers` and `MapResults`, about half full for map rows), or `failed` |
| `abfs.gcp.cache.ttl_expiries` | `cache` | Cache lookups of `headers` or `content` that found an entry older than `--cache-ttl` |
| `abfs.gcp.gc.map_results_dropped` | | Map results left out because their object is missing |
| `abfs.gcp.gc.heartbeat_failures` | | Failed heartbeat writes |
| `abfs.gcp.gc.answer_seconds` | `call` | Histogram of the seconds from the start of a call that can answer that objects are present to its answer, by call: `headers`, `put_object`, `get_object`, `map_results`; a call of 2 minutes or more fails |

`gc run` sets three gauges once, at the end of its cycle, through the metrics provider of the command (`--kv metrics-provider=<name>`; none by default). Their names have no dots and no unit, so OTLP and a Prometheus endpoint export them under the same name, for example `prometheus.googleapis.com/abfs_gc_cycle_keys/gauge`. Each run is a new series.

| Metric | Labels | Value |
| --- | --- | --- |
| `abfs_gc_cycle_end_timestamp_seconds` | `mode`, `result` | The end of the cycle in seconds since the epoch; `result` is `ok`, `condemn_refused`, `delete_refused`, `delete_stopped`, `chunks_skipped` (status 3 to 6) or `error` (status 1, or a signal) |
| `abfs_gc_cycle_keys` | `mode`, `action` | `condemned`: `condemn.condemned`, in condemn and delete mode; `deleted`: `delete.step.deleted`, in delete mode |
| `abfs_gc_cycle_chunks_deleted` | `depot` | `bigtable`: `delete.chunks.chunk_rows_deleted`; `bucket`: `delete.chunks.bucket_objects_deleted`; delete mode only |

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

* Only servers with `--gc` write a heartbeat. A server started without `--gc` writes no row, so the collector cannot see it: it does not touch or rescue what its clients use, and the dry run gives no warning about it. The condemn and delete modes check the number of server hosts, which is how they see such a server. Delete mode also refuses when an object written during its consistency scan has no `abfsgc` row; objects written before show as missing `abfsgc` rows. A server without `--gc` that only reads, or writes objects that exist, leaves no trace.
* A condemned object read from a server's cache is not rescued, and neither are reads that never reach a server: cacheman's local content, signed URL reads within their 10 minutes, and ninjago's input checks from local attributes. Object touches that a server dropped or failed to write, and touches in its buffer when it crashed or was killed, are lost the same way. A lost touch refuses nothing: the next condemn can mark such an object, and a read of it during the hold rescues it.
* A pause of a server process under 10 seconds is not recorded.

Clients:

* A client that holds only a hash, not the content, and uses it more than `--grace`, a cycle and the [hold](#the-hold) after its last server read, about 7 to 8 days at the defaults and daily cycles, gets not found once the object is deleted.
* With `--accept-mounts-without-keepalive`, content that a mount without keep-alive wrote and its cache evicted is lost once deleted.
* A mount whose keep-alive did not touch for longer than about the hold less the cache TTL, 82 hours at the defaults, such as a workspace unmounted for days or a stopped cacheman, can lose content it wrote that its cache evicted.
* A cacheman start parks cached content the server lacks for 96 hours. A mount that holds such content and does not name it in a keep-alive pass within that time, such as a workspace unmounted across a cacheman restart, can lose it once its cache evicts it. The 96 hours are fixed and do not follow `--grace`: with a longer grace the limit above grows past them, and this one comes first.
* Cacheman can evict a blob between casfs storing it and cacheman indexing it; it counts this in `remote_checks.lost_before_index` with the hash. A write whose notification casfs dropped is never indexed and is not protected.
* Cacheman cannot push back a cached tree or eval response the server lacks when one of its children, at any depth, is in no local store, such as a child a lazy mount never read. It drops such a push at the first retry of that child once the child has been missing for 30 minutes, up to about two minutes after that, and counts this once per entry in `remote_checks.dropped`; the entry is parked as at a start, and the next keep-alive pass that names it pushes it again. It does not drop the push while an upload, a map result, a wait or a ref update needs the same child. While a mount is connected and the child stays missing, each keep-alive cycle can queue the push again, which then waits and is dropped again.
* The keep-alive does not touch files under `.git` and `.ctrl` directories.
* A cacheman restarted with `gc-keepalive` off under a live mount checks no local writes until the mount's next keep-alive tick, up to 15 minutes later, turns the mount's field false.
* A cache that reaches its eviction threshold during the start pass evicts entries the pass has not checked when it fills faster than the pass goes through its entries, or when the server cannot be reached; cacheman counts them in `remote_checks.evicted_unchecked`.
* A cache whose eviction finds no other entry left evicts parked entries, oldest park first, down to 80 % of its threshold, including content a mount has not named yet; cacheman counts them in `remote_checks.evicted_parked`.
* An entry a start finds missing after its park ended, and not used since, goes to the young end of the eviction order instead of a new park. When it is content a mount wrote that cacheman never indexed, the cache can still evict it if it turns over entirely before the mount's next keep-alive pass names it.
* A local write the server lacks that cacheman cannot push, because it is over the offline threshold or marked not to be pushed, or because cacheman is remote read-only, is not parked: it keeps its check marker and stays out of eviction for good, and each start asks the server about it again.
* After a server lost a map row touch, the outputs of a map result that a build applied up to 8 hours after a server returned it are kept by the hold until the next keep-alive pass names them, for a pass that reaches them up to the longest touch interval of the live mounts, a tenth and an hour after the answer. A longer keep-alive outage, such as a stopped cacheman or a server that cannot be reached, can lose them. Without a lost map row touch, the eval map row keeps them for `--map-retention`.
* Cacheman keeps map results for 8 hours for every mount it serves, and its start pass confirms entries before mounts connect, so a mount that starts after the snapshot of a delete cycle can apply a result while its touch interval does not count in that cycle's hold. The next cycle counts it; at the defaults any touch interval within the keep-alive limit fits the 53 hours by which the 88 hour hold exceeds what it needs.

Builds and records:

* `abfs rerun`, `abfs file-access` and `abfs export` on a build whose eval map rows are older than `--map-retention` fail on the missing eval responses, even while the build itself is kept by `--build-retention`.
* A map row touch that a server dropped or failed to write leaves the row's last use earlier, so an eval map row in use can expire earlier than `--map-retention` after its last use. A build then runs the step again.

Leaks, left for a later orphan sweep:

* A collector that lost its lease and deletes late can leave a row with only a condemn marker, or an `abfsgc` row without an object row.
* A map touch that a server wrote after the delete of its row writes the row again.
* An undo that failed after it kept the chunks of an object it did not finish leaves those chunks, unless it is run again.

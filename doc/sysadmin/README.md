# ABFS / System Admin

## High Level View

At its core, the ABFS remote service runs a Content-Addressable Storage (CAS) server. This server has all of the Android source code, build artifacts, and ABFS custom objects. We include the Android Source so that our ABFS objects can point to other valid objects in the CAS.

The source is pulled from your Gerrit (or other Git) host and pushed to the CAS server. Our component **Git-pusher** accomplishes this. It fetches new changes using git, pushes blobs, objects, refs, etc. to the CAS server, and sleeps before the next fetch. It supports horizontally scaling and is usually run with 3 pushers in the pool.

## First Time Setup

### Prerequisites

These instructions assume that you have installed ABFS on all of the machines you are working on. The client machine also needs CASFS installed. See the [main README doc](../../README.md) for installation instructions.

We suggest 5 machines total:

* 1 CAS server that needs a large disk. This will store a clone of your entire Android source history and also all build artifacts you produce. This initial release doesn’t contain any garbage collection features so this will need to be discarded when it is full. We run a 64 core VM with 512GB of RAM, but the CPU utilization is low and the minimum specs haven’t been tested here.  
* 3 git-pusher servers. We run 3 96 core VMs with 768GB of RAM, but again the CPU utilization is low. The initial run where they need to push your entire git history is more demanding than the maintenance running where only new changes are fetched and pushed. It’s possible to downsize the VMs after the initial push is complete, but the minimum has not been tested.  
* 1 client machine used for configuring and testing out the CAS server

<details>
<summary>CAS Server</summary>

The current release stores all source code, developer code changes, intermediate and build artifacts, etc. on a single server. This will need to be periodically wiped and started over since the contents are not garbage collected. The plan is to make this smoother in upcoming releases. There is a warning for developers in [WORKSPACES.md](../android-dev/WORKSPACES.md#not-for-permanent-source-control) that their changes should be merged or backed up to a different source control system since the ABFS server won't live forever.

Depending on your Android history, the full git-pusher source code run could take 2 TB or more. An initial full Android build can add around 150 GB with all intermediates, images, and zip files. If another ABFS build is done with the same exact source, it will take close to 0 GB incremental storage and just store some user metadata. As the source code changes, ABFS will deduplicate all of the intermediates that it can. But full builds on top of changed source code will still lead to changed images and zips. So you can still expect 25-40 GB of new data per full build that has source code changes.


</details>

### CAS server

Start the main CAS gRPC server with an existing backing directory.

To keep the CAS in Google Cloud (Spanner, Bigtable and Cloud Storage) instead of a local directory, see [GCP_SERVER.md](GCP_SERVER.md). Its garbage collection is described in [GCP_GC.md](GCP_GC.md).

> [!WARNING]
> This release is missing authentication and authorization. Anyone who has network access to the CAS server can read and write any data. Please ensure it is firewalled appropriately.

#### Choose a transport mode

You should choose a transport mode that satisfies your company's security requirements. You might choose to go without TLS if you are running everything inside the same isolated network. 

| | **With TLS** | **Without TLS** |
| --- | --- | --- |
| Traffic between clients, git-pushers, and CAS server | Encrypted and clients verify the server's identity | Plaintext |
| Setup | A server certificate (ideally from a CA your clients already trust) | No certificates |
| Client authentication/authorization | **None** (TLS doesn't identify clients) | **None** |

Based on your selection, you will need to provide different initialization instructions to your developers or anyone else using an ABFS client. So you should decide before onboarding clients because existing clients would all need to modify their configuration.

If you run a reverse proxy or other similar service that terminates TLS, you would still need to instruct clients to use the "With TLS" setup regardless of if you run TLS on the CAS server itself or not.

<details>
<summary>With TLS</summary>

**Get a server certificate**

ABFS clients will trust the operating system's CA store by default. If you use a server certificate that your client machines already trust, you won't need to distribute a certificate to all of the clients. Common sources:

* Your company's internal CA (if your clients already trust it)
* A public CA such as Let's Encrypt

If none of these are available, you can use a private CA and distribute a certificate to all of your clients.

Whichever option you use, the certificate must:

* Include the hostname that clients will use in their `--remote-servers` client configuration to connect to the CAS server. It's easiest to use a fully qualified name that all clients can use instead of a short name that only works within a specific network segment.
* Be a PEM file containing the server certificate followed by any intermediate certificates, plus a PEM private key.

**Start the CAS Server with TLS**

Fill in empty env vars and start the server:

```sh
TLS_CERT_FILE=
TLS_KEY_FILE=
SERVER_DIR=
mkdir -p "$SERVER_DIR"
# sample rate of 1 will log every request
LOG_SAMPLE_RATE=.01
# Can use any available port, 50051 is just an example
abfs server --port 50051 --log-sample-rate $LOG_SAMPLE_RATE --tls-cert-file "$TLS_CERT_FILE" --tls-key-file "$TLS_KEY_FILE" file-based --local-dir "$SERVER_DIR"
```

The output should include "TLS enabled." If you pass an empty `TLS_CERT_FILE` or `TLS_KEY_FILE`, it will fall back to running without TLS.

The server only reads certificates at startup so it needs to be restarted when the certificates are renewed.
</details>

<details>
<summary>Without TLS</summary>

> [!WARNING]
> This server has no TLS/authentication/authorization enabled

Fill in `SERVER_DIR` and start the server:

```sh
SERVER_DIR=
mkdir -p "$SERVER_DIR"
# sample rate of 1 will log every request
LOG_SAMPLE_RATE=.01
# Can use any available port, 50051 is just an example
abfs server --port 50051 --log-sample-rate $LOG_SAMPLE_RATE file-based --local-dir "$SERVER_DIR"
```

</details>

#### Health checks while serving

By default the server's gRPC health service reports `SERVING` for as long as the server runs, whatever the state of its storage. With `--health-probe-interval` the server probes its storage backend at that interval and reports `NOT_SERVING` for the overall status (service name `""`) and for the `abfs.ABFS` service while the backend fails, so that load balancers, Kubernetes readiness probes and other health checkers stop sending requests to it. Set it on every server behind a load balancer or watched by a health checker, together with `--drain-delay` (see below). It is a flag of `abfs server`, so it goes before the backend:

```sh
abfs server --port 50051 --health-probe-interval 10s --drain-delay 15s file-based --local-dir "$SERVER_DIR"
```

* A probe round runs the checks of the backend in parallel. It fails when a check returns an error or does not return within `--health-probe-timeout` (5s by default, not above the interval). A round still running when the next one is due counts as one more failure each interval, and its result is ignored when it comes. The server reports `NOT_SERVING` when `--health-probe-failures` (N, 3 by default) of the last 2N-1 rounds failed, and `SERVING` again after N rounds in a row passed. With the defaults and a 10s interval, the server reports `NOT_SERVING` 20 to 35 seconds after a backend failure starts, and `SERVING` again 20 to 30 seconds after it ends. A load balancer or readiness probe adds its own interval and thresholds on top.
* The file-based server checks that `--local-dir` is still the directory it started with, writes, syncs and removes a small file named `.abfs-health-probe` in it, and reads one stored object from disk. A full disk, a read-only remount, a permission change and a moved or unmounted directory fail the probe. The probe does not wait for the server's I/O limit, so a disk saturated by requests can fail it too.
* The probe of the GCP server only reads. A lost write permission or an exhausted write quota fails every upload while the probe passes.
* The in-memory server has no backend to probe. With the flag set it logs that once and keeps reporting `SERVING`.
* A backend outage shared by several servers marks all of them `NOT_SERVING` together. Any load balancer with no healthy backend then fails every request, and a Kubernetes Service whose pods are all not ready has no ready endpoints, so connections to it fail. For example, a Google Cloud regional internal Application Load Balancer answers HTTP 503 to every call and has no fail-open mode (see "Behavior when all backends are unhealthy" in the [health check concepts](https://docs.cloud.google.com/load-balancing/docs/health-check-concepts) and "Failover" in the [internal Application Load Balancer overview](https://docs.cloud.google.com/load-balancing/docs/l7-internal)). Running clients retry such calls under their retry policy; new client processes fail their connect check within 10 seconds with "error during health check". A failure of one backend takes the server out even when requests that do not use that backend would still work.
* With a single server and no load balancer, every new client process that does the connect check (the CLI, the `abfs mount` preflight, a git-pusher start) fails at connect with "not healthy, status was NOT_SERVING" while the backend is down. Cacheman and running mounts are not affected.
* The probe sets only the overall status and `abfs.ABFS`. A load balancer for proxied ARES traffic (`--ares-enabled`) should check the ARES service name, not the overall status.
* Two gauges follow the probe: `abfs_server_serving` is 1 while the server reports `SERVING` and 0 after the failure rule and from the shutdown signal on; `abfs_server_backend_up{backend}` is 1 for each backend whose check passed in the last round and 0 for one that failed or did not return in time. Every failed round logs its elapsed time and the error of each failed backend.
* On a shutdown signal the health service reports `NOT_SERVING` for good; the probe never sets it back to `SERVING`.

#### Stopping the server

The server stops gracefully on `SIGTERM` or `SIGINT` (Ctrl+C): it stops accepting connections, lets in-flight requests finish, closes its files and exits with status 0. Requests still running after `--shutdown-timeout` (25s by default) have their connections closed, and the server still exits with status 0, so a client that stopped reading or went away cannot keep the server from exiting. Size it from the time the largest transfer should be given to finish, and keep it below the time your service manager, container runtime or orchestrator waits before it kills the server. `--shutdown-timeout 0` waits for every request, however long it takes. It is a flag of `abfs server`, so it goes before the backend:

```sh
abfs server --port 50051 --shutdown-timeout 60s file-based --local-dir "$SERVER_DIR"
```

* The signal must reach the `abfs` process. If a wrapper script starts the server, start it with `exec` so that the server replaces the script.
* A repeated `SIGTERM` is ignored while the server shuts down. A `SIGINT` after the first signal makes it exit at once, without waiting for in-flight requests. Every command of the `abfs` binary (including cacheman and git-pusher) handles signals this way.
* With `--ares-enabled`, proxied ARES requests can run much longer than the default timeout; set `--shutdown-timeout` to match.
* On the first signal the server's gRPC health service reports `NOT_SERVING` for every service. Behind a load balancer or a health checker that uses it, set `--drain-delay` to keep serving new requests for that long before the server stops accepting connections, so the balancer moves traffic away first. Size it from the health check interval times the unhealthy threshold, and count it in the time your process manager waits: the delay comes before `--shutdown-timeout`. The default, `0`, starts the drain at once. A client that watches health with the streaming `Watch` call keeps its stream open until `--shutdown-timeout` closes it, so with `--shutdown-timeout 0` such a client keeps the server from exiting.

#### Server metrics

The server records the metrics below through the metrics provider that the `metrics-provider` config key selects, when `record-client-metrics` is `true`. Without a provider it records none of them.

* `abfs_server_requests_total{method,result}` counts requests by method and result.
* `abfs_server_requests_in_flight{method}` is the number of requests in progress.
* `abfs_server_headers_keys_total{result}` counts the keys that `Headers` looked up, with `result` `found` or `not_found`.
* `abfs_server_headers_batch_keys` is a histogram of the number of keys in each `Headers` call.
* `abfs_server_object_bytes_total{direction}` counts encoded object bytes, `in` for `PutObject` and `out` for `GetObject`.

The `result` of a request is `ok`, `not_found` (the object does not exist, or the ref's server is unknown), `missing_links` (the server does not have every object the request links to), `permission_denied`, `ref_mismatch` (`XchgRef` or `XchgRefs` found a ref at a value other than the expected one), `canceled` (the client canceled the request or its deadline passed) or `error` (any other error).

### Configuration

Some configuration of ABFS is still done through command-line flags, but we are trying to move everything to use configuration stored in the CAS itself for dynamic update purposes. We call this feature Hashpaths and you can read more in the [Hashpath code comments](../../shared/hashpath/README.md).

#### Set up a client

The bootstrapping process could use some improvements here, but you currently need an abfs client configured to write these hashpaths. So start by running `init` on your client machine to set up the background cache manager (cacheman) systemd service:

<details>
<summary>With TLS</summary>

If you used a private CA, also pass `--root-ca-file` with the absolute path to the certificate file before the `init` part of the command.

```sh
SERVER= # must match a name in the certificate
abfs --remote-servers $SERVER:50051 init
```

</details>
<details>
<summary>Without TLS</summary>

```sh
SERVER=
abfs --remote-servers $SERVER:50051 --disable-tls init
```

</details>

Ensure the below exits successfully, which shows that it has a connection to the CAS server. It prints `retrying...` while cacheman is still starting.

```sh
abfs cacheman remote-ok
# 0 exit code means connected
echo $?
```

If there are issues, you need to inspect `abfs cacheman tail` and debug.

#### Create required Hashpaths

This is run on the client you set up above.

Create the main config ref for storing hashpaths

```sh
abfs edit --add config:.
```

Replace the default values in the YAML below and then run this command to create the git-pusher config. Read about the many configuration options in the [git-pusher doc](../../gitpusher/README.md).

> [!WARNING]
> By default, git-pusher sleeps for 1 minute between runs of `git fetch` for every project in your manifest file. Ensure your Gerrit host can handle this increase in traffic.

* **port**: Git-pusher assumes it is running inside of a firewalled network, and will listen on the specified `port` below on all interfaces without TLS. Exclude the port and it won't listen on HTTP.
* **fetch-ref-spec**: By default, git-pusher will fetch and push all refs for every project in the provided manifest file. This is great if you want a complete history and the ability to mount based on any branch, tag, or change. But it isn't great if you only care about limited branches and minimizing disk usage by the CAS Server. See the comment in the YAML below if you want to include a **fetch-ref-spec** to limit the branches fetched and pushed.

```sh
abfs edit -a --stdin config:git-pusher/default << 'EOF'
name: default
pushers-count: 3 # replace with your count
defaults:
    port: 9998 # HTTP listening port for info and debug

pushers:
  - name: pusher-0 # each pusher's --instance name (defaults to `hostname` output)
  - name: pusher-1
  - name: pusher-2

repos:
  - manifest:
      url: https://android.googlesource.com/platform/manifest
      files:
        - branch: android-latest-release
          file: default.xml
    # uncomment the below 3 lines and use your manifest's default revision instead of "android17-release" if you only want limited branches pushed
    # project-defaults:
    #   fetch-ref-spec:
    #     - +refs/heads/android17-release:refs/heads/android17-release
EOF
```

### Git-pusher servers

This is run on each of the git-pusher servers.

#### Authentication with git

For authentication, git-pusher will use the git binary at `/usr/bin/git` (this can be changed with the `--git-bin` flag). This binary needs to be able to read your git repos via your global git configuration. So ensure you have set it up and run a test `git clone` to see if that works.

For example, to authenticate against a Gerrit instance as a Google user, you would visit the `/new-password` page e.g. https://android.googlesource.com/new-password. It then gives you a command to write a `~/.gitcookies` file and configures `git` to use it.

#### Run git-pusher

On each of your git-pusher servers, run the following:

<details>
<summary>With TLS</summary>

If you used a private CA, also pass `--root-ca-file` with the certificate path before the `git-pusher` part of the command.

```sh
PUSHER_DIR=
SERVER= # must match a name in the certificate
mkdir -p "$PUSHER_DIR"
# pass --instance <name> after `git-pusher` if you don't want to use the default of `hostname` output
# this will produce a lot of logs
abfs --remote-servers $SERVER:50051 git-pusher --storage-path $PUSHER_DIR
```

</details>
<details>
<summary>Without TLS</summary>

```sh
PUSHER_DIR=
SERVER=
mkdir -p "$PUSHER_DIR"
# pass --instance <name> after `git-pusher` if you don't want to use the default of `hostname` output
# this will produce a lot of logs
abfs --remote-servers $SERVER:50051 --disable-tls git-pusher --storage-path $PUSHER_DIR
```

</details>

#### Seed progress

If you collect metrics from the git-pushers (see Metrics below), you can follow the seed in your monitoring system as well as with the `/info` check under [First Mount](#first-mount). Each pusher reports `abfs_pusher_projects` for its own projects, labeled by push state and by `seeded` (pushed successfully at least once), and `abfs_pusher_pool_projects` for all projects in the pool. The seed progress is:

```promql
sum(abfs_pusher_projects{seeded="true"}) / max(abfs_pusher_pool_projects)
```

The denominator is the pool gauge because a pusher that has stopped also stops reporting, so a sum over the running pushers alone would look complete. A pusher is missing or has not reported yet when this is above 0:

```promql
max(abfs_pusher_pool_projects) - sum(abfs_pusher_projects)
```

The seed is ready when all of these hold. The first line rules out a pool whose manifests are not listed yet.

```promql
max(abfs_pusher_pool_projects) > 0
sum(abfs_pusher_projects{seeded="true"}) == max(abfs_pusher_pool_projects)
sum(abfs_pusher_projects_failing) == 0
sum(abfs_pusher_repos{listed="false"}) == 0
```

### Default Client Configuration

This is run on the client you set up above.

Every client running abfs has their own configuration file. This is written by their initial `abfs init` and can be viewed and edited with `abfs config` flags and subcommands. However, most clients connected to your CAS server should be using the same configuration, so we have this feature to manage most of the configuration centrally.

Don't put `disable-tls` or `root-ca-file` into this central configuration. Clients must already set these flags with correct values locally to be able to connect to the server and download this config.

Replace the `manifest-server:` and any other needed values below for your setup. On your client machine, run the following:

```sh
abfs edit --add --stdin config:clients/configs << 'EOF'
# Applies to all clients
- select:
  config:
    manifest-scheme: https
    # replace with your gerrit host if not using AOSP
    manifest-server: android.googlesource.com
    manifest-project-name: platform/manifest
    git-prefetching: false
    concurrent-uploads: 200
    sync.auto-setup: true
    force-workspaces: true
    setup.psi-mem: 0
    # shouldn't be needed with 26Q4 release when build/soong/soong.environment.available is more deterministic
    ignore-soong-env-avail: true
EOF
```

`sync.auto-setup` makes every mount, remount and `repo sync` run `abfs setup`, which installs
the ABFS build hooks in each Android tree of the workspace. If your manifest puts Android in a
subdirectory or holds more than one Android tree, read [Manifests and tree layouts](MANIFESTS.md).

This can always be changed later interactively with `abfs edit config:clients/configs`.

If your manifest layers lists with `remove-project`, `extend-project` or include groups, or
your builds mount another entry file than `default.xml`, see
[Manifests and tree layouts](MANIFESTS.md) for the client and git-pusher setup.

### When the server is unavailable

Clients wait for a server that is stopped or restarting, or whose address does not answer, so a server restart does not fail the builds, mounted workspaces and git-pushers that use it:

* Calls to the server keep trying for up to `server-wait-timeout` (1 minute by default), counted from the first failed call to that server. After that, calls fail with `server unreachable for ...`, without trying while gRPC knows the server is down, until the server answers again. Set it with `--server-wait-timeout` or in the client configuration; `0` makes calls fail after gRPC's own retries, about 7 seconds.
* `server-wait-timeout` is kept below `fetch-timeout` (2 minutes by default); a larger value is reduced to half of `fetch-timeout`. To wait longer, raise both.
* A download that is cut continues where it stopped once the server is back. The server sends the object again from its start and the client skips what it already has, which takes time for very large objects.
* An upload that cannot start waits like any other call. An upload that is cut while it runs starts over: cacheman sends it again with a pause that grows after each failure, up to 10 minutes, so `abfs cacheman wait` can take that long after the server is back; git-pusher sends it again on its next cycle, every minute by default; a command that uploads directly, such as `abfs --no-local-cache push`, fails and has to be run again.
* In a mounted workspace, reads of files that are not cached yet wait for the server. After `server-wait-timeout` they fail with an I/O error until the server is back. Files already cached can still be read, and writes are not affected.
* After the server is back, clients reconnect within about 5 seconds. When the server's address drops packets instead of refusing connections, for example while its machine restarts, each attempt to connect takes up to 20 seconds, and noticing the server's return takes up to about 25 seconds.
* Clients notice a server that is gone when its connections are closed or refused, as when the server process stops or its machine restarts. They also notice a server that stops answering without closing them, as when its machine crashes or loses its network or the server process hangs: a client sends gRPC keepalive pings while a call is open and nothing has arrived from the server for 30 seconds, and closes the connection when a ping gets no answer within 20 seconds or, on Linux, when data it sent stays unacknowledged for 20 seconds. Calls then wait for the server as described above, so they fail about 1.5 to 2.5 minutes after the server stopped answering. A client whose own address changes, for example a laptop that moves to another network, also closes its connection after about 20 seconds and connects again from the new address. A network stall of more than 20 seconds with data in flight also closes the connection.
* A call that is waiting when the server comes back at the same address after more than `server-wait-timeout`, for example after its machine restarted, fails. Without pings it waited until the restarted server reset the old connection, often several minutes, and then completed.
* The 20 second limit uses a Linux socket option on the client's connection. On other systems, and with `--tunnel-ports`, where that connection goes to a local SSH tunnel that always acknowledges data, a silent server is noticed by the pings, within about 50 seconds.
* Upgrade servers before clients. The server accepts pings as often as every 10 seconds; older servers accept one every 5 minutes. When such a server writes nothing on an open call for about 1.5 to 2 minutes, it closes the connection with `too_many_pings` and the call fails or is sent again (with ARES, a long proxied `Execute` up to 5 times), and each time the client halves how often it pings that server, until the client restarts.
* A server whose process runs but whose request handlers are stuck still answers pings, so calls to it are not ended early. With `--ares-enabled`, the connections from the CAS server to ARES send no pings.
* `abfs mount` and other commands that check the server when they start fail while it is unreachable, also within `server-wait-timeout`. cacheman starts without the server and connects once it is back.
* If cacheman itself restarts, the mount refuses writes until cacheman runs again, whether or not the server is reachable.
* With several `--remote-servers`, objects are spread over the servers and an unreachable server affects only its share of them. All refs are kept on one of the servers, so ref calls wait for that one.

### Push repo

This is run on the client you set up above.

To enable faster source control operations, ABFS requires a slightly patched version of the [`repo` tool](https://gerrit.googlesource.com/git-repo). The patches enable `repo` to use the content-addressable feature of ABFS to speed up some operations. We store the patched version of `repo` inside the ABFS CAS server so that it can be cloned when developers mount a workspace. This will guide you through the initial seeding steps to patch `repo` and push it into the CAS.

1. Clone `repo` and apply the ABFS patches to it:

    ```sh
    abfs repo-setup
    ```

    It prints the directory of the patched clone, e.g. `Repository patched successfully in /tmp/repo-setup-1234567890`, followed by the commands for the next step.

2. Enter the `REPO_DIR` value below. Then push the patched clone to the CAS server and point `stable-abfs` at it:

    ```sh
    REPO_DIR= # the directory printed by abfs repo-setup
    abfs --no-local-cache push -r "$REPO_DIR"
    echo "$(git -C "$REPO_DIR" rev-parse HEAD) refs/heads/stable-abfs" | abfs test-client put-ref -s gerrit.googlesource.com -p git-repo
    ```

3. Check that the ref is there. This prints the commit hash followed by `refs/heads/stable-abfs`:

    ```sh
    echo refs/heads/stable-abfs | abfs test-client get-ref -s gerrit.googlesource.com -p git-repo
    ```

To update `repo` later, repeat these steps.

[Manifests and tree layouts](MANIFESTS.md) lists the manifest elements ABFS applies and the
known limits of `repo sync` and `repo-hooks` inside a mount.

### Metrics

Commands of the `abfs` binary, such as the server, git-pusher and cacheman, export the metrics they record with OpenTelemetry. So does the standalone `cacheman` binary that `make cacheman` and `make cacheman-static` build. Export is off by default. Two config keys turn it on:

* `metrics-provider: otel` selects the OpenTelemetry backend.
* `record-client-metrics: true` turns recording on for every command, the server and git-pusher included. Without it nothing is exported.

Set them for one command with `--kv`, a global flag that goes before the subcommand, or write them to the instance config file with `abfs config -w`:

```sh
abfs --kv metrics-provider=otel --kv record-client-metrics=true cacheman run
abfs --kv metrics-provider=otel --kv record-client-metrics=true config -w
```

`OTEL_METRICS_EXPORTER` chooses the exporter:

* `otlp` (the default) sends OTLP to a collector. `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` or `OTEL_EXPORTER_OTLP_PROTOCOL` sets the protocol, `http/protobuf` (the default) or `grpc`.
* `prometheus` serves `/metrics` for scraping on `OTEL_EXPORTER_PROMETHEUS_HOST` (default `localhost`) and `OTEL_EXPORTER_PROMETHEUS_PORT` (default `9464`). It serves values only while the process runs.
* `console` writes JSON to standard output, which is useful to check the setup.
* `none` exports nothing.

A comma-separated list combines them, for example `OTEL_METRICS_EXPORTER=otlp,prometheus` to push to a collector and serve `/metrics` at once. In a list, `OTEL_METRICS_PRODUCERS` does not apply, and a name listed twice or a name other than these four turns export off with a warning.

The other standard `OTEL_*` variables apply as well, for example `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_METRIC_EXPORT_INTERVAL` (in milliseconds, 60000 by default), `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`:

```sh
OTEL_METRICS_EXPORTER=console OTEL_METRIC_EXPORT_INTERVAL=5000 \
  abfs --kv metrics-provider=otel --kv record-client-metrics=true cacheman run
```

Each process describes itself with the resource attributes `service.name` (`abfs`), `service.version`, `service.instance.id` (the host name), `abfs.instance` (the ABFS instance name), `abfs.runID` (new for each process) and `abfs.username` (a hash of the user name), plus the SDK's `telemetry.sdk.name`, `telemetry.sdk.language` and `telemetry.sdk.version`. `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` override any of ours and add others, such as `cloud.region`. A collector can also add the attributes of the platform it runs on.

Things to know:

* Each command waits up to 15 seconds at exit for its final export. With `otlp`, that export comes at least 5 seconds after the previous export, since some backends reject a point less than 5 seconds after the previous point of the same series. A command can take up to 5 seconds longer to exit. If an export still in flight leaves too little time, the final export is skipped with a warning.
* `OTEL_METRICS_PRODUCERS` does not apply to `otlp`.
* The server records `rpc.server.call.duration` for each gRPC method and status code, in the gRPC handler that also traces its calls. Its buckets run from 50 microseconds to 1 hour. gRPC health checks and reflection are not recorded.
* The server makes its meter provider, and its tracer provider when it traces, the global OpenTelemetry providers, so libraries it uses that record through the global providers export the same way. Other commands set no global provider.
* Each process reports as the instance named by its host name. Two processes on one host that write the same metric with the same labels share a series in a backend that identifies a series by `service.name`, `service.instance.id` and the metric labels. `abfs.runID` is not part of that key.

The cacheman service that `abfs init` or `cacheman init` sets up runs under `systemd` and does not see `OTEL_*` variables set in a shell. Without them it exports with the defaults: OTLP over HTTP with TLS to `https://localhost:4318`. Set them for the service in a drop-in, then restart it (replace `default` with the instance name). The drop-in survives a later `abfs init`, which rewrites only the unit file:

```sh
systemctl --user edit cacheman@default
abfs cacheman restart
```

```ini
[Service]
Environment=OTEL_EXPORTER_OTLP_PROTOCOL=grpc
Environment=OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

`systemctl --user edit` needs a terminal. A script writes the same lines to `~/.config/systemd/user/cacheman@default.service.d/override.conf`; `abfs cacheman restart` reloads the unit files itself.

### Traces

Commands of the `abfs` binary trace with OpenTelemetry when `OTEL_TRACES_EXPORTER` is set to an exporter other than `none`. Without it no tracer is set up and nothing changes. Tracing does not depend on `metrics-provider` or `record-client-metrics`. Set the variables for each process that should trace: a server records spans only for the calls of a client that traces too. cacheman started by `systemd` does not see variables set in a shell, so set them in its drop-in (see Metrics above).

`OTEL_TRACES_EXPORTER` chooses the exporter:

* `otlp` sends OTLP to a collector. `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` or `OTEL_EXPORTER_OTLP_PROTOCOL` sets the protocol, `http/protobuf` (the default) or `grpc`, and `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` the address.
* `console` writes spans as JSON to standard output, which is useful to check the setup.

```sh
OTEL_TRACES_EXPORTER=otlp OTEL_EXPORTER_OTLP_PROTOCOL=grpc OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  abfs git-pusher
```

Sampling follows the caller. When `OTEL_TRACES_SAMPLER` is empty:

* git-pusher starts a trace for the first push of each project after it starts, and for the later pushes at the ratio in `OTEL_TRACES_SAMPLER_ARG` (`0.01` by default). With the default push interval of 60 seconds every project is pushed again each minute, so a ratio of `1` makes a trace per project per minute.
* All other processes, the server included, start no traces. They record a call only when the caller sent a sampled W3C `traceparent` header.

`OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` replace this default with a standard sampler. A server with `parentbased_always_on` records every call of every client that does not trace. An `OTEL_TRACES_SAMPLER` value that is not a standard sampler name logs a warning and keeps the default.

Only git-pusher starts traces, so set the variables for git-pusher and the server. On cacheman and the CLI they cost a little on every call and record nothing.

A push trace has a `push` span with the `abfs.project` and `abfs.push.first` attributes. Its children are one span per push state, named like the state: `created` (the first push only), `fetch`, `diff`, `push-blobs`, `push-objects` and `push-packs`, with the child spans `missing-hashes`, `lfs` and `tree-helpers`. The calls `Headers`, `GetRefs` and `PutRefs` have a client span and a server span. The per-object calls `PutObject`, `GetObject`, `AddMapResult` and `MapResults` have no span and send no `traceparent`, so a push of millions of objects stays a trace of tens of spans. The state spans carry the object, blob and ref counts, plus `abfs.process.remote_wait_seconds` (time waited for `max-remote-pushes`) and `abfs.process.remote_bytes_sent`; these two count all pushes that run in the process at the same time.

Things to know:

* Spans carry the same resource attributes as metrics.
* gRPC health checks and reflection are not traced.
* At exit, spans flush in parallel with metrics, within the same 15 seconds. A push that is still running at exit loses its spans.
* Spans are dropped without a log when the export queue is full (`OTEL_BSP_MAX_QUEUE_SIZE`, 2048 by default).
* With a sampler that records cacheman's calls, a successful `GetObject` client span ends with status `CANCELLED` and lasts until the reader is closed.

## First Mount

It could take a few days for the push to complete depending on the size of your git history and the number and size of your git-pusher servers. `abfs ready` tells you when a manifest branch can be mounted. It reads the manifest from the ABFS server, resolves every project the same way `abfs mount` does, and lists the projects that are still missing:

```sh
abfs ready -b android-latest-release
```

It exits 0 when every project resolves, 1 when something is still missing and 2 on other errors. Add `--wait` to check again every 30 seconds until the branch is ready or `--timeout` (one hour by default) runs out, and `--json` for a report that scripts can read.

For each missing project it reads the git-pusher config (the same `config:git-pusher/default` hash-path as git-pusher, or `--pusher-config`) and the project's push status, and gives the cause:

| Cause | Meaning |
|---|---|
| `not pushed yet` | git-pusher has not pushed this revision yet; the detail gives the last push it reported for the project, which can lag by up to 20 minutes |
| `last push failed` | the last push of the project failed; the detail has the error |
| `manifest not in the git-pusher config` | no `repos` entry has this manifest |
| `branch not in the git-pusher config` | no `manifest.files` entry of that repo has this file and the branch, or a branch pattern that matches it, and git-pusher has not pushed the project for another file yet |
| `excluded by the git-pusher config` | an override with `exclude: true` matches the project |
| `invalid fetch-ref-spec in the git-pusher config` | git-pusher rejects the project's `fetch-ref-spec`, so it pushes nothing of that repo |
| `outside the git-pusher fetch-ref-spec` | the project's `fetch-ref-spec` does not fetch this revision |

The config causes only go away when the config changes. When nothing else in the config could still push the revision, the report marks the cause as blocked (`"blocked": true` in `--json`) and `--wait` stops at once. That is not the case when the config has more than one repo or a `projects` entry for the project, when the branch is not in `manifest.files` (another file may list the project), or for a revision that could be a tag (git also fetches tags that point into fetched branches).

Once it reports ready, mount the branch:

```sh
MOUNT=mnt
mkdir $MOUNT
abfs mount --branch android-latest-release --tmp-ws $MOUNT
```

A failed mount unmounts itself. If its error says that the unmount failed too, unmount it before you try again:

```sh
abfs unmount -k $MOUNT
```

### Mounting from CI

A manifest branch keeps moving while you build. To build exactly what was checked, write the pinned manifest with `-o` and mount that file:

```sh
abfs ready -b android-latest-release --wait --timeout 2h -o pinned.xml
abfs mount -b android-latest-release -f pinned.xml --tmp-ws $MOUNT
```

Some things to know:

- `abfs ready` writes `pinned.xml` only when the branch is ready, and removes it when not, so a later step cannot mount an old file.
- Pass the same `-b` and `-g` to both commands, and do not pass `--local-manifest-dir` with a pinned file.
- If the branch uses a manifest file other than `default.xml`, pass it to `abfs ready` with `--branch-file` and mount the pinned file. Without `-f` or `-h`, `abfs mount` reads the file that `--branch-file` names (default `default.xml`), with or without `--tmp-ws` and `--ws`; with `-f` or `-h` it reads the pinned manifest and ignores `--branch-file`.
- A ref can resolve before git-pusher has written its deep tree helper objects. The mount then builds them itself, which is slower.

`abfs ready` can report a branch as not ready while a mount would still work in a few cases:

- It takes `-b` as a branch name under `refs/heads/`. Tags and commit hashes are not accepted.
- `abfs mount --fallback-to-git` clones projects that are missing from ABFS with git.
- With `--tmp-ws` or `--ws`, a group filter that names a group both ways (`-g -tools,tools`) selects differently.

### Release branches

To push new release branches without a config change, list them in `manifest.files` with a branch pattern. A pattern has one `*`, as in a git refspec, and `*` also matches `/`. Add one entry per manifest file name:

```yaml
repos:
  - manifest:
      url: https://android.googlesource.com/platform/manifest
      files:
        - branch: main
          file: default.xml
        - branch: release/*
          file: default.xml
        - branch: release/*
          file: release.xml # for release branches whose manifest has another name
```

Every `check-interval` (60 seconds by default) each git-pusher fetches every branch of the manifest repository and reads the file on each matching branch that has it. When a release branch is created:

1. Within one `check-interval`, git-pusher adds the projects that only the new branch's manifest lists and starts pushing them; the first push of a project takes as long as its history. Projects it already pushes fetch the new branch on their next run (`push-interval`, 60 seconds by default) if their `fetch-ref-spec` covers it.
2. Wait for the branch, pin it and mount the pinned file:

   ```sh
   abfs ready -b release/2026-11 --branch-file release.xml --wait --timeout 2h -o pinned.xml
   abfs mount -b release/2026-11 -f pinned.xml --tmp-ws $MOUNT
   ```

Some things to know:

- A narrowed `fetch-ref-spec` has to cover the release branches too, for the projects and for the manifest project, which also gets `project-defaults`. Otherwise `abfs ready` reports `outside the git-pusher fetch-ref-spec`.
- A pattern pushes the projects of every branch it matches, and each check reads the manifest of every matching branch. Keep patterns to the branches you build.
- A deleted branch, a missing file or a manifest that fails to parse is skipped: the projects that only it listed stop being pushed, and what is already on the server stays. The other files are read as before. `Skipped` in `/info` lists them per repo.
- When two files list one project with different URLs, the first file in `files` decides, and the other URL is listed in `Skipped`.
- A change to `files` takes effect without restarting git-pusher or any project's pusher. A change to `project-defaults` or to an override restarts the pushers of the projects whose fetch settings it changes.

### git-pusher status

If you want more details, each git-pusher exposes an HTTP `/info` endpoint that returns JSON on the port we set in the git-pusher config above. Each git-pusher reports only its own share of the projects. You can use `jq` (`sudo apt-get install jq` or see https://jqlang.org/download/) to summarize the project info with something like below run on **each** of the git-pushers:

```sh
curl -s localhost:9998/info | jq '{
  total:   (.AllProjects | length),
  done:    ([.Projects[] | select(.State=="sleep" and .LastError=="")] | length),
  failed:  [.Projects[] | select(.LastError!="") | {Name, LastError}],
  repos:   [.Repos[] | {Name, State, LastSuccess, LastError, Skipped}]
}'
```

`failed` lists the projects whose last push failed on that git-pusher, with the error.

Build hosts and CI jobs that share a cache should mount the workspace at the same absolute path, or they share fewer cache entries and can replay links into another host's workspace. See [Mount at the same path on every host](../android-dev/WORKSPACES.md#mount-at-the-same-path-on-every-host).

Once your mount succeeds, you are ready to move to the [Android Developer docs](../android-dev/README.md) and try out a build before sharing with your other developers.

## Client cache

Each client keeps its objects in a local cache directory managed by cacheman. Two mechanisms keep it bounded.

**Eviction.** Objects that the CAS server also holds are tracked in an LRU. When the LRU passes 90% of `evict-threshold` (default 1 TiB, `0` turns eviction off), cacheman removes the least recently used objects until it is at 80%, in batches of up to 10,000 objects. Eviction starts on index writes, after uploads and at the maintenance pass every 3 hours. An evicted object is fetched from the server again when it is read. Set the threshold below the size of the cache disk and leave room for local-only objects (below). Global flags go before the subcommand:

```sh
abfs --evict-threshold 500GB config -w
```

**Local-only objects.** Objects that never reached the server are not evicted, since the client could not get them back. Most build outputs are uploaded together with the cache entry of their step, but some stay local: outputs of failed steps, of steps that run without tapping, and files hashed outside ninja steps. When no mount has been connected to the cache directory for 15 seconds, cacheman removes local-only files that were not written or kept alive in the last hour, except files that a cache entry from the last 8 hours refers to. It waits until pending uploads are done and the server is reachable, and pauses while a mount is connected. After cacheman starts it first waits 60 seconds, and it runs once each time the last mount goes away, or once an hour while nothing is mounted. Reading a file does not count as keeping it alive. Set `idle-local-gc: false` to turn this off. Clients in `remote-read-only` mode clean up from the mount every 15 minutes instead, and `local-only` clients are not cleaned up.

Limits:

* Trees and other objects (two per ninjago step) are not removed.
* A cache directory shared by builds that never leave a 15 second gap is not cleaned up.
* A workspace snapshot that never reached the server loses its local-only files once it is unmounted. Mounting it again by hash still works, but reading those files fails with "No such file or directory".

`abfs cacheman stats` shows the cache. Total Size is everything in the index and LRU Size is the evictable part, so the difference is roughly the local-only data. Mounts is the number of mounts connected. The command keeps updating until you stop it.

To start over, stop cacheman, remove the whole cache directory and run `abfs init` again. `abfs cacheman clean` removes single parts of the cache.

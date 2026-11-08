# ABFS on GKE

This folder deploys the open-source ABFS GCP server, the git-pushers and the CI build node setup on GKE Standard. Google Cloud resources come from Config Connector (KCC), workloads from a Helm chart.

> [!WARNING]
> The server has no authentication and no TLS by default. Any host that reaches the Gateway address can read and write the whole store. The network is the only boundary. See [Security](docs/runbook.md#security).

## What it deploys

```
                       VPC, one region
  abfs-ci namespace (PSA privileged)       abfs namespace (PSA baseline)
  +-------------------------------+        +-------------------------------+
  | ComputeClass abfs-ci          |        | ComputeClass abfs-server      |
  |  Ubuntu nodes, any zone       |  h2c   |  abfs-server Deployment, HPA  |---> Spanner (refs)
  |  casfs-loader DaemonSet       | -----> |  zone of the Bigtable cluster |---> Bigtable (objects)
  |  cacheman DaemonSet (hostPath)|  VIP   |  behind Gateway gke-l7-rilb   |---> Cloud Storage (blobs)
  |  build pods                   |        +-------------------------------+
  +-------------------------------+        | abfs-pusher-seed / -steady    |
                                           |  git-pusher StatefulSet (PVC) |<--- Gerrit
                                           |  bootstrap Job (config in CAS)|
                                           +-------------------------------+
```

- The server (`abfs-server`) keeps refs in Spanner, objects in Bigtable and large blobs in a Cloud Storage bucket. It is the only workload with Google credentials (Workload Identity).
- An internal Gateway (`gke-l7-rilb`) gives the servers one regional address. Pushers, cacheman and build pods use it.
- The git-pushers fetch from Gerrit and push to the server. Each pusher has one PersistentVolumeClaim; nothing else in the deployment has one.
- A Helm hook Job writes the pusher pool config and the central client config into the store.
- On each CI node a DaemonSet builds and loads the casfs kernel module, and one cacheman serves every build pod on the node from a cache on the boot disk.
- Four ComputeClasses (`abfs-ci`, `abfs-server`, `abfs-pusher-seed`, `abfs-pusher-steady`) let GKE create the nodes, with a fallback over four machine families.

## Layout

```
deploy/gke/
  README.md          this file
  Makefile           lint, template, kustomize, ddl, ddl-check, images, bundle
  chart/abfs/        Helm chart; values.yaml, values-example.yaml, ci/ test values
  kcc/
    base/            namespace, APIs, service accounts, Workload Identity, Gateway address
    components/
      backend-new/       Spanner, Bigtable, tables, bucket and the server's roles
      backend-existing/  the server's roles on a backend that already exists
      gc/                the garbage collector's Bigtable parts, with backend-new, only when GC is on
      network/proxy-subnet/  the proxy-only subnet, for a network without one
      settings/          fills the base from the overlay's settings.yaml
    overlays/example/  a worked example
  images/            Dockerfiles of abfs-server, abfs-pusher, abfs-client, casfs-loader
  scripts/           gen-ddl.sh, check-ddl.sh (with ddl-lib.sh), build-images.sh, bundle.sh, load-images.sh
  docs/              usage, prerequisites, runbook, configuration, troubleshooting, ci-job
```

## Quick start

1. Prepare the cluster, the network, Config Connector (installed by hand from its operator bundle at a pinned version, 1.130.1 or later, not the GKE add-on) and its identity: [prerequisites](docs/prerequisites.md).
2. Put the images into your Artifact Registry. From a release bundle, copy its image archives and keep the printed digest values:

   ```sh
   scripts/load-images.sh europe-west4-docker.pkg.dev/<project>/abfs >values-images.yaml
   ```

   From a source tree, build and push them instead, and keep `images.txt`:

   ```sh
   scripts/build-images.sh --casfs <abfs-casfs repo> --push \
     europe-west4-docker.pkg.dev/<project>/abfs <commit>
   ```

3. Copy `kcc/overlays/example`, set `settings.yaml`, pick one backend component, and apply it. Wait until every object is Ready:

   ```sh
   kubectl apply -k kcc/overlays/<name>
   kubectl -n abfs wait --for=condition=Ready --all gcp --timeout=30m
   ```

4. Copy `chart/abfs/values-example.yaml`, set the digests (or pass `-f values-images.yaml` after your values file), `gateway.vip` and the Gerrit manifest, create the Gerrit Secret, and install without `--wait`:

   ```sh
   helm install abfs chart/abfs -n abfs -f <values.yaml> --timeout 30m
   helm test abfs -n abfs
   ```

5. On a new store, check that the bootstrap Job seeded the patched `repo` tool, which every `mount -b` needs: [runbook](docs/runbook.md#6-check-the-patched-repo-tool).

The [runbook](docs/runbook.md) has every step with its checks, and the seed, steady state, upgrades and teardown.

## Documents

- [usage.md](docs/usage.md): how ABFS is meant to be used here: the parts, how a build uses the store and the node cache, the first days and steady state, scaling and upgrades. Read it first.
- [prerequisites.md](docs/prerequisites.md): cluster settings, network, identities, the IOPS fallback node pool.
- [runbook.md](docs/runbook.md): install order, seed and steady phase, upgrades, cache reset, backend replacement, teardown, security.
- [configuration.md](docs/configuration.md): every chart value with its default, the KCC to chart name table, right-sizing.
- [ci-job.md](docs/ci-job.md): the build pod contract for any CI system, the example Job, Airflow, node sizing.
- [troubleshooting.md](docs/troubleshooting.md): known failures and their causes.

## Checks

The Makefile runs the offline checks. They need `helm` and `kubectl`; a schema source given as a commit also needs `git`:

```sh
make check        # lint, template, kustomize, ddl-check against the hash in the ddl file
make check DDL_REPO=<abfs repo> DDL_SOURCE=<commit>   # ddl-check also against the GCP server schema
make ddl DDL_REPO=<abfs repo> DDL_SOURCE=<commit>     # regenerate the ddl list into out/ and compare it
make images CASFS=<abfs-casfs repo> COMMIT=<commit>   # local build, no push; needs docker
make bundle CASFS=<abfs-casfs repo> COMMIT=<commit>  # after make images; needs docker
```

The release bundle (`scripts/bundle.sh`) holds this README, the Makefile, `chart/`, `kcc/`, `docs/`, `scripts/`, the four images as OCI archives, `images.txt`, the source of the ABFS and abfs-casfs commits in `source/`, `SHA256SUMS` and the commit id ([runbook](docs/runbook.md#bundle-and-source-tree)).

## Limits

- GKE Standard only. Autopilot does not allow the privileged CI pods.
- CI nodes run Ubuntu and build casfs on each node. The casfs commit in the image does not build on kernel series 6.14 and 6.19.
- No TLS, no authentication, no garbage collection by default: the server's `--gc` is off, and the backend gets no garbage collector tables until it is turned on ([runbook](docs/runbook.md#garbage-collection)).
- Terraform for Compute Engine VMs, Cloud Workstations and local SSD caches are not part of this folder.

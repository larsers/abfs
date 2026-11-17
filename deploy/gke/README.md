# ABFS on GKE

This folder deploys the open-source ABFS GCP server, the git-pushers and the CI build node setup on GKE Standard. Google Cloud resources come from Config Connector (KCC), workloads from a Helm chart. Coding agents start at [AGENTS.md](AGENTS.md).

> [!WARNING]
> The server has no authentication and no TLS by default. Any host that reaches the Gateway address can read and write the whole store. The network is the only boundary. See [Security](docs/runbook.md#security).

## What it deploys

```
                       VPC, one region
  abfs-ci namespace (PSA privileged)       abfs namespace (PSA baseline)
  +-------------------------------+        +-------------------------------+
  | ComputeClass abfs-ci          |        | ComputeClass abfs-server      |
  |  COS nodes, any zone          |  h2c   |  abfs-server Deployment, HPA  |---> Spanner (refs)
  |  casfs-loader DaemonSet       | -----> |  zone of the Bigtable cluster |---> Bigtable (objects)
  |  cacheman DaemonSet (hostPath)|  VIP   |  behind Gateway gke-l7-rilb   |---> Cloud Storage (blobs)
  |  build pods                   |        +-------------------------------+
  +-------------------------------+        | abfs-pusher-seed / -steady    |
                                           |  git-pusher StatefulSet (PVC) |<--- Gerrit
                                           |  bootstrap Job (config in CAS)|
                                           +-------------------------------+
```

- `abfs-server` keeps refs in Spanner, objects in Bigtable and large blobs in Cloud Storage. It is the only workload with Google credentials (Workload Identity).
- An internal Gateway (`gke-l7-rilb`) gives the servers one regional address for pushers, cacheman and build pods.
- The git-pushers fetch from Gerrit and push to the server. Each has one PersistentVolumeClaim.
- A Helm hook Job writes the pusher pool config and the client config into the store.
- On each CI node a DaemonSet loads the casfs module of the COS node image, and one cacheman serves every build pod from a cache on the boot disk.
- Four ComputeClasses (`abfs-ci`, `abfs-server`, `abfs-pusher-seed`, `abfs-pusher-steady`) let GKE create the nodes.

## Prerequisites

Details in [prerequisites.md](docs/prerequisites.md); the install order in [install.md](docs/install.md).

- GKE Standard with a regional control plane. Autopilot does not allow the privileged CI pods.
- A GKE version whose COS image ships casfs 0.1.24 or later: COS 133 from build 44-193, first in GKE 1.37.2-gke.1325000 on the Rapid channel ([prerequisites](docs/prerequisites.md#gke-version-for-the-ci-nodes)). `ci.casfs.provider: daemonset` instead runs Ubuntu CI nodes that build casfs on each node; it does not build on kernel series 6.14 and 6.19, and cannot run under `constraints/compute.requireShieldedVm` ([prerequisites](docs/prerequisites.md#secure-boot-on-the-ci-nodes)).
- Config Connector installed by hand from its operator bundle, 1.130.1 or later, not the GKE add-on.
- A VPC with a proxy-only subnet. With private nodes: a Private Service Connect endpoint for Google APIs with its DNS zones, Private Google Access and Cloud NAT.
- Control plane access through the DNS-based endpoint only.
- Spanner, Bigtable and Cloud Storage, new or existing; an Artifact Registry repository for the four images.
- Tools: `gcloud`, `kubectl` with kustomize, Helm 4, `skopeo`, `jq`, `make`; `docker` for the backend check and image builds.
- Garbage collection is off by default ([runbook](docs/runbook.md#garbage-collection)). Terraform for Compute Engine VMs, Cloud Workstations and local SSD caches are not part of this folder.

## Checks

The offline checks need `helm` and `kubectl`; a schema source given as a commit also needs `git`:

```sh
make check        # lint, template, kustomize, ddl-check against the hash in the ddl file
make check DDL_REPO=<abfs repo> DDL_SOURCE=<commit>   # ddl-check also against the GCP server schema
make ddl DDL_REPO=<abfs repo> DDL_SOURCE=<commit>     # regenerate the ddl list into out/ and compare it
make images CASFS=<abfs-casfs repo> COMMIT=<commit>   # local build, no push; needs docker
make bundle CASFS=<abfs-casfs repo> COMMIT=<commit>  # after make images; needs docker
```

From a source tree, build the images instead of loading them ([runbook](docs/runbook.md#bundle-and-source-tree)).

## Documents

- [install.md](docs/install.md): the install in one order, every command and check, the tests, and the seed of your manifest.
- [usage.md](docs/usage.md): how ABFS is used here. Read it first.
- [prerequisites.md](docs/prerequisites.md): cluster, network, identities and Config Connector.
- [runbook.md](docs/runbook.md): backend modes, garbage collection, seed and steady phase, upgrades, cache reset, teardown, security, the bundle.
- [configuration.md](docs/configuration.md): every chart value and the KCC to chart name table.
- [ci-job.md](docs/ci-job.md): the build pod contract, the example Job, Airflow, node sizing.
- [troubleshooting.md](docs/troubleshooting.md): known failures and their causes.

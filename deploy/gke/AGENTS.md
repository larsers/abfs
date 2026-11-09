# AGENTS.md: ABFS on GKE

Orientation for anyone, person or tool, who works in this tree: what it is, where things are, which commands check a change, and the rules that protect a running deployment. The user documents are in `README.md` and `docs/`; this file points into them and does not repeat them.

## Where you are

This tree is one of two things:

- **A release bundle** (`abfs-gke-<version>/`): the root has `COMMIT`, `images.txt`, `images/`, `source/` and `SHA256SUMS`. It deploys one ABFS release; the images are built, and the source they were built from is in `source/`.
- **`deploy/gke` of the ABFS repository**: no `COMMIT` file; `images/` holds Dockerfiles. Here the images are built and the bundle is made.

Paths below are relative to this root. Commands that differ between the two say so.

## What it deploys

The open-source ABFS (Android Build File System) on GKE Standard, for CI builds of Android trees:

- `abfs-server`: the content-addressable store over gRPC. Refs in Spanner, objects in Bigtable, large blobs in Cloud Storage. Behind an internal regional Gateway. The only workload with Google credentials (Workload Identity).
- git-pushers (StatefulSet, one PVC each): fetch from Gerrit and push into the store.
- CI nodes (Ubuntu): a DaemonSet builds and loads the casfs kernel module; one cacheman per node serves every build pod from a cache on the boot disk. Build pods mount a workspace that loads file content on read.
- A bootstrap Job writes the pusher pool config and the central client config into the store.

Google Cloud resources come from Config Connector (KCC, `kcc/`), workloads from a Helm chart (`chart/abfs`). The diagram and the parts are in `README.md`; how a build uses the store is in `docs/usage.md`.

## Layout

```
AGENTS.md            this file
README.md            overview, quick start, document list, limits
Makefile             offline checks (check, lint, template, kustomize, ddl-check); ddl, images, bundle
chart/abfs/          Helm chart: values.yaml (defaults), values.schema.json, values-example.yaml,
                     templates/, ci/ (test values; ci/fail-*.yaml must fail to render)
kcc/
  base/              namespace, APIs, service accounts, Workload Identity, Gateway address
  components/
    backend-new/       KCC creates Spanner (base schema only), Bigtable (objects, maps, chunks), bucket, roles
    backend-existing/  only the server's roles, on a backend that already exists
    gc/                garbage collector resources in Bigtable; only with backend-new, only when GC is on
    network/proxy-subnet/  the proxy-only subnet, for a network without one
    settings/          fills the base from the overlay's settings.yaml
  overlays/example/  a worked overlay: copy it, set settings.yaml, pick the components
scripts/             load-images.sh, build-images.sh, bundle.sh, gen-ddl.sh, check-ddl.sh, ddl-lib.sh
docs/                usage, prerequisites, runbook, configuration, ci-job, troubleshooting
```

Only in a bundle:

```
COMMIT               the ABFS commit of the release
images.txt           name, reference and digest of each image
images/*.oci.tar     abfs-server, abfs-pusher, abfs-client, casfs-loader as OCI archives
source/              abfs-<version>.tar.gz (the ABFS tree of COMMIT) and abfs-casfs-<commit>.tar.gz
SHA256SUMS           of every other file
```

## Which document answers what

| Question | Document |
| --- | --- |
| How is ABFS used here, what are the parts, how does it scale | `docs/usage.md` (read first) |
| What must exist before the install (cluster, network, KCC, identities, registry) | `docs/prerequisites.md` |
| Install order, backend modes, garbage collection, upgrades, teardown, security | `docs/runbook.md` |
| What a chart value does, its default, which names must match the KCC tree | `docs/configuration.md` |
| What a CI build pod needs, the example Job, Airflow | `docs/ci-job.md` |
| A known error message | `docs/troubleshooting.md` |
| The ABFS server, client and pusher themselves | `doc/sysadmin/` and `doc/android-dev/` in the ABFS source tree (`source/` in a bundle); the GCP server in `doc/sysadmin/GCP_SERVER.md`, its garbage collector in `doc/sysadmin/GCP_GC.md` |

## Commands

Tools: `kubectl` (with kustomize), `helm` (the chart is checked with Helm 4), `gcloud`, `docker`, and `skopeo` for `load-images.sh`.

```sh
sha256sum -c SHA256SUMS                       # bundle only: check the unpacked files
make check                                    # offline: helm lint, helm template of chart/abfs/ci, kustomize of every
                                              # overlay and component rule, ddl-check against the hash in the ddl file
kubectl kustomize kcc/overlays/<name>         # review what KCC will create
helm template abfs chart/abfs -n abfs -f <values.yaml>    # review what the chart will create
scripts/load-images.sh <region>-docker.pkg.dev/<project>/<repo> >values-images.yaml   # bundle: images into a registry
```

`make check` needs no cloud access. Run it after every change to `chart/`, `kcc/` or `scripts/`, and add a case to it (or a values file under `chart/abfs/ci/`) when a change adds a rule. The repository-only commands (`scripts/build-images.sh`, `make images`, `make bundle`, `make ddl`) and how to rebuild the images from `source/` are in `docs/runbook.md`, section "Bundle and source tree".

Install and apply steps are in `docs/runbook.md` in order; follow it rather than composing commands. Two that are easy to get wrong: install and upgrade the chart without `--wait` (the pushers wait for a Job that runs after the install), and wait for KCC objects with `kubectl -n abfs wait --for=condition=Ready --all gcp --timeout=30m`.

## Rules that protect a running deployment

- **The Spanner `ddl` list is append-only.** `kcc/components/backend-new/spanner-database-ddl.yaml` holds the schema of the release that first created the database. Never edit, reorder or remove an entry; a schema change is a new entry appended at the end. Config Connector refuses a changed entry. Run `make ddl-check` after any change to the file.
- **One backend mode per overlay.** `backend-new` creates resources and must never point at resources that exist: adopting a live Bigtable table can delete its column families. For an existing backend use `backend-existing`. Listing both fails the build.
- **Garbage collection is off by default.** The default backend has no garbage collector resources, and the server runs without `--gc`. Turning it on: `components/gc` after `backend-new` (applied before `init --gc`), then `abfs server gcp init --gc`, then `server.gc.enabled=true`. The garbage collector's Spanner tables are never entries of the ddl list; `init --gc` creates them in both modes. With `backend-new`, Config Connector owns every Bigtable table and family: a family that `init` creates alone is deleted at the next reconcile, so Bigtable changes go into `components/gc`. While the resources exist, keep `components/gc` in the overlay, or Config Connector deletes the family `objects:g` and its condemn markers. Details: `docs/runbook.md`, section "Garbage collection".
- **`abfs server gcp init` runs with admin credentials from a workstation**, through the `abfs` binary of the `abfs-server` image, never as the server's service account (it holds only runtime roles). It creates what is missing and changes nothing else; it is also the conformance check of a backend.
- **Images are referenced by digest.** The chart refuses an empty digest unless `image.allowTag=true` (development only). Take the digests from `load-images.sh` or `images.txt`.
- **cacheman is never restarted under running builds.** Its DaemonSet uses `OnDelete`; a stopped cacheman stops every mount on its node. Upgrades recycle CI nodes (`docs/runbook.md`, "Upgrades").
- **Client tools and the casfs module come from the same release.** They speak one interface version. The CI nodes take both from this bundle's images; do not mix in packages from other ABFS releases or from a node image.
- **The pusher count is fixed** for a pool: each pusher owns a hash share of the projects and keeps their clones on its PVC (`docs/runbook.md`, "The pusher count is fixed").
- **No authentication, no TLS.** The internal Gateway address and the NetworkPolicy are the only boundary. Do not expose the address beyond the CI nodes and admin hosts (`docs/runbook.md`, "Security").
- **Config Connector is installed by hand** from its operator bundle at a pinned version, not as the GKE add-on (`docs/prerequisites.md`).

## Working on the tree

- Chart values: defaults in `chart/abfs/values.yaml`, types in `values.schema.json`; keep both and `docs/configuration.md` in step. Render-time checks (`fail` in the templates) are tested with values files `chart/abfs/ci/fail-*.yaml`, which `make check` expects to fail; add one with each new check.
- KCC: names shared with the chart are listed in `docs/configuration.md`, section "KCC names and chart values". Settings flow from `kcc/overlays/<name>/settings.yaml` through `components/settings` and the replacements in each component; check a change with `kubectl kustomize` of the example overlay and `make check`.
- Docs describe behaviour and planning figures; keep them true to the chart and the KCC tree when either changes.
- Source files carry the Apache-2.0 header of their neighbours.
- In the ABFS source tree, follow its own `CONTRIBUTING.md` and the style of the surrounding code. The Go version is in its `go.mod`.

## When something fails

- KCC: `kubectl -n abfs get gcp` lists every Config Connector object; `kubectl -n abfs describe <kind> <name>` shows why one is not Ready.
- Workloads: `kubectl -n abfs get pods`, `kubectl -n abfs-ci get pods`, then the pod logs.
- Known messages and their causes: `docs/troubleshooting.md`.

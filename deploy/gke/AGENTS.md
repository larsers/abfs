# AGENTS.md: ABFS on GKE

Read this first. It routes a coding agent or a person who installs ABFS from this tree.

## What this is

The open-source ABFS (Android Build File System) on GKE Standard, for CI builds of Android trees. `abfs-server` is a content-addressable store with refs in Spanner, objects in Bigtable and blobs in Cloud Storage, behind an internal Gateway. Git-pushers push from Gerrit into it. CI nodes load casfs and run one cacheman for all build pods. Config Connector (`kcc/`) creates the Google Cloud resources, a Helm chart (`chart/abfs`) the workloads. A release bundle (`abfs-gke-<version>/`) has `COMMIT`, `images.txt`, `images/`, `source/` and `SHA256SUMS` at its root; `deploy/gke` of the ABFS repository has no `COMMIT` file and builds the images.

## Rules for the agent

- **Ask the human before** any step that spends money, creates or deletes a cloud resource, or changes IAM. Show the step's **Creates** line and command, and wait for a yes. Read-only commands (`describe`, `list`, `get`, `logs`, `helm template`, `kubectl kustomize`, `make check`) need no approval.
- **Check the admin's roles** against `docs/install.md` step 2 before step 2. A missing `roles/iam.serviceAccountUser` on `abfs-system-nodes` fails step 6; a missing `roles/logging.viewer` fails step 14. Granting a role needs a yes.
- **The network is shared.** Run the read-only check of step 4.0, show it, and create nothing in step 4 until the human or the network owner agrees what to create and what to reuse.
- **Never skip a check.** Compare each check's output with `docs/install.md`; start the next step only after it passes.
- **Stop on a failed check.** Report the command, output and troubleshooting pointer, and propose a fix. Do not improvise, and do not retry a step that creates resources without asking.
- **Never print secrets**: `.netrc`, `.gitcookies`, Application Default Credentials, access tokens. Refer to files by path.
- **Report cost.** Before the install, estimate what runs by the hour (server, pusher and CI nodes, Spanner, Bigtable, Cloud NAT, the Private Service Connect endpoint) from the pricing pages. Report the spend after each billable step.
- **Tests of step 15.** Level 1 (`helm test`, the patched repo tool check) always runs. Then offer level 2 (quick end-to-end test, about 15 minutes, recommended before a first full seed) and level 3 (AAOS acceptance test, hours; the store grows by up to about 1 TiB that stays, as garbage collection is off; state the monthly storage cost). Run each only on a yes. The seed of the human's manifest needs its own approval.
- **Follow the documents.** Use the commands, names and order of `docs/install.md`.

## Rules that protect a running deployment

- **The Spanner `ddl` list is append-only** (`kcc/components/backend-new/spanner-database-ddl.yaml`). Never edit, reorder or remove an entry; append. Run `make ddl-check` after any change.
- **One backend mode per overlay.** `backend-new` must never point at existing resources: adopting a live Bigtable table can delete its column families. Use `backend-existing` for those.
- **Garbage collection is off by default.** To turn it on: `components/gc` after `backend-new`, then `abfs server gcp init --gc`, then `server.gc.enabled=true`, then the collector CronJob (`gcCollector`) in mode `dry-run`, then `condemn` for at least 7 daily cycles, then `delete`. Each mode change needs a yes; `delete` deletes data. The garbage collector's Spanner tables never go into the ddl list; `init --gc` creates them. With `backend-new`, Bigtable changes go into `components/gc`: Config Connector deletes a family that `init` creates alone. Keep `components/gc` in the overlay while its resources exist, or Config Connector deletes the family `objects:abfsgc`. See `docs/runbook.md`, "Garbage collection".
- **`abfs server gcp init` runs with admin credentials from a workstation**, never as the server's service account. It creates what is missing and changes nothing else.
- **Images are referenced by digest.** Take the digests from `load-images.sh` (`values-images.yaml`) or `images.txt`. `image.allowTag=true` is for development only.
- **Never restart cacheman under running builds.** A stopped cacheman stops every mount on its node. Build pods stay below the PriorityClass `<release>-ci-node` of cacheman and the casfs loader.
- **Client tools and casfs must match.** With `ci.casfs.provider: image`, the COS image must ship casfs 0.1.24 or later; a wrong module cannot be unloaded, so the node must be replaced. Do not install client packages from other ABFS releases.
- **Hold minor upgrades** with a `no_minor_upgrades` exclusion until the next minor version's COS image ships casfs 0.1.24 or later.
- **The pusher count is fixed** for a pool: each pusher owns a share of the projects on its PVC. Never `kubectl scale` the StatefulSet.
- **No authentication, no TLS.** Do not expose the Gateway address beyond the CI nodes and admin hosts.
- **Config Connector is installed by hand** at a pinned version, not as the GKE add-on. Never delete the `ConfigConnectorContext` while KCC objects exist.
- Install and upgrade the chart without `--wait`. Wait for KCC objects with `kubectl -n abfs wait --for=condition=Ready --all gcp --timeout=30m`.
- Run `make check` (offline) after every change to `chart/`, `kcc/` or `scripts/`.
- On a failure: `kubectl -n abfs get gcp`, `kubectl -n abfs describe <kind> <name>`, `kubectl -n abfs get pods`, `kubectl -n abfs-ci get pods`, the pod logs, then `docs/troubleshooting.md`.

## Inputs to collect before you start

Ask for each; say which defaults they may keep.

- Project, region, zone of the Bigtable cluster and servers.
- VPC network, subnet, network project (`NET_P`) and its owner; in a Shared VPC the Pods and Services secondary ranges.
- What the network has (step 4.0): private zones for `googleapis.com`, `pkg.dev`, `gcr.io`, endpoints for Google APIs, a Cloud NAT gateway; whether other subnets may get NAT egress.
- An existing proxy-only subnet, or a free `/23`.
- A free IP for the Private Service Connect endpoint; whether admin hosts need the `gke.goog` zone.
- Cluster name, GKE version and channel (default `1.37.2-gke.1325000`, Rapid), maintenance window.
- The admin account, and who gets cluster access with which role.
- The CI system, where it runs, and its Google service account (attached or through Workload Identity Federation, never a key).
- Backend mode: `backend-new`, or `backend-existing` with the existing resource names.
- Bigtable node cap (default 3); Artifact Registry repository (default `abfs`) and its project.
- Gerrit: manifest URL, branches, files, `.netrc` or `.gitcookies`; or upstream AOSP only.
- `pusher.count` (default 3; fixed after the first full seed).
- The CI build image and its registry.
- Optional tests: level 2 (an image with `git` and `python3`), level 3 (release and lunch target, for example `android-15.0.0_r36` with `aosp_cf_x86_64_auto-bp1a-userdebug`).
- Budget caps with stop times for the install, the acceptance test and the seed.
- Alerts on or off (`alerts.enabled`, `alerts.clusterName`).

## Which document answers what

| Question | Document |
| --- | --- |
| The install in order, every command and check, the tests, the first seed | `docs/install.md` |
| How ABFS is used here | `docs/usage.md` |
| What the cluster, network, identities and Config Connector need | `docs/prerequisites.md` |
| Backend modes, garbage collection, seed and steady phase, upgrades, cache reset, teardown, security, building images | `docs/runbook.md` |
| Chart values, defaults, names shared with the KCC tree | `docs/configuration.md` |
| What a CI build pod needs, the example Job, Airflow, node disk | `docs/ci-job.md` |
| A known error message | `docs/troubleshooting.md` |
| The ABFS server, client and pusher | `doc/sysadmin/` and `doc/android-dev/` in the ABFS source tree (`source/` in a bundle) |

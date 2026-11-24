# ABFS on GKE / CI build pods

The chart sets up the CI nodes: the class `abfs-ci`, the casfs module, one cacheman per node and the ABFS tools in `/var/lib/abfs/bin`. Your CI system creates the build pods.

## Build pod contract

The values assume the chart defaults (`ci.uid` and `ci.gid` 1000, `ci.hostCacheDir` `/var/lib/abfs`).

- [ ] **Namespace `abfs-ci`**, with Pod Security level `privileged`.
- [ ] **`nodeSelector: cloud.google.com/compute-class: abfs-ci`.** GKE adds the class toleration. Do not select a node label of your own: the autoscaler cannot scale up from zero for it.
- [ ] **`securityContext.privileged: true`** on the build container.
- [ ] **`runAsUser: 1000` and `runAsGroup: 1000`**, the same pair as cacheman. With another uid the mount preflight fails with "permission denied". Without `runAsGroup` the group comes from the image's passwd entry, or 0.
- [ ] **`HOME` set**, to a directory the build user can write.
- [ ] **A passwd entry for uid 1000, or `USER=1000`.** Without either, every `abfs` command stops with "could not determine current user".
- [ ] **`PATH` starting with `/var/lib/abfs/bin`.** It holds the static ABFS tools, the seven `-no-cgo` build hooks, and static `ps` and `fuser`. The build image needs no minimum glibc.
- [ ] **hostPath `/var/lib/abfs` mounted at `/var/lib/abfs`**, the same path as in cacheman.
- [ ] **`ABFS_BASE_DIR=/var/lib/abfs/base`**, and the ABFS instance `ci` (`abfs -i ci ...`).
- [ ] **A mount point owned by uid 1000 with owner `rwx`.** Otherwise `casfs-mounter` fails with "owned by a different user". `mkdir -p "$WORK/src"` in an `emptyDir` meets it.
- [ ] **An init container that waits for `/sys/module/casfs`** up to a timeout (15 minutes in the chart), then fails, so the CI system retries the pod on another node.
- [ ] **A wait for `/var/lib/abfs/bin/abfs`** and a responding cacheman (`timeout -k 2 8 /var/lib/abfs/bin/abfs -i ci cacheman ping -t 5s`, not only a socket file).
- [ ] **`resources.requests.ephemeral-storage: 400Gi`** (`ci.buildDisk`), and **no ephemeral-storage limit and no `sizeLimit`** on the `emptyDir` ([Node disk](#node-disk)).
- [ ] **A priority below 1000000000**, the value of the chart's PriorityClass `<release>-ci-node` (`abfs-ci-node` for the release `abfs`). A pod without a `priorityClassName` has priority 0 unless the cluster has a global default class. cacheman and the casfs loader run at that class and preempt build pods; a build pod at the same priority can keep cacheman Pending.
- [ ] **A PID 1 that reaps orphaned processes**, such as a shell that runs the build steps (`/bin/bash -c ...`) or `tini`. With `sleep` as PID 1, `abfs unmount` waits 30 s and prints "process did not die in 30s".
- [ ] **The annotation `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"`**.
- [ ] **Retries in the CI system** for a build whose node went away. Auto-repair can replace a node during a build, and a drain waits at most one hour ([prerequisites](prerequisites.md#node-upgrades)).
- [ ] **No copy of the ABFS tools into the image.** The preflight fails a mount when the CLI and cacheman interface versions differ.

## Build steps

```sh
abfs -i ci ready -b "$BRANCH" --wait --timeout 2h -o "$WORK/pinned.xml"
abfs -i ci mount -b "$BRANCH" -f "$WORK/pinned.xml" -r "ci/$JOB_ID" --tmp-ws "$WORK/src"
cd "$WORK/src"
# the build
cd /
abfs -i ci unmount "$WORK/src"
```

`ready` waits until the branch's projects are in the store and writes the pinned manifest. `-r` gives each job its own meta ref prefix; use a unique job id. Unmount in every exit path, for example in a shell `trap`.

Every mount, remount and unmount pings cacheman over its socket, so the build pod needs no host PID namespace. When cacheman does not answer, the command fails with "could not detect a running cacheman" ([troubleshooting](troubleshooting.md#could-not-detect-a-running-cacheman)).

### Build cache hits

The mount path is part of many cache keys. Every build pod must mount at the same path, `$WORK/src` with `WORK=/work` (`/work/src`) as in the example Job. Outputs built at another path do not serve it, so the first builds of a new deployment run cold, even on a store that other clients use. After that, a build of the same commit and lunch target takes nearly all steps from the cache.

## Example Job

The chart renders a Job with the full contract when `ci.exampleJob.enabled` is true. Render it, adapt it, or start it as the canary build of the runbook:

```sh
helm template abfs chart/abfs -n abfs -f <values.yaml> \
  --set ci.exampleJob.enabled=true --set ci.exampleJob.image=<build image> \
  --show-only templates/ci-example-job.yaml
```

The rendered Job, with the chart labels left out:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: abfs-example-build
  namespace: abfs-ci
spec:
  backoffLimit: 2
  activeDeadlineSeconds: 43200
  podFailurePolicy:
    rules:
      - action: Ignore
        onPodConditions:
          - type: DisruptionTarget
  template:
    metadata:
      annotations:
        cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      nodeSelector:
        cloud.google.com/compute-class: abfs-ci
      initContainers:
        - name: wait-for-casfs
          image: <abfs-client image>
          command:
            - /bin/sh
            - -c
            - |
              end=$(( $(date +%s) + 900 ))
              until [ -d /sys/module/casfs ]; do
                if [ "$(date +%s)" -ge "$end" ]; then
                  echo "casfs is not loaded on this node after 15m; see the abfs-casfs-loader logs on the node" >&2
                  exit 1
                fi
                sleep 5
              done
          securityContext:
            runAsNonRoot: true
            runAsUser: 1000
            runAsGroup: 1000
            allowPrivilegeEscalation: false
            capabilities: {drop: ["ALL"]}
        - name: wait-for-cacheman
          image: <abfs-client image>
          command:
            - /bin/sh
            - -c
            - |
              end=$(( $(date +%s) + 900 ))
              until [ -x /var/lib/abfs/bin/abfs ] && timeout -k 2 8 /var/lib/abfs/bin/abfs -i ci cacheman ping -t 5s; do
                if [ "$(date +%s)" -ge "$end" ]; then
                  echo "no ABFS tools or responding cacheman on this node; see the abfs-cacheman pod" >&2
                  exit 1
                fi
                sleep 5
              done
          env:
            - {name: USER, value: "1000"}
            - {name: ABFS_BASE_DIR, value: /var/lib/abfs/base}
          securityContext:
            runAsNonRoot: true
            runAsUser: 1000
            runAsGroup: 1000
            allowPrivilegeEscalation: false
            capabilities: {drop: ["ALL"]}
          resources:
            requests: {cpu: 10m, memory: 32Mi}
            limits: {memory: 128Mi}
          volumeMounts:
            - name: abfs
              mountPath: /var/lib/abfs
              readOnly: true
      containers:
        - name: build
          image: <build image>
          command:
            - /bin/bash
            - -c
            - |
              set -euo pipefail
              mkdir -p "$HOME" "$WORK/src"
              abfs -i ci ready -b "$BRANCH" --wait --timeout 2h -o "$WORK/pinned.xml"
              abfs -i ci mount -b "$BRANCH" -f "$WORK/pinned.xml" -r "ci/$JOB_ID" --tmp-ws "$WORK/src"
              trap 'cd /; abfs -i ci unmount "$WORK/src"' EXIT
              cd "$WORK/src"
              set +u
              source build/envsetup.sh && lunch "$TARGET" && m
          env:
            - {name: BRANCH, value: "main"}
            - {name: TARGET, value: "aosp_cf_x86_64_phone-trunk_staging-userdebug"}
            - name: JOB_ID
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - {name: WORK, value: /work}
            - {name: HOME, value: /work/home}
            - {name: USER, value: "1000"}
            - {name: ABFS_BASE_DIR, value: /var/lib/abfs/base}
            - name: PATH
              value: /var/lib/abfs/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
            - {name: OTEL_EXPORTER_OTLP_PROTOCOL, value: "grpc"}
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: "http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317"
          securityContext:
            privileged: true
            runAsUser: 1000
            runAsGroup: 1000
          resources:
            requests: {cpu: "28", ephemeral-storage: 400Gi, memory: 80Gi}
            limits: {memory: 80Gi}
          volumeMounts:
            - {name: abfs, mountPath: /var/lib/abfs}
            - {name: work, mountPath: /work}
      volumes:
        - name: abfs
          hostPath: {path: /var/lib/abfs, type: DirectoryOrCreate}
        - name: work
          emptyDir: {}
```

The build image needs `bash` and the Android build host tools. The Job's pod name is the job id. The ABFS steps run under `set -euo pipefail`, so a failed `ready` or `mount` ends the pod. The build command (`ci.exampleJob.command`) runs after `set +u`, because `build/envsetup.sh`, `lunch` and `m` read unset variables (`TOP: unbound variable`). A build script of your own needs the same `set +u` before it sources `build/envsetup.sh`. `-e` and `pipefail` stay on: join `source build/envsetup.sh`, `lunch` and the build with `&&`, as the default command does.

`backoffLimit: 2` counts build failures. A pod that was preempted, lost its node, or was evicted by kubelet gets the condition `DisruptionTarget` and the `podFailurePolicy` rule retries it without counting. `activeDeadlineSeconds` (`ci.exampleJob.deadlineSeconds`, 12 hours) ends the Job, retries included. A CI system of your own needs the same distinction in its retries, and a limit on the retries it does not count.

## Airflow as the CI system

Airflow starts each build as a pod in the ABFS cluster with `GKEStartPodOperator`. Do not use `KubernetesPodOperator`: it runs pods only in the Airflow environment's own cluster.

**Requirements:**

- Airflow 2.11.1 or later, or Airflow 3, with `apache-airflow-providers-google` 22.0.0 or later.
- `GKEStartPodOperator` with `use_dns_endpoint=True`. The cluster has only its DNS-based endpoint ([Control plane access](prerequisites.md#control-plane-access)).
- The Airflow environment in a project other than the ABFS cluster's project. Its service account has `roles/composer.worker`, which carries every `container.*` permission; in a shared project the Role below would limit nothing. Anyone who can change the DAGs, the environment or its images can act as this account, so keep its rights to build pods in `abfs-ci`.
- `roles/container.clusterViewer` for the environment's service account on the ABFS cluster's project, and the Role and RoleBinding below in `abfs-ci`. The role holds `container.clusters.get` and `container.clusters.connect` and grants nothing inside the cluster. Do not grant `roles/container.viewer`, `roles/container.developer` or `roles/container.admin`: they open every namespace.

```sh
gcloud projects add-iam-policy-binding example-project \
  --member serviceAccount:<environment service account email> \
  --role roles/container.clusterViewer
```

Then give the account a Role in `abfs-ci` that allows only build pods:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: airflow-builds, namespace: abfs-ci}
rules:
  - apiGroups: [""]
    resources: [pods]
    verbs: [create, get, list, watch, delete, patch]
  - apiGroups: [""]
    resources: [pods/log]
    verbs: [get]
  - apiGroups: [""]
    resources: [events]
    verbs: [list]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: airflow-builds, namespace: abfs-ci}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: airflow-builds}
subjects:
  - {kind: User, name: <environment service account email>}
```

[Check CI access to the control plane](install.md#check-ci-access-to-the-control-plane) tests both grants with the account's own credentials, without Airflow.

**Network.** The workers and the triggerer call the endpoint under `gke.goog`. A Private IP environment needs Private Google Access on the `private.googleapis.com` range; with a Private Service Connect endpoint, add a `gke.goog` zone ([Private Service Connect endpoint for Google APIs](prerequisites.md#private-service-connect-endpoint-for-google-apis)). A VPC Service Controls perimeter on `container.googleapis.com` must include or allow the environment's project.

**Pod template.** Pass the pod spec of the [example Job](#example-job) as `pod_template_file`, with the build container renamed from `build` to `base`. Set `BRANCH` and `TARGET` with `env_vars`, and `JOB_ID` from the task's run id.

**Operator settings:**

| Setting | Value | Why |
| --- | --- | --- |
| `project_id`, `location`, `cluster_name` | the ABFS cluster: `example-project`, `europe-west4`, `abfs` | without `project_id` the operator takes the default project of its Airflow connection |
| `use_dns_endpoint` | `True` | the cluster has no IP-based endpoint |
| `namespace` | `abfs-ci` | Pod Security level `privileged` |
| `startup_timeout_seconds` | 1800 or more | a new node, casfs and cacheman |
| `retries` | 1 or more | lost nodes, nodes without casfs |
| `deferrable` | `True` | frees the worker slot; needs a triggerer with the workers' network path |
| `get_logs` | `True` | the build log in the task log |

The task fails when the build fails.

## Sizing

casfs keeps per-workspace state in kernel slab memory, which kubelet does not reserve. Per node:

1. **Build pods per node.** From the node's allocatable CPU and memory, subtract the DaemonSets: cacheman 2 CPU and 4Gi, the loader 100m and 256Mi, GKE system about 1 CPU. Divide the rest by the build pod request. One build pod per node: CPU request <= vCPUs - 3.3 (28 on 32 vCPU). GKE reserves about 2.6 GiB of memory on a 128 GB COS node in pools created on GKE 1.37.2-gke.1325000 or later, and about 9.8 GiB (128 GB) or 12.5 GiB (256 GB) in pools created earlier (possible with `ci.casfs.provider: daemonset`); `kubectl describe node` shows `Capacity` and `Allocatable`. The disk sets a further bound ([Node disk](#node-disk)).
2. **`ci.casfs.maxMounts`** = build pods per node + 2. The default is 4. It is fixed when the module loads, so a change applies to new nodes only.
3. **Slab budget** = `maxMounts` x `ci.casfs.mountGbPerWorkspace` (6 GB by default). To measure casfs slab, boot with `slab_nomerge` or read the aliases under `/sys/kernel/slab`.
4. **Rule:** build pod memory requests + slab budget + DaemonSet memory + GKE's memory reservation must fit the node's memory. The loader caps the slab budget at `ci.casfs.maxRamShare` (0.4) of `MemTotal`.

**Build pod memory.** Size for a cold build. A cold full-platform `m droid` needs more than 40Gi; a pod that runs out of memory is OOMKilled and loses its `emptyDir` with the logs. Start from the example Job's 80Gi. Two 40Gi pods fit a 128 GB node, but each can run out of memory on a cold build.

| Build pods per node | Request each | Node shape | Memory sum |
| --- | --- | --- | --- |
| 1 | 28 CPU, 80Gi | 32 vCPU, 128 GB (for example n4-standard-32; the class floor is 32 vCPU and 120 GB, `computeClasses.ci.minCores`, `computeClasses.ci.minMemoryGb`) | 86 + 24 slab + 5 DaemonSets + 3 reserved = 118 GB (126 GB with the earlier reservation) |
| 2 | 28 CPU, 80Gi | 64 vCPU, 256 GB (for example n4-standard-64) | 172 + 24 + 5 + 3 = 204 GB (215 GB earlier); 56 CPU + 3.1 fit 63.75 allocatable; also needs a larger boot disk ([Node disk](#node-disk)) |

A request of 30 CPU no longer fits a 32-vCPU node, and GKE creates a 48-vCPU node for each pod. With `optimize-utilization`, builds pack onto busy nodes and share one boot disk. For more disk speed, use the [IOPS fallback pool](prerequisites.md#iops-fallback-node-pool-for-ci).

## Node disk

Each CI node has one Hyperdisk Balanced boot disk of `computeClasses.ci.bootDiskGb` (1,100 GiB).

Build outputs land in the node cache, `/var/lib/abfs/base/ci/cache` in the hostPath, which kubelet does not count against any pod. So the chart reserves the disk with requests, not limits:

| Value | Default | Request of |
| --- | --- | --- |
| `ci.cacheman.cacheDisk` | 400Gi | cacheman |
| `ci.buildDisk` | 400Gi | each build pod |
| `computeClasses.ci.bootDiskGb` | 1100 | fits the cache and one build; two builds per node need 1,600 |

cacheman evicts uploaded content, least recently used first, above its threshold of 90 % of `ci.cacheman.cacheDisk` (360 GiB), down to 80 % of it. It runs at the next index write or maintenance pass, so the cache can pass the threshold for a while. Outputs not uploaded are never evicted; an idle sweep removes them after 60 s with no mount, which a busy node may never run.

At 85 % disk use kubelet starts image garbage collection and GKE reports `StoragePressureRootFileSystem`. At 90 % kubelet evicts pods, which does not shrink the hostPath.

With `alerts.enabled`, the rule `AbfsNodeDiskHigh` fires when a node of the cluster `alerts.clusterName` stays above 85 % for 5 minutes. It covers every node of the cluster, not only CI nodes. Without it, query the node disk use in Cloud Monitoring (PromQL):

```
kubernetes_io:node_ephemeral_storage_used_bytes{monitored_resource="k8s_node", cluster_name="abfs"}
  / kubernetes_io:node_ephemeral_storage_total_bytes{monitored_resource="k8s_node", cluster_name="abfs"}
```

**To change the values.** 400 GiB per build is a planning figure; output size depends on the lunch target and on `dist`. Measure the disk growth of a full build of your largest target with the query above, and set `ci.buildDisk` to that plus a margin. The render fails when `ci.cacheman.cacheDisk` plus one `ci.buildDisk` do not fit the boot disk. That check covers one build: for more builds per node, check `cacheDisk` plus that many times `buildDisk` yourself. Node pool auto-creation never enlarges the boot disk, so a build pod whose request does not fit stays Pending. For more builds per node, raise `computeClasses.ci.bootDiskGb`; it applies to new CI node pools only ([runbook](runbook.md#upgrades)). A pool in `computeClasses.ci.nodepools` needs the same boot disk size, or cacheman does not fit and its build pods wait; the render check does not see it. Local SSD is not supported (`ci.localSsd`).

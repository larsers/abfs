# ABFS on GKE / CI build pods

The chart sets up the CI nodes: the class `abfs-ci`, the casfs module, one cacheman per node and the ABFS tools in `/var/lib/abfs/bin`. The build pods come from your CI system. This page is the contract every build pod must meet, whatever creates it, the example Job of the chart, and how to size the pods and the nodes together.

## Build pod contract

Check each item in the pod spec your CI system creates. The values assume the chart defaults (`ci.uid` and `ci.gid` 1000, `ci.hostCacheDir` `/var/lib/abfs`).

- [ ] **Namespace `abfs-ci`**, with Pod Security level `privileged`.
- [ ] **`nodeSelector: cloud.google.com/compute-class: abfs-ci`.** GKE adds the class toleration. Do not select a node label of your own: the autoscaler cannot scale up from zero for a label no node template carries.
- [ ] **`securityContext.privileged: true`** on the build container. `casfs-mounter` runs as the build user and gets `CAP_SYS_ADMIN` only from its file capabilities, which need a full bounding set.
- [ ] **`runAsUser: 1000` and `runAsGroup: 1000`**, the same pair as cacheman. The cache directory and the cacheman sockets belong to that pair; with another uid the mount preflight fails with "permission denied". Without `runAsGroup` the runtime takes the group from the image's passwd entry, or 0.
- [ ] **`HOME` set**, to a directory the build user can write.
- [ ] **A passwd entry for uid 1000, or `USER=1000`.** Without an entry `abfs` takes the user name from `$USER`. Without either, every `abfs` command stops with "could not determine current user".
- [ ] **`PATH` starting with `/var/lib/abfs/bin`.** It holds the static ABFS tools, the seven `-no-cgo` build hooks that `abfs setup` copies into each Soong tree, and static `ps` and `fuser`. The tools are static, so the build image needs no minimum glibc.
- [ ] **hostPath `/var/lib/abfs` mounted at `/var/lib/abfs`**, the same path as in cacheman. The kernel reconnects to the cache socket by its host path.
- [ ] **`ABFS_BASE_DIR=/var/lib/abfs/base`**, and the ABFS instance `ci` (`abfs -i ci ...`). cacheman writes the instance config there, with the Gateway VIP as the server and the manifest server and project of the pusher pool.
- [ ] **A mount point owned by uid 1000 with owner `rwx`.** `casfs-mounter` checks the owner and the mode of the mount point and fails with "owned by a different user" otherwise. A directory that the build user creates in an `emptyDir`, as `mkdir -p "$WORK/src"` does, meets it.
- [ ] **An init container that waits for `/sys/module/casfs`** up to a timeout (15 minutes in the chart), then fails, so the CI system retries the pod on another node.
- [ ] **A wait for `/var/lib/abfs/bin/abfs`** and the cacheman socket `/var/lib/abfs/base/ci/cache/socket`.
- [ ] **A PID 1 that reaps orphaned processes**, such as a shell that runs the build steps (`/bin/bash -c ...`, as in the example) or `tini`. `abfs mount` starts a daemon that the pod's PID 1 inherits. A PID 1 that never reaps, such as `sleep`, leaves the daemon as a zombie when it exits, and `abfs unmount` then waits 30 s for it and prints "process did not die in 30s".
- [ ] **The annotation `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"`**, so the autoscaler does not remove the node during a build.
- [ ] **No copy of the ABFS tools into the image.** The tools come from the node, so they always match the node's cacheman. The preflight fails a mount when the CLI and cacheman interface versions differ.

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

Every mount, remount and unmount first pings the instance's cacheman over its socket, so the build pod needs no host PID namespace. When cacheman does not answer, the command fails with "could not detect a running cacheman" and the reason ([troubleshooting](troubleshooting.md#could-not-detect-a-running-cacheman)).

### Build cache hits

The mount path enters the cache keys of many build steps. Every build pod must therefore mount at the same path, `$WORK/src` with `WORK=/work` as in the example Job, for its steps to hit the outputs of other pods. The cache warms from builds at that path: outputs that clients elsewhere pushed from another mount path do not serve a pod at `/work/src`, so most steps of the first builds at that path miss.

Plan for the first builds of a new deployment to run cold, even on a store that other clients use. Once a build at `/work/src` has filled the cache, a later build of the same commit and lunch target takes nearly all of its steps from the cache.

## Example Job

The chart renders a Job with the full contract when `ci.exampleJob.enabled` is true. Render it without installing it, adapt it, or start it as the canary build of the runbook:

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
              until [ -x /var/lib/abfs/bin/abfs ] && [ -S /var/lib/abfs/base/ci/cache/socket ]; do
                if [ "$(date +%s)" -ge "$end" ]; then
                  echo "no ABFS tools or cacheman socket on this node; see the abfs-cacheman pod" >&2
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
            requests: {cpu: "28", memory: 80Gi}
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

The build image needs `bash` and the Android build host tools. The Job's pod name serves as the job id.

## Airflow as the CI system

Airflow, for example in Managed Service for Apache Airflow, starts each build as a pod in the ABFS cluster with `GKEStartPodOperator`. `KubernetesPodOperator` runs pods only in the Airflow environment's own cluster, which has no CI nodes. Nothing of ABFS runs in the Airflow environment.

**Access.** Grant the Airflow environment's service account `roles/container.clusterViewer` on the ABFS cluster's project, which lets it connect, and give it a Role in `abfs-ci` that allows only build pods:

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

The Airflow workers must reach the cluster's control plane endpoint.

**Pod template.** Write the pod spec of the [example Job](#example-job) as a Pod manifest and pass it as `pod_template_file`. The operator fills in its own main container, which it finds by the name `base`, so rename the build container from `build` to `base`. Keep everything else of the contract in the template: the namespace, the class selector, both init containers, the security contexts, the hostPath, the annotation and the environment. Set the per-build values with `env_vars` (`BRANCH`, `TARGET`; `JOB_ID` from the task's run id).

**Operator settings:**

| Setting | Value | Why |
| --- | --- | --- |
| `namespace` | `abfs-ci` | Pod Security level `privileged` |
| `startup_timeout_seconds` | 1800 or more | the pod waits for a new node, the casfs build and cacheman; the init containers alone may wait 15 minutes each |
| `retries` | 1 or more | a pod whose node never loads casfs fails after 15 minutes and must run again on another node |
| `deferrable` | `True` | a build takes up to hours; a deferred task does not hold a worker slot while it waits |
| `get_logs` | `True` | the build log in the task log |

The task fails when the build fails, because the pod's exit code is the build's.

## Sizing

casfs keeps per-workspace state in kernel slab memory, which kubelet does not reserve. Size the build pods and the module together, per node:

1. **Build pods per node.** A node's allocatable CPU is its vCPUs minus the CPU GKE reserves (about 0.15 CPU on 32 vCPU, 0.25 on 64). From that, subtract the DaemonSet requests: cacheman 2 CPU and 4Gi, the loader 100m and 256Mi, and the GKE system DaemonSets (leave about 1 CPU; `kubectl describe node` lists them). Divide the rest by the build pod request. Memory works the same way, with GKE's memory reservation (about 9.3 GiB on 128 GB). The rule for one build pod per node: CPU request <= vCPUs - 3.3 (28 on 32 vCPU).
2. **`ci.casfs.maxMounts`** = build pods per node + 2, for remounts. The default is 4. The value is fixed when the module loads, so a change applies to new nodes only.
3. **Slab budget** = `maxMounts` x `ci.casfs.mountGbPerWorkspace` (6 GB per active full-tree workspace by default). An active full-tree workspace holds a few GB of casfs slab during a build, and the slab goes away with the unmount; 6 GB keeps a margin. casfs caches without a constructor merge into other caches of the same size, so measure with `slab_nomerge` on the kernel command line, or read the aliases under `/sys/kernel/slab`.
4. **Rule:** build pod memory requests + slab budget + DaemonSet memory + GKE's memory reservation must fit the node's memory. The loader enforces only an upper bound: it lowers `maxMounts` until the slab budget fits `ci.casfs.maxRamShare` (0.4) of `MemTotal`.

**Build pod memory.** Size for a cold build, not for the warm case. A cold full-platform `m droid`, with most steps run locally, needs more than 40Gi; a pod that runs out of memory is OOMKilled, and its `emptyDir` with the logs goes with it. The example Job's 80Gi is the size to start from. With the class floor of 32 vCPU and 120 GB (`computeClasses.ci.minCores`, `computeClasses.ci.minMemoryGb`), one 80Gi pod fits a 128 GB node, and two need a 256 GB node, as the example below shows. Two 40Gi pods fit a 128 GB node, but each of them can run out of memory on a cold build.

Example with the example Job's request of 28 CPU and 80Gi. A 32-vCPU node of the class floor, for example n4-standard-32 with 128 GB, has about 31.85 allocatable CPU; after cacheman, the loader and about 1 CPU of GKE DaemonSets, 28.75 remain, so one build pod fits. `maxMounts` = 1 + 2 = 3, and the default of 4 is enough. The slab budget is 4 x 6 = 24 GB. 86 GB (80Gi) + 24 GB + about 5 GB of DaemonSets + 10 GB reserved = 125 GB, which fits in 128 GB. Two such pods per node need a 64-vCPU shape such as n4-standard-64 with 256 GB: 56 CPU plus 3.1 for the DaemonSets fit the 63.75 allocatable, `maxMounts` is 4, and 172 GB + 24 GB + 5 GB + 13 GB = 214 GB fits. A larger request, such as 30 CPU, no longer fits a 32-vCPU node, and GKE creates a 48-vCPU node for each pod.

Larger nodes let more builds share one warm cache; smaller nodes scale more finely. With the `optimize-utilization` profile the scheduler packs builds onto busy nodes, so all builds of a node share its boot disk. When builds are limited by boot disk speed, use the [IOPS fallback pool](prerequisites.md#iops-fallback-node-pool-for-ci).

The cache eviction threshold (`ci.cacheman.evictThreshold`, 500GB) is 80 % of the boot disk minus the space the build outputs of the node's pods need. With a 1,000 GiB boot disk and about 300 GB of outputs: 0.8 x 1,000 - 300 = 500 GB.

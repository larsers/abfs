{{/*
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
*/}}
{{/*
Names are fixed, never prefixed with the release name: the Workload Identity binding names
the KSA abfs/abfs-server, and the pusher pod names must equal the pool entries. Selector
labels are immutable on live objects; keep them byte-stable and put new labels in
abfs.labels only.
*/}}

{{- define "abfs.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "abfs.labels" -}}
helm.sh/chart: {{ include "abfs.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: abfs
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* Selector labels of one component. Arg: component name. */}}
{{- define "abfs.selectorLabels" -}}
app.kubernetes.io/name: abfs
app.kubernetes.io/component: {{ . }}
{{- end -}}

{{- define "abfs.server.name" -}}abfs-server{{- end -}}
{{- define "abfs.pusher.name" -}}{{ .Values.pusher.namePrefix }}{{- end -}}
{{- define "abfs.server.port" -}}50051{{- end -}}

{{/* In-cluster server address, used by the bootstrap Job and the Helm test only. */}}
{{- define "abfs.server.service" -}}
{{ include "abfs.server.name" . }}.{{ .Release.Namespace }}.svc:{{ include "abfs.server.port" . }}
{{- end -}}

{{/* Gateway VIP address, used by pushers, cacheman and build pods. */}}
{{- define "abfs.vip" -}}
{{- if not .Values.gateway.vip -}}
{{- fail "gateway.vip is required: the IP of the KCC ComputeAddress named by gateway.addressName" -}}
{{- end -}}
{{ .Values.gateway.vip }}:{{ include "abfs.server.port" . }}
{{- end -}}

{{/* Plaintext client flags for a server address. Arg: address. */}}
{{- define "abfs.clientFlags" -}}
--remote-servers {{ . }} --tunnel-ports 0 --disable-tls --auth-type none
{{- end -}}

{{/*
Image reference by digest. With image.allowTag an empty digest falls back to the tag.
Arg: dict "root" . "image" "<server|pusher|client|casfsLoader>".
*/}}
{{- define "abfs.image" -}}
{{- $v := .root.Values.image -}}
{{- $img := index $v .image -}}
{{- if $img.digest -}}
{{- printf "%s/%s@%s" $v.registry $img.name $img.digest -}}
{{- else if $v.allowTag -}}
{{- printf "%s/%s:%s" $v.registry $img.name ($v.tag | default .root.Chart.AppVersion) -}}
{{- else -}}
{{- fail (printf "image.%s.digest is empty; set the digest, or image.allowTag=true for development" .image) -}}
{{- end -}}
{{- end -}}

{{/*
Manifest keys of the clients, as YAML. The client looks the manifest ref up by
manifest-server and manifest-project-name, which must equal the server and project the
pool registers the manifest under. The pool reads pusher.pool.manifest as follows, and
so does this helper: a URL without "://" gets "https://" (or "https:" before "//"); the
URL must have a host and a path; server defaults to the URL host (port included), project
to the URL path; both are cleaned of empty and "." segments and of leading and trailing
slashes. Without a URL, server and project must both be set, with scheme https.
pusher.pool.manifest is read whether or not the pushers run here. Keys set in
bootstrap.clientConfig win; manifest-scheme defaults to https. The render fails when no
manifest-server or no manifest-project-name comes out.
*/}}
{{- define "abfs.clientManifest" -}}
{{- $m := .Values.pusher.pool.manifest -}}
{{- $out := dict -}}
{{- if $m.url -}}
{{- $raw := $m.url -}}
{{- if hasPrefix "//" $raw -}}
{{- $raw = printf "https:%s" $raw -}}
{{- else if not (contains "://" $raw) -}}
{{- $raw = printf "https://%s" $raw -}}
{{- end -}}
{{- $u := urlParse $raw -}}
{{- if not $u.host -}}
{{- fail (printf "pusher.pool.manifest.url %q has no host" $m.url) -}}
{{- end -}}
{{- if not (include "abfs.cleanPath" $u.path) -}}
{{- fail (printf "pusher.pool.manifest.url %q has no path; the pool rejects it" $m.url) -}}
{{- end -}}
{{- $_ := set $out "manifest-scheme" $u.scheme -}}
{{- $_ := set $out "manifest-server" (include "abfs.cleanPath" ($m.server | default $u.host)) -}}
{{- $_ := set $out "manifest-project-name" (include "abfs.cleanPath" ($m.project | default $u.path)) -}}
{{- else if or $m.server $m.project -}}
{{- if not (and $m.server $m.project) -}}
{{- fail "pusher.pool.manifest: without url, set both server and project" -}}
{{- end -}}
{{- $_ := set $out "manifest-scheme" "https" -}}
{{- $_ := set $out "manifest-server" (include "abfs.cleanPath" $m.server) -}}
{{- $_ := set $out "manifest-project-name" (include "abfs.cleanPath" $m.project) -}}
{{- end -}}
{{- range $k := list "manifest-scheme" "manifest-server" "manifest-project-name" -}}
{{- with index $.Values.bootstrap.clientConfig $k -}}
{{- $_ := set $out $k (toString .) -}}
{{- end -}}
{{- end -}}
{{- if not (index $out "manifest-scheme") -}}
{{- $_ := set $out "manifest-scheme" "https" -}}
{{- end -}}
{{- if not (and (index $out "manifest-server") (index $out "manifest-project-name")) -}}
{{- fail "no manifest for the clients: set pusher.pool.manifest.url (also when the pushers run elsewhere), or manifest-server and manifest-project-name in bootstrap.clientConfig" -}}
{{- end -}}
{{- toYaml $out -}}
{{- end -}}

{{/*
A path as the pool cleans it: empty and "." segments dropped, no leading or trailing
slash. ".." segments fail the render, because the chart does not resolve them.
*/}}
{{- define "abfs.cleanPath" -}}
{{- $segs := list -}}
{{- range splitList "/" . -}}
{{- if eq . ".." -}}
{{- fail "a manifest server or project with \"..\" is not supported" -}}
{{- else if and . (ne . ".") -}}
{{- $segs = append $segs . -}}
{{- end -}}
{{- end -}}
{{- join "/" $segs -}}
{{- end -}}

{{/*
USER for pods that run abfs as ci.uid. The client images have no passwd entry for it, and
abfs falls back to $USER for the current user; without it every abfs call panics.
*/}}
{{- define "abfs.userEnv" -}}
- name: USER
  value: {{ .Values.ci.uid | int | quote }}
{{- end -}}

{{/*
GOMAXPROCS from the CPU request, rounded up. Without a CPU limit Go would otherwise use
the node's core count.
*/}}
{{- define "abfs.gomaxprocsEnv" -}}
- name: GOMAXPROCS
  valueFrom:
    resourceFieldRef:
      resource: requests.cpu
      divisor: "1"
{{- end -}}

{{/*
OTLP environment. Arg: dict "root" . "queue" <span queue size, 0 for none> "traces" <bool>.
*/}}
{{- define "abfs.otelEnv" -}}
{{- $o := .root.Values.otel -}}
{{- if $o.enabled }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ $o.protocol | quote }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $o.endpoint | quote }}
{{- if .traces }}
- name: OTEL_METRICS_EXPORTER
  value: otlp
- name: OTEL_TRACES_EXPORTER
  value: otlp
{{- end }}
{{- if .queue }}
- name: OTEL_BSP_MAX_QUEUE_SIZE
  value: {{ .queue | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/* Instance keys for telemetry, as root flags. */}}
{{- define "abfs.otelKv" -}}
{{- if .Values.otel.enabled -}}
--kv metrics-provider=otel --kv record-client-metrics=true
{{- end -}}
{{- end -}}

{{/*
Probe. Arg: dict "cfg" <probe values> "type" <grpc|tcpSocket> "port" <port>.
*/}}
{{- define "abfs.probe" -}}
{{- if eq .type "grpc" }}
grpc:
  port: {{ .port }}
{{- else }}
tcpSocket:
  port: {{ .port }}
{{- end }}
{{- with .cfg.initialDelaySeconds }}
initialDelaySeconds: {{ . }}
{{- end }}
periodSeconds: {{ .cfg.periodSeconds | default 10 }}
timeoutSeconds: {{ .cfg.timeoutSeconds | default 1 }}
failureThreshold: {{ .cfg.failureThreshold | default 3 }}
{{- end -}}

{{/* Seconds of a duration such as 90s, 15m or 2h. */}}
{{- define "abfs.seconds" -}}
{{- $d := toString . -}}
{{- if hasSuffix "h" $d -}}{{ mul (trimSuffix "h" $d | int64) 3600 }}
{{- else if hasSuffix "m" $d -}}{{ mul (trimSuffix "m" $d | int64) 60 }}
{{- else -}}{{ trimSuffix "s" $d | int64 }}
{{- end -}}
{{- end -}}

{{/* Bytes of a quantity such as 48Gi, 500G or 1024. */}}
{{- define "abfs.bytes" -}}
{{- $q := toString . -}}
{{- $units := dict "Ki" 1024 "Mi" 1048576 "Gi" 1073741824 "Ti" 1099511627776 "K" 1000 "M" 1000000 "G" 1000000000 "T" 1000000000000 -}}
{{- $u := regexFind "[KMGT]i?$" $q -}}
{{- if $u -}}{{ mul (trimSuffix $u $q | int64) (get $units $u) }}{{- else -}}{{ $q | int64 }}{{- end -}}
{{- end -}}

{{/* Restricted container security context. Arg: dict "uid" <uid> "gid" <gid>. */}}
{{- define "abfs.restrictedSecurityContext" -}}
runAsNonRoot: true
runAsUser: {{ int .uid }}
runAsGroup: {{ int .gid }}
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/* Toleration of the abfs-ci class taint, for DaemonSets. */}}
{{- define "abfs.ci.scheduling" -}}
nodeSelector:
  cloud.google.com/compute-class: abfs-ci
tolerations:
  - key: cloud.google.com/compute-class
    operator: Equal
    value: abfs-ci
    effect: NoSchedule
{{- end -}}

{{/*
Placement of a pod that belongs to no node role: the bootstrap Job and the Helm tests.
Without it they land on the system pool, whose node account often cannot pull from
Artifact Registry. A class adds its node selector and the toleration of its taint to
nodeSelector and tolerations. Arg: dict "root" . "cfg" <values with computeClass,
nodeSelector, tolerations> "what" <values path for the error>.
*/}}
{{- define "abfs.placement" -}}
{{- $sel := deepCopy (.cfg.nodeSelector | default dict) -}}
{{- $tol := .cfg.tolerations | default list -}}
{{- with .cfg.computeClass -}}
{{- if and (eq . "abfs-server") (not $.root.Values.server.enabled) -}}
{{- fail (printf "%s.computeClass abfs-server needs server.enabled: the chart creates that class with the server only" $.what) -}}
{{- end -}}
{{- $_ := set $sel "cloud.google.com/compute-class" . -}}
{{- $tol = append $tol (dict "key" "cloud.google.com/compute-class" "operator" "Equal" "value" . "effect" "NoSchedule") -}}
{{- end -}}
{{- with $sel }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $tol }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
wait-for-casfs init container. Waits for the module on the node up to
ci.waitForCasfsTimeout, then fails the pod so the CI system retries it elsewhere.
/sys/module reflects the host kernel, so no privilege is needed.
*/}}
{{- define "abfs.waitForCasfsInit" -}}
- name: wait-for-casfs
  image: {{ include "abfs.image" (dict "root" . "image" "client") }}
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  command:
    - /bin/sh
    - -c
    - |
      end=$(( $(date +%s) + {{ include "abfs.seconds" .Values.ci.waitForCasfsTimeout }} ))
      until [ -d /sys/module/casfs ]; do
        if [ "$(date +%s)" -ge "$end" ]; then
          echo "casfs is not loaded on this node after {{ .Values.ci.waitForCasfsTimeout }}; see the abfs-casfs-loader logs on the node" >&2
          exit 1
        fi
        sleep 5
      done
      echo "casfs is loaded"
  securityContext:
    {{- include "abfs.restrictedSecurityContext" (dict "uid" .Values.ci.uid "gid" .Values.ci.gid) | nindent 4 }}
  resources:
    requests:
      cpu: 10m
      memory: 32Mi
    limits:
      memory: 64Mi
{{- end -}}

{{/*
Checks that do not belong to one object: the backend names and the server memory bound.
*/}}
{{- define "abfs.validate" -}}
{{- $b := .Values.backend -}}
{{- if .Values.server.enabled -}}
{{- range $k := list "project" "spannerInstance" "spannerDatabase" "bigtableInstance" "bucket" "zone" -}}
{{- if not (index $b $k) -}}
{{- fail (printf "backend.%s is required and must equal the KCC name" $k) -}}
{{- end -}}
{{- end -}}
{{- $c := .Values.server.caches -}}
{{- $need := add (mul (int64 $c.headers) 180) (mul (int64 $c.contents) 49000) 6442450944 -}}
{{- $limit := include "abfs.bytes" .Values.server.memoryLimit | int64 -}}
{{- if gt $need $limit -}}
{{- fail (printf "server.memoryLimit %s (%d bytes) is below the cache bounds plus 6 GiB (%d bytes): headers %d x 180 B, contents %d x 49 KB" (toString .Values.server.memoryLimit) $limit $need (int64 $c.headers) (int64 $c.contents)) -}}
{{- end -}}
{{- end -}}
{{- if .Values.ci.localSsd.enabled -}}
{{- fail "ci.localSsd.enabled is not implemented in this chart version" -}}
{{- end -}}
{{- end -}}

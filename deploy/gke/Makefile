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

# Checks and builds of the GKE deployment. Run from deploy/gke or the root of the
# release bundle. lint, template, kustomize, examples and ddl-check need helm and kubectl,
# and no network. ddl, and ddl-check with DDL_SOURCE, read the GCP server schema
# from a source tree or a commit of DDL_REPO. images and bundle need the ABFS
# repository and docker; bundle also needs the output of images.

SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

CHART := chart/abfs
CI := $(CHART)/ci
KCC := kcc
OUT := out

# Test values: full values files, and overlays that go on top of the seed values.
CI_VALUES := $(CI)/seed-values.yaml $(CI)/steady-values.yaml
CI_OVERLAYS := $(filter-out $(CI)/fail-%,$(wildcard $(CI)/*-overlay.yaml))
# Overlays whose render must fail.
CI_FAILS := $(wildcard $(CI)/fail-*-overlay.yaml)
# The example values carry no digests and no VIP; these stand in for them.
EXAMPLE_VALUES := $(CHART)/values-example.yaml
EXAMPLE_SET := --set image.allowTag=true --set gateway.vip=10.0.0.10

OVERLAYS := $(wildcard $(KCC)/overlays/*/kustomization.yaml)

# The smoke test files of docs/install.md.
EXAMPLES := docs/examples

# images and bundle.
REGISTRY ?= europe-west4-docker.pkg.dev/example-project/abfs
COMMIT ?= HEAD
CASFS ?=
IMAGES_TXT ?= $(OUT)/images/images.txt

.PHONY: all check lint template kustomize examples ddl ddl-check images bundle clean

all: check

check: lint template kustomize examples ddl-check

lint:
	@for v in $(CI_VALUES); do \
	  echo "== helm lint $$v"; \
	  helm lint --strict $(CHART) -f $$v; \
	done
	@for o in $(CI_OVERLAYS); do \
	  echo "== helm lint $(CI)/seed-values.yaml + $$o"; \
	  helm lint --strict $(CHART) -f $(CI)/seed-values.yaml -f $$o; \
	done
	@echo "== helm lint $(EXAMPLE_VALUES)"
	@helm lint --strict $(CHART) -f $(EXAMPLE_VALUES) $(EXAMPLE_SET)

# Helm 4 `lint --strict` reports a template fail() only as information and does
# not fail on it, so the render checks and the expected failures are here. An
# overlay's "# expect: <text>" lines name text its render must contain.
template:
	@mkdir -p $(OUT)/template
	@for v in $(CI_VALUES); do \
	  n=$$(basename $$v .yaml); \
	  helm template abfs $(CHART) -n abfs -f $$v >$(OUT)/template/$$n.yaml; \
	  echo "OK: $$v ($$(grep -c '^kind:' $(OUT)/template/$$n.yaml) objects)"; \
	done
	@for o in $(CI_OVERLAYS); do \
	  n=$$(basename $$o .yaml); \
	  helm template abfs $(CHART) -n abfs -f $(CI)/seed-values.yaml -f $$o >$(OUT)/template/$$n.yaml; \
	  echo "OK: seed-values + $$o ($$(grep -c '^kind:' $(OUT)/template/$$n.yaml) objects)"; \
	  srv=; proj=; read -r srv proj < <(sed -n 's/^# expect-manifest: //p' $$o) || true; \
	  if [[ -n "$$srv" ]]; then \
	    for want in "manifest-server: $$srv" "manifest-project-name: $$proj" \
	        "--kv \"manifest-server=$$srv\"" "--kv \"manifest-project-name=$$proj\""; do \
	      grep -qF -- "$$want" $(OUT)/template/$$n.yaml || { echo "FAIL: $$o: no '$$want' in the render" >&2; exit 1; }; \
	    done; \
	    echo "OK: $$o gives the clients $$srv $$proj"; \
	  fi; \
	  while IFS= read -r want; do \
	    grep -qF -- "$$want" $(OUT)/template/$$n.yaml || { echo "FAIL: $$o: no '$$want' in the render" >&2; exit 1; }; \
	    echo "OK: $$o renders '$$want'"; \
	  done < <(sed -n 's/^# expect: //p' $$o); \
	done
	@helm template abfs $(CHART) -n abfs -f $(EXAMPLE_VALUES) $(EXAMPLE_SET) >$(OUT)/template/example.yaml
	@echo "OK: $(EXAMPLE_VALUES) ($$(grep -c '^kind:' $(OUT)/template/example.yaml) objects)"
	@show() { helm template abfs $(CHART) -n abfs -f $(CI)/seed-values.yaml --show-only "templates/$$1" "$${@:2}"; } ; \
	want() { grep -qxF -- "$$2" <<<"$$1" || { echo "FAIL: $$3 lacks '$$2'" >&2; exit 1; }; } ; \
	s=$$(show server-deployment.yaml); \
	for w in '      automountServiceAccountToken: false' '          topologyKey: kubernetes.io/hostname' \
	    '          whenUnsatisfiable: ScheduleAnyway' '          nodeTaintsPolicy: Honor' '          startupProbe:'; do \
	  want "$$s" "$$w" "the server Deployment"; \
	done; \
	[[ $$(grep -A2 '^          startupProbe:$$' <<<"$$s" | tail -n 1) == '              port: 50051' ]] || \
	  { echo "FAIL: the server startup probe is not TCP on 50051" >&2; exit 1; }; \
	echo "OK: the server has a TCP startup probe, spreads over nodes and mounts no token"; \
	[[ $$(show pdb.yaml | grep -cxF '  unhealthyPodEvictionPolicy: AlwaysAllow') == 2 ]] || \
	  { echo "FAIL: not both PodDisruptionBudgets allow the eviction of pods that are not ready" >&2; exit 1; }; \
	echo "OK: both PodDisruptionBudgets allow the eviction of pods that are not ready"; \
	p=$$(show pusher-statefulset.yaml); \
	want "$$p" '              path: /info' "the pusher StatefulSet"; \
	if grep -qE '^          (liveness|startup)Probe:' <<<"$$p"; then echo "FAIL: the pusher has a liveness or startup probe" >&2; exit 1; fi; \
	echo "OK: the pusher is ready on /info and has no liveness probe"; \
	v=$$(show cacheman-vpa.yaml); \
	for w in '    kind: DaemonSet' '    updateMode: "Off"' '        controlledResources: ["memory"]'; do \
	  want "$$v" "$$w" "the cacheman VerticalPodAutoscaler"; \
	done; \
	if show cacheman-vpa.yaml --set ci.enabled=false | grep -q '^kind:'; then \
	  echo "FAIL: ci.enabled=false still renders the cacheman VerticalPodAutoscaler" >&2; exit 1; \
	fi; \
	echo "OK: the cacheman VerticalPodAutoscaler is Off, memory only, and only with ci.enabled"; \
	pc=$$(show ci-priorityclass.yaml); \
	for w in 'kind: PriorityClass' '  name: abfs-ci-node' 'value: 1000000000' 'globalDefault: false' \
	    'preemptionPolicy: PreemptLowerPriority'; do \
	  want "$$pc" "$$w" "the CI PriorityClass"; \
	done; \
	for t in cacheman-daemonset.yaml casfs-loader-daemonset.yaml; do \
	  want "$$(show $$t)" '      priorityClassName: abfs-ci-node' "templates/$$t"; \
	done; \
	if show ci-priorityclass.yaml --set ci.enabled=false | grep -q '^kind:'; then \
	  echo "FAIL: ci.enabled=false still renders the CI PriorityClass" >&2; exit 1; \
	fi; \
	echo "OK: cacheman and the casfs loader run at the PriorityClass abfs-ci-node, which preempts build pods"; \
	c=$$(show computeclasses.yaml | awk '/^  name: abfs-ci$$/ { c = 1 } /^---$$/ { c = 0 } c'); \
	for w in '    imageType: cos_containerd' '      enableSecureBoot: true'; do \
	  want "$$c" "$$w" "the CI ComputeClass with ci.casfs.provider image"; \
	done; \
	echo "OK: the CI ComputeClass runs COS with secure boot on for ci.casfs.provider image"; \
	j=$$(show ci-example-job.yaml --set ci.exampleJob.enabled=true --set ci.exampleJob.image=example.com/android-build:1); \
	want "$$j" '              set -euo pipefail' "the example Job"; \
	[[ $$(grep -A2 -xF '              cd "$$WORK/src"' <<<"$$j" | tail -n 2 | tr '\n' '|') == \
	    '              set +u|              source build/envsetup.sh && lunch "$$TARGET" && m|' ]] || \
	  { echo "FAIL: the example Job does not run its default command after set +u" >&2; exit 1; }; \
	echo "OK: the example Job runs the ABFS steps under set -euo pipefail and its command after set +u"; \
	helm template abfs $(CHART) -n abfs -f $(CI)/seed-values.yaml -f $(CI)/fail-evict-threshold-overlay.yaml \
	  --set ci.cacheman.evictThreshold=null >/dev/null || \
	  { echo "FAIL: --set ci.cacheman.evictThreshold=null does not clear the removed value" >&2; exit 1; }; \
	echo "OK: --set ci.cacheman.evictThreshold=null clears the removed value"
	@for o in $(CI_FAILS); do \
	  if helm template abfs $(CHART) -n abfs -f $(CI)/seed-values.yaml -f $$o >/dev/null 2>$(OUT)/template/fail.err; then \
	    echo "FAIL: seed-values + $$o rendered, but must fail" >&2; exit 1; \
	  fi; \
	  echo "OK: seed-values + $$o fails as expected: $$(sed -n 's/^Error: execution error at ([^)]*): //p' $(OUT)/template/fail.err | head -n 1)"; \
	done
	@if helm template abfs $(CHART) -n abfs -f $(CI)/seed-values.yaml --set image.server.digest= >/dev/null 2>$(OUT)/template/fail.err; then \
	  echo "FAIL: an empty digest rendered, but must fail" >&2; exit 1; \
	fi
	@echo "OK: an empty digest fails as expected: $$(grep -o 'image.server.digest.*' $(OUT)/template/fail.err)"

# The smoke test values render on the example values, replace its pool, and give the
# clients the server and project of the manifest host; the host file builds, and its
# Service has the name, namespace and port of the pool URL.
examples:
	@mkdir -p $(OUT)/template $(OUT)/kustomize
	@v=$(EXAMPLES)/values-smoke.yaml; r=$(OUT)/template/smoke.yaml; \
	helm template abfs $(CHART) -n abfs -f $(EXAMPLE_VALUES) $(EXAMPLE_SET) -f $$v >$$r; \
	url=$$(sed -n 's/^ *url: //p' $$v); \
	[[ $$url =~ ^http://([a-z0-9-]+)\.([a-z0-9-]+)\.svc\.cluster\.local:([0-9]+)/([a-z]+)$$ ]] || \
	  { echo "FAIL: $$v: the pool URL $$url is not http://<service>.<namespace>.svc.cluster.local:<port>/<project>" >&2; exit 1; }; \
	svc=$${BASH_REMATCH[1]}; ns=$${BASH_REMATCH[2]}; port=$${BASH_REMATCH[3]}; proj=$${BASH_REMATCH[4]}; \
	for want in "url: $$url" "manifest-server: $$svc.$$ns.svc.cluster.local:$$port" "manifest-project-name: $$proj" \
	    "manifest-scheme: http" "cloud.google.com/compute-class: abfs-pusher-steady"; do \
	  grep -qF -- "$$want" $$r || { echo "FAIL: $$v: no '$$want' in the render" >&2; exit 1; }; \
	done; \
	if grep -q 'platform/manifest' $$r; then echo "FAIL: $$v keeps the pool of $(EXAMPLE_VALUES)" >&2; exit 1; fi; \
	echo "OK: $$v gives the clients $$svc.$$ns.svc.cluster.local:$$port $$proj"; \
	t=$$(mktemp -d); trap 'rm -rf "$$t"' EXIT; \
	cp $(EXAMPLES)/smoke-manifest-host.yaml "$$t/"; \
	printf 'apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - smoke-manifest-host.yaml\n' >"$$t/kustomization.yaml"; \
	h=$(OUT)/kustomize/smoke-manifest-host.yaml; kubectl kustomize "$$t" >$$h; \
	[[ $$(grep -c '^kind:' $$h) == 3 ]] || { echo "FAIL: smoke-manifest-host.yaml does not hold three objects" >&2; exit 1; }; \
	awk '/^kind: Service$$/ { s = 1 } /^---$$/ { s = 0 } s' $$h >"$$t/svc.yaml"; \
	for want in "  name: $$svc" "  namespace: $$ns" "    port: $$port"; do \
	  grep -qxF -- "$$want" "$$t/svc.yaml" || { echo "FAIL: the Service of smoke-manifest-host.yaml lacks '$$want'" >&2; exit 1; }; \
	done; \
	grep -qF "path: /$$proj/info/refs" $$h || { echo "FAIL: smoke-manifest-host.yaml does not serve /$$proj" >&2; exit 1; }; \
	echo "OK: $(EXAMPLES)/smoke-manifest-host.yaml (3 objects; Service $$svc.$$ns port $$port serves /$$proj)"

# Each overlay, then the components the example overlay leaves out, then the
# rules that the two backend modes exclude each other, that backend-new holds
# no garbage collector resource, and that gc builds only with backend-new.
kustomize:
	@mkdir -p $(OUT)/kustomize
	@for k in $(OVERLAYS); do \
	  d=$$(dirname $$k); n=$$(basename $$d); \
	  kubectl kustomize $$d >$(OUT)/kustomize/$$n.yaml; \
	  echo "OK: $$d ($$(grep -c '^kind:' $(OUT)/kustomize/$$n.yaml) objects)"; \
	done
	@t=$$(mktemp -d); trap 'rm -rf "$$t"' EXIT; \
	k=$$(realpath --relative-to="$$t" $(KCC)); \
	cp $(KCC)/overlays/example/settings.yaml "$$t/"; \
	kz() { printf 'apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - %s/base\n  - settings.yaml\ncomponents:\n' "$$k"; \
	  for c in "$$@"; do printf '  - %s/components/%s\n' "$$k" "$$c"; done; } ; \
	kz backend-existing network/proxy-subnet settings >"$$t/kustomization.yaml"; \
	kubectl kustomize "$$t" >$(OUT)/kustomize/existing-proxy-subnet.yaml; \
	echo "OK: backend-existing + network/proxy-subnet ($$(grep -c '^kind:' $(OUT)/kustomize/existing-proxy-subnet.yaml) objects)"; \
	kz backend-new backend-existing settings >"$$t/kustomization.yaml"; \
	if kubectl kustomize "$$t" >/dev/null 2>&1; then \
	  echo "FAIL: backend-new and backend-existing together built, but must fail" >&2; exit 1; \
	fi; \
	echo "OK: backend-new + backend-existing fails as expected"; \
	kz backend-new settings >"$$t/kustomization.yaml"; \
	kubectl kustomize "$$t" >$(OUT)/kustomize/new.yaml; \
	if grep -qE 'name: abfs-(gc|table-gc|gc-policy-(gc|objects-g))$$|family: g$$|Gc[A-Z][a-z]+ \(' $(OUT)/kustomize/new.yaml; then \
	  echo "FAIL: backend-new without gc holds a garbage collector resource" >&2; exit 1; \
	fi; \
	echo "OK: backend-new without gc holds no garbage collector resource ($$(grep -c '^kind:' $(OUT)/kustomize/new.yaml) objects)"; \
	kz backend-new gc settings >"$$t/kustomization.yaml"; \
	kubectl kustomize "$$t" >$(OUT)/kustomize/new-gc.yaml; \
	[[ $$(grep -cE '^  name: abfs-(table-gc|gc-policy-(gc-[et]|objects-g))$$|^  - family: g$$' $(OUT)/kustomize/new-gc.yaml) == 5 ]] || \
	  { echo "FAIL: backend-new + gc lacks the table gc, the family objects:g or their policies" >&2; exit 1; }; \
	fam() { awk '/^kind: BigtableTable$$/ { t = 1 } /^---$$/ { t = 0 } t && /^  name: abfs-table-objects$$/ { o = 1 } \
	  o && /^  - family: / { printf "%s ", $$3 } /^---$$/ { o = 0 }' "$$1"; } ; \
	[[ $$(fam $(OUT)/kustomize/new.yaml) == "meta data " ]] || \
	  { echo "FAIL: the table objects of backend-new has the families $$(fam $(OUT)/kustomize/new.yaml), not meta data" >&2; exit 1; }; \
	[[ $$(fam $(OUT)/kustomize/new-gc.yaml) == "meta data g " ]] || \
	  { echo "FAIL: the table objects of backend-new + gc has the families $$(fam $(OUT)/kustomize/new-gc.yaml), not meta data g" >&2; exit 1; }; \
	awk '/^kind: BigtableAppProfile$$/ { p = 1 } /^---$$/ { p = 0 } p' $(OUT)/kustomize/new-gc.yaml >"$$t/profile.yaml"; \
	for want in '  name: abfs-gc' '  resourceID: abfs-gc' '    name: abfs-objects' \
	    '    allowTransactionalWrites: true' '    clusterId: abfs-objects-c1' '    priority: PRIORITY_LOW'; do \
	  grep -qxF -- "$$want" "$$t/profile.yaml" || { echo "FAIL: the app profile abfs-gc of backend-new + gc lacks '$$want'" >&2; exit 1; }; \
	done; \
	echo "OK: backend-new + gc ($$(grep -c '^kind:' $(OUT)/kustomize/new-gc.yaml) objects; objects families meta data g; app profile abfs-gc)"; \
	kz backend-existing gc settings >"$$t/kustomization.yaml"; \
	if kubectl kustomize "$$t" >/dev/null 2>&1; then \
	  echo "FAIL: backend-existing and gc together built, but must fail" >&2; exit 1; \
	fi; \
	echo "OK: backend-existing + gc fails as expected"

# The schema source of ddl and ddl-check: an ABFS source tree, or a commit of
# DDL_REPO that has the GCP server, for example
#   make check DDL_REPO=<abfs repo> DDL_SOURCE=<commit>
DDL_FILE := $(KCC)/components/backend-new/spanner-database-ddl.yaml
DDL_SOURCE ?=
DDL_REPO ?= .

# Writes the ddl list of DDL_SOURCE to out/ for review. It never writes the KCC
# file: entries there are only ever appended by hand (see docs/runbook.md).
ddl:
	@[[ -n "$(DDL_SOURCE)" ]] || { echo "set DDL_SOURCE=<source dir | commit>, and DDL_REPO=<abfs repo> for a commit" >&2; exit 2; }
	@mkdir -p $(OUT)
	@scripts/gen-ddl.sh -C $(DDL_REPO) -o $(OUT)/spanner-database-ddl.yaml $(DDL_SOURCE)
	@if diff -q $(OUT)/spanner-database-ddl.yaml $(DDL_FILE) >/dev/null; then \
	  echo "OK: $(OUT)/spanner-database-ddl.yaml equals the KCC file"; \
	else \
	  echo "$(OUT)/spanner-database-ddl.yaml differs from the KCC file:"; \
	  diff -u $(DDL_FILE) $(OUT)/spanner-database-ddl.yaml || true; \
	fi

# Without DDL_SOURCE the list is checked against the ddl-base hash of the file.
ddl-check:
	@scripts/check-ddl.sh -C $(DDL_REPO) $(DDL_FILE) $(DDL_SOURCE)

# Builds the four images locally, without a push. CASFS is the abfs-casfs
# repository. The base images and Go modules must be in the local caches to
# build offline.
images:
	@[[ -n "$(CASFS)" ]] || { echo "set CASFS=<abfs-casfs repository>" >&2; exit 2; }
	scripts/build-images.sh --casfs $(CASFS) --work $(OUT)/images $(REGISTRY) $(COMMIT)

# The release bundle from the local images of `make images`.
bundle:
	@[[ -f "$(IMAGES_TXT)" ]] || { echo "$(IMAGES_TXT) not found; run make images first" >&2; exit 2; }
	@[[ -n "$(CASFS)" ]] || { echo "set CASFS=<abfs-casfs repository>" >&2; exit 2; }
	scripts/bundle.sh --images $(IMAGES_TXT) --casfs $(CASFS) --from-docker --out $(OUT) $(COMMIT)

clean:
	rm -rf $(OUT)

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
# release bundle. lint, template, kustomize and ddl-check need helm and kubectl,
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

# images and bundle.
REGISTRY ?= europe-west4-docker.pkg.dev/example-project/abfs
COMMIT ?= HEAD
CASFS ?=
IMAGES_TXT ?= $(OUT)/images/images.txt

.PHONY: all check lint template kustomize ddl ddl-check images bundle clean

all: check

check: lint template kustomize ddl-check

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
# not fail on it, so the render checks and the expected failures are here.
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
	done
	@helm template abfs $(CHART) -n abfs -f $(EXAMPLE_VALUES) $(EXAMPLE_SET) >$(OUT)/template/example.yaml
	@echo "OK: $(EXAMPLE_VALUES) ($$(grep -c '^kind:' $(OUT)/template/example.yaml) objects)"
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

# Each overlay, then the components the example overlay leaves out, then the
# rule that the two backend modes exclude each other.
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
	echo "OK: backend-new + backend-existing fails as expected"

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
	scripts/bundle.sh --images $(IMAGES_TXT) --from-docker --out $(OUT) $(COMMIT)

clean:
	rm -rf $(OUT)

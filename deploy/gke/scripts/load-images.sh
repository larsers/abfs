#!/bin/bash

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

# Copies the four images of a release bundle into a registry and prints the
# image values of the chart, with the digests the registry reports:
#   load-images.sh europe-west4-docker.pkg.dev/<project>/abfs >values-images.yaml
#   helm install ... -f <values.yaml> -f values-images.yaml
# Each image keeps the tag of its images.txt line. skopeo takes the registry
# credentials from the docker configuration, so for Artifact Registry run
# `gcloud auth configure-docker <region>-docker.pkg.dev` first.
#
# Usage: load-images.sh [--bundle <dir>] <registry>
#   --bundle <dir>   root of the unpacked bundle (default: the parent of this script)

set -euo pipefail

usage() {
  sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

bundle=$(cd "$(dirname "$0")/.." && pwd)
args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bundle) bundle=$2; shift 2 ;;
    -h | --help) usage ;;
    -*) echo "$0: unknown option $1" >&2; usage ;;
    *) args+=("$1"); shift ;;
  esac
done
[[ ${#args[@]} -eq 1 ]] || usage
registry=${args[0]%/}
[[ -f $bundle/images.txt ]] || { echo "$0: $bundle/images.txt not found; pass --bundle <dir>" >&2; exit 1; }
command -v skopeo >/dev/null || { echo "$0: skopeo not found" >&2; exit 2; }

# The chart's value key of each image name.
declare -A key=([abfs-server]=server [abfs-pusher]=pusher [abfs-client]=client [casfs-loader]=casfsLoader)
declare -A digest=()
while read -r name ref _; do
  [[ -n $name ]] || continue
  [[ -n ${key[$name]:-} ]] || { echo "$0: unknown image $name in images.txt" >&2; exit 1; }
  archive=$bundle/images/$name.oci.tar
  [[ -f $archive ]] || { echo "$0: $archive not found" >&2; exit 1; }
  dest=$registry/$name:${ref##*:}
  echo "== $name -> $dest" >&2
  skopeo copy --quiet "oci-archive:$archive" "docker://$dest" >&2
  digest[$name]=$(skopeo inspect --format '{{.Digest}}' "docker://$dest")
  echo "   ${digest[$name]}" >&2
done <"$bundle/images.txt"
((${#digest[@]} == 4)) || { echo "$0: copied ${#digest[@]} images, not 4" >&2; exit 1; }

echo "image:"
echo "  registry: $registry"
for name in abfs-server abfs-pusher abfs-client casfs-loader; do
  echo "  ${key[$name]}:"
  echo "    digest: ${digest[$name]}"
done

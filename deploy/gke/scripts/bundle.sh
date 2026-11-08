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

# Writes the release bundle abfs-gke-<version>.tar.gz of an ABFS commit:
#   abfs-gke-<version>/
#     COMMIT          the commit id
#     AGENTS.md, README.md, Makefile, chart/, kcc/, docs/, scripts/   deploy/gke of that commit
#     images.txt      name, reference and digest of each image
#     images/<name>.oci.tar            the images as OCI archives
#     source/abfs-<version>.tar.gz     the ABFS tree of the commit
#     source/abfs-casfs-<commit>.tar.gz   the abfs-casfs tree of the loader image
#     SHA256SUMS      of every other file
# The images are those of the images.txt that build-images.sh wrote for the
# same commit. By default they are copied from the registry by digest with
# skopeo; --from-docker saves the local images with docker save instead.
#
# Usage: bundle.sh [options] <commit>
#   -C <repo>            ABFS repository (default: the one holding this script)
#   --images <file>      images.txt of build-images.sh (required)
#   --casfs <repo>       abfs-casfs repository (required)
#   --casfs-commit <c>   the casfs commit of the loader image (default c946f8e)
#   --from-docker        take the images from the local docker engine
#   --deploy-dir <dir>   take deploy/gke from <dir>, not from the commit
#   --out <dir>          where to write the bundle (default: .)

set -euo pipefail

usage() {
  sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

here=$(cd "$(dirname "$0")" && pwd)
repo=
images=
casfs_repo=
casfs_commit=c946f8e
from_docker=false
deploy_dir=
out=.
args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -C) repo=$2; shift 2 ;;
    --images) images=$2; shift 2 ;;
    --casfs) casfs_repo=$2; shift 2 ;;
    --casfs-commit) casfs_commit=$2; shift 2 ;;
    --from-docker) from_docker=true; shift ;;
    --deploy-dir) deploy_dir=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    -h | --help) usage ;;
    -*) echo "$0: unknown option $1" >&2; usage ;;
    *) args+=("$1"); shift ;;
  esac
done
if [[ -z $repo ]]; then
  repo=$(git -C "$here" rev-parse --show-toplevel 2>/dev/null) ||
    { echo "$0: $here is not in a git checkout; pass -C <ABFS repository>" >&2; exit 2; }
fi
[[ ${#args[@]} -eq 1 && -n $images && -n $casfs_repo ]] || usage
[[ -f $images ]] || { echo "$0: $images not found" >&2; exit 1; }
commit=$(git -C "$repo" rev-parse --verify "${args[0]}^{commit}")
version=$(git -C "$repo" describe --match 'v*' "$commit")
version=${version#v}
casfs_commit=$(git -C "$casfs_repo" rev-parse --verify "$casfs_commit^{commit}")

name=abfs-gke-$version
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
dir=$tmp/$name
mkdir -p "$dir/images"

# The deployment files, from the commit unless a directory is given.
if [[ -n $deploy_dir ]]; then
  cp -a "$deploy_dir/." "$tmp/deploy"
elif git -C "$repo" cat-file -e "$commit:deploy/gke" 2>/dev/null; then
  mkdir -p "$tmp/deploy"
  git -C "$repo" archive "$commit" deploy/gke | tar -x -C "$tmp/deploy" --strip-components=2
else
  echo "$0: $commit has no deploy/gke; pass --deploy-dir" >&2
  exit 1
fi
for part in AGENTS.md README.md Makefile chart kcc docs scripts; do
  if [[ -e $tmp/deploy/$part ]]; then
    cp -a "$tmp/deploy/$part" "$dir/"
  else
    echo "warning: deploy/gke/$part is missing" >&2
  fi
done
echo "$commit" >"$dir/COMMIT"

# The source of the release: what the images were built from.
mkdir -p "$dir/source"
git -C "$repo" archive --format=tar.gz --prefix="abfs-$version/" \
  -o "$dir/source/abfs-$version.tar.gz" "$commit"
git -C "$casfs_repo" archive --format=tar.gz --prefix="abfs-casfs-${casfs_commit:0:7}/" \
  -o "$dir/source/abfs-casfs-${casfs_commit:0:7}.tar.gz" "$casfs_commit"
cp "$images" "$dir/images.txt"

# Each line of images.txt: <name> <reference> <digest>.
while read -r image ref digest; do
  [[ -n $image ]] || continue
  archive=$dir/images/$image.oci.tar
  echo "== $image: $ref $digest"
  if $from_docker; then
    docker save -o "$archive" "$ref"
  else
    [[ $digest == sha256:* ]] || { echo "$0: $image has no registry digest" >&2; exit 1; }
    skopeo copy --quiet "docker://${ref%:*}@$digest" "oci-archive:$archive:${ref##*/}"
  fi
done <"$images"
count=$(find "$dir/images" -name '*.oci.tar' | wc -l)
((count == 4)) || echo "warning: the bundle has $count images, not 4" >&2

(cd "$dir" && find . -type f -printf '%P\n' | LC_ALL=C sort |
  xargs -d '\n' sha256sum) >"$tmp/SHA256SUMS"
mv "$tmp/SHA256SUMS" "$dir/SHA256SUMS"

mkdir -p "$out"
tar -C "$tmp" -czf "$out/$name.tar.gz" "$name"
echo "bundle: $out/$name.tar.gz"

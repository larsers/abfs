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

# Builds the four images of the GKE deployment from one ABFS commit and, with
# --push, pushes them. The build context is a staging directory:
#   abfs/   the ABFS tree of <commit> (git archive, no git metadata)
#   casfs/  the abfs-casfs tree of --casfs-commit
# The version is `git describe --match 'v*'` of <commit>: a plain describe can
# pick a tag that is not a release tag. Writes images.txt (name, reference,
# digest) to the work directory and prints it. Without --push the digest is
# the local image ID; the chart needs the registry digests of a push.
#
# Usage: build-images.sh [options] <registry> <commit>
#   <registry>          e.g. europe-west4-docker.pkg.dev/example-project/abfs
#   -C <repo>           ABFS repository (default: the one holding this script)
#   --casfs <repo>      abfs-casfs repository (needed for casfs-loader)
#   --casfs-commit <c>  default c946f8e
#   --only <names>      comma-separated subset of the four images
#   --work <dir>        staging and output directory (default: a new temp dir)
#   --push              push the images (default: build only)

set -euo pipefail

usage() {
  sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

here=$(cd "$(dirname "$0")" && pwd)
repo=
casfs_repo=
casfs_commit=c946f8e
only=abfs-server,abfs-pusher,abfs-client,casfs-loader
work=
push=false
args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -C) repo=$2; shift 2 ;;
    --casfs) casfs_repo=$2; shift 2 ;;
    --casfs-commit) casfs_commit=$2; shift 2 ;;
    --only) only=$2; shift 2 ;;
    --work) work=$2; shift 2 ;;
    --push) push=true; shift ;;
    -h | --help) usage ;;
    -*) echo "$0: unknown option $1" >&2; usage ;;
    *) args+=("$1"); shift ;;
  esac
done
if [[ -z $repo ]]; then
  repo=$(git -C "$here" rev-parse --show-toplevel 2>/dev/null) ||
    { echo "$0: $here is not in a git checkout; pass -C <ABFS repository>" >&2; exit 2; }
fi
[[ ${#args[@]} -eq 2 ]] || usage
registry=${args[0]%/}
commit=$(git -C "$repo" rev-parse --verify "${args[1]}^{commit}")

version=$(git -C "$repo" describe --match 'v*' "$commit")
if [[ ! $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]]; then
  echo "$0: $version is not a release version" >&2
  exit 1
fi
version=${version#v}

[[ -n $work ]] || work=$(mktemp -d)
ctx=$work/context
rm -rf "$ctx"
mkdir -p "$ctx/abfs"
git -C "$repo" archive "$commit" | tar -x -C "$ctx/abfs"

if [[ ,$only, == *,casfs-loader,* ]]; then
  [[ -n $casfs_repo ]] || { echo "$0: casfs-loader needs --casfs <abfs-casfs repo>" >&2; exit 2; }
  casfs_commit=$(git -C "$casfs_repo" rev-parse --verify "$casfs_commit^{commit}")
  mkdir -p "$ctx/casfs"
  git -C "$casfs_repo" archive "$casfs_commit" | tar -x -C "$ctx/casfs"
  casfs_version=$(tr -d '[:space:]' <"$ctx/casfs/VERSION")
fi

echo "ABFS $commit, version $version; context $ctx"
: >"$work/images.txt"
for name in ${only//,/ }; do
  [[ -f $here/../images/$name/Dockerfile ]] || { echo "$0: no image $name: $here/../images/$name/Dockerfile not found (the release bundle carries no Dockerfiles; run the copy in deploy/gke of an ABFS checkout)" >&2; exit 2; }
  ref=$registry/$name:$version
  build_args=(--build-arg "VERSION=$version")
  if [[ $name == casfs-loader ]]; then
    ref=$registry/$name:$casfs_version-$version
    build_args=(--build-arg "CASFS_VERSION=$casfs_version" --build-arg "CASFS_COMMIT=$casfs_commit")
  fi
  echo "== $name: $ref"
  docker build --platform linux/amd64 -f "$here/../images/$name/Dockerfile" \
    "${build_args[@]}" --label "org.opencontainers.image.revision=$commit" \
    -t "$ref" "$ctx"
  if $push; then
    docker push "$ref" >/dev/null
    digest=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$ref" |
      sed -n "s|^${registry}/${name}@||p" | head -n 1)
    [[ -n $digest ]] || { echo "$0: no registry digest for $ref after the push" >&2; exit 1; }
  else
    digest=$(docker image inspect --format '{{.Id}}' "$ref")
  fi
  echo "$name $ref $digest" >>"$work/images.txt"
done

echo "== images ($([[ $push == true ]] && echo "pushed" || echo "local, not pushed"))"
column -t "$work/images.txt" 2>/dev/null || cat "$work/images.txt"
echo "images.txt: $work/images.txt"

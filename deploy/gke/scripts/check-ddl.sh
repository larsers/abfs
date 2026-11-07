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

# Checks the prefix rule of the SpannerDatabase ddl list: the list must start
# with the statements of the release that first created the database (the
# base), entry for entry; entries after them are appended migrations.
#
# The "# ddl-base:" line of the file records the sha256 and the count of the
# base entries, so the check needs no schema source. Given a schema source
# (as gen-ddl.sh takes it), it also regenerates the base from it, checks that
# its hash equals the recorded one, and shows any entry that differs.
#
# Appended entries carry no hash and are not checked: review each change of
# the file by hand, where an applied entry must stay unchanged. A hash kept in
# the same file would be updated in the same edit, so it adds no guard.
#
# Usage: check-ddl.sh [-C <repo>] [<ddl yaml> [<schema source: source dir | commit>]]

set -euo pipefail

usage() {
  echo "Usage: $0 [-C <repo>] [<ddl yaml> [<schema source: source dir | commit>]]" >&2
  exit 2
}

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=SCRIPTDIR/ddl-lib.sh
. "$here/ddl-lib.sh"

repo=.
while getopts "C:h" opt; do
  case "$opt" in
    C) repo=$OPTARG ;;
    *) usage ;;
  esac
done
shift $((OPTIND - 1))
[[ $# -le 2 ]] || usage
yaml=${1:-$here/../kcc/components/backend-new/spanner-database-ddl.yaml}
src=${2:-}

read -r want_hash want < <(ddl_base "$yaml") ||
  { echo "$0: no \"# ddl-base: sha256:<hex> <n> entries\" line in $yaml" >&2; exit 2; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

have=$(ddl_entries "$yaml" "$tmp/have")
if ((have < want)); then
  echo "FAIL: $yaml has $have ddl entries, fewer than the $want of its base" >&2
  exit 1
fi

if [[ -n $src ]]; then
  "$here/gen-ddl.sh" -C "$repo" -o "$tmp/base.yaml" "$src"
  read -r src_hash src_n < <(ddl_base "$tmp/base.yaml")
  ddl_entries "$tmp/base.yaml" "$tmp/want" >/dev/null
  for ((i = 1; i <= src_n && i <= have; i++)); do
    f=$(printf %04d "$i")
    if ! cmp -s "$tmp/have/$f" "$tmp/want/$f"; then
      echo "FAIL: ddl entry $i of $yaml differs from the schema source $src:" >&2
      diff -u "$tmp/want/$f" "$tmp/have/$f" | tail -n +3 >&2 || true
      exit 1
    fi
  done
  if [[ $src_hash != "$want_hash" || $src_n != "$want" ]]; then
    echo "FAIL: the schema source $src gives $src_n entries with sha256 $src_hash, not the base of $yaml ($want entries, $want_hash)" >&2
    exit 1
  fi
  echo "OK: the schema source $src gives the base of $yaml ($want entries)"
fi

have_hash=$(ddl_hash "$tmp/have" "$want")
if [[ $have_hash != "$want_hash" ]]; then
  echo "FAIL: the first $want ddl entries of $yaml have sha256 $have_hash, not the base $want_hash; an entry of the base was changed" >&2
  exit 1
fi
echo "OK: $yaml has $have ddl entries; the first $want are the base (sha256 $want_hash), $((have - want)) appended"

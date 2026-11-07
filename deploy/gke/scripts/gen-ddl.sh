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

# Writes the ddl patch of the SpannerDatabase from the GCP server schema,
# shared/remote/servers/gcp/casdb/schema.sql and gcschema.sql: the statements
# of `abfs server gcp init --gc` in the same order. The schema source is an
# ABFS source tree, that casdb directory, or a commit of the repository given
# with -C that has the GCP server.
#
# Run it once, for the release that first creates the database. Config
# Connector sends only the entries past the old list length and refuses a
# changed entry, so a later schema change is an entry appended by hand, never a
# new run of this script over the file. check-ddl.sh checks that rule against
# the ddl-base line, the sha256 of the entries written here.
#
# Usage: gen-ddl.sh [-C <repo>] [-o <file>] <schema source: source dir | commit>

set -euo pipefail

usage() {
  echo "Usage: $0 [-C <repo>] [-o <file>] <schema source: source dir | commit>" >&2
  exit 2
}

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=SCRIPTDIR/ddl-lib.sh
. "$here/ddl-lib.sh"

repo=.
out=-
while getopts "C:o:h" opt; do
  case "$opt" in
    C) repo=$OPTARG ;;
    o) out=$OPTARG ;;
    *) usage ;;
  esac
done
shift $((OPTIND - 1))
[[ $# -eq 1 ]] || usage
src=$1

casdb=shared/remote/servers/gcp/casdb
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Read the two schema files from a directory or from a commit.
if [[ -d $src ]]; then
  dir=$src
  [[ -f $dir/$casdb/schema.sql ]] && dir=$dir/$casdb
  for f in schema.sql gcschema.sql; do
    [[ -f $dir/$f ]] || { echo "$0: $dir/$f not found" >&2; exit 1; }
    cp "$dir/$f" "$tmp/$f"
  done
else
  commit=$(git -C "$repo" rev-parse --verify --quiet "$src^{commit}") ||
    { echo "$0: $src is neither a directory nor a commit of $repo" >&2; exit 1; }
  for f in schema.sql gcschema.sql; do
    git -C "$repo" show "$commit:$casdb/$f" >"$tmp/$f" 2>/dev/null ||
      { echo "$0: $commit of $repo has no $casdb/$f" >&2; exit 1; }
  done
fi

# Split as casdb.splitStatements does: blank the comment lines, split on ";",
# trim. Each statement becomes one block scalar entry.
statements() {
  awk '
    /^[ \t]*--/ { $0 = "" }
    { buf = buf $0 "\n" }
    END {
      n = split(buf, parts, ";")
      for (i = 1; i <= n; i++) {
        s = parts[i]
        sub(/^[ \t\r\n]+/, "", s)
        sub(/[ \t\r\n]+$/, "", s)
        if (s == "") continue
        print "    - |-"
        m = split(s, lines, "\n")
        for (j = 1; j <= m; j++) {
          if (lines[j] == "") print ""
          else print "      " lines[j]
        }
      }
    }' "$1"
}

{
  echo "  ddl:"
  statements "$tmp/schema.sql"
  statements "$tmp/gcschema.sql"
} >"$tmp/list.yaml"
n=$(ddl_entries "$tmp/list.yaml" "$tmp/entries")
hash=$(ddl_hash "$tmp/entries" "$n")

{
  sed -n '3,15p' "$0"
  echo
  echo "# Written by scripts/gen-ddl.sh from the GCP server schema files"
  echo "# casdb/schema.sql and casdb/gcschema.sql of the release that first created"
  echo "# the database. Never edit or remove an entry: Config Connector sends only"
  echo "# the entries past the old list length and refuses a changed one. A later"
  echo "# schema change is a new ALTER or CREATE ... IF NOT EXISTS entry appended at"
  echo "# the end, in the same \"- |-\" form. The base line is the sha256 of the"
  echo "# first entries; scripts/check-ddl.sh checks the list against it."
  echo "# ddl-base: sha256:$hash $n entries"
  echo "apiVersion: spanner.cnrm.cloud.google.com/v1beta1"
  echo "kind: SpannerDatabase"
  echo "metadata:"
  echo "  name: abfs-cas"
  echo "  namespace: abfs"
  echo "spec:"
  cat "$tmp/list.yaml"
} >"$tmp/out.yaml"

if [[ $out == - ]]; then
  cat "$tmp/out.yaml"
else
  cp "$tmp/out.yaml" "$out"
fi

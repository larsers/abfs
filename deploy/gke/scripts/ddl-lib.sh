# shellcheck shell=bash

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

# Functions shared by gen-ddl.sh and check-ddl.sh. Source it; it runs nothing.
#
# The base of the ddl list is recorded in the KCC file as
#   # ddl-base: sha256:<hex> <n> entries
# where <hex> is the sha256 of the first <n> entries, each with its block
# indentation removed and followed by a NUL byte. The hash is taken from the
# entries as the file holds them, so a check needs no schema source.

# ddl_entries <yaml> <dir>: writes each entry of the ddl list in <yaml> to its
# own file under <dir>, named by its position, with the block indentation
# removed, and prints the number of entries. Entries must be "- |-" block
# scalars, the form gen-ddl.sh writes.
ddl_entries() {
  mkdir -p "$2"
  awk -v dir="$2" '
    function indent(s) { match(s, /^ */); return RLENGTH }
    function fail(msg) { printf "%s:%d: %s\n", FILENAME, NR, msg > "/dev/stderr"; bad = 1; exit 1 }
    !inlist && /^ *ddl: *$/ { inlist = 1; ddl = indent($0); next }
    !inlist { next }
    /^ *$/ { if (n) pending = pending "\n"; next }
    indent($0) <= ddl { exit }
    /^ *- / {
      if ($0 !~ /^ *- \|- *$/) fail("ddl entry is not a \"- |-\" block")
      n++; dash = indent($0); body = -1; pending = ""
      file = sprintf("%s/%04d", dir, n)
      printf "" > file
      next
    }
    {
      if (!n || indent($0) <= dash) fail("unexpected line in the ddl list")
      if (body < 0) body = indent($0)
      if (indent($0) < body) fail("line is indented less than its block")
      printf "%s%s%s", (started[n] ? "\n" : ""), pending, substr($0, body + 1) > file
      started[n] = 1; pending = ""
    }
    END {
      if (bad) exit 1
      if (!inlist) { printf "%s: no ddl list\n", FILENAME > "/dev/stderr"; exit 1 }
      print n + 0
    }' "$1"
}

# ddl_hash <dir> <n>: the sha256 of the first <n> entries that ddl_entries
# wrote to <dir>.
ddl_hash() {
  local i
  for ((i = 1; i <= $2; i++)); do
    cat "$1/$(printf %04d "$i")"
    printf '\0'
  done | sha256sum | cut -d ' ' -f 1
}

# ddl_base <yaml>: prints "<hex> <n>" from the ddl-base line of <yaml>.
ddl_base() {
  sed -n 's/^# ddl-base: sha256:\([0-9a-f]\{64\}\) \([0-9]\{1,\}\) entries$/\1 \2/p' "$1" | head -n 1
}

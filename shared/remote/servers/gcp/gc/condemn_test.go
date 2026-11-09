// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Each documented precondition of a condemn stops it on its own, and the
// counts that do not hide a root do not.
func TestCondemnBlockers(t *testing.T) {
	o := DefaultCondemnOpts()
	o.ExpectedReplicas = 3
	now := time.Now()
	replica := func(id string) Replica {
		return Replica{ID: id, Version: ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour, LastSeen: now.Add(-time.Minute)}
	}
	replicas := func(ids ...string) func(r *Report) {
		return func(r *Report) {
			var rows []Replica
			for _, id := range ids {
				rows = append(rows, replica(id))
			}
			r.Replicas = checkReplicas(rows, now, &o.DryRunOpts)
		}
	}
	dangling := func(class string) func(r *Report) {
		return func(r *Report) { r.Roots[class] = &RootCounts{Roots: 5, Dangling: 1} }
	}
	tests := []struct {
		name   string
		change func(r *Report)
		want   []string
	}{
		{"clean", func(r *Report) {}, nil},
		{"replica warning", func(r *Report) {
			bad := replica("srv-2-cc")
			bad.TouchInterval = 0
			r.Replicas = checkReplicas([]Replica{replica("srv-0-aa"), replica("srv-1-bb"), bad}, now, &o.DryRunOpts)
		}, []string{"replica srv-2-cc: writes no touches"}},
		{"one GC host short", replicas("srv-0-aa", "srv-1-bb"), []string{"2 server hosts write a GC heartbeat, but 3 are expected"}},
		// One host more than expected can stand in for a host without GC.
		{"one GC host more than expected", replicas("srv-0-aa", "srv-1-bb", "srv-2-cc", "srv-3-dd"), []string{"4 server hosts write a GC heartbeat, but 3 are expected"}},
		{"two live rows from one host", replicas("srv-0-aa", "srv-0-dd", "srv-1-bb", "srv-2-cc"), []string{"host srv-0 has 2 live GC heartbeat rows"}},
		// A restarted host's old row must not stand in for a host without GC.
		{"one GC host short with a restarted host's old row live", replicas("srv-0-aa", "srv-0-dd", "srv-1-bb"), []string{
			"2 server hosts write a GC heartbeat, but 3 are expected", "host srv-0 has 2 live GC heartbeat rows",
		}},
		{"no consistency check", func(r *Report) { r.Consistency.Ran = false }, []string{"the consistency check did not run"}},
		{"missing gc rows", func(r *Report) { r.Consistency.MissingGCRows = 2 }, []string{"2 objects have no gc row"}},
		{"outdated rows", func(r *Report) { r.Objects.OutdatedRows = 1 }, []string{"1 gc rows have outdated edge rules"}},
		{"newer rows", func(r *Report) { r.Objects.NewerRows = 1 }, []string{"1 gc rows have newer edge rules"}},
		{"mount read error", func(r *Report) { r.LiveMounts.Errors = 1 }, []string{"the pins of 1 live mounts could not be read"}},
		{"workspace read error", func(r *Report) { r.Workspaces.Errors = 1 }, []string{"the pins of 1 workspaces could not be read"}},
		{"snapshot read error", func(r *Report) { r.Snapshots.Errors = 1 }, []string{"the pins of 1 snapshots could not be read"}},
		{"dangling source root", dangling("source"), []string{"1 roots of class source have no gc row"}},
		{"dangling project-status root", dangling("project-status"), []string{"class project-status"}},
		{"dangling pushed-packs root", dangling("pushed-packs"), []string{"class pushed-packs"}},
		{"dangling workspace root", dangling("workspace"), []string{"class workspace"}},
		{"dangling snapshot root", dangling("snapshot"), []string{"class snapshot"}},
		{"dangling mount root", dangling("mount"), []string{"class mount"}},
		{"dangling permanent root", dangling("permanent"), []string{"class permanent"}},
		// Projects skipped with --ignore-missing-projects pin commits that were
		// never pushed; a missing object pins nothing.
		{"dangling pinned revision", dangling(RootPinnedRevisions), nil},
		{"missing mount details", func(r *Report) { r.LiveMounts.NotFound, r.Workspaces.NotFound, r.Snapshots.NotFound = 1, 1, 1 }, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Report{Roots: map[string]*RootCounts{"source": {Roots: 10}}, Consistency: ConsistencyReport{Ran: true}}
			replicas("srv-0-aa", "srv-1-bb", "srv-2-cc")(r)
			tt.change(r)
			got := condemnBlockers(r, &o)
			require.Len(t, got, len(tt.want), "%q", got)
			for i, w := range tt.want {
				require.Contains(t, got[i], w)
			}
		})
	}
}

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

func TestCheckReplicas(t *testing.T) {
	o := DefaultDryRunOpts()
	now := time.Now()
	good := Replica{Version: ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour, LastSeen: now.Add(-time.Minute)}
	tests := []struct {
		name   string
		change func(r *Replica)
		want   string
	}{
		{"default settings", func(r *Replica) {}, ""},
		{"stale", func(r *Replica) { r.LastSeen = now.Add(-time.Hour); r.TouchInterval = 0 }, ""},
		{"no GC", func(r *Replica) { r.GC = false }, "runs without the garbage collector hooks"},
		{"old edge rules", func(r *Replica) { r.Version = ExtractorVersion - 1 }, "older than version"},
		{"no touches", func(r *Replica) { r.TouchInterval = 0 }, "writes no touches"},
		{"long touch interval", func(r *Replica) { r.TouchInterval = 37 * time.Hour }, "touch interval 37h0m0s is longer than half the grace (36h0m0s)"},
		{"no TTL", func(r *Replica) { r.CacheTTL = 0 }, "without a TTL"},
		{"TTL of a cycle", func(r *Replica) { r.CacheTTL = 24 * time.Hour }, "cache TTL 24h0m0s is not shorter than the cycle (24h0m0s)"},
		{"TTL above the grace check", func(r *Replica) { r.CacheTTL = 7 * time.Hour }, "longer than the cache TTL of the grace check (6h0m0s)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := good
			r.ID = "r1"
			tt.change(&r)
			rep := checkReplicas([]Replica{r}, now, &o)
			if tt.want == "" {
				require.Empty(t, rep.Warnings)
				return
			}
			require.Len(t, rep.Warnings, 1)
			require.Contains(t, rep.Warnings[0], "replica r1: ")
			require.Contains(t, rep.Warnings[0], tt.want)
		})
	}
	good.ID = "srv-0-1a2b3c"
	rep := checkReplicas([]Replica{good, {ID: "srv-1-4d5e6f", LastSeen: now.Add(-time.Hour)}}, now, &o)
	require.Equal(t, ReplicaReport{Live: 1, Stale: 1, Hosts: 1, RowsPerHost: map[string]int64{"srv-0": 1}}, rep)
}

// The rows of GcServers count in the replica checks, except those of
// processes that stopped without losing touches at the stop. A loss recorded
// before the stop does not make a restart count twice on its host.
func TestServerReplicas(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	row := func(id string, stopped bool, lost time.Time) Server {
		s := Server{ID: id, Version: ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour, LastSeen: now.Add(-time.Minute), TouchLossTime: lost}
		if stopped {
			s.StoppedAt = now.Add(-time.Minute)
		}
		return s
	}
	atStop, earlier := now.Add(-time.Minute), now.Add(-24*time.Hour)
	replicas, stopped := serverReplicas([]Server{
		row("srv-0-aa", false, time.Time{}), row("srv-1-bb", true, time.Time{}), row("srv-1-cc", false, time.Time{}),
		row("srv-2-dd", true, atStop), row("srv-3-ee", true, earlier), row("srv-3-ff", false, time.Time{}),
	})
	require.Equal(t, int64(2), stopped)
	o := DefaultDryRunOpts()
	rep := checkReplicas(replicas, now, &o)
	require.Equal(t, int64(4), rep.Live)
	require.Equal(t, map[string]int64{"srv-0": 1, "srv-1": 1, "srv-2": 1, "srv-3": 1}, rep.RowsPerHost)
}

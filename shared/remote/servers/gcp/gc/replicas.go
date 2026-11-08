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
	"fmt"
	"strings"
	"time"
)

const (
	// liveReplicaAge is how recent the heartbeat of a live replica is.
	liveReplicaAge = 30 * time.Minute
	// staleReplicaAge is the age after which the collector deletes a row.
	staleReplicaAge = 24 * time.Hour
)

// ReplicaReport reports the server replicas seen in GcReplicas and
// GcServers.
type ReplicaReport struct {
	// Live counts the replicas whose heartbeat is younger than liveReplicaAge.
	Live int64 `json:"live"`
	// Stale counts the other rows.
	Stale int64 `json:"stale"`
	// Removed counts the rows of GcReplicas not seen for a day, which the
	// run deleted.
	Removed int64 `json:"removed"`
	// Stopped counts the rows of GcServers of processes that stopped without
	// losing touches at the stop. They are not checked here and stay.
	Stopped int64 `json:"stopped"`
	// Hosts counts the distinct host names among the live replicas, and
	// RowsPerHost the live rows of each host. A host has more than one live
	// row after a server restarted without removing its row.
	Hosts       int64            `json:"hosts"`
	RowsPerHost map[string]int64 `json:"rows_per_host,omitempty"`
	// Warnings lists the live replicas whose settings break the timing rules
	// of the collector. A condemn mode would refuse to run with any of them.
	Warnings []string `json:"warnings,omitempty"`
	// ServersRemoved counts the rows of GcServers that the run deleted, those
	// older than any grace a delete could still need them for.
	ServersRemoved int64 `json:"servers_removed"`
	// LegacyServersGoneTime is when a cycle first found GcReplicas empty, so
	// that no server of an older version, which keeps no row after it stops,
	// can have run since; nil while GcReplicas holds a row.
	LegacyServersGoneTime *time.Time `json:"legacy_servers_gone_time,omitempty"`
}

// ReplicaHost returns the host name of a replica ID made by NewReplicaID.
func ReplicaHost(id string) string {
	if i := strings.LastIndexByte(id, '-'); i > 0 {
		return id[:i]
	}
	return id
}

// serverReplicas returns the rows of GcServers that checkReplicas checks,
// all but those of processes that stopped without losing touches at the
// stop, and how many it left out. A process whose stop recorded a loss counts
// until its row is stale, as a row that a server keeps in GcReplicas then. A
// loss recorded by an earlier heartbeat does not keep a stopped row live:
// such a row of GcReplicas went away at the stop.
func serverReplicas(servers []Server) ([]Replica, int64) {
	var replicas []Replica
	var stopped int64
	for _, s := range servers {
		if !s.StoppedAt.IsZero() && !s.TouchLossTime.Equal(s.StoppedAt) {
			stopped++
			continue
		}
		replicas = append(replicas, Replica{ID: s.ID, Version: s.Version, GC: s.GC, TouchInterval: s.TouchInterval, CacheTTL: s.CacheTTL, LastSeen: s.LastSeen})
	}
	return replicas, stopped
}

// checkReplicas checks the settings of the replicas seen within
// liveReplicaAge before now against the timing rules of replicaProblems.
func checkReplicas(replicas []Replica, now time.Time, o *DryRunOpts) ReplicaReport {
	var rep ReplicaReport
	for i := range replicas {
		r := &replicas[i]
		if now.Sub(r.LastSeen) > liveReplicaAge {
			rep.Stale++
			continue
		}
		rep.Live++
		if rep.RowsPerHost == nil {
			rep.RowsPerHost = make(map[string]int64)
		}
		host := ReplicaHost(r.ID)
		if rep.RowsPerHost[host] == 0 {
			rep.Hosts++
		}
		rep.RowsPerHost[host]++
		for _, p := range replicaProblems(r, o) {
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("replica %s: %s", r.ID, p))
		}
	}
	return rep
}

// replicaProblems returns how the settings of r break the timing rules: a
// used object must be touched at least once per half grace, and a replica
// must read a cached entry again within one cycle and within the TTL the
// grace allows for.
func replicaProblems(r *Replica, o *DryRunOpts) []string {
	if !r.GC {
		return []string{"runs without the garbage collector hooks"}
	}
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	if r.Version < ExtractorVersion {
		add("writes gc rows of edge rules version %d, older than version %d of this collector", r.Version, ExtractorVersion)
	}
	switch {
	case r.TouchInterval <= 0:
		add("writes no touches, so objects that mounts and builds use are not recorded")
	case r.TouchInterval > o.Grace/2:
		add("touch interval %s is longer than half the grace (%s)", r.TouchInterval, o.Grace/2)
	}
	switch {
	case r.CacheTTL <= 0:
		add("keeps cache entries without a TTL, so it can answer for an object long after a condemn")
	case r.CacheTTL >= o.Cycle:
		add("cache TTL %s is not shorter than the cycle (%s)", r.CacheTTL, o.Cycle)
	case r.CacheTTL > o.CacheTTL:
		add("cache TTL %s is longer than the cache TTL of the grace check (%s)", r.CacheTTL, o.CacheTTL)
	}
	return problems
}

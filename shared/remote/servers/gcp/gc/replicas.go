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
	// liveReplicaAge caps serveLimit; the longest real limit is 20.5 minutes.
	liveReplicaAge = 30 * time.Minute
	// staleReplicaAge is the shortest time a row of AbfsGcServers is kept.
	staleReplicaAge = 24 * time.Hour
)

// ReplicaReport reports the server processes seen in AbfsGcServers.
type ReplicaReport struct {
	// Live counts rows without a stop within their serve limit, Stale the others.
	Live    int64 `json:"live"`
	Stale   int64 `json:"stale"`
	Stopped int64 `json:"stopped"`
	// Hosts counts the distinct hosts of the live rows.
	Hosts       int64            `json:"hosts"`
	RowsPerHost map[string]int64 `json:"rows_per_host,omitempty"`
	// Warnings lists live rows that break the timing rules; condemn refuses them.
	Warnings       []string `json:"warnings,omitempty"`
	ServersRemoved int64    `json:"servers_removed"`
	// MapRowLossTime is the latest time a server recorded lost map row touches.
	MapRowLossTime *time.Time `json:"map_row_loss_time,omitempty"`
}

func replicaHost(id string) string {
	if i := strings.LastIndexByte(id, '-'); i > 0 {
		return id[:i]
	}
	return id
}

func serverReplicas(servers []Server) ([]Server, int64) {
	var replicas []Server
	var stopped int64
	for i := range servers {
		if !servers[i].StoppedAt.IsZero() {
			stopped++
			continue
		}
		replicas = append(replicas, servers[i])
	}
	return replicas, stopped
}

// serveLimit is how long after its last heartbeat the process of r may serve.
func serveLimit(r *Server) time.Duration {
	interval := r.HeartbeatInterval
	if interval <= 0 || interval > MaxHeartbeatInterval {
		interval = MaxHeartbeatInterval
	}
	return min(2*interval+HeartbeatTimeout+clockSkewMargin, liveReplicaAge)
}

func checkReplicas(replicas []Server, now time.Time, o *DryRunOpts) ReplicaReport {
	var rep ReplicaReport
	for i := range replicas {
		r := &replicas[i]
		if now.Sub(r.LastSeen) > serveLimit(r) {
			rep.Stale++
			continue
		}
		rep.Live++
		if rep.RowsPerHost == nil {
			rep.RowsPerHost = make(map[string]int64)
		}
		host := replicaHost(r.ID)
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

func replicaProblems(r *Server, o *DryRunOpts) []string {
	if !r.GC {
		return []string{"runs without the garbage collector hooks"}
	}
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	if r.Version < ExtractorVersion {
		add("follows the GC rules of version %d, older than version %d of this collector", r.Version, ExtractorVersion)
	}
	switch {
	case r.TouchInterval <= 0:
		add("writes no touches, so objects that mounts and builds use are not recorded")
	case r.TouchInterval > o.Grace/2:
		add("touch interval %s is longer than half the grace (%s)", r.TouchInterval, o.Grace/2)
	}
	if r.CacheTTL <= 0 {
		add("keeps cache entries without a TTL, so it can answer for an object long after a condemn")
	}
	return problems
}

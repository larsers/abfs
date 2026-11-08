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
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"cloud.google.com/go/spanner"
)

// serverEvidence is what the collector knows of the server processes: the
// heartbeat rows of both tables and the time GcReplicas was last found to
// hold no row.
type serverEvidence struct {
	// servers are the rows of GcServers, read at readTime.
	servers  []Server
	readTime time.Time
	// replicas are the rows of GcReplicas, which servers of an older version
	// write and delete when they stop.
	replicas []Replica
	// legacyGone is LegacyServersGoneTime, zero while GcReplicas holds a row.
	legacyGone time.Time
}

// readServerEvidence runs the guard of GcReplicas for the garbage collector
// name, then reads GcServers.
func readServerEvidence(ctx context.Context, client *spanner.Client, name string) (*serverEvidence, error) {
	replicas, gone, err := checkLegacyServers(ctx, client, name)
	if err != nil {
		return nil, err
	}
	servers, read, err := readServers(ctx, client)
	if err != nil {
		return nil, err
	}
	return &serverEvidence{servers: servers, readTime: read, replicas: replicas, legacyGone: gone}, nil
}

// checkLegacyServers reads GcReplicas and records in the GcState row name
// since when it has been empty. A server of an older version deletes its row when it stops
// and leaves no other trace, so the only evidence that none ran in a window is
// that GcReplicas was found empty before the window started and every cycle
// since found it empty too. The transaction sets LegacyServersGoneTime to its
// commit time when it finds no row and the time is not set, and clears it
// when it finds a row. It returns the rows and the time, zero when not set.
// Both writes state a fact of the read, so a collector that lost its lease
// may make them too.
func checkLegacyServers(ctx context.Context, client *spanner.Client, name string) ([]Replica, time.Time, error) {
	var replicas []Replica
	var gone time.Time
	var set bool
	commit, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, stateTable, spanner.Key{name}, []string{"LegacyServersGoneTime"})
		if err != nil {
			return err
		}
		var stored spanner.NullTime
		if err := row.Columns(&stored); err != nil {
			return err
		}
		if replicas, err = scanReplicas(txn.Read(ctx, replicasTable, spanner.AllKeys(), replicaColumns)); err != nil {
			return err
		}
		gone, set = stored.Time, false
		var value any
		switch {
		case len(replicas) == 0 && !stored.Valid:
			value, set = spanner.CommitTimestamp, true
		case len(replicas) > 0 && stored.Valid:
			value, gone = spanner.NullTime{}, time.Time{}
		default:
			return nil
		}
		return txn.BufferWrite([]*spanner.Mutation{spanner.Update(stateTable, []string{"Name", "LegacyServersGoneTime"}, []any{name, value})})
	})
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to check the GC replicas of older servers: %w", err)
	}
	if set {
		gone = commit
	}
	return replicas, gone, nil
}

// windowStart returns the start of the window of a delete cycle whose
// snapshot is snapshot: a use before it cannot keep an object recent at the
// mark of that cycle, so only the processes that may have run since count.
func windowStart(snapshot time.Time, grace time.Duration) time.Time {
	return snapshot.Add(-grace - clockSkewMargin)
}

// mayHaveRun reports whether the process of s may have run at or after
// start: it stopped then or later, or it did not stop and its last heartbeat
// is not older than a live row may be.
func (s *Server) mayHaveRun(start time.Time) bool {
	if !s.StoppedAt.IsZero() {
		return !s.StoppedAt.Before(start)
	}
	return !s.LastSeen.Add(liveReplicaAge).Before(start)
}

// inWindow returns the rows of GcServers whose process may have run since
// start.
func (e *serverEvidence) inWindow(start time.Time) []Server {
	var in []Server
	for _, s := range e.servers {
		if s.mayHaveRun(start) {
			in = append(in, s)
		}
	}
	return in
}

// longestTouchInterval returns the largest touch interval of the processes
// that may have run since start: an object a client touches is recent for
// the grace less that interval at the next mark.
func (e *serverEvidence) longestTouchInterval(start time.Time) time.Duration {
	var d time.Duration
	for _, s := range e.inWindow(start) {
		d = max(d, s.TouchInterval)
	}
	return d
}

// longestCacheTTL returns the largest cache TTL of the processes that may
// have run since start.
func (e *serverEvidence) longestCacheTTL(start time.Time) time.Duration {
	var d time.Duration
	for _, s := range e.inWindow(start) {
		d = max(d, s.CacheTTL)
	}
	return d
}

// windowBlockers returns the reasons in e why a cycle whose snapshot is
// snapshot must not delete: a process that may have run in the window and
// crashed, lost touches, ran without link validation or with settings that
// break the timing rules, or a server of an older version that may have run
// in it.
func windowBlockers(e *serverEvidence, snapshot time.Time, o *DryRunOpts) []string {
	var reasons []string
	add := func(format string, args ...any) {
		reasons = append(reasons, fmt.Sprintf(format, args...))
	}
	start := windowStart(snapshot, o.Grace)
	in := e.inWindow(start)
	slices.SortFunc(in, func(a, b Server) int { return cmp.Compare(a.ID, b.ID) })
	for i := range in {
		s := &in[i]
		live := e.readTime.Sub(s.LastSeen) <= liveReplicaAge
		if s.StoppedAt.IsZero() && !live {
			add("server %s last wrote its heartbeat at %s and did not record a stop: it crashed and its buffered touches may be lost; deletes wait until that is before the window start", s.ID, formatTime(s.LastSeen))
		}
		if !s.TouchLossTime.IsZero() && !s.TouchLossTime.Before(start) {
			add("server %s lost touches at %s, after the window start %s", s.ID, formatTime(s.TouchLossTime), formatTime(start))
		}
		if s.GC && !s.LinkValidation {
			add("server %s runs without link validation, so it can accept an object or ref that links a condemned object without reading it", s.ID)
		}
		r := Replica{ID: s.ID, Version: s.Version, GC: s.GC, TouchInterval: s.TouchInterval, CacheTTL: s.CacheTTL}
		for _, p := range replicaProblems(&r, o) {
			add("server %s: %s", s.ID, p)
		}
	}
	switch {
	case len(e.replicas) > 0:
		add("GcReplicas holds %d rows of servers of an older version, which keep no evidence of the touches they lose", len(e.replicas))
	case e.legacyGone.IsZero():
		add("no cycle has recorded that GcReplicas is empty")
	case e.legacyGone.After(start):
		add("GcReplicas was first found empty at %s, after the window start %s: a server of an older version may have run in the window", formatTime(e.legacyGone), formatTime(start))
	}
	return reasons
}

// liveBlockers returns the reasons in e why a cycle must not condemn or
// delete that the live heartbeat rows of both tables show, as condemnBlockers
// does for the first read.
func liveBlockers(e *serverEvidence, o *CondemnOpts) []string {
	fromServers, _ := serverReplicas(e.servers)
	rep := checkReplicas(slices.Concat(e.replicas, fromServers), e.readTime, &o.DryRunOpts)
	return append(rep.Warnings, replicaCountProblems(&rep, o)...)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

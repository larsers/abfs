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

const (
	// clientMapReuse is how long cacheman answers map lookups from its copy
	// of the results a server returned, without asking a server again
	// (DefaultTTLHours in shared/cacheman/server).
	clientMapReuse = 8 * time.Hour
	// keepAlivePassAllowance is the time a keep-alive pass may take to reach
	// an object of its mount, from the time the pass was due: a pass sends at
	// most 10,000 hashes a second, so a mount of 10 million hashes takes
	// about 17 minutes, and a failed pass is retried after 5 minutes.
	keepAlivePassAllowance = time.Hour
)

// serverEvidence is what the collector knows of the server processes: their
// heartbeat rows in AbfsGcServers.
type serverEvidence struct {
	// servers are the rows of AbfsGcServers, read at readTime.
	servers  []Server
	readTime time.Time
}

// readServerEvidence reads AbfsGcServers.
func readServerEvidence(ctx context.Context, client *spanner.Client) (*serverEvidence, error) {
	servers, read, err := readServers(ctx, client)
	if err != nil {
		return nil, err
	}
	return &serverEvidence{servers: servers, readTime: read}, nil
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

// inWindow returns the rows of AbfsGcServers whose process may have run since
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

// graceTTL returns the server with the largest cache TTL among those that
// may have run since start, and false when no such row exists.
func (e *serverEvidence) graceTTL(start time.Time) (Server, bool) {
	var longest Server
	found := false
	for _, s := range e.inWindow(start) {
		if !found || s.CacheTTL > longest.CacheTTL {
			longest, found = s, true
		}
	}
	return longest, found
}

// graceProblem returns why the grace of o does not cover the largest cache
// TTL of the servers that may have run since start and the longest push, or
// "" when it does. Without such a server the grace cannot be checked.
func (e *serverEvidence) graceProblem(start time.Time, o *DryRunOpts) string {
	r, found := e.graceTTL(start)
	switch {
	case !found:
		return "no server that may have run within the grace wrote a GC heartbeat, so the grace cannot be checked against the cache TTL of the servers"
	case o.Grace < r.CacheTTL+o.LongestPush:
		return fmt.Sprintf("grace %s is shorter than the cache TTL %s of server %s plus the longest push %s", o.Grace, r.CacheTTL, r.ID, o.LongestPush)
	}
	return ""
}

// keepAliveTouchLimit returns how soon after a server answered that a mount's
// object is present the keep-alive must touch it again, so that the object is
// recent at the mark of every cycle until then, for a cycle whose snapshot is
// snapshot: the grace less the longest touch interval and the longest cache
// TTL of the processes that may have run in its window, and the clock skew
// margin. An answer from the header cache rests on a touch made when the
// entry was looked up, up to the TTL before the answer. A mount that keeps
// to it needs no rescue of what it holds; the hold of the condemn markers
// does not rest on it.
func (e *serverEvidence) keepAliveTouchLimit(snapshot time.Time, grace time.Duration) time.Duration {
	start := windowStart(snapshot, grace)
	return grace - e.longestTouchInterval(start) - e.longestCacheTTL(start) - clockSkewMargin
}

// mapRowLoss returns the latest time a server that may have run since the
// window start of a cycle whose snapshot is snapshot recorded lost map row
// touches, or any time from clientMapReuse and keepAlivePassAllowance before
// that start, and whether there is one. It is reported, not checked.
func (e *serverEvidence) mapRowLoss(snapshot time.Time, grace time.Duration) (time.Time, bool) {
	start := windowStart(snapshot, grace).Add(-clientMapReuse - keepAlivePassAllowance)
	var latest time.Time
	for _, s := range e.inWindow(start) {
		if !s.MapRowLossTime.Before(start) && s.MapRowLossTime.After(latest) {
			latest = s.MapRowLossTime
		}
	}
	return latest, !latest.IsZero()
}

// windowBlockers returns the reasons in e why a cycle whose snapshot is
// snapshot must not delete: a process that may have run in the window ran
// without link validation, with an older version of the GC rules or with
// settings that break the timing rules, or the grace does not cover the
// largest cache TTL. A process that crashed or lost touches does not count:
// the delete rule rests on no touch that a server buffers.
func windowBlockers(e *serverEvidence, snapshot time.Time, o *DryRunOpts) []string {
	var reasons []string
	add := func(format string, args ...any) {
		reasons = append(reasons, fmt.Sprintf(format, args...))
	}
	start := windowStart(snapshot, o.Grace)
	in := e.inWindow(start)
	if p := e.graceProblem(start, o); p != "" {
		add("%s", p)
	}
	slices.SortFunc(in, func(a, b Server) int { return cmp.Compare(a.ID, b.ID) })
	for i := range in {
		s := &in[i]
		if s.GC && !s.LinkValidation {
			add("server %s runs without link validation, so it can accept an object or ref that links a condemned object without reading it", s.ID)
		}
		r := s.replica()
		for _, p := range replicaProblems(&r, o) {
			add("server %s: %s", s.ID, p)
		}
	}
	return reasons
}

// listCacheTTL returns the largest cache TTL of the processes that may have
// run since the window start of a cycle whose snapshot is snapshot, which a
// condemn records with its list, and the reasons why it must not complete the
// list: a server that has no row there may have used information read before
// the markers, and one without the GC hooks, without a cache TTL or of an
// older version of the GC rules does not bound its answers.
func (e *serverEvidence) listCacheTTL(snapshot time.Time, grace time.Duration) (time.Duration, []string) {
	var reasons []string
	in := e.inWindow(windowStart(snapshot, grace))
	if len(in) == 0 {
		return 0, []string{"no server that may have run within the grace wrote a GC heartbeat, so the largest cache TTL is not known"}
	}
	slices.SortFunc(in, func(a, b Server) int { return cmp.Compare(a.ID, b.ID) })
	var ttl time.Duration
	for _, s := range in {
		ttl = max(ttl, s.CacheTTL)
		if !s.GC {
			reasons = append(reasons, fmt.Sprintf("server %s runs without the garbage collector hooks", s.ID))
			continue
		}
		if s.CacheTTL <= 0 {
			reasons = append(reasons, fmt.Sprintf("server %s keeps cache entries without a TTL", s.ID))
		}
		if s.Version < ExtractorVersion {
			reasons = append(reasons, fmt.Sprintf("server %s follows the GC rules of version %d, older than version %d of this collector", s.ID, s.Version, ExtractorVersion))
		}
	}
	return ttl, reasons
}

// liveBlockers returns the reasons in e why a cycle must not condemn or
// delete that the live heartbeat rows show, as condemnBlockers does for the
// first read.
func liveBlockers(e *serverEvidence, o *CondemnOpts) []string {
	replicas, _ := serverReplicas(e.servers)
	rep := checkReplicas(replicas, e.readTime, &o.DryRunOpts)
	return append(rep.Warnings, replicaCountProblems(&rep, o)...)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

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
	// clientMapReuse is DefaultTTLHours in shared/cacheman/server.
	clientMapReuse = 8 * time.Hour
	// keepAlivePassAllowance covers a 10M-hash pass at 10,000 hashes/s and a retry.
	keepAlivePassAllowance = time.Hour
)

type serverEvidence struct {
	servers  []Server
	readTime time.Time
}

func readServerEvidence(ctx context.Context, client *spanner.Client) (*serverEvidence, error) {
	servers, read, err := readServers(ctx, client)
	if err != nil {
		return nil, err
	}
	return &serverEvidence{servers: servers, readTime: read}, nil
}

// A use before the window start cannot keep an object recent at the mark.
func windowStart(snapshot time.Time, grace time.Duration) time.Time {
	return snapshot.Add(-grace - clockSkewMargin)
}

func (s *Server) mayHaveRun(start time.Time) bool {
	if !s.StoppedAt.IsZero() {
		return !s.StoppedAt.Before(start)
	}
	return !s.LastSeen.Add(liveReplicaAge).Before(start)
}

func (e *serverEvidence) inWindow(start time.Time) []Server {
	var in []Server
	for _, s := range e.servers {
		if s.mayHaveRun(start) {
			in = append(in, s)
		}
	}
	return in
}

func (e *serverEvidence) longestTouchInterval(start time.Time) time.Duration {
	var d time.Duration
	for _, s := range e.inWindow(start) {
		d = max(d, s.TouchInterval)
	}
	return d
}

func (e *serverEvidence) longestCacheTTL(start time.Time) time.Duration {
	var d time.Duration
	for _, s := range e.inWindow(start) {
		d = max(d, s.CacheTTL)
	}
	return d
}

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

// keepAliveTouchLimit subtracts the cache TTL because a cached presence answer
// rests on a touch made up to the TTL before it.
func (e *serverEvidence) keepAliveTouchLimit(snapshot time.Time, grace time.Duration) time.Duration {
	start := windowStart(snapshot, grace)
	return grace - e.longestTouchInterval(start) - e.longestCacheTTL(start) - clockSkewMargin
}

// mapRowLoss is reported, not checked.
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

// windowBlockers ignores crashes and lost touches: the delete rule rests on no
// touch that a server buffers.
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
		for _, p := range replicaProblems(s, o) {
			add("server %s: %s", s.ID, p)
		}
	}
	return reasons
}

// listCacheTTL returns the largest cache TTL in the window and why the list must
// not complete: a server with no row may answer from reads before the markers.
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

func liveBlockers(e *serverEvidence, o *CondemnOpts) []string {
	replicas, _ := serverReplicas(e.servers)
	rep := checkReplicas(replicas, e.readTime, &o.DryRunOpts)
	return append(rep.Warnings, replicaCountProblems(&rep, o)...)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

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
	"context"
	"fmt"
	"log"
	"maps"
	"slices"
	"time"
)

// condemnHoldMargin covers map outputs on mounts without keep-alive. Tests change it.
var condemnHoldMargin = 16 * time.Hour

// holdOf returns how long after a list completed a cycle may delete its keys, so
// that no client still relies on a present answer read before the marker.
func holdOf(ttl, keepAlive time.Duration, o *DryRunOpts) time.Duration {
	reliance := max(o.LongestPush, clientMapReuse+keepAlive+keepAlive/10+keepAlivePassAllowance)
	return max(o.Grace+condemnHoldMargin, ttl+ValidationWriteBound+reliance+clockSkewMargin)
}

type listHolds struct {
	records map[int64]*ListRecord
	err     error
}

// A failed read is kept, so the cycle deletes nothing and rewrites no marker.
func readListHolds(ctx context.Context, lease *Lease) (*listHolds, error) {
	records, err := lease.ReadListRecords(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		log.Printf("Warning: %v", err)
	}
	return &listHolds{records: records, err: err}, nil
}

func (h *listHolds) record(value []byte) *ListRecord {
	if h.err != nil {
		return nil
	}
	r := h.records[markerCycle(value)]
	if r == nil || !r.Complete() {
		return nil
	}
	return r
}

// With unread records every marker counts as recorded, so a condemn rewrites none.
func (h *listHolds) recorded(value []byte) bool {
	return h.err != nil || h.record(value) != nil
}

// ListHoldReport reports the hold of the condemned list of one cycle.
type ListHoldReport struct {
	Cycle        int64     `json:"cycle"`
	CompleteTime time.Time `json:"complete_time"`
	// The cycle may delete the keys of the list when Eligible.
	CacheTTLHours float64   `json:"cache_ttl_hours"`
	HoldHours     float64   `json:"hold_hours"`
	EligibleTime  time.Time `json:"eligible_time"`
	Eligible      bool      `json:"eligible"`
	// Unreachable counts the unreachable objects that hold its marker.
	Unreachable int64 `json:"unreachable"`
}

func (h *listHolds) eligible(values [][]byte, unreachable []sizedNode, snapshot time.Time, keepAlive time.Duration, o *DryRunOpts) ([]bool, []ListHoldReport) {
	ok := make([]bool, len(values)+1)
	reports := make(map[int64]*ListHoldReport)
	byValue := make([]*ListHoldReport, len(values)+1)
	for i, v := range values {
		r := h.record(v)
		if r == nil {
			continue
		}
		hold := holdOf(r.CacheTTL, keepAlive, o)
		at := r.CompleteTime.Add(hold)
		ok[i+1] = !snapshot.Before(at)
		rep := &ListHoldReport{Cycle: r.Cycle, CompleteTime: r.CompleteTime, CacheTTLHours: r.CacheTTL.Hours(), HoldHours: hold.Hours(), EligibleTime: at, Eligible: ok[i+1]}
		reports[r.Cycle], byValue[i+1] = rep, rep
	}
	for _, n := range unreachable {
		if rep := byValue[n.marker]; rep != nil {
			rep.Unreachable++
		}
	}
	var list []ListHoldReport
	for _, c := range slices.Sorted(maps.Keys(reports)) {
		list = append(list, *reports[c])
	}
	return ok, list
}

// A later marker of a pruned cycle has no record and is written again.
func (h *listHolds) prunable(cycle int64, values [][]byte) []int64 {
	if h.err != nil {
		return nil
	}
	held := make(map[int64]bool, len(values))
	for _, v := range values {
		held[markerCycle(v)] = true
	}
	var cycles []int64
	for c := range h.records {
		if c < cycle-1 && !held[c] {
			cycles = append(cycles, c)
		}
	}
	slices.Sort(cycles)
	return cycles
}

func (h *listHolds) describe() string {
	return fmt.Sprintf("the GC list records could not be read: %v", h.err)
}

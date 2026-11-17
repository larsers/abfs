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

// condemnHoldMargin is how much longer than the grace a condemn marker holds
// at least before its key may be deleted. It covers the outputs of a map
// result that a mount without the keep-alive applies, which only the touch of
// the server's answer keeps, for a cache TTL of up to 6 hours 39 minutes;
// tests change it.
var condemnHoldMargin = 16 * time.Hour

// holdOf returns how long after the completion of a list a cycle may delete
// the keys that hold its marker, for a list whose servers had the largest
// cache TTL ttl and a cycle whose live mounts touch at most every keepAlive:
// the grace plus condemnHoldMargin, or, when longer, ttl, the write bound,
// the longest time a client relies on a present answer, and the clock skew
// margin. A server answers present for a key read before its marker landed
// until ttl and the write bound after that, and a client relies on the answer
// for the longest push, or, on a keep-alive mount, for the time clients
// reuse map results, the touch interval a tenth late, and the time a
// keep-alive pass may take.
func holdOf(ttl, keepAlive time.Duration, o *DryRunOpts) time.Duration {
	reliance := max(o.LongestPush, clientMapReuse+keepAlive+keepAlive/10+keepAlivePassAllowance)
	return max(o.Grace+condemnHoldMargin, ttl+ValidationWriteBound+reliance+clockSkewMargin)
}

// listHolds holds the records of the condemned lists of a garbage collector,
// read once after the lease was taken, and why they could not be read.
type listHolds struct {
	records map[int64]*ListRecord
	err     error
}

// readListHolds reads the list records of the lease. A failed read is kept,
// so that the cycle deletes nothing and rewrites no marker; it fails only
// when ctx ends.
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

// record returns the complete record of the list whose marker is value, or
// nil.
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

// recorded reports whether the marker value has a complete record, so that a
// condemn must not write the key again; with an unread record every marker
// counts as recorded.
func (h *listHolds) recorded(value []byte) bool {
	return h.err != nil || h.record(value) != nil
}

// ListHoldReport reports the hold of the condemned list of one cycle.
type ListHoldReport struct {
	Cycle        int64     `json:"cycle"`
	CompleteTime time.Time `json:"complete_time"`
	// CacheTTLHours is the largest cache TTL of the servers recorded with
	// the list, HoldHours the hold of its markers for this cycle and
	// EligibleTime when it ends; the cycle may delete its keys when
	// Eligible.
	CacheTTLHours float64   `json:"cache_ttl_hours"`
	HoldHours     float64   `json:"hold_hours"`
	EligibleTime  time.Time `json:"eligible_time"`
	Eligible      bool      `json:"eligible"`
	// Unreachable counts the unreachable objects that hold its marker.
	Unreachable int64 `json:"unreachable"`
}

// eligible returns for each marker value of a scan, by its number, whether a
// cycle whose snapshot is snapshot and whose live mounts touch at most every
// keepAlive may delete the keys that hold it, and the holds of the lists
// that unreachable objects hold the markers of, by cycle.
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

// prunable returns the cycles of the records older than the cycle before
// cycle whose marker no objects row of the scan holds. A later marker of such
// a cycle has no record and is written again.
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

// describe says why the list records could not be read.
func (h *listHolds) describe() string {
	return fmt.Sprintf("the GC list records could not be read: %v", h.err)
}

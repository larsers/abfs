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
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/shared/entities"
)

const (
	objectsMetaFamily = "meta"
	objectsSizeColumn = "meta:size"
	maxMissingKept    = 10
	// clockSkewMargin covers the skew between the clocks of the servers, which
	// stamp object rows, and of the collector.
	clockSkewMargin = time.Minute
)

// ConsistencyReport is the result of the check that every object has a gc
// row.
type ConsistencyReport struct {
	Ran     bool  `json:"ran"`
	Objects int64 `json:"objects"`
	// MissingGCRows counts objects without a gc row, written before the scan
	// of the table gc started. Any of them stops a later condemn.
	MissingGCRows int64    `json:"missing_gc_rows"`
	FirstMissing  []string `json:"first_missing,omitempty"`
	// TooNew counts objects without a gc row written too close to the scan to
	// tell.
	TooNew int64 `json:"too_new"`
	// GCRowsWithoutObject counts gc rows without an object row: uploads in
	// flight, or failed after the gc row was written.
	GCRowsWithoutObject int64 `json:"gc_rows_without_object"`
}

// objectBytes sums the sizes of the objects that the mark found reachable and
// unreachable.
type objectBytes struct {
	reachable, unreachable int64
}

// checkConsistency scans the meta family of the table objects. It checks that
// every object written before checkBefore has a node in g, and sums the sizes
// of the reachable and unreachable objects.
func checkConsistency(ctx context.Context, table *bigtable.Table, bounds []string, g *graph, live bitmap, checkBefore time.Time) (*ConsistencyReport, objectBytes, error) {
	filter := bigtable.ChainFilters(bigtable.FamilyFilter(objectsMetaFamily), bigtable.LatestNFilter(1))
	seen := newBitmap(len(g.keys))
	var mu sync.Mutex
	report := &ConsistencyReport{Ran: true}
	var sizes objectBytes
	eg, gctx := errgroup.WithContext(ctx)
	for i := range len(bounds) - 1 {
		eg.Go(func() error {
			var r ConsistencyReport
			var b objectBytes
			err := table.ReadRows(gctx, bigtable.NewRange(bounds[i], bounds[i+1]), func(row bigtable.Row) bool {
				items := row[objectsMetaFamily]
				if len(items) == 0 {
					return true
				}
				r.Objects++
				id := noNode
				if entities.IsHash(row.Key()) {
					id = g.find(entities.NewHash(row.Key()))
				}
				if id == noNode {
					if latest(items).Before(checkBefore) {
						r.MissingGCRows++
						if len(r.FirstMissing) < maxMissingKept {
							r.FirstMissing = append(r.FirstMissing, row.Key())
						}
					} else {
						r.TooNew++
					}
					return true
				}
				seen.setAtomic(id)
				for _, item := range items {
					if item.Column == objectsSizeColumn && len(item.Value) == 8 {
						size := int64(binary.BigEndian.Uint64(item.Value))
						if live.has(id) {
							b.reachable += size
						} else {
							b.unreachable += size
						}
					}
				}
				return true
			}, bigtable.RowFilter(filter))
			if err != nil {
				return fmt.Errorf("failed to scan the table objects from %q: %w", bounds[i], err)
			}
			mu.Lock()
			defer mu.Unlock()
			report.Objects += r.Objects
			report.MissingGCRows += r.MissingGCRows
			report.TooNew += r.TooNew
			report.FirstMissing = append(report.FirstMissing, r.FirstMissing...)
			sizes.reachable += b.reachable
			sizes.unreachable += b.unreachable
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, objectBytes{}, err
	}
	slices.Sort(report.FirstMissing)
	report.FirstMissing = report.FirstMissing[:min(len(report.FirstMissing), maxMissingKept)]
	report.GCRowsWithoutObject = int64(len(g.keys)) - seen.count()
	return report, sizes, nil
}

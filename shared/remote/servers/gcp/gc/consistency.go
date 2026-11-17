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
	// of the table abfsgc started. Any of them stops a later condemn.
	MissingGCRows int64    `json:"missing_gc_rows"`
	FirstMissing  []string `json:"first_missing,omitempty"`
	// TooNew counts objects without a gc row written too close to the scan to
	// tell. A delete cycle checks each of them again while the scan runs.
	TooNew int64 `json:"too_new"`
	// GCRowsWithoutObject counts gc rows without an object row: uploads in
	// flight, or failed after the gc row was written.
	GCRowsWithoutObject int64 `json:"gc_rows_without_object"`
	// Marked counts the objects with a condemn marker, and MarkedLive those
	// of them the mark found live, whose markers a condemn cycle clears.
	Marked     int64 `json:"marked"`
	MarkedLive int64 `json:"marked_live"`
}

// objectBytes sums the sizes of the objects that the mark found reachable and
// unreachable.
type objectBytes struct {
	reachable, unreachable int64
}

// sizedNode is a node, the size of its object and its condemn marker: zero
// for none, else the position in the scan's table of marker values, from 1.
// The marker fills the padding, so a node stays 16 bytes.
type sizedNode struct {
	id     uint32
	marker uint32
	size   int64
}

// markerTable numbers the distinct condemn marker values that a scan reads
// from 1, in the order it first reads them.
type markerTable struct {
	mu     sync.Mutex
	index  map[string]uint32
	values [][]byte
}

// id returns the number of the marker value v.
func (t *markerTable) id(v []byte) uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if i, ok := t.index[string(v)]; ok {
		return i
	}
	t.values = append(t.values, slices.Clone(v))
	i := uint32(len(t.values))
	t.index[string(v)] = i
	return i
}

// consistencyScan is the result of the consistency check.
type consistencyScan struct {
	report *ConsistencyReport
	sizes  objectBytes
	// seen holds the nodes that have an objects row.
	seen bitmap
	// unreachable lists the unreachable nodes that have an objects row, in
	// key order, and marked the live nodes whose objects row has a condemn
	// marker, when the scan was asked to keep them. markers are the marker
	// values that sizedNode.marker numbers, of every row the scan read.
	unreachable []sizedNode
	marked      []sizedNode
	markers     [][]byte
	// tooNew is the check of the objects counted in TooNew, when the scan was
	// asked to run it, and tooNewErr why it failed.
	tooNew    *TooNewReport
	tooNewErr error
}

// checkConsistency scans the meta family and the newest condemn marker of the
// table objects. It checks that every object written before checkBefore has
// a node in g, and sums the sizes of the reachable and unreachable objects.
// With keep it also lists the unreachable nodes that have an objects row and
// the live nodes with a marker, each with its marker. With a check it sends
// the keys of the objects written too close to the scan to tell to it, while
// the scan runs. A failed or incomplete check does not fail the scan.
func checkConsistency(ctx context.Context, table *bigtable.Table, bounds []string, g *graph, live bitmap, checkBefore time.Time, keep bool, check *tooNewCheck) (*consistencyScan, error) {
	filter := bigtable.InterleaveFilters(
		bigtable.ChainFilters(bigtable.FamilyFilter(objectsMetaFamily), bigtable.LatestNFilter(1)),
		bigtable.ChainFilters(bigtable.FamilyFilter(ObjectsFamily), bigtable.ColumnFilter(CondemnedColumn), bigtable.LatestNFilter(1)),
	)
	seen := newBitmap(g.size())
	var mu sync.Mutex
	report := &ConsistencyReport{Ran: true}
	var sizes objectBytes
	markers := &markerTable{index: make(map[string]uint32)}
	unreachable := make([][]sizedNode, len(bounds)-1)
	marked := make([][]sizedNode, len(bounds)-1)
	if check != nil {
		check.start(ctx)
	}
	eg, gctx := errgroup.WithContext(ctx)
	for i := range len(bounds) - 1 {
		eg.Go(func() error {
			var r ConsistencyReport
			var b objectBytes
			var pending []string
			// local caches the numbers of the marker values of this range.
			local := make(map[string]uint32)
			sendFailed := false
			// send hands pending to the check and reports whether the scan
			// goes on, which it does unless gctx ended. A batch the check
			// does not take is dropped: the check then counts fewer keys
			// than TooNew, which refuses the deletes.
			send := func() bool {
				check.send(gctx, pending)
				pending = nil
				if gctx.Err() != nil {
					sendFailed = true
					return false
				}
				return true
			}
			err := table.ReadRows(gctx, bigtable.NewRange(bounds[i], bounds[i+1]), func(row bigtable.Row) bool {
				items := row[objectsMetaFamily]
				if len(items) == 0 {
					return true
				}
				r.Objects++
				var marker uint32
				for _, item := range row[ObjectsFamily] {
					var ok bool
					if marker, ok = local[string(item.Value)]; !ok {
						marker = markers.id(item.Value)
						local[string(item.Value)] = marker
					}
					r.Marked++
				}
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
						if check != nil {
							pending = append(pending, row.Key())
							if len(pending) == tooNewBatch {
								return send()
							}
						}
					}
					return true
				}
				seen.setAtomic(id)
				var size int64
				for _, item := range items {
					if item.Column == objectsSizeColumn && len(item.Value) == 8 {
						size = int64(binary.BigEndian.Uint64(item.Value))
					}
				}
				switch {
				case live.has(id):
					b.reachable += size
					if marker != 0 {
						r.MarkedLive++
						if keep {
							marked[i] = append(marked[i], sizedNode{id: id, marker: marker})
						}
					}
				default:
					b.unreachable += size
					if keep {
						unreachable[i] = append(unreachable[i], sizedNode{id: id, marker: marker, size: size})
					}
				}
				return true
			}, bigtable.RowFilter(filter))
			if err == nil && !sendFailed && len(pending) > 0 {
				send()
			}
			if err == nil && sendFailed {
				err = gctx.Err()
			}
			if err != nil {
				return fmt.Errorf("failed to scan the table objects from %q: %w", bounds[i], err)
			}
			mu.Lock()
			defer mu.Unlock()
			report.Objects += r.Objects
			report.MissingGCRows += r.MissingGCRows
			report.TooNew += r.TooNew
			report.Marked += r.Marked
			report.MarkedLive += r.MarkedLive
			report.FirstMissing = append(report.FirstMissing, r.FirstMissing...)
			sizes.reachable += b.reachable
			sizes.unreachable += b.unreachable
			return nil
		})
	}
	err := eg.Wait()
	var tooNew *TooNewReport
	var tooNewErr error
	if check != nil {
		if err != nil {
			check.cancel()
		}
		tooNew, tooNewErr = check.wait()
	}
	if err != nil {
		return nil, err
	}
	slices.Sort(report.FirstMissing)
	report.FirstMissing = report.FirstMissing[:min(len(report.FirstMissing), maxMissingKept)]
	report.GCRowsWithoutObject = int64(g.size()) - seen.count()
	scan := &consistencyScan{report: report, sizes: sizes, seen: seen, markers: markers.values, tooNew: tooNew, tooNewErr: tooNewErr}
	for i := range unreachable {
		scan.unreachable = append(scan.unreachable, unreachable[i]...)
		scan.marked = append(scan.marked, marked[i]...)
	}
	return scan, nil
}

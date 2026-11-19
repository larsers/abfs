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
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
)

const (
	DefaultDeleteRate    = 2000
	DefaultDeleteWorkers = 32
	// deleteReadBatch is the most keys Bigtable recommends for one ReadRows.
	deleteReadBatch      = 100
	deleteReadsInFlight  = 4
	maxLevelsReported    = 100
	objectsDataFamily    = "data"
	objectsContentColumn = "content"
)

// Delete runs one garbage collection cycle that deletes the objects whose
// hold has ended, then condemns as a condemn cycle does. A cycle whose
// deletes are refused only condemns and lists why in Report.Delete.Refused.
func Delete(ctx context.Context, sp *spanner.Client, bt *bigtable.Client, bucket *storage.BucketHandle, objs remote.RemoteProvider, checks InstanceChecks, opts *DeleteOpts) (*Result, error) {
	o := *opts
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if o.BatchSize == 0 {
		o.BatchSize = DefaultCondemnBatch
	}
	c := &condemner{opts: &o.CondemnOpts, bt: bt, bucket: bucket}
	c.del = &deleter{opts: &o, sp: sp, bt: bt, bucket: bucket, checks: checks}
	return runCycle(ctx, sp, bt, objs, &o.DryRunOpts, c)
}

// DeleteStepReport reports the delete step of a cycle.
type DeleteStepReport struct {
	Records        string           `json:"records"`
	RecordsWritten int64            `json:"records_written"`
	RecordBytes    int64            `json:"record_bytes"`
	Deleted        int64            `json:"deleted"`
	DeletedBytes   int64            `json:"deleted_bytes"`
	Types          map[string]int64 `json:"types,omitempty"`
	// NotMatched counts keys whose marker changed since the scan, as after a
	// rescue; Unplaced counts keys on a cycle of edges.
	NotMatched       int64    `json:"not_matched"`
	NotMatchedSample []string `json:"not_matched_sample,omitempty"`
	Gone             int64    `json:"gone"`
	Blocked          int64    `json:"blocked"`
	TypeNotAllowed   int64    `json:"type_not_allowed,omitempty"`
	Unplaced         int64    `json:"unplaced,omitempty"`
	NotSent          int64    `json:"not_sent,omitempty"`
	// Unknown counts keys whose delete or read failed, read again at the end
	// of the step.
	Unknown        int64 `json:"unknown"`
	UnknownDeleted int64 `json:"unknown_deleted"`
	UnknownPresent int64 `json:"unknown_present"`
	GCRowsDeleted  int64 `json:"gc_rows_deleted"`
	GCRowsKept     int64 `json:"gc_rows_kept"`
	GCRowErrors    int64 `json:"gc_row_errors"`
	MapRowsDeleted int64 `json:"map_rows_deleted"`
	MapRowsKept    int64 `json:"map_rows_kept"`
	MapRowErrors   int64 `json:"map_row_errors"`
	// Levels times the first levels, level 0 being the map rows.
	Levels  []LevelReport `json:"levels,omitempty"`
	Error   string        `json:"error,omitempty"`
	Seconds float64       `json:"seconds"`
}

// LevelReport times one level of the delete step.
type LevelReport struct {
	Level        int       `json:"level"`
	Keys         int64     `json:"keys"`
	Seconds      float64   `json:"seconds"`
	RangeSeconds []float64 `json:"range_seconds"`
}

type deleter struct {
	opts   *DeleteOpts
	sp     *spanner.Client
	bt     *bigtable.Client
	bucket *storage.BucketHandle
	checks InstanceChecks
	budget writeBudget
	rep    *DeleteReport
}

// check removes the expired refs and records why the cycle must not delete.
// The cycle condemns either way.
func (d *deleter) check(ctx context.Context, lease *Lease, c *condemner, res *Result) error {
	r := res.Report
	d.rep = &DeleteReport{}
	if r.Delete != nil {
		d.rep.Plan, d.rep.PlanError = r.Delete.Plan, r.Delete.PlanError
	}
	r.Delete = d.rep
	d.budget = c.budget
	ev := newDeleteEvidence(lease, res)
	d.rep.Refs, ev.refRemovalErr = d.removeExpiredRefs(ctx, lease, res)
	if stops(ctx, ev.refRemovalErr) {
		return ev.refRemovalErr
	}
	ev.instance = readInstanceEvidence(ctx, d.checks, d.budget)
	ev.tooNew, ev.tooNewErr = res.tooNew, res.tooNewErr
	d.rep.TooNew = ev.tooNew
	d.rep.Refused, d.rep.Warnings = deleteBlockers(r, ev, d.opts)
	if res.plan == nil && len(d.rep.Refused) == 0 {
		d.rep.Refused = append(d.rep.Refused, "the cycle has no plan of the keys to delete")
	}
	for _, w := range d.rep.Warnings {
		log.Printf("Warning: GC cycle %d deletes although %s", r.Cycle, w)
	}
	d.logRefused(r.Cycle)
	return nil
}

func (d *deleter) logRefused(cycle int64) {
	for _, reason := range d.rep.Refused {
		log.Printf("Warning: GC cycle %d does not delete and only condemns: %s", cycle, reason)
	}
}

// run returns the nodes it deleted or found without a row, which the cycle
// must not condemn.
func (d *deleter) run(ctx context.Context, lease *Lease, res *Result) (bitmap, error) {
	if err := d.deleteChunks(ctx, res.Report.Cycle); err != nil {
		return nil, err
	}
	if len(d.rep.Refused) > 0 {
		return nil, nil
	}
	if d.opts.StartDelay > 0 {
		select {
		case <-time.After(d.opts.StartDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	reasons, err := recheckServers(ctx, lease, res.Report, d.opts)
	if err != nil {
		return nil, err
	}
	if len(reasons) > 0 {
		d.rep.Refused = reasons
		d.logRefused(res.Report.Cycle)
		return nil, nil
	}
	return d.deleteStep(ctx, lease, res)
}

type step struct {
	d       *deleter
	p       *deletePlan
	g       *graph
	bounds  []string
	share   int
	lim     *limiter
	reads   writeBudget
	header  recordHeader
	state   string
	objects *bigtable.Table
	gcTable *bigtable.Table
	maps    *bigtable.Table
	// deleteObjects holds the conditional delete per marker number.
	deleteObjects                                  map[uint32]*bigtable.Mutation
	deleteGCRow, deleteMapRow, deleteExpiredMapRow *bigtable.Mutation
	stop                                           atomic.Bool
	errOnce                                        sync.Once
	err                                            error
	// mu guards the counts of rep.
	mu  sync.Mutex
	rep *DeleteStepReport
}

// deleteStep deletes the map rows first, then each level in every range
// before the next starts. It fails when the read of the keys of unknown
// outcome fails, so that no such key is left out of the list of this cycle,
// and when the run must stop.
func (d *deleter) deleteStep(ctx context.Context, lease *Lease, res *Result) (bitmap, error) {
	start := time.Now()
	p := res.plan
	r := res.Report
	o := d.opts
	ranges := len(res.bounds) - 1
	s := &step{
		d: d, p: p, g: res.graph, bounds: res.bounds,
		share:   max(1, o.DeleteWorkers/ranges),
		lim:     newLimiter(o.DeleteRate),
		reads:   writeBudget{deadline: o.WriteDeadline},
		state:   o.StateName,
		objects: d.bt.Open(objectsTable),
		gcTable: d.bt.Open(TableName),
		maps:    d.bt.Open(mapsTableName),
		header: recordHeader{
			cycle:       r.Cycle,
			runID:       runID(o.Holder, lease.Epoch),
			gcRowsAfter: r.SnapshotTime.Add(-o.Grace),
		},
		rep: &DeleteStepReport{Records: recordPrefixOf(o.StateName) + fmt.Sprint(r.Cycle) + "/", Types: make(map[string]int64)},
	}
	d.rep.Step = s.rep
	s.deleteObjects = make(map[uint32]*bigtable.Mutation)
	for _, m := range p.markers {
		if s.deleteObjects[m] == nil {
			s.deleteObjects[m] = conditionalDelete(markerPredicate(p.values[m-1]), true)
		}
	}
	s.deleteGCRow = conditionalDelete(newerCellPredicate(EdgesFamily, LinksColumn, s.header.gcRowsAfter), false)
	s.deleteMapRow = conditionalDelete(newerCellPredicate(mapsFamily, "", r.SnapshotTime.Add(-o.Grace)), false)
	s.deleteExpiredMapRow = conditionalDelete(newerCellPredicate(mapsFamily, "", r.SnapshotTime.Add(-o.MapRetention)), false)

	s.runMapLevel(ctx)
	for l := 1; l < len(p.levels) && !s.stop.Load(); l++ {
		s.runObjectLevel(ctx, l)
	}
	for i, oc := range p.outcome {
		if oc == outcomePending {
			p.outcome[i] = outcomeNotSent
		}
	}
	if s.err != nil {
		s.rep.Error = s.err.Error()
		log.Printf("Warning: GC cycle %d stopped its delete step: %v", r.Cycle, s.err)
	}
	if err := s.resolveUnknown(ctx); err != nil {
		return nil, err
	}

	deleted := newBitmap(s.g.size())
	for i, oc := range p.outcome {
		id := p.cands[i]
		switch oc {
		case outcomeDeleted:
			deleted.set(id)
			s.rep.Deleted++
			s.rep.DeletedBytes += p.sizes[i]
			s.rep.Types[typeNames[s.g.types[id]]]++
		case outcomeNotMatched:
			s.rep.NotMatched++
			if len(s.rep.NotMatchedSample) < maxSamples {
				s.rep.NotMatchedSample = append(s.rep.NotMatchedSample, s.g.key(id).String())
			}
		case outcomeGone:
			// A marker would recreate the gone row.
			deleted.set(id)
			s.rep.Gone++
		case outcomeBlocked:
			s.rep.Blocked++
		case outcomeTypeNotAllowed:
			s.rep.TypeNotAllowed++
		case outcomeUnplaced:
			s.rep.Unplaced++
		case outcomeNotSent:
			s.rep.NotSent++
		}
	}
	s.rep.Seconds = time.Since(start).Seconds()
	log.Printf("GC cycle %d deleted %d objects of %d bytes; %d not matched, %d blocked, %d unknown, %d map rows deleted",
		r.Cycle, s.rep.Deleted, s.rep.DeletedBytes, s.rep.NotMatched, s.rep.Blocked, s.rep.Unknown, s.rep.MapRowsDeleted)
	// A run that must stop writes no list and no marker after the step.
	if stops(ctx, s.err) {
		return nil, fmt.Errorf("GC cycle %d stopped its delete step: %w", r.Cycle, s.err)
	}
	return deleted, nil
}

func conditionalDelete(predicate bigtable.Filter, onMatch bool) *bigtable.Mutation {
	del := bigtable.NewMutation()
	del.DeleteRow()
	if onMatch {
		return bigtable.NewCondMutation(predicate, del, nil)
	}
	return bigtable.NewCondMutation(predicate, nil, del)
}

func markerPredicate(marker []byte) bigtable.Filter {
	end := append(append([]byte{}, marker...), 0)
	return bigtable.ChainFilters(
		bigtable.FamilyFilter(ObjectsFamily),
		bigtable.ColumnFilter(CondemnedColumn),
		bigtable.LatestNFilter(1),
		bigtable.ValueRangeFilter(append([]byte{}, marker...), end),
	)
}

func newerCellPredicate(family, column string, t time.Time) bigtable.Filter {
	filters := []bigtable.Filter{bigtable.FamilyFilter(family)}
	if column != "" {
		filters = append(filters, bigtable.ColumnFilter(column))
	}
	return bigtable.ChainFilters(append(filters, bigtable.LatestNFilter(1), bigtable.TimestampRangeFilter(t, time.Time{}))...)
}

// condApply reports in sent whether an attempt was sent, so whether the row
// may have changed.
func condApply(ctx context.Context, b writeBudget, table *bigtable.Table, row string, mut *bigtable.Mutation) (matched, sent bool, err error) {
	err = retryWith(ctx, firstRetryWait, b, func(actx context.Context) error {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		sent = true
		return table.Apply(actx, row, mut, bigtable.GetCondMutationResult(&matched))
	})
	return matched, sent, err
}

func (s *step) rangeOf(key string) int {
	return sort.Search(len(s.bounds)-2, func(i int) bool { return s.bounds[i+1] > key })
}

func (s *step) fail(err error) {
	s.errOnce.Do(func() { s.err = err })
	s.stop.Store(true)
}

func (s *step) runRanges(level int, items [][]int32, work func(rng int, items []int32) error) {
	lr := LevelReport{Level: level, RangeSeconds: make([]float64, len(items))}
	start := time.Now()
	var wg sync.WaitGroup
	for rng, its := range items {
		lr.Keys += int64(len(its))
		if len(its) == 0 {
			continue
		}
		wg.Go(func() {
			rs := time.Now()
			if err := work(rng, its); err != nil {
				s.fail(err)
			}
			lr.RangeSeconds[rng] = time.Since(rs).Seconds()
		})
	}
	wg.Wait()
	lr.Seconds = time.Since(start).Seconds()
	if len(s.rep.Levels) < maxLevelsReported {
		s.rep.Levels = append(s.rep.Levels, lr)
	}
}

func (s *step) inFlight(ctx context.Context, items []int32, call func(i int32) error) error {
	sem := make(chan struct{}, s.share)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for _, i := range items {
		sem <- struct{}{}
		if s.stop.Load() {
			<-sem
			break
		}
		if ctx.Err() != nil {
			<-sem
			mu.Lock()
			if first == nil {
				first = context.Cause(ctx)
			}
			mu.Unlock()
			s.stop.Store(true)
			break
		}
		wg.Go(func() {
			defer func() { <-sem }()
			if err := call(i); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
				s.stop.Store(true)
			}
		})
	}
	wg.Wait()
	return first
}

// runMapLevel deletes the map rows. A row that names a candidate and is kept
// or fails keeps both ends.
func (s *step) runMapLevel(ctx context.Context) {
	p := s.p
	items := make([][]int32, len(s.bounds)-1)
	for i := range p.mapRows {
		rng := s.rangeOf(p.mapRows[i].key)
		items[rng] = append(items[rng], int32(i))
	}
	for _, its := range items {
		slices.SortFunc(its, func(a, b int32) int { return strings.Compare(p.mapRows[a].key, p.mapRows[b].key) })
	}
	failed := make([][]int32, len(items))
	s.runRanges(0, items, func(rng int, its []int32) error {
		records := 0
		for lo := 0; lo < len(its); lo += s.d.opts.BatchSize {
			batch := its[lo:min(lo+s.d.opts.BatchSize, len(its))]
			if s.stop.Load() {
				failed[rng] = append(failed[rng], its[lo:]...)
				return nil
			}
			if ctx.Err() != nil {
				failed[rng] = append(failed[rng], its[lo:]...)
				return context.Cause(ctx)
			}
			if err := s.lim.wait(ctx, len(batch)); err != nil {
				failed[rng] = append(failed[rng], its[lo:]...)
				return err
			}
			entries := make([]recordEntry, len(batch))
			for k, i := range batch {
				m := &p.mapRows[i]
				entries[k] = recordEntry{kind: recordMapRow}
				entries[k].key, entries[k].mapTo = mapRowEnds(m.key)
			}
			if err := s.writeRecords(ctx, 0, rng, &records, entries); err != nil {
				failed[rng] = append(failed[rng], its[lo:]...)
				return err
			}
			err := s.inFlight(ctx, batch, func(i int32) error {
				m := &p.mapRows[i]
				mut := s.deleteMapRow
				if m.expired {
					mut = s.deleteExpiredMapRow
				}
				kept, _, err := condApply(ctx, s.d.budget, s.maps, m.key, mut)
				s.mu.Lock()
				defer s.mu.Unlock()
				switch {
				case err != nil:
					s.rep.MapRowErrors++
					failed[rng] = append(failed[rng], i)
					if stops(ctx, err) {
						return err
					}
				case kept:
					s.rep.MapRowsKept++
					failed[rng] = append(failed[rng], i)
				default:
					s.rep.MapRowsDeleted++
				}
				return nil
			})
			if err != nil {
				failed[rng] = append(failed[rng], its[lo+len(batch):]...)
				return err
			}
		}
		return nil
	})
	for _, its := range failed {
		for _, i := range its {
			m := &p.mapRows[i]
			for _, id := range []uint32{m.from, m.to} {
				if j := p.index(id); j >= 0 && p.outcome[j] == outcomePending {
					p.outcome[j] = outcomeBlocked
				}
			}
		}
	}
}

// runObjectLevel fails every key of level l that it did not delete, so that
// no child of it is tried.
func (s *step) runObjectLevel(ctx context.Context, l int) {
	p := s.p
	items := make([][]int32, len(s.bounds)-1)
	for _, i := range p.levels[l] {
		if p.outcome[i] != outcomePending {
			continue
		}
		rng := s.rangeOf(s.g.key(p.cands[i]).String())
		items[rng] = append(items[rng], i)
	}
	s.runRanges(l, items, func(rng int, its []int32) error {
		records := 0
		for lo := 0; lo < len(its); lo += s.d.opts.BatchSize {
			if s.stop.Load() {
				return nil
			}
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			batch := its[lo:min(lo+s.d.opts.BatchSize, len(its))]
			if err := s.lim.wait(ctx, len(batch)); err != nil {
				return err
			}
			if err := s.deleteBatch(ctx, l, rng, &records, batch); err != nil {
				return err
			}
		}
		return nil
	})
	for _, i := range p.levels[l] {
		if p.outcome[i] != outcomeDeleted {
			p.fail(int(i))
		}
	}
}

type rowState struct {
	present bool
	marker  []byte
	content []byte
}

func (s *step) readRows(ctx context.Context, rows []string) (map[string]rowState, error) {
	return readObjectRows(ctx, s.objects, s.reads, rows)
}

func readObjectRows(ctx context.Context, objects *bigtable.Table, b writeBudget, rows []string) (map[string]rowState, error) {
	filter := bigtable.ChainFilters(bigtable.LatestNFilter(1), bigtable.InterleaveFilters(
		bigtable.ChainFilters(bigtable.FamilyFilter(objectsMetaFamily), bigtable.StripValueFilter()),
		bigtable.ChainFilters(bigtable.FamilyFilter(ObjectsFamily), bigtable.ColumnFilter(CondemnedColumn)),
		bigtable.ChainFilters(bigtable.FamilyFilter(objectsDataFamily), bigtable.ColumnFilter(objectsContentColumn)),
	))
	states := make(map[string]rowState, len(rows))
	err := readRowList(ctx, objects, b, rows, filter, func(row bigtable.Row) {
		var st rowState
		st.present = len(row[objectsMetaFamily]) > 0
		for _, item := range row[ObjectsFamily] {
			st.marker = item.Value
		}
		for _, item := range row[objectsDataFamily] {
			st.content = item.Value
		}
		states[row.Key()] = st
	})
	if err != nil {
		return nil, err
	}
	return states, nil
}

func readRowList(ctx context.Context, table *bigtable.Table, b writeBudget, rows []string, filter bigtable.Filter, keep func(bigtable.Row)) error {
	var mu sync.Mutex
	eg, ectx := errgroup.WithContext(ctx)
	eg.SetLimit(deleteReadsInFlight)
	for lo := 0; lo < len(rows) && ectx.Err() == nil; lo += deleteReadBatch {
		chunk := rows[lo:min(lo+deleteReadBatch, len(rows))]
		eg.Go(func() error {
			return retryWith(ectx, firstRetryWait, b, func(actx context.Context) error {
				return table.ReadRows(actx, bigtable.RowList(chunk), func(row bigtable.Row) bool {
					mu.Lock()
					keep(row)
					mu.Unlock()
					return true
				}, bigtable.RowFilter(filter))
			})
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

func (s *step) readDeleteRows(ctx context.Context, rows []string) (objects, gcRows map[string]bigtable.Row, err error) {
	objects = make(map[string]bigtable.Row, len(rows))
	gcRows = make(map[string]bigtable.Row, len(rows))
	filter := bigtable.ChainFilters(bigtable.LatestNFilter(1), bigtable.InterleaveFilters(
		bigtable.FamilyFilter(objectsMetaFamily+"|"+objectsDataFamily),
		bigtable.ChainFilters(bigtable.FamilyFilter(ObjectsFamily), bigtable.ColumnFilter(CondemnedColumn)),
	))
	if err := readRowList(ctx, s.objects, s.reads, rows, filter, func(row bigtable.Row) { objects[row.Key()] = row }); err != nil {
		return nil, nil, err
	}
	filter = bigtable.ChainFilters(bigtable.FamilyFilter(EdgesFamily), bigtable.LatestNFilter(1))
	if err := readRowList(ctx, s.gcTable, s.reads, rows, filter, func(row bigtable.Row) { gcRows[row.Key()] = row }); err != nil {
		return nil, nil, err
	}
	return objects, gcRows, nil
}

// deleteBatch records the candidates whose marker is still the one the scan
// read before it deletes them.
func (s *step) deleteBatch(ctx context.Context, level, rng int, records *int, items []int32) error {
	p := s.p
	rows := make([]string, len(items))
	for k, i := range items {
		rows[k] = s.g.key(p.cands[i]).String()
	}
	objects, gcRows, err := s.readDeleteRows(ctx, rows)
	if err != nil {
		return fmt.Errorf("failed to read the objects to delete: %w", err)
	}
	var left []int32
	var entries []recordEntry
	for k, i := range items {
		row := objects[rows[k]]
		var marker []byte
		for _, item := range row[ObjectsFamily] {
			marker = item.Value
		}
		switch {
		case len(row[objectsMetaFamily]) == 0:
			p.outcome[i] = outcomeGone
		case !slices.Equal(marker, p.values[p.markers[i]-1]):
			p.outcome[i] = outcomeNotMatched
		default:
			left = append(left, i)
			entries = append(entries, objectEntry(s.g.key(p.cands[i]), row, gcRows[rows[k]]))
		}
	}
	if len(left) == 0 || s.stop.Load() {
		return nil
	}
	if err := s.writeRecords(ctx, level, rng, records, entries); err != nil {
		return err
	}
	return s.inFlight(ctx, left, func(i int32) error {
		return s.deleteKey(ctx, int(i))
	})
}

// deleteKey reads a key that did not match again, because a retry after a
// lost response finds the row deleted. A delete that was sent and failed
// leaves the outcome unknown.
func (s *step) deleteKey(ctx context.Context, i int) error {
	p := s.p
	row := s.g.key(p.cands[i]).String()
	matched, sent, err := condApply(ctx, s.d.budget, s.objects, row, s.deleteObjects[p.markers[i]])
	if err != nil {
		if sent {
			p.outcome[i] = outcomeUnknown
		}
		if !sent || !retryable(ctx, err) {
			return fmt.Errorf("failed to delete the objects row of %s: %w", row, err)
		}
		return nil
	}
	if !matched {
		states, err := s.readRows(ctx, []string{row})
		switch {
		case err != nil:
			p.outcome[i] = outcomeUnknown
			if stops(ctx, err) {
				return err
			}
			return nil
		case states[row].present:
			p.outcome[i] = outcomeNotMatched
			return nil
		}
	}
	p.outcome[i] = outcomeDeleted
	kept, _, err := condApply(ctx, s.d.budget, s.gcTable, row, s.deleteGCRow)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err != nil:
		s.rep.GCRowErrors++
		if stops(ctx, err) {
			return err
		}
	case kept:
		s.rep.GCRowsKept++
	default:
		s.rep.GCRowsDeleted++
	}
	return nil
}

func (s *step) writeRecords(ctx context.Context, level, rng int, next *int, entries []recordEntry) error {
	for len(entries) > 0 {
		n, size := 1, entries[0].size()
		for n < len(entries) && size+entries[n].size() <= maxRecordBytes {
			size += entries[n].size()
			n++
		}
		h := s.header
		name := recordObjectName(s.state, h.cycle, level, rng, *next)
		written, err := writeRecord(ctx, s.d.budget, s.d.bucket, name, &h, entries[:n])
		if err != nil {
			return err
		}
		*next++
		entries = entries[n:]
		s.mu.Lock()
		s.rep.RecordsWritten++
		s.rep.RecordBytes += written
		s.mu.Unlock()
	}
	return nil
}

func (s *step) resolveUnknown(ctx context.Context) error {
	p := s.p
	var unknown []int
	var rows []string
	for i, oc := range p.outcome {
		if oc == outcomeUnknown {
			unknown = append(unknown, i)
			rows = append(rows, s.g.key(p.cands[i]).String())
		}
	}
	s.rep.Unknown = int64(len(unknown))
	if len(unknown) == 0 {
		return nil
	}
	states, err := s.readRows(ctx, rows)
	if err != nil {
		return fmt.Errorf("failed to read the %d objects whose delete had an unknown outcome; the cycle stops before its list, and those still present keep their markers: %w", len(unknown), err)
	}
	for k, i := range unknown {
		if states[rows[k]].present {
			s.rep.UnknownPresent++
			continue
		}
		p.outcome[i] = outcomeDeleted
		s.rep.UnknownDeleted++
	}
	return nil
}

// mapRowEnds ignores the Cut result because the scan checked the key.
func mapRowEnds(key string) (from, to Hash) {
	f, t, _ := strings.Cut(key, "#")
	return entities.NewHash(f), entities.NewHash(t)
}

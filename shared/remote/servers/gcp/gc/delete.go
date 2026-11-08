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
	"errors"
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
	DefaultChunkDelay    = time.Hour
	// signedURLLifetime is the lifetime of the signed URLs of bucket chunks
	// that the server hands out.
	signedURLLifetime = 10 * time.Minute
	// deleteReadBatch is the number of row keys of one read of the delete
	// step, the most Bigtable recommends for one ReadRows request, and
	// deleteReadsInFlight how many such reads run at once.
	deleteReadBatch     = 100
	deleteReadsInFlight = 4
	// maxLevelsReported is the number of levels the report times one by one.
	maxLevelsReported = 100
	// The objects row keeps the descriptor of the content in data:content.
	objectsDataFamily    = "data"
	objectsContentColumn = "content"
)

// Delete runs one garbage collection cycle that deletes. It runs the steps
// of a condemn cycle and, when every check of deleteBlockers holds, deletes
// the objects of the list of the cycle before that are still unreachable and
// still hold its marker: the map rows that name them first, then each object
// after every parent of it, each batch recorded in bucket before its
// deletes. It then condemns what is still unreachable and was not deleted. A
// cycle whose deletes are refused condemns as a condemn cycle does and lists
// the reasons in Report.Delete.Refused. checks reads the Bigtable instance.
func Delete(ctx context.Context, sp *spanner.Client, bt *bigtable.Client, bucket *storage.BucketHandle, objs remote.RemoteProvider, checks InstanceChecks, opts *DeleteOpts) (*Result, error) {
	o := *opts
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if o.BatchSize == 0 {
		o.BatchSize = DefaultCondemnBatch
	}
	c := &condemner{opts: &o.CondemnOpts, bt: bt, bucket: bucket}
	c.del = &deleter{opts: &o, bt: bt, bucket: bucket, checks: checks}
	return runCycle(ctx, sp, bt, bucket, objs, &o.DryRunOpts, c)
}

// DeleteStepReport reports the delete step of a cycle.
type DeleteStepReport struct {
	// Records is the prefix of the delete records of the cycle and
	// RecordsWritten their number.
	Records        string `json:"records"`
	RecordsWritten int64  `json:"records_written"`
	// Deleted counts the objects deleted, per type in Types, and their size.
	Deleted      int64            `json:"deleted"`
	DeletedBytes int64            `json:"deleted_bytes"`
	Types        map[string]int64 `json:"types,omitempty"`
	// NotMatched counts keys whose marker was not the one of the list, as
	// after a rescue, Gone keys whose row was gone before the step, Blocked
	// keys with a parent or map row that was not deleted, TypeNotAllowed keys
	// of types the cycle may not delete, Unplaced keys on a cycle of edges,
	// and NotSent keys the step did not reach because a worker failed.
	NotMatched       int64    `json:"not_matched"`
	NotMatchedSample []string `json:"not_matched_sample,omitempty"`
	Gone             int64    `json:"gone"`
	Blocked          int64    `json:"blocked"`
	TypeNotAllowed   int64    `json:"type_not_allowed,omitempty"`
	Unplaced         int64    `json:"unplaced,omitempty"`
	NotSent          int64    `json:"not_sent,omitempty"`
	// Unknown counts keys whose delete or read failed, read again at the end
	// of the step: UnknownDeleted were gone, UnknownPresent stay listed.
	Unknown        int64 `json:"unknown"`
	UnknownDeleted int64 `json:"unknown_deleted"`
	UnknownPresent int64 `json:"unknown_present"`
	// GCRowsDeleted counts the gc rows of deleted objects deleted, GCRowsKept
	// those of objects written again since the grace start, and GCRowErrors
	// those whose delete failed, which the chunk step repeats.
	GCRowsDeleted int64 `json:"gc_rows_deleted"`
	GCRowsKept    int64 `json:"gc_rows_kept"`
	GCRowErrors   int64 `json:"gc_row_errors"`
	// MapRowsDeleted counts the map rows deleted, MapRowsKept those written
	// or used since their threshold, and MapRowErrors those whose delete
	// failed; a kept or failed row that names a candidate keeps both ends.
	MapRowsDeleted int64 `json:"map_rows_deleted"`
	MapRowsKept    int64 `json:"map_rows_kept"`
	MapRowErrors   int64 `json:"map_row_errors"`
	// Levels times the first levels, level 0 being the map rows.
	Levels []LevelReport `json:"levels,omitempty"`
	// Error is the worker error that stopped the step; the keys it had not
	// reached are condemned again.
	Error   string  `json:"error,omitempty"`
	Seconds float64 `json:"seconds"`
}

// LevelReport times one level of the delete step.
type LevelReport struct {
	Level int   `json:"level"`
	Keys  int64 `json:"keys"`
	// Seconds is the time of the level, the time of its slowest range;
	// RangeSeconds the time of each range.
	Seconds      float64   `json:"seconds"`
	RangeSeconds []float64 `json:"range_seconds"`
}

// deleter runs the steps of a delete cycle.
type deleter struct {
	opts   *DeleteOpts
	bt     *bigtable.Client
	bucket *storage.BucketHandle
	checks InstanceChecks
	budget writeBudget
	rep    *DeleteReport
	// listComplete is when the list the cycle deletes from was complete.
	listComplete time.Time
}

// check runs the checks of deleteBlockers but the second server read, right
// after condemnBlockers, and records in the report why the cycle must not
// delete. The cycle condemns either way.
func (d *deleter) check(ctx context.Context, lease *Lease, c *condemner, res *Result) error {
	r := res.Report
	d.rep = &DeleteReport{}
	if r.Delete != nil {
		d.rep.Plan = r.Delete.Plan
	}
	r.Delete = d.rep
	d.budget = c.budget
	d.listComplete = c.prev.state.CompleteTime
	ev := newDeleteEvidence(lease, c, res)
	ev.instance = readInstanceEvidence(ctx, d.checks, d.budget, d.opts)
	if n := r.Consistency.TooNew; n > 0 && n <= maxTooNew {
		ev.tooNew, ev.tooNewErr = checkTooNew(ctx, d.bt, res.tooNew)
		if ev.tooNewErr != nil && ctx.Err() != nil {
			return ev.tooNewErr
		}
		d.rep.TooNew = ev.tooNew
	}
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

// run runs the steps of a delete cycle after the markers of the live keys of
// the previous list were cleared: the chunk step of the records of earlier
// cycles, the second server read and, unless a check refused, the delete
// step. It returns the nodes it deleted or found without a row, which the
// cycle must not condemn.
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
	reasons, err := waitForServerStats(ctx, lease, res.Report, d.opts)
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

// step is one delete step.
type step struct {
	d      *deleter
	p      *deletePlan
	g      *graph
	bounds []string
	// share is the number of conditional writes in flight per range.
	share   int
	lim     *limiter
	reads   writeBudget
	header  recordHeader
	state   string
	objects *bigtable.Table
	gcTable *bigtable.Table
	maps    *bigtable.Table
	// deleteObject, deleteGCRow, deleteMapRow and deleteExpiredMapRow are
	// the conditional mutations.
	deleteObject, deleteGCRow, deleteMapRow, deleteExpiredMapRow *bigtable.Mutation
	marker                                                       []byte
	// stop is set by the first worker error; no worker starts a call after
	// it.
	stop    atomic.Bool
	errOnce sync.Once
	err     error
	// mu guards the counts of rep.
	mu  sync.Mutex
	rep *DeleteStepReport
}

// deleteStep deletes the candidates of the plan of res, map rows first, then
// level by level, each level in every range before the next one starts, and
// reads again the keys whose outcome is unknown. It fails only when that
// read fails, so that no key of unknown state is left out of the list of
// this cycle.
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
		marker:  markerValue(p.report.ListCycle),
		header: recordHeader{
			cycle:        r.Cycle,
			runID:        runID(o.Holder, lease.Epoch),
			gcRowsAfter:  r.SnapshotTime.Add(-o.Grace),
			listComplete: d.listComplete,
		},
		rep: &DeleteStepReport{Records: recordPrefixOf(o.StateName) + fmt.Sprint(r.Cycle) + "/", Types: make(map[string]int64)},
	}
	d.rep.Step = s.rep
	s.deleteObject = conditionalDelete(markerPredicate(s.marker), true)
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
			// A marker would give the gone row a row of its own.
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
	log.Printf("GC cycle %d deleted %d objects of %d bytes from the list of cycle %d; %d not matched, %d blocked, %d unknown, %d map rows deleted",
		r.Cycle, s.rep.Deleted, s.rep.DeletedBytes, p.report.ListCycle, s.rep.NotMatched, s.rep.Blocked, s.rep.Unknown, s.rep.MapRowsDeleted)
	return deleted, nil
}

// conditionalDelete returns a mutation that deletes the row when predicate
// matches, with onMatch, or when it does not.
func conditionalDelete(predicate bigtable.Filter, onMatch bool) *bigtable.Mutation {
	del := bigtable.NewMutation()
	del.DeleteRow()
	if onMatch {
		return bigtable.NewCondMutation(predicate, del, nil)
	}
	return bigtable.NewCondMutation(predicate, nil, del)
}

// markerPredicate matches an objects row whose newest condemn marker holds
// exactly marker.
func markerPredicate(marker []byte) bigtable.Filter {
	end := append(append([]byte{}, marker...), 0)
	return bigtable.ChainFilters(
		bigtable.FamilyFilter(ObjectsFamily),
		bigtable.ColumnFilter(CondemnedColumn),
		bigtable.LatestNFilter(1),
		bigtable.ValueRangeFilter(append([]byte{}, marker...), end),
	)
}

// newerCellPredicate matches a row whose newest cell of family, and of column
// unless it is empty, was written at or after t.
func newerCellPredicate(family, column string, t time.Time) bigtable.Filter {
	filters := []bigtable.Filter{bigtable.FamilyFilter(family)}
	if column != "" {
		filters = append(filters, bigtable.ColumnFilter(column))
	}
	return bigtable.ChainFilters(append(filters, bigtable.LatestNFilter(1), bigtable.TimestampRangeFilter(t, time.Time{}))...)
}

// condApply applies the conditional mutation mut to row with the retries of
// the collector, each attempt within the budget b. sent tells whether an
// attempt was sent, so whether the row may have changed.
func condApply(ctx context.Context, b writeBudget, table *bigtable.Table, row string, mut *bigtable.Mutation) (matched, sent bool, err error) {
	err = retryWith(ctx, firstRetryWait, b, func(actx context.Context) error {
		sent = true
		return table.Apply(actx, row, mut, bigtable.GetCondMutationResult(&matched))
	})
	return matched, sent, err
}

// rangeOf returns the scan range of key.
func (s *step) rangeOf(key string) int {
	return sort.Search(len(s.bounds)-2, func(i int) bool { return s.bounds[i+1] > key })
}

// fail records the first worker error and stops the step.
func (s *step) fail(err error) {
	s.errOnce.Do(func() { s.err = err })
	s.stop.Store(true)
}

// runRanges runs work for each range that has items, one worker per range,
// and returns when every worker returned, so when every call it sent has an
// outcome. It times the level.
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

// inFlight runs call for each of items, with at most share calls in flight,
// started in order, and waits for all. It starts none after the step
// stopped. A call returns an error only when the step must stop.
func (s *step) inFlight(items []int32, call func(i int32) error) error {
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

// runMapLevel deletes the map rows of the plan, level 0, each row by the
// worker of the range of its request. A row that names a candidate and is
// kept or fails keeps both ends.
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
		for b, lo := 0, 0; lo < len(its); b, lo = b+1, lo+s.d.opts.BatchSize {
			batch := its[lo:min(lo+s.d.opts.BatchSize, len(its))]
			if s.stop.Load() {
				failed[rng] = append(failed[rng], its[lo:]...)
				return nil
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
			if err := s.writeRecord(ctx, 0, rng, b, entries); err != nil {
				failed[rng] = append(failed[rng], its[lo:]...)
				return err
			}
			err := s.inFlight(batch, func(i int32) error {
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
					if errors.Is(err, errLeaseExpiring) {
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

// runObjectLevel deletes the candidates of level l that are still to try,
// then fails every key of the level that was not deleted, so that no child
// of it is tried.
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
		for b, lo := 0, 0; lo < len(its); b, lo = b+1, lo+s.d.opts.BatchSize {
			if s.stop.Load() {
				return nil
			}
			batch := its[lo:min(lo+s.d.opts.BatchSize, len(its))]
			if err := s.lim.wait(ctx, len(batch)); err != nil {
				return err
			}
			if err := s.deleteBatch(ctx, l, rng, b, batch); err != nil {
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

// rowState is what the delete step read of an objects row.
type rowState struct {
	present bool
	marker  []byte
	content []byte
}

// readRows reads the meta, condemn marker and content descriptor of rows.
func (s *step) readRows(ctx context.Context, rows []string) (map[string]rowState, error) {
	return readObjectRows(ctx, s.objects, s.reads, rows)
}

// readObjectRows reads the meta, condemn marker and content descriptor of rows
// of the objects table, in reads of deleteReadBatch keys, a few at a time,
// each attempt within the budget b.
func readObjectRows(ctx context.Context, objects *bigtable.Table, b writeBudget, rows []string) (map[string]rowState, error) {
	filter := bigtable.RowFilter(bigtable.ChainFilters(bigtable.LatestNFilter(1), bigtable.InterleaveFilters(
		bigtable.ChainFilters(bigtable.FamilyFilter(objectsMetaFamily), bigtable.StripValueFilter()),
		bigtable.ChainFilters(bigtable.FamilyFilter(ObjectsFamily), bigtable.ColumnFilter(CondemnedColumn)),
		bigtable.ChainFilters(bigtable.FamilyFilter(objectsDataFamily), bigtable.ColumnFilter(objectsContentColumn)),
	)))
	states := make(map[string]rowState, len(rows))
	var mu sync.Mutex
	var eg errgroup.Group
	eg.SetLimit(deleteReadsInFlight)
	for lo := 0; lo < len(rows); lo += deleteReadBatch {
		chunk := rows[lo:min(lo+deleteReadBatch, len(rows))]
		eg.Go(func() error {
			return retryWith(ctx, firstRetryWait, b, func(actx context.Context) error {
				return objects.ReadRows(actx, bigtable.RowList(chunk), func(row bigtable.Row) bool {
					var st rowState
					st.present = len(row[objectsMetaFamily]) > 0
					for _, item := range row[ObjectsFamily] {
						st.marker = item.Value
					}
					for _, item := range row[objectsDataFamily] {
						st.content = item.Value
					}
					mu.Lock()
					states[row.Key()] = st
					mu.Unlock()
					return true
				}, filter)
			})
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return states, nil
}

// deleteBatch runs the delete of one batch of candidates of a range: it
// reads their rows, fails those whose marker is not the one of the list,
// records the rest with their content descriptors, and then deletes each
// whose newest marker is still that one, and its gc row.
func (s *step) deleteBatch(ctx context.Context, level, rng, batch int, items []int32) error {
	p := s.p
	rows := make([]string, len(items))
	for k, i := range items {
		rows[k] = s.g.key(p.cands[i]).String()
	}
	states, err := s.readRows(ctx, rows)
	if err != nil {
		return fmt.Errorf("failed to read the objects to delete: %w", err)
	}
	var left []int32
	var entries []recordEntry
	for k, i := range items {
		st := states[rows[k]]
		switch {
		case !st.present:
			p.outcome[i] = outcomeGone
		case !slices.Equal(st.marker, s.marker):
			p.outcome[i] = outcomeNotMatched
		default:
			left = append(left, i)
			entries = append(entries, recordEntry{kind: recordObject, key: s.g.key(p.cands[i]), content: recordedContent(st.content)})
		}
	}
	if len(left) == 0 || s.stop.Load() {
		return nil
	}
	if err := s.writeRecord(ctx, level, rng, batch, entries); err != nil {
		return err
	}
	return s.inFlight(left, func(i int32) error {
		return s.deleteKey(ctx, int(i))
	})
}

// deleteKey deletes the objects row of candidate i if its newest marker is
// the one of the list, and then its gc row unless a write since the grace
// start made it newer. A key that did not match is read again, because a
// retry after a lost response finds the row deleted. It returns an error
// only when the step must stop.
func (s *step) deleteKey(ctx context.Context, i int) error {
	p := s.p
	row := s.g.key(p.cands[i]).String()
	matched, sent, err := condApply(ctx, s.d.budget, s.objects, row, s.deleteObject)
	if err != nil {
		if !sent {
			return err
		}
		p.outcome[i] = outcomeUnknown
		if errors.Is(err, errLeaseExpiring) {
			return err
		}
		return nil
	}
	if !matched {
		states, err := s.readRows(ctx, []string{row})
		switch {
		case err != nil:
			p.outcome[i] = outcomeUnknown
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
		if errors.Is(err, errLeaseExpiring) {
			return err
		}
	case kept:
		s.rep.GCRowsKept++
	default:
		s.rep.GCRowsDeleted++
	}
	return nil
}

// writeRecord writes the delete record of one batch, before any delete of it.
func (s *step) writeRecord(ctx context.Context, level, rng, batch int, entries []recordEntry) error {
	h := s.header
	name := recordObjectName(s.state, h.cycle, level, rng, batch)
	if err := writeRecord(ctx, s.d.budget, s.d.bucket, name, &h, entries); err != nil {
		return err
	}
	s.mu.Lock()
	s.rep.RecordsWritten++
	s.mu.Unlock()
	return nil
}

// resolveUnknown reads the keys whose delete had an unknown outcome, after
// every call of the step ended: a key without a row was deleted, a key with
// one stays in the list of this cycle.
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
		return fmt.Errorf("failed to read the %d objects whose delete had an unknown outcome; the cycle stops before its list, so the next cycle does not delete: %w", len(unknown), err)
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

// mapRowEnds returns the two keys of the map row key, which the scan checked
// to be two hashes.
func mapRowEnds(key string) (from, to Hash) {
	f, t, _ := strings.Cut(key, "#")
	return entities.NewHash(f), entities.NewHash(t)
}

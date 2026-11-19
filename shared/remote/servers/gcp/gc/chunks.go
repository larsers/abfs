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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DefaultChunkRate = 500
	// chunkRecordWorkers bounds the records; DeleteWorkers bounds the calls.
	chunkRecordWorkers = 16
	chunksDoneMetadata = "chunks-done"
	gcRowsDoneMetadata = "gc-rows-done"
	// gcRowPassDelay is how long after its record the step repeats the gc
	// row deletes that a stopped delete step left.
	gcRowPassDelay  = 15 * time.Minute
	chunksTableName = "chunks"
	bigtableDepot   = "bigtable"
	bucketDepot     = "bucket"
)

// ChunkStepReport reports the chunk step of a delete cycle.
type ChunkStepReport struct {
	Skipped           []string `json:"skipped,omitempty"`
	RecordsSkipped    int64    `json:"records_skipped"`
	ListFailed        bool     `json:"list_failed,omitempty"`
	Records           int64    `json:"records"`
	RecordsDone       int64    `json:"records_done"`
	RecordsWaiting    int64    `json:"records_waiting"`
	RecordsGCRowsDone int64    `json:"records_gc_rows_done"`
	RecordErrors      int64    `json:"record_errors,omitempty"`
	// Unflagged counts records past the undo window and a day still without
	// the flag; they are never pruned.
	Unflagged              int64   `json:"unflagged"`
	OldestUnflaggedSeconds float64 `json:"oldest_unflagged_seconds,omitempty"`
	Keys                   int64   `json:"keys"`
	KeptRowPresent         int64   `json:"kept_row_present"`
	RePutAfterDelete       int64   `json:"reput_after_delete"`
	RowAbsent              int64   `json:"row_absent"`
	ChunkRowsDeleted       int64   `json:"chunk_rows_deleted"`
	BucketObjectsDeleted   int64   `json:"bucket_objects_deleted"`
	ChunksGone             int64   `json:"chunks_gone"`
	RestoredAfterRecord    int64   `json:"restored_after_record"`
	DeleteRefused          int64   `json:"delete_refused"`
	UnknownDepot           int64   `json:"unknown_depot,omitempty"`
	GCRowsDeleted          int64   `json:"gc_rows_deleted"`
	GCRowsKept             int64   `json:"gc_rows_kept"`
	Errors                 int64   `json:"errors"`
	ErrorSample            string  `json:"error_sample,omitempty"`
	Seconds                float64 `json:"seconds"`
}

type chunkStep struct {
	d       *deleter
	cycle   int64
	rep     *ChunkStepReport
	lim     *limiter
	reads   writeBudget
	objects *bigtable.Table
	chunks  *bigtable.Table
	gcTable *bigtable.Table
	calls   chan struct{}
	// mu guards the counts of rep.
	mu sync.Mutex
}

// deleteChunks deletes the chunks named by delete records past the undo
// window and flags each finished record. It fails only when the lease or the
// run ends; any other failure leaves the record for the next cycle.
func (d *deleter) deleteChunks(ctx context.Context, cycle int64) error {
	start := time.Now()
	s := &chunkStep{
		d: d, cycle: cycle, rep: &ChunkStepReport{},
		lim:     newLimiter(d.opts.ChunkRate),
		reads:   writeBudget{deadline: d.opts.WriteDeadline},
		objects: d.bt.Open(objectsTable),
		chunks:  d.bt.Open(chunksTableName),
		gcTable: d.bt.Open(TableName),
		calls:   make(chan struct{}, d.opts.DeleteWorkers),
	}
	d.rep.Chunks = s.rep
	defer func() { s.rep.Seconds = time.Since(start).Seconds() }()
	unflagged, listErr := s.listRecords(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if listErr != nil {
		s.rep.ListFailed = true
		s.rep.Skipped = append(s.rep.Skipped, listErr.Error())
	} else {
		skipped, err := s.preconditions(ctx)
		if err != nil {
			return err
		}
		s.rep.Skipped = skipped
	}
	done := make(map[string]bool)
	if listErr == nil {
		now := wallNow()
		eg, ectx := errgroup.WithContext(ctx)
		eg.SetLimit(chunkRecordWorkers)
		for _, attrs := range unflagged {
			chunksDue := !attrs.Created.Add(undoWindow).After(now)
			switch {
			case !chunksDue:
				s.rep.RecordsWaiting++
			case len(s.rep.Skipped) > 0:
				s.rep.RecordsSkipped++
			}
			switch {
			case chunksDue && len(s.rep.Skipped) == 0:
				eg.Go(func() error {
					ok, err := s.record(ectx, attrs)
					s.mu.Lock()
					done[attrs.Name] = ok
					s.mu.Unlock()
					return err
				})
			case !attrs.Created.Add(gcRowPassDelay).After(now) && attrs.Metadata[gcRowsDoneMetadata] == "":
				eg.Go(func() error {
					return s.gcRows(ectx, attrs)
				})
			}
		}
		if err := eg.Wait(); err != nil {
			return err
		}
	}
	for _, reason := range s.rep.Skipped {
		log.Printf("Warning: GC cycle %d deletes no chunks: %s", cycle, reason)
	}
	for _, attrs := range unflagged {
		age := wallNow().Sub(attrs.Created)
		if done[attrs.Name] || age < undoWindow+24*time.Hour {
			continue
		}
		s.rep.Unflagged++
		s.rep.OldestUnflaggedSeconds = max(s.rep.OldestUnflaggedSeconds, age.Seconds())
	}
	log.Printf("GC cycle %d deleted %d chunk rows and %d bucket objects of %d delete records; %d records wait for the undo window, %d stay without the flag past it",
		cycle, s.rep.ChunkRowsDeleted, s.rep.BucketObjectsDeleted, s.rep.RecordsDone, s.rep.RecordsWaiting, s.rep.Unflagged)
	return nil
}

func (s *chunkStep) preconditions(ctx context.Context) ([]string, error) {
	var attrs *storage.BucketAttrs
	err := retryWith(ctx, firstRetryWait, s.reads, func(actx context.Context) error {
		var err error
		attrs, err = s.d.bucket.Attrs(actx)
		return err
	})
	if stops(ctx, err) {
		return nil, err
	}
	if err != nil {
		return []string{fmt.Sprintf("cannot read the settings of the bucket%s: %v", permissionHint(err, "storage.buckets.get"), err)}, nil
	}
	return bucketProblems(attrs), nil
}

func bucketProblems(attrs *storage.BucketAttrs) []string {
	var reasons []string
	if attrs.VersioningEnabled {
		reasons = append(reasons, "the bucket has Object Versioning on, under which a delete keeps the chunk as a noncurrent version")
	}
	if p := attrs.RetentionPolicy; p != nil && p.RetentionPeriod > 0 {
		reasons = append(reasons, fmt.Sprintf("the bucket has a retention policy of %s, which refuses deletes of younger objects", p.RetentionPeriod))
	}
	if attrs.DefaultEventBasedHold {
		reasons = append(reasons, "the bucket puts an event-based hold on each new object, which refuses its delete")
	}
	return reasons
}

// listRecords returns the unflagged delete records, oldest first.
func (s *chunkStep) listRecords(ctx context.Context) ([]*storage.ObjectAttrs, error) {
	prefix := recordPrefixOf(s.d.opts.StateName)
	var records []*storage.ObjectAttrs
	it := s.d.bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return records, fmt.Errorf("failed to list the delete records under %s: %w", prefix, err)
		}
		if attrs.Metadata[chunksDoneMetadata] == "" {
			records = append(records, attrs)
		}
	}
	slices.SortStableFunc(records, func(a, b *storage.ObjectAttrs) int { return a.Created.Compare(b.Created) })
	return records, nil
}

func (s *chunkStep) gcRows(ctx context.Context, attrs *storage.ObjectAttrs) error {
	h, entries, err := readRecordKeys(ctx, s.d.bucket, attrs.Name)
	if err != nil {
		s.fail(&s.rep.RecordErrors, err)
		return ctx.Err()
	}
	var rows []string
	for _, e := range entries {
		if e.kind == recordObject {
			rows = append(rows, e.key.String())
		}
	}
	states, err := readObjectRows(ctx, s.objects, s.reads, rows)
	if err != nil {
		s.fail(&s.rep.Errors, fmt.Errorf("failed to read the objects of the delete record %s: %w", attrs.Name, err))
		return ctx.Err()
	}
	gcRowDelete := conditionalDelete(newerCellPredicate(EdgesFamily, LinksColumn, h.gcRowsAfter), false)
	eg, ectx := errgroup.WithContext(ctx)
	var mu sync.Mutex
	allDone := true
	for _, row := range rows {
		if states[row].present {
			continue
		}
		if !s.acquire(ectx) {
			break
		}
		eg.Go(func() error {
			defer func() { <-s.calls }()
			ok, err := s.gcRow(ectx, row, gcRowDelete)
			mu.Lock()
			allDone = allDone && ok
			mu.Unlock()
			return err
		})
	}
	if err := eg.Wait(); err != nil || !allDone {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := s.flag(ctx, attrs, gcRowsDoneMetadata); err != nil {
		if stops(ctx, err) {
			return err
		}
		s.fail(&s.rep.Errors, fmt.Errorf("failed to flag the delete record %s: %w", attrs.Name, err))
		return nil
	}
	s.count(&s.rep.RecordsGCRowsDone)
	return nil
}

// record reports whether it flagged the record.
func (s *chunkStep) record(ctx context.Context, attrs *storage.ObjectAttrs) (bool, error) {
	h, entries, err := readRecord(ctx, s.d.bucket, attrs.Name)
	if err != nil {
		s.fail(&s.rep.RecordErrors, err)
		return false, ctx.Err()
	}
	s.count(&s.rep.Records)
	var keys []*recordEntry
	var rows []string
	for i := range entries {
		if e := &entries[i]; e.kind == recordObject {
			keys = append(keys, e)
			rows = append(rows, e.key.String())
		}
	}
	s.mu.Lock()
	s.rep.Keys += int64(len(keys))
	s.mu.Unlock()
	states, err := readObjectRows(ctx, s.objects, s.reads, rows)
	if err != nil {
		s.fail(&s.rep.Errors, fmt.Errorf("failed to read the objects of the delete record %s: %w", attrs.Name, err))
		return false, ctx.Err()
	}
	// A chunk row written at or after the record, as by an undo, stays.
	chunkRowDelete := conditionalDelete(bigtable.TimestampRangeFilter(attrs.Created, time.Time{}), false)
	var gcRowDelete *bigtable.Mutation
	if attrs.Metadata[gcRowsDoneMetadata] == "" {
		gcRowDelete = conditionalDelete(newerCellPredicate(EdgesFamily, LinksColumn, h.gcRowsAfter), false)
	}
	eg, ectx := errgroup.WithContext(ctx)
	var mu sync.Mutex
	allDone := true
	for _, e := range keys {
		if !s.acquire(ectx) {
			break
		}
		eg.Go(func() error {
			defer func() { <-s.calls }()
			ok, err := s.key(ectx, e, states[e.key.String()], attrs.Created, chunkRowDelete, gcRowDelete)
			mu.Lock()
			allDone = allDone && ok
			mu.Unlock()
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return false, err
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if !allDone {
		return false, nil
	}
	if err := s.flag(ctx, attrs, chunksDoneMetadata); err != nil {
		if stops(ctx, err) {
			return false, err
		}
		s.fail(&s.rep.Errors, fmt.Errorf("failed to flag the delete record %s: %w", attrs.Name, err))
		return false, nil
	}
	s.count(&s.rep.RecordsDone)
	return true, nil
}

func (s *chunkStep) acquire(ctx context.Context) bool {
	select {
	case s.calls <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *chunkStep) flag(ctx context.Context, attrs *storage.ObjectAttrs, key string) error {
	metadata := maps.Clone(attrs.Metadata)
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[key] = strconv.FormatInt(s.cycle, 10)
	return retryWith(ctx, firstRetryWait, s.d.budget, func(actx context.Context) error {
		_, err := s.d.bucket.Object(attrs.Name).Update(actx, storage.ObjectAttrsToUpdate{Metadata: metadata})
		return err
	})
}

type recordedChunk struct {
	Depot string `json:"depot"`
	ID    string `json:"id"`
}

// key deletes the recorded chunks of e unless its row still holds them. It
// reports whether the key is done.
func (s *chunkStep) key(ctx context.Context, e *recordEntry, st rowState, recordCreated time.Time, chunkRowDelete, gcRowDelete *bigtable.Mutation) (bool, error) {
	var recorded, current []recordedChunk
	content := e.content()
	if len(content) > 0 {
		if err := json.Unmarshal(content, &recorded); err != nil {
			s.fail(&s.rep.Errors, fmt.Errorf("invalid content descriptor of %s in its delete record: %w", e.key, err))
			return false, nil
		}
	}
	if st.present {
		// Only a row written again with other chunks proves the delete
		// happened; anything else keeps the chunks.
		if len(recorded) == 0 || len(st.content) == 0 || bytes.Equal(st.content, content) || json.Unmarshal(st.content, &current) != nil {
			s.count(&s.rep.KeptRowPresent)
			return true, nil
		}
		s.count(&s.rep.RePutAfterDelete)
	} else {
		s.count(&s.rep.RowAbsent)
	}
	done := true
	for _, c := range recorded {
		if slices.Contains(current, c) {
			continue
		}
		ok, err := s.chunk(ctx, c, recordCreated, chunkRowDelete)
		if err != nil {
			return false, err
		}
		done = done && ok
	}
	if st.present || gcRowDelete == nil {
		return done, nil
	}
	ok, err := s.gcRow(ctx, e.key.String(), gcRowDelete)
	return done && ok, err
}

// gcRow deletes the gc row of an object without a row; a key written again
// since has a newer e:links cell and keeps it.
func (s *chunkStep) gcRow(ctx context.Context, row string, gcRowDelete *bigtable.Mutation) (bool, error) {
	kept, _, err := condApply(ctx, s.d.budget, s.gcTable, row, gcRowDelete)
	switch {
	case err != nil:
		if stops(ctx, err) {
			return false, err
		}
		s.fail(&s.rep.Errors, fmt.Errorf("failed to delete the gc row of %s: %w", row, err))
		return false, nil
	case kept:
		s.count(&s.rep.GCRowsKept)
	default:
		s.count(&s.rep.GCRowsDeleted)
	}
	return true, nil
}

func (s *chunkStep) chunk(ctx context.Context, c recordedChunk, recordCreated time.Time, chunkRowDelete *bigtable.Mutation) (bool, error) {
	switch c.Depot {
	case inlineDepot:
		return true, nil
	case bigtableDepot:
		if err := s.lim.wait(ctx, 1); err != nil {
			return false, err
		}
		kept, _, err := condApply(ctx, s.d.budget, s.chunks, c.ID, chunkRowDelete)
		switch {
		case err != nil:
			if stops(ctx, err) {
				return false, err
			}
			s.fail(&s.rep.Errors, fmt.Errorf("failed to delete the chunk row %s: %w", c.ID, err))
			return false, nil
		case kept:
			s.count(&s.rep.RestoredAfterRecord)
		default:
			s.count(&s.rep.ChunkRowsDeleted)
		}
		return true, nil
	case bucketDepot:
		return s.bucketChunk(ctx, c.ID, recordCreated)
	}
	s.count(&s.rep.UnknownDepot)
	return false, nil
}

type bucketOutcome int

const (
	bucketDeleted bucketOutcome = iota
	bucketGone
	bucketPreconditionFailed
	bucketRefused
	bucketFailed
)

// bucketChunk deletes the bucket object id under the generation and
// metageneration it read, so an object restored or changed in between stays.
func (s *chunkStep) bucketChunk(ctx context.Context, id string, recordCreated time.Time) (bool, error) {
	obj := s.d.bucket.Object(id)
	attrs, err := s.chunkAttrs(ctx, obj)
	switch {
	case errors.Is(err, storage.ErrObjectNotExist):
		s.count(&s.rep.ChunksGone)
		return true, nil
	case err != nil:
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		s.fail(&s.rep.Errors, fmt.Errorf("failed to read the bucket chunk %s: %w", id, err))
		return false, nil
	case restoredSince(attrs, recordCreated):
		s.count(&s.rep.RestoredAfterRecord)
		return true, nil
	case held(attrs, wallNow()):
		s.count(&s.rep.DeleteRefused)
		return false, nil
	}
	if err := s.lim.wait(ctx, 1); err != nil {
		return false, err
	}
	err = retryWith(ctx, firstRetryWait, s.d.budget, func(actx context.Context) error {
		return obj.If(storage.Conditions{GenerationMatch: attrs.Generation, MetagenerationMatch: attrs.Metageneration}).Delete(actx)
	})
	if stops(ctx, err) {
		return false, err
	}
	switch bucketDeleteOutcome(err) {
	case bucketDeleted:
		s.count(&s.rep.BucketObjectsDeleted)
	case bucketGone:
		s.count(&s.rep.ChunksGone)
	case bucketPreconditionFailed:
		// Only a new generation or an undo mark keeps the chunk for good;
		// anything else, such as a hold, is a refusal retried later.
		now, err := s.chunkAttrs(ctx, obj)
		switch {
		case errors.Is(err, storage.ErrObjectNotExist):
			s.count(&s.rep.ChunksGone)
		case err != nil:
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			s.fail(&s.rep.Errors, fmt.Errorf("failed to read the bucket chunk %s after a refused delete: %w", id, err))
			return false, nil
		case now.Generation != attrs.Generation || restoredSince(now, recordCreated):
			s.count(&s.rep.RestoredAfterRecord)
		default:
			s.count(&s.rep.DeleteRefused)
			return false, nil
		}
	case bucketRefused:
		s.count(&s.rep.DeleteRefused)
		return false, nil
	default:
		s.fail(&s.rep.Errors, fmt.Errorf("failed to delete the bucket chunk %s: %w", id, err))
		return false, nil
	}
	return true, nil
}

func (s *chunkStep) chunkAttrs(ctx context.Context, obj *storage.ObjectHandle) (*storage.ObjectAttrs, error) {
	var attrs *storage.ObjectAttrs
	err := retryWith(ctx, firstRetryWait, s.reads, func(actx context.Context) error {
		var err error
		attrs, err = obj.Attrs(actx)
		return err
	})
	return attrs, err
}

// restoredSince reports whether the chunk was written, restored or kept by an
// undo at or after recordCreated. A metadata change alone is no restore.
func restoredSince(attrs *storage.ObjectAttrs, recordCreated time.Time) bool {
	if !attrs.Created.Before(recordCreated) {
		return true
	}
	_, undone := attrs.Metadata[undoMetadata]
	return undone && !attrs.Updated.Before(recordCreated)
}

func held(attrs *storage.ObjectAttrs, now time.Time) bool {
	return attrs.EventBasedHold || attrs.TemporaryHold ||
		(attrs.Retention != nil && attrs.Retention.RetainUntil.After(now)) ||
		attrs.RetentionExpirationTime.After(now)
}

func bucketDeleteOutcome(err error) bucketOutcome {
	var gerr *googleapi.Error
	switch {
	case err == nil:
		return bucketDeleted
	case errors.Is(err, storage.ErrObjectNotExist):
		return bucketGone
	case preconditionFailed(err):
		return bucketPreconditionFailed
	case errors.As(err, &gerr) && gerr.Code == http.StatusForbidden, status.Code(err) == codes.PermissionDenied:
		return bucketRefused
	}
	return bucketFailed
}

func stops(ctx context.Context, err error) bool {
	return errors.Is(err, errLeaseExpiring) || (err != nil && ctx.Err() != nil)
}

func (s *chunkStep) count(n *int64) {
	s.mu.Lock()
	*n++
	s.mu.Unlock()
}

// fail counts err in n and keeps the first error of the step.
func (s *chunkStep) fail(n *int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*n++
	if s.rep.ErrorSample == "" {
		s.rep.ErrorSample = err.Error()
	}
}

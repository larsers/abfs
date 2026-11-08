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
	// chunkRecordWorkers is the number of delete records the chunk step works
	// on at once. The records are independent; the calls in flight of all of
	// them are bounded by the delete workers.
	chunkRecordWorkers = 16
	// chunksDoneMetadata flags a delete record whose chunks are deleted,
	// with the cycle that deleted them.
	chunksDoneMetadata = "chunks-done"
	// minSoftDeleteRetention is the shortest time the bucket must keep a
	// deleted object restorable for the chunk step to delete bucket chunks.
	minSoftDeleteRetention = 7 * 24 * time.Hour
	// The depots of the server: the chunks table keeps a row per chunk,
	// named by the chunk ID, and the bucket an object.
	chunksTableName = "chunks"
	bigtableDepot   = "bigtable"
	bucketDepot     = "bucket"
)

// ChunkStepReport reports the chunk step of a delete cycle, which deletes the
// chunks of the objects that the delete records of earlier cycles name.
type ChunkStepReport struct {
	// Skipped lists the reasons why the step deleted nothing.
	Skipped []string `json:"skipped,omitempty"`
	// Records counts the records the step read and RecordsDone those it
	// flagged. RecordsWaiting counts the records younger than the chunk
	// delay, RecordsWithoutBackup those without a ready backup of the chunks
	// table started after their list was complete, and RecordErrors those
	// that could not be read.
	Records              int64 `json:"records"`
	RecordsDone          int64 `json:"records_done"`
	RecordsWaiting       int64 `json:"records_waiting"`
	RecordsWithoutBackup int64 `json:"records_without_backup,omitempty"`
	RecordErrors         int64 `json:"record_errors,omitempty"`
	// Unflagged counts the records left without the flag after the step,
	// which are never pruned, and OldestUnflaggedSeconds the age of the
	// oldest.
	Unflagged              int64   `json:"unflagged"`
	OldestUnflaggedSeconds float64 `json:"oldest_unflagged_seconds,omitempty"`
	// Keys counts the objects of the records read. KeptRowPresent still have
	// their row with the recorded chunks, which stay; RePutAfterDelete have a
	// row written again with other chunks, and RowAbsent no row, and lose
	// the recorded chunks.
	Keys             int64 `json:"keys"`
	KeptRowPresent   int64 `json:"kept_row_present"`
	RePutAfterDelete int64 `json:"reput_after_delete"`
	RowAbsent        int64 `json:"row_absent"`
	// ChunkRowsDeleted and BucketObjectsDeleted count the chunks deleted and
	// ChunksGone those already gone. RestoredAfterRecord counts the chunks
	// written or changed after their record, as by an undo, which stay.
	// DeleteRefused counts the bucket deletes the bucket refused, as for an
	// object hold, and UnknownDepot the chunks of a depot the step does not
	// know; both leave their record without the flag.
	ChunkRowsDeleted     int64 `json:"chunk_rows_deleted"`
	BucketObjectsDeleted int64 `json:"bucket_objects_deleted"`
	ChunksGone           int64 `json:"chunks_gone"`
	RestoredAfterRecord  int64 `json:"restored_after_record"`
	DeleteRefused        int64 `json:"delete_refused"`
	UnknownDepot         int64 `json:"unknown_depot,omitempty"`
	// GCRowsDeleted counts the gc rows of objects without a row that the
	// delete step left, deleted now, and GCRowsKept those of objects
	// written again since the grace start of the record.
	GCRowsDeleted int64 `json:"gc_rows_deleted"`
	GCRowsKept    int64 `json:"gc_rows_kept"`
	// Errors counts the reads and deletes that failed, the first in
	// ErrorSample; each leaves its record without the flag.
	Errors      int64   `json:"errors"`
	ErrorSample string  `json:"error_sample,omitempty"`
	Seconds     float64 `json:"seconds"`
}

// chunkStep is one chunk step.
type chunkStep struct {
	d     *deleter
	cycle int64
	rep   *ChunkStepReport
	lim   *limiter
	reads writeBudget
	// backups are the ready backups of the chunks table, when checked.
	backups        []Backup
	backupsChecked bool
	objects        *bigtable.Table
	chunks         *bigtable.Table
	gcTable        *bigtable.Table
	// calls bounds the keys worked on at once over all records.
	calls chan struct{}
	// mu guards the counts of rep.
	mu sync.Mutex
}

// deleteChunks is the chunk step: it deletes the chunks of the objects that
// the delete records of earlier cycles name, from the records created at
// least the chunk delay ago that carry no flag yet, and flags each record
// whose keys are all done. A key whose row is present keeps its chunks. The
// step fails only when the lease or the run ends; any other failure leaves
// a record without the flag, to try again in the next cycle.
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
	s.rep.Skipped = s.preconditions(ctx)
	unflagged, err := s.listRecords(ctx)
	if err != nil {
		s.rep.Skipped = append(s.rep.Skipped, err.Error())
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	done := make(map[string]bool)
	if len(s.rep.Skipped) == 0 {
		now := wallNow()
		eg, ectx := errgroup.WithContext(ctx)
		eg.SetLimit(chunkRecordWorkers)
		for _, attrs := range unflagged {
			if attrs.Created.Add(d.opts.ChunkDelay).After(now) {
				s.rep.RecordsWaiting++
				continue
			}
			eg.Go(func() error {
				ok, err := s.record(ectx, attrs)
				s.mu.Lock()
				done[attrs.Name] = ok
				s.mu.Unlock()
				return err
			})
		}
		if err := eg.Wait(); err != nil {
			return err
		}
	}
	for _, reason := range s.rep.Skipped {
		log.Printf("Warning: GC cycle %d deletes no chunks: %s", cycle, reason)
	}
	for _, attrs := range unflagged {
		if done[attrs.Name] {
			continue
		}
		s.rep.Unflagged++
		s.rep.OldestUnflaggedSeconds = max(s.rep.OldestUnflaggedSeconds, wallNow().Sub(attrs.Created).Seconds())
	}
	log.Printf("GC cycle %d deleted %d chunk rows and %d bucket objects of %d delete records; %d records wait for the chunk delay, %d stay without the flag",
		cycle, s.rep.ChunkRowsDeleted, s.rep.BucketObjectsDeleted, s.rep.RecordsDone, s.rep.RecordsWaiting, s.rep.Unflagged)
	return nil
}

// preconditions returns why the step must not delete: settings of the bucket
// that would make a delete permanent or turn it into something else, or
// backups of the chunks table that cannot be read.
func (s *chunkStep) preconditions(ctx context.Context) []string {
	o := s.d.opts
	var attrs *storage.BucketAttrs
	err := retryWith(ctx, firstRetryWait, s.reads, func(actx context.Context) error {
		var err error
		attrs, err = s.d.bucket.Attrs(actx)
		return err
	})
	if err != nil {
		return []string{fmt.Sprintf("cannot read the settings of the bucket%s: %v", permissionHint(err, "storage.buckets.get"), err)}
	}
	reasons := bucketProblems(attrs, o.AllowNoSoftDelete)
	if !o.NoBackupCheck {
		backups, err := s.d.checks.Backups(ctx)
		if err != nil {
			return append(reasons, fmt.Sprintf("cannot list the backups of the Bigtable instance%s: %v", permissionHint(err, "bigtable.backups.list"), err))
		}
		s.backupsChecked = true
		now := wallNow()
		for _, b := range backups {
			if b.Table == chunksTableName && b.Ready && (b.Expire.IsZero() || b.Expire.After(now)) {
				s.backups = append(s.backups, b)
			}
		}
	}
	return reasons
}

// bucketProblems returns the settings of a bucket with attrs under which the
// chunk step must not delete bucket chunks.
func bucketProblems(attrs *storage.BucketAttrs, allowNoSoftDelete bool) []string {
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
	if attrs.ObjectRetentionMode != "" {
		// The JSON API reports it as "Enabled"; the gRPC API does not report it.
		reasons = append(reasons, fmt.Sprintf("the bucket has object retention %s, under which an object can refuse its delete", attrs.ObjectRetentionMode))
	}
	var retention time.Duration
	if p := attrs.SoftDeletePolicy; p != nil {
		retention = p.RetentionDuration
	}
	if !allowNoSoftDelete && retention < minSoftDeleteRetention {
		reasons = append(reasons, fmt.Sprintf("the bucket keeps deleted objects for %s in soft delete, less than %s, so a deleted chunk could not be restored; pass --allow-no-soft-delete to delete anyway", retention, minSoftDeleteRetention))
	}
	return reasons
}

// hasBackupAfter reports whether a backup of the chunks table started after
// listComplete, when one is needed: it holds every chunk row of the objects
// of the list completed then.
func (s *chunkStep) hasBackupAfter(listComplete time.Time) bool {
	return !s.backupsChecked || slices.ContainsFunc(s.backups, func(b Backup) bool { return b.Start.After(listComplete) })
}

// listRecords returns the delete records of the state without the flag,
// oldest first, and counts them.
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

// record deletes the chunks of the objects of one record and flags it when
// every key is done. It reports whether it flagged the record.
func (s *chunkStep) record(ctx context.Context, attrs *storage.ObjectAttrs) (bool, error) {
	h, entries, err := readRecord(ctx, s.d.bucket, attrs.Name)
	if err != nil {
		s.fail(&s.rep.RecordErrors, err)
		return false, ctx.Err()
	}
	s.count(&s.rep.Records)
	if !s.hasBackupAfter(h.listComplete) {
		s.count(&s.rep.RecordsWithoutBackup)
		return false, nil
	}
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
	gcRowDelete := conditionalDelete(newerCellPredicate(EdgesFamily, LinksColumn, h.gcRowsAfter), false)
	var eg errgroup.Group
	var mu sync.Mutex
	allDone := true
	for _, e := range keys {
		eg.Go(func() error {
			select {
			case s.calls <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			defer func() { <-s.calls }()
			ok, err := s.key(ctx, e, states[e.key.String()], attrs.Created, chunkRowDelete, gcRowDelete)
			mu.Lock()
			allDone = allDone && ok
			mu.Unlock()
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return false, err
	}
	if !allDone {
		return false, nil
	}
	if err := s.flag(ctx, attrs); err != nil {
		if stops(ctx, err) {
			return false, err
		}
		s.fail(&s.rep.Errors, fmt.Errorf("failed to flag the delete record %s: %w", attrs.Name, err))
		return false, nil
	}
	s.count(&s.rep.RecordsDone)
	return true, nil
}

// flag sets the metadata chunksDoneMetadata of the record to the cycle.
func (s *chunkStep) flag(ctx context.Context, attrs *storage.ObjectAttrs) error {
	metadata := maps.Clone(attrs.Metadata)
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[chunksDoneMetadata] = strconv.FormatInt(s.cycle, 10)
	return retryWith(ctx, firstRetryWait, s.d.budget, func(actx context.Context) error {
		_, err := s.d.bucket.Object(attrs.Name).Update(actx, storage.ObjectAttrsToUpdate{Metadata: metadata})
		return err
	})
}

// recordedChunk is a chunk of a content descriptor.
type recordedChunk struct {
	Depot string `json:"depot"`
	ID    string `json:"id"`
}

// key deletes the recorded chunks of the object e, whose row the step read
// as st, unless the row still holds them, and repeats the gc row delete of
// an object without a row. It reports whether the key is done, and fails
// only when the step must stop.
func (s *chunkStep) key(ctx context.Context, e *recordEntry, st rowState, recordCreated time.Time, chunkRowDelete, gcRowDelete *bigtable.Mutation) (bool, error) {
	var recorded, current []recordedChunk
	if len(e.content) > 0 {
		if err := json.Unmarshal(e.content, &recorded); err != nil {
			s.fail(&s.rep.Errors, fmt.Errorf("invalid content descriptor of %s in its delete record: %w", e.key, err))
			return false, nil
		}
	}
	if st.present {
		// A descriptor that does not parse or is empty keeps the chunks: only
		// a row written again with other chunks proves that the delete
		// happened.
		if len(recorded) == 0 || len(st.content) == 0 || bytes.Equal(st.content, e.content) || json.Unmarshal(st.content, &current) != nil {
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
	if st.present {
		return done, nil
	}
	// The delete step may have stopped between the objects row and the gc
	// row. A key written again since has a newer e:links cell and keeps it.
	kept, _, err := condApply(ctx, s.d.budget, s.gcTable, e.key.String(), gcRowDelete)
	switch {
	case err != nil:
		if stops(ctx, err) {
			return false, err
		}
		s.fail(&s.rep.Errors, fmt.Errorf("failed to delete the gc row of %s: %w", e.key, err))
		return false, nil
	case kept:
		s.count(&s.rep.GCRowsKept)
	default:
		s.count(&s.rep.GCRowsDeleted)
	}
	return done, nil
}

// chunk deletes the chunk c of a record created at recordCreated unless it
// was written after it. It reports whether the chunk is done, and fails only
// when the step must stop.
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

// bucketOutcome is the outcome of a conditional delete of a bucket chunk.
type bucketOutcome int

const (
	bucketDeleted bucketOutcome = iota
	bucketGone
	bucketPreconditionFailed
	bucketRefused
	bucketFailed
)

// bucketChunk deletes the bucket object id unless it was written, restored
// or kept by an undo at or after recordCreated, or is held, under the
// generation and metageneration it read, so that an object restored or
// changed in between stays.
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
		// Only a new generation, as of a restore from soft delete, or the
		// mark of an undo keeps the chunk for good. Any other change, such
		// as a hold placed in between, or the same generation and
		// metageneration, as an object hold may answer, is a refusal, and a
		// later step reads the chunk again.
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

// chunkAttrs reads the attributes of the bucket chunk obj.
func (s *chunkStep) chunkAttrs(ctx context.Context, obj *storage.ObjectHandle) (*storage.ObjectAttrs, error) {
	var attrs *storage.ObjectAttrs
	err := retryWith(ctx, firstRetryWait, s.reads, func(actx context.Context) error {
		var err error
		attrs, err = obj.Attrs(actx)
		return err
	})
	return attrs, err
}

// restoredSince reports whether the bucket chunk with attrs was written or
// restored from soft delete, which gives it a new creation time, or kept by
// an undo, at or after recordCreated. An undo marks each chunk it keeps,
// after the record. A change of the metadata alone, as when a hold is placed
// or released, is no restore.
func restoredSince(attrs *storage.ObjectAttrs, recordCreated time.Time) bool {
	if !attrs.Created.Before(recordCreated) {
		return true
	}
	_, undone := attrs.Metadata[undoMetadata]
	return undone && !attrs.Updated.Before(recordCreated)
}

// held reports whether the object with attrs refuses a delete at now: it has
// a hold, or a retention that has not expired.
func held(attrs *storage.ObjectAttrs, now time.Time) bool {
	return attrs.EventBasedHold || attrs.TemporaryHold ||
		(attrs.Retention != nil && attrs.Retention.RetainUntil.After(now)) ||
		attrs.RetentionExpirationTime.After(now)
}

// bucketDeleteOutcome classifies the error of a conditional delete of a
// bucket chunk. A failed precondition means the object got a new generation
// or metageneration since it was read, as by a restore from soft delete, or
// may be the answer to an object hold; a refusal comes from an object hold
// or a retention policy.
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

// stops reports whether err of a write under ctx must stop the step.
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

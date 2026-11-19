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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// A ref record lists the refs a delete cycle removes, so they can be put back.
const (
	refRecordPrefix = "abfsgc/refs/"
	refsTable       = "Refs"
	// refRemovalBatch keeps transactions short: servers may write the same refs.
	refRemovalBatch = 500
)

func refRecordPrefixOf(state string) string {
	return refRecordPrefix + state + "/"
}

func refRecordName(state string, cycle int64) string {
	return refRecordPrefixOf(state) + strconv.FormatInt(cycle, 10)
}

type expiredRef struct {
	class              RefClass
	projectID          int64
	host, project, ref string
	updated            time.Time
	target             string
}

type refRecord struct {
	Cycle    int64            `json:"cycle"`
	Snapshot time.Time        `json:"snapshot"`
	Run      string           `json:"run"`
	Refs     []refRecordEntry `json:"refs"`
}

type refRecordEntry struct {
	Class      RefClass  `json:"class"`
	ProjectID  int64     `json:"project_id"`
	Host       string    `json:"host"`
	Project    string    `json:"project"`
	Ref        string    `json:"ref"`
	UpdateTime time.Time `json:"update_time"`
	Target     string    `json:"target"`
}

// RefRemovalReport reports the removal of the refs past their retention.
type RefRemovalReport struct {
	// Record is the ref record of the cycle, written before any removal.
	Record  string                         `json:"record,omitempty"`
	Classes map[RefClass]*RefRemovalCounts `json:"classes,omitempty"`
	// Error is why the removal stopped; the cycle then does not delete.
	Error          string `json:"error,omitempty"`
	RecordsRemoved int64  `json:"records_removed"`
}

// RefRemovalCounts counts the removal of the refs of one class.
type RefRemovalCounts struct {
	// Changed refs were written since the snapshot and stay; Gone refs had no row.
	Recorded int64 `json:"recorded"`
	Removed  int64 `json:"removed"`
	Changed  int64 `json:"changed"`
	Gone     int64 `json:"gone"`
}

func (r *RefRemovalReport) class(c RefClass) *RefRemovalCounts {
	if r.Classes == nil {
		r.Classes = make(map[RefClass]*RefRemovalCounts)
	}
	n := r.Classes[c]
	if n == nil {
		n = &RefRemovalCounts{}
		r.Classes[c] = n
	}
	return n
}

const (
	refRemoved = iota + 1
	refChanged
	refGone
)

// removeExpiredRefs records the expired refs, then removes each whose update
// time and target are still those of the snapshot; a newer write keeps it.
func (d *deleter) removeExpiredRefs(ctx context.Context, lease *Lease, res *Result) (*RefRemovalReport, error) {
	r := res.Report
	rep := &RefRemovalReport{}
	refs := res.expiredRefs
	if len(refs) == 0 {
		return rep, nil
	}
	name := refRecordName(d.opts.StateName, r.Cycle)
	rec := &refRecord{Cycle: r.Cycle, Snapshot: r.SnapshotTime, Run: runID(d.opts.Holder, lease.Epoch), Refs: make([]refRecordEntry, len(refs))}
	for i, e := range refs {
		rec.Refs[i] = refRecordEntry{Class: e.class, ProjectID: e.projectID, Host: e.host, Project: e.project, Ref: e.ref, UpdateTime: e.updated, Target: e.target}
	}
	err := createObject(ctx, d.budget, d.bucket, name, rec.Run, "application/json", func(w io.Writer) error {
		return json.NewEncoder(w).Encode(rec)
	})
	if err != nil {
		err = fmt.Errorf("failed to record the refs past their retention: %w", err)
		rep.Error = err.Error()
		log.Printf("Warning: GC cycle %d removes no refs: %v", r.Cycle, err)
		return rep, err
	}
	rep.Record = name
	for _, e := range refs {
		rep.class(e.class).Recorded++
	}
	for batch := range slices.Chunk(refs, refRemovalBatch) {
		outcomes, err := removeRefBatch(ctx, d.sp, d.budget, batch)
		if err != nil {
			err = fmt.Errorf("failed to remove %d refs past their retention: %w", len(batch), err)
			rep.Error = err.Error()
			log.Printf("Warning: GC cycle %d stopped the removal of refs: %v", r.Cycle, err)
			return rep, err
		}
		for i, oc := range outcomes {
			c := rep.class(batch[i].class)
			switch oc {
			case refRemoved:
				c.Removed++
			case refChanged:
				c.Changed++
			case refGone:
				c.Gone++
			}
		}
	}
	var removed, changed int64
	for _, c := range rep.Classes {
		removed += c.Removed
		changed += c.Changed
	}
	log.Printf("GC cycle %d removed %d refs past their retention, recorded in %s; %d were written again since the snapshot", r.Cycle, removed, name, changed)
	return rep, nil
}

// removeRefBatch removes the unchanged refs of batch; a retry reads them again.
func removeRefBatch(ctx context.Context, sp *spanner.Client, b writeBudget, batch []expiredRef) ([]int, error) {
	type refKey struct {
		projectID int64
		ref       string
	}
	type refState struct {
		updated time.Time
		target  spanner.NullString
	}
	keys := make([]spanner.KeySet, len(batch))
	for i, e := range batch {
		keys[i] = spanner.Key{e.projectID, e.ref}
	}
	outcomes := make([]int, len(batch))
	err := retryWith(ctx, firstRetryWait, b, func(actx context.Context) error {
		_, err := sp.ReadWriteTransaction(actx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			current := make(map[refKey]refState, len(batch))
			err := txn.Read(ctx, refsTable, spanner.KeySets(keys...), []string{"ProjectId", "Ref", "UpdateTime", "Target"}).Do(func(row *spanner.Row) error {
				var k refKey
				var s refState
				if err := row.Columns(&k.projectID, &k.ref, &s.updated, &s.target); err != nil {
					return err
				}
				current[k] = s
				return nil
			})
			if err != nil {
				return err
			}
			var muts []*spanner.Mutation
			for i, e := range batch {
				s, ok := current[refKey{e.projectID, e.ref}]
				switch {
				case !ok:
					outcomes[i] = refGone
				case !s.updated.Equal(e.updated) || !s.target.Valid || s.target.StringVal != e.target:
					outcomes[i] = refChanged
				default:
					outcomes[i] = refRemoved
					muts = append(muts, spanner.Delete(refsTable, spanner.Key{e.projectID, e.ref}))
				}
			}
			return txn.BufferWrite(muts)
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return outcomes, nil
}

// pruneRefRecords keeps the newest keep records and those within the retention.
func pruneRefRecords(ctx context.Context, b writeBudget, bucket *storage.BucketHandle, state string, keep int) (int64, error) {
	prefix := refRecordPrefixOf(state)
	byCycle := make(map[int64]*storage.ObjectAttrs)
	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to list the ref records under %s: %w", prefix, err)
		}
		cycle, err := strconv.ParseInt(strings.TrimPrefix(attrs.Name, prefix), 10, 64)
		if err != nil {
			continue
		}
		byCycle[cycle] = attrs
	}
	cycles := slices.Sorted(maps.Keys(byCycle))
	oldest := wallNow().Add(-recordRetention)
	var n int64
	for _, c := range cycles[:max(0, len(cycles)-keep)] {
		attrs := byCycle[c]
		if !attrs.Created.Before(oldest) {
			continue
		}
		if err := deleteObject(ctx, b, bucket.Object(attrs.Name)); err != nil {
			return n, fmt.Errorf("failed to delete the old ref record %s: %w", attrs.Name, err)
		}
		n++
	}
	return n, nil
}

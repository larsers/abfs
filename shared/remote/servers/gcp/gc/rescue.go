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
	"time"

	"cloud.google.com/go/bigtable"
)

const (
	objectsTableName = "objects"
	// A marker that changes this often while being cleared means a collector
	// that condemns faster than once per read; give up rather than loop.
	maxRescueAttempts = 5
)

// Rescuer clears the condemn marker of objects that are used again, so that
// the collector does not delete them.
type Rescuer struct {
	objects   *bigtable.Table
	gcTable   *bigtable.Table
	retryWait time.Duration
}

// NewRescuer returns a Rescuer for the tables of client's instance.
func NewRescuer(client *bigtable.Client) *Rescuer {
	return &Rescuer{
		objects:   client.Open(objectsTableName),
		gcTable:   client.Open(TableName),
		retryWait: firstRetryWait,
	}
}

// Rescue touches the object key, which a read found set to condemned, and
// then clears its condemn marker. It returns false when the object row is
// gone, because the collector deleted it first. The marker is cleared only
// while it still holds the value read, so the result tells which side won.
//
// The touch comes first and a failed touch fails the rescue, with the marker
// left in place: a condemn write that is retried or lands late can set the
// marker again after it was cleared, and then only the touch keeps the next
// mark from finding the object unreachable. A touch of a row that turns out
// to be gone leaves a gc row with only the touch.
func (r *Rescuer) Rescue(ctx context.Context, key Hash, condemned []byte) (bool, error) {
	row := key.String()
	mut := bigtable.NewMutation()
	mut.Set(TouchFamily, TouchColumn, bigtable.ServerTime, nil)
	if err := r.retry(ctx, func() error { return r.gcTable.Apply(ctx, row, mut) }); err != nil {
		return false, fmt.Errorf("failed to rescue %s: failed to touch it: %v", key, err) //nolint:errorlint // a missing table must not reach the client as a missing object
	}
	for attempt := 1; ; attempt++ {
		matched, err := r.clear(ctx, row, condemned)
		if err != nil {
			return false, fmt.Errorf("failed to rescue %s: %v", key, err) //nolint:errorlint // a missing table must not reach the client as a missing object
		}
		if matched {
			break
		}
		exists, current, err := r.marker(ctx, row)
		if err != nil {
			return false, fmt.Errorf("failed to rescue %s: %v", key, err) //nolint:errorlint // a missing table must not reach the client as a missing object
		}
		if !exists {
			return false, nil
		}
		if current == nil {
			// Another replica cleared it.
			break
		}
		if attempt == maxRescueAttempts {
			return false, fmt.Errorf("failed to rescue %s: its condemn marker keeps changing", key)
		}
		condemned = current
	}
	return true, nil
}

// clear deletes the marker if it still holds condemned. Retrying it is safe:
// after a lost response that deleted the marker, the retry does not match and
// the marker read finds it cleared.
func (r *Rescuer) clear(ctx context.Context, row string, condemned []byte) (bool, error) {
	end := append(append([]byte{}, condemned...), 0)
	predicate := bigtable.ChainFilters(
		bigtable.FamilyFilter(ObjectsFamily),
		bigtable.ColumnFilter(CondemnedColumn),
		bigtable.LatestNFilter(1),
		bigtable.ValueRangeFilter(append([]byte{}, condemned...), end),
	)
	del := bigtable.NewMutation()
	del.DeleteCellsInColumn(ObjectsFamily, CondemnedColumn)
	mut := bigtable.NewCondMutation(predicate, del, nil)
	var matched bool
	err := r.retry(ctx, func() error {
		return r.objects.Apply(ctx, row, mut, bigtable.GetCondMutationResult(&matched))
	})
	return matched, err
}

// marker reads whether the object row exists and its condemn marker, nil when
// it has none.
func (r *Rescuer) marker(ctx context.Context, row string) (bool, []byte, error) {
	var res bigtable.Row
	err := r.retry(ctx, func() error {
		var err error
		res, err = r.objects.ReadRow(ctx, row, bigtable.RowFilter(bigtable.ChainFilters(
			bigtable.FamilyFilter("meta|"+ObjectsFamily), bigtable.LatestNFilter(1))))
		return err
	})
	if err != nil || len(res["meta"]) == 0 {
		return false, nil, err
	}
	for _, item := range res[ObjectsFamily] {
		if item.Column == ObjectsFamily+":"+CondemnedColumn {
			return true, append([]byte{}, item.Value...), nil
		}
	}
	return true, nil, nil
}

// retry calls f until it succeeds, fails with an error that is not transient,
// or has run writeAttempts times, as EdgeWriter.applyWithRetry does.
func (r *Rescuer) retry(ctx context.Context, f func() error) error {
	wait := r.retryWait
	for attempt := 1; ; attempt++ {
		err := f()
		if err == nil || attempt == writeAttempts || !retryable(ctx, err) {
			return err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
		wait = min(2*wait, maxRetryWait)
	}
}

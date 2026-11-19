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

const maxRescueAttempts = 5

// Rescuer clears the condemn marker of objects that are used again.
type Rescuer struct {
	objects        *bigtable.Table
	gcTable        *bigtable.Table
	retryWait      time.Duration
	attemptTimeout time.Duration
}

func NewRescuer(client *bigtable.Client) *Rescuer {
	return &Rescuer{
		objects:        client.Open(objectsTable),
		gcTable:        client.Open(TableName),
		retryWait:      firstRetryWait,
		attemptTimeout: touchWriteTimeout,
	}
}

// Rescue touches key, which a read found condemned, then clears its marker if it
// still holds the value read. It returns false when the collector deleted the
// row first. The touch must come first: a late condemn write can set the marker
// again, and then only the touch keeps the next mark from finding it unreachable.
func (r *Rescuer) Rescue(ctx context.Context, key Hash, condemned []byte) (bool, error) {
	row := key.String()
	mut := bigtable.NewMutation()
	mut.Set(TouchFamily, TouchColumn, bigtable.ServerTime, nil)
	if err := r.retry(ctx, func(ctx context.Context) error { return r.gcTable.Apply(ctx, row, mut) }); err != nil {
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

// clear deletes the marker if it still holds condemned. A retry after a lost
// answer does not match, and the marker read then finds it cleared.
func (r *Rescuer) clear(ctx context.Context, row string, condemned []byte) (bool, error) {
	del := bigtable.NewMutation()
	del.DeleteCellsInColumn(ObjectsFamily, CondemnedColumn)
	mut := bigtable.NewCondMutation(markerPredicate(condemned), del, nil)
	var matched bool
	err := r.retry(ctx, func(ctx context.Context) error {
		return r.objects.Apply(ctx, row, mut, bigtable.GetCondMutationResult(&matched))
	})
	return matched, err
}

func (r *Rescuer) marker(ctx context.Context, row string) (bool, []byte, error) {
	var res bigtable.Row
	err := r.retry(ctx, func(ctx context.Context) error {
		var err error
		res, err = r.objects.ReadRow(ctx, row, bigtable.RowFilter(bigtable.ChainFilters(
			bigtable.FamilyFilter(objectsMetaFamily+"|"+ObjectsFamily), bigtable.LatestNFilter(1))))
		return err
	})
	if err != nil || len(res[objectsMetaFamily]) == 0 {
		return false, nil, err
	}
	for _, item := range res[ObjectsFamily] {
		if item.Column == ObjectsFamily+":"+CondemnedColumn {
			return true, append([]byte{}, item.Value...), nil
		}
	}
	return true, nil, nil
}

// retry bounds each attempt so that a stalled write cannot land after the rescue gave up.
func (r *Rescuer) retry(ctx context.Context, f func(ctx context.Context) error) error {
	return retryWith(ctx, r.retryWait, writeBudget{deadline: r.attemptTimeout}, f)
}

// Retry calls f with the retries of the collector writes.
func Retry(ctx context.Context, f func() error) error {
	return retryWith(ctx, firstRetryWait, writeBudget{}, func(context.Context) error { return f() })
}

func retryWith(ctx context.Context, wait time.Duration, b writeBudget, f func(ctx context.Context) error) error {
	for attempt := 1; ; attempt++ {
		actx, cancel, err := b.attempt(ctx)
		if err != nil {
			return err
		}
		err = f(actx)
		cancel()
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

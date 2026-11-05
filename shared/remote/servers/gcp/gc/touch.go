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
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"
)

const (
	// TouchColumn holds the last touch of an object in its gc row: the cell
	// timestamp, in server time. The value is empty.
	TouchColumn = "touch"
	// CondemnedColumn in the family g of an objects row marks the object as
	// condemned; the value is the number of the cycle that condemned it.
	CondemnedColumn    = "condemned"
	touchBatchSize     = 1000
	touchFlushInterval = time.Second
	touchWriteTimeout  = 10 * time.Second
	// touchWorkers write batches in parallel, so that a burst of hits, such as
	// the keep-alive of a large mount, drains at the rate of several writers.
	touchWorkers = 4
	// touchEnqueueWait is how long a touch waits for room in a full buffer
	// before it is dropped. The wait slows the call that found the objects to
	// the rate the workers write.
	touchEnqueueWait = 500 * time.Millisecond
	// The rows of the table maps, as casdb.AddMapResult writes them.
	mapsTableName = "maps"
	mapsFamily    = "cf"
)

// TouchStats counts the touches of a Toucher.
type TouchStats struct {
	// Written counts the touches written.
	Written int64
	// Dropped counts the touches dropped because the buffer stayed full.
	Dropped int64
	// Failed counts the touches that could not be written.
	Failed int64
}

type touch struct {
	mapRow bool
	row    string
}

// Toucher writes touches in the background: the touch cell of an object's gc
// row, and the cell of a map row, whose timestamp then records the last use.
// Touches wait in a bounded buffer and are written in batches by several
// workers. When the buffer is full, TouchObject and TouchMapRow wait up to
// touchEnqueueWait for room and then drop the touch; after such a timeout,
// touches are dropped without waiting until the buffer has room again, so that
// writes that stall delay the calls only once. TryTouchObject never waits. The
// cells get server timestamps, so all replicas write on one clock.
type Toucher struct {
	gcTable     *bigtable.Table
	mapsTable   *bigtable.Table
	queue       chan touch
	retryWait   time.Duration
	enqueueWait time.Duration
	stalled     atomic.Bool
	stop        chan struct{}
	stopOnce    sync.Once
	done        chan struct{}
	written     atomic.Int64
	dropped     atomic.Int64
	failed      atomic.Int64
}

// NewToucher returns a running Toucher for the tables of client's instance that
// buffers up to size touches. Close stops it.
func NewToucher(client *bigtable.Client, size int) *Toucher {
	t := newToucher(client, size)
	go t.run()
	return t
}

func newToucher(client *bigtable.Client, size int) *Toucher {
	return &Toucher{
		gcTable:     client.Open(TableName),
		mapsTable:   client.Open(mapsTableName),
		queue:       make(chan touch, size),
		retryWait:   firstRetryWait,
		enqueueWait: touchEnqueueWait,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// TouchObject queues a touch of the object key. It returns false when the
// buffer stayed full and the touch was dropped.
func (t *Toucher) TouchObject(key Hash) bool {
	return t.add(touch{row: key.String()}, true)
}

// TryTouchObject queues a touch of the object key without waiting. It returns
// false when the buffer was full and the touch was dropped.
func (t *Toucher) TryTouchObject(key Hash) bool {
	return t.add(touch{row: key.String()}, false)
}

// TouchMapRow queues a rewrite of the cell of the map row with the given key. It
// returns false when the buffer stayed full and the touch was dropped.
func (t *Toucher) TouchMapRow(row string) bool {
	return t.add(touch{mapRow: true, row: row}, true)
}

func (t *Toucher) add(tc touch, wait bool) bool {
	select {
	case t.queue <- tc:
		if t.stalled.Load() {
			t.stalled.Store(false)
		}
		return true
	default:
	}
	if wait && !t.stalled.Load() {
		timer := time.NewTimer(t.enqueueWait)
		defer timer.Stop()
		select {
		case t.queue <- tc:
			return true
		case <-timer.C:
			t.stalled.Store(true)
		case <-t.stop:
		}
	}
	t.dropped.Add(1)
	return false
}

// Stats returns the counts of the touches so far.
func (t *Toucher) Stats() TouchStats {
	return TouchStats{
		Written: t.written.Load(),
		Dropped: t.dropped.Load(),
		Failed:  t.failed.Load(),
	}
}

// Close writes the buffered touches and stops the Toucher. Touches queued
// after Close are not written.
func (t *Toucher) Close() {
	t.stopOnce.Do(func() { close(t.stop) })
	<-t.done
}

func (t *Toucher) run() {
	defer close(t.done)
	var wg sync.WaitGroup
	for range touchWorkers {
		wg.Go(t.work)
	}
	wg.Wait()
}

func (t *Toucher) work() {
	ticker := time.NewTicker(touchFlushInterval)
	defer ticker.Stop()
	batch := make([]touch, 0, touchBatchSize)
	for {
		select {
		case tc := <-t.queue:
			batch = append(batch, tc)
			if len(batch) < touchBatchSize {
				continue
			}
		case <-ticker.C:
		case <-t.stop:
			t.drain(batch)
			return
		}
		if len(batch) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), touchWriteTimeout)
			t.flush(ctx, batch)
			cancel()
			batch = batch[:0]
		}
	}
}

// drain writes batch and the touches left in the buffer.
func (t *Toucher) drain(batch []touch) {
	ctx, cancel := context.WithTimeout(context.Background(), touchWriteTimeout)
	defer cancel()
	for {
		select {
		case tc := <-t.queue:
			batch = append(batch, tc)
			if len(batch) < touchBatchSize {
				continue
			}
		default:
			t.flush(ctx, batch)
			return
		}
		t.flush(ctx, batch)
		batch = batch[:0]
	}
}

func (t *Toucher) flush(ctx context.Context, batch []touch) {
	var objectRows, mapRows []string
	for _, tc := range batch {
		if tc.mapRow {
			mapRows = append(mapRows, tc.row)
		} else {
			objectRows = append(objectRows, tc.row)
		}
	}
	objectTouch := bigtable.NewMutation()
	objectTouch.Set(TouchFamily, TouchColumn, bigtable.ServerTime, nil)
	mapTouch := bigtable.NewMutation()
	mapTouch.Set(mapsFamily, "", bigtable.ServerTime, []byte("1"))
	t.write(ctx, t.gcTable, TableName, objectRows, objectTouch)
	t.write(ctx, t.mapsTable, mapsTableName, mapRows, mapTouch)
}

// write applies mut to rows and retries the rows that fail with a transient
// error, which the client library does not do for server timestamps. A retry
// after a lost response writes a later timestamp, which only moves the touch
// later.
func (t *Toucher) write(ctx context.Context, table *bigtable.Table, name string, rows []string, mut *bigtable.Mutation) {
	wait := t.retryWait
	failedBefore := t.failed.Load()
	var lastErr error
	for attempt := 1; len(rows) > 0; attempt++ {
		muts := make([]*bigtable.Mutation, len(rows))
		for i := range muts {
			muts[i] = mut
		}
		errs, err := table.ApplyBulk(ctx, rows, muts)
		var retry []string
		switch {
		case err != nil:
			lastErr = err
			if retryable(ctx, err) {
				retry = rows
			} else {
				t.failed.Add(int64(len(rows)))
			}
		case errs == nil:
			t.written.Add(int64(len(rows)))
		default:
			for i, e := range errs {
				switch {
				case e == nil:
					t.written.Add(1)
				case retryable(ctx, e):
					retry = append(retry, rows[i])
					lastErr = e
				default:
					t.failed.Add(1)
					lastErr = e
				}
			}
		}
		if len(retry) > 0 && attempt == writeAttempts {
			t.failed.Add(int64(len(retry)))
			retry = nil
		}
		if len(retry) > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				t.failed.Add(int64(len(retry)))
				retry = nil
			}
			wait = min(2*wait, maxRetryWait)
		}
		rows = retry
	}
	if failed := t.failed.Load() - failedBefore; failed > 0 {
		log.Printf("Warning: failed to write %d touches to Bigtable table %s: %v", failed, name, lastErr)
	}
}

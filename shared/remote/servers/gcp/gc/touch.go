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
	// TouchColumn holds the last touch of an object in its gc row as the cell
	// timestamp, in server time.
	TouchColumn = "touch"
	// CondemnedColumn holds the cycle that condemned an object.
	CondemnedColumn    = "condemned"
	touchBatchSize     = 1000
	touchFlushInterval = time.Second
	touchWriteTimeout  = 10 * time.Second
	touchWorkers       = 4
	// touchEnqueueWait slows the caller to the rate the workers write.
	touchEnqueueWait = 500 * time.Millisecond
	// The table maps, as casdb.AddMapResult writes it.
	mapsTableName   = "maps"
	mapsFamily      = "cf"
	TouchBufferSize = 100 * 1000
	// TouchDrainBound bounds the time from a use to the write of its touch,
	// or to its count as failed.
	TouchDrainBound = TouchBufferSize/touchBatchSize/touchWorkers*touchWriteTimeout + touchFlushInterval
	touchAgeSlot    = time.Second
	touchAgeSlots   = 4096
)

// TouchStats counts the touches of a Toucher.
type TouchStats struct {
	Written int64
	Dropped int64
	// Failed also counts touches whose write could end past TouchDrainBound.
	Failed         int64
	MapRowsWritten int64
	MapRowsDropped int64
	MapRowsFailed  int64
}

type touchCounts struct {
	written, dropped, failed atomic.Int64
}

type touch struct {
	mapRow bool
	row    string
	queued time.Time
}

// Toucher writes the touches of objects and map rows in the background, with
// server timestamps so all replicas write on one clock.
type Toucher struct {
	gcTable     *bigtable.Table
	mapsTable   *bigtable.Table
	queue       chan touch
	retryWait   time.Duration
	enqueueWait time.Duration
	lateAfter   time.Duration
	epoch       time.Time
	now         func() time.Time
	pending     [touchAgeSlots]atomic.Int64
	writing     [touchAgeSlots]atomic.Int64
	stalled     atomic.Bool
	stop        chan struct{}
	stopOnce    sync.Once
	// enqueues is read-locked while a touch is queued, so the workers drain
	// only once no touch can still land in the buffer.
	enqueues sync.RWMutex
	drained  chan struct{}
	done     chan struct{}
	objects  touchCounts
	mapRows  touchCounts
	// TouchObject waits on room, not on the channel: senders parked on a full
	// channel would keep it full for the touches that do not wait.
	waitLimit int
	room      chan struct{}
}

// NewToucher returns a running Toucher. Close stops it.
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
		waitLimit:   size - size/5,
		room:        make(chan struct{}, 1),
		lateAfter:   TouchDrainBound - touchWriteTimeout,
		epoch:       time.Now(),
		now:         time.Now,
		stop:        make(chan struct{}),
		drained:     make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// TouchObject queues a touch of the object key, waiting while the buffer is
// about 80% full. It returns false when the touch was dropped.
func (t *Toucher) TouchObject(key Hash) bool {
	return t.add(touch{row: key.String()}, true)
}

// TryTouchObject queues a touch of the object key without waiting.
func (t *Toucher) TryTouchObject(key Hash) bool {
	return t.add(touch{row: key.String()}, false)
}

// TouchMapRow queues a touch of a map row while the buffer is less than half
// full.
func (t *Toucher) TouchMapRow(row string) bool {
	return t.add(touch{mapRow: true, row: row}, false)
}

func (t *Toucher) add(tc touch, wait bool) bool {
	tc.queued = t.now()
	if !tc.mapRow {
		t.slot(&t.pending, tc).Add(1)
	}
	t.enqueues.RLock()
	queued := t.enqueue(tc, wait)
	t.enqueues.RUnlock()
	if !queued {
		if !tc.mapRow {
			t.slot(&t.pending, tc).Add(-1)
		}
		t.counts(tc).dropped.Add(1)
		return false
	}
	return true
}

func (t *Toucher) counts(tc touch) *touchCounts {
	if tc.mapRow {
		return &t.mapRows
	}
	return &t.objects
}

func (t *Toucher) enqueue(tc touch, wait bool) bool {
	select {
	case <-t.stop:
		return false
	default:
	}
	if tc.mapRow {
		if len(t.queue) >= cap(t.queue)/2 {
			return false
		}
		select {
		case t.queue <- tc:
			return true
		default:
			return false
		}
	}
	below := len(t.queue) < t.waitLimit
	if below || !wait {
		select {
		case t.queue <- tc:
			if below && t.stalled.Load() {
				t.stalled.Store(false)
			}
			return true
		default:
		}
	}
	if !wait || t.stalled.Load() {
		return false
	}
	timer := time.NewTimer(t.enqueueWait)
	defer timer.Stop()
	for {
		select {
		case <-t.room:
		case <-timer.C:
			t.stalled.Store(true)
			return false
		case <-t.stop:
			return false
		}
		if len(t.queue) < t.waitLimit {
			select {
			case t.queue <- tc:
				// Pass the single wake on to the next waiting touch.
				t.freed()
				return true
			default:
			}
		}
	}
}

func (t *Toucher) freed() {
	if len(t.queue) < t.waitLimit {
		select {
		case t.room <- struct{}{}:
		default:
		}
	}
}

// Stats returns the counts of the touches so far.
func (t *Toucher) Stats() TouchStats {
	return TouchStats{
		Written:        t.objects.written.Load(),
		Dropped:        t.objects.dropped.Load(),
		Failed:         t.objects.failed.Load(),
		MapRowsWritten: t.mapRows.written.Load(),
		MapRowsDropped: t.mapRows.dropped.Load(),
		MapRowsFailed:  t.mapRows.failed.Load(),
	}
}

// Late reports whether an object touch is overdue. A frozen process counts
// its late touches only once its workers run again; Late lets a heartbeat
// report them before that.
func (t *Toucher) Late() bool {
	return t.overdue(&t.pending, t.lateAfter) || t.overdue(&t.writing, TouchDrainBound)
}

func (t *Toucher) overdue(slots *[touchAgeSlots]atomic.Int64, bound time.Duration) bool {
	age := t.now().Sub(t.epoch)
	now := int64(age / touchAgeSlot)
	newest := int64((age-bound)/touchAgeSlot) - 1
	for slot := max(0, now-touchAgeSlots+1); slot <= newest; slot++ {
		if slots[slot%touchAgeSlots].Load() > 0 {
			return true
		}
	}
	return false
}

func (t *Toucher) slot(slots *[touchAgeSlots]atomic.Int64, tc touch) *atomic.Int64 {
	return &slots[int64(tc.queued.Sub(t.epoch)/touchAgeSlot)%touchAgeSlots]
}

// Close writes the buffered touches and stops the Toucher. Touches queued
// after Close are dropped.
func (t *Toucher) Close() {
	t.stopOnce.Do(func() {
		close(t.stop)
		t.enqueues.Lock()
		close(t.drained)
		t.enqueues.Unlock()
	})
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
			t.freed()
			batch = append(batch, tc)
			if len(batch) < touchBatchSize {
				continue
			}
		case <-ticker.C:
		case <-t.drained:
			ctx, cancel := context.WithTimeout(context.Background(), touchWriteTimeout)
			t.drain(ctx, batch)
			cancel()
			return
		}
		if len(batch) > 0 {
			t.flushBatch(batch)
			batch = batch[:0]
		}
	}
}

// drain writes all batches within the one deadline of ctx, so that Close
// ends within the stop grace; the touches left past it count as failed.
func (t *Toucher) drain(ctx context.Context, batch []touch) {
	for {
		select {
		case tc := <-t.queue:
			batch = append(batch, tc)
			if len(batch) < touchBatchSize {
				continue
			}
			t.flush(ctx, batch)
			batch = batch[:0]
		default:
			if len(batch) > 0 {
				t.flush(ctx, batch)
			}
			return
		}
	}
}

func (t *Toucher) flushBatch(batch []touch) {
	ctx, cancel := context.WithTimeout(context.Background(), touchWriteTimeout)
	defer cancel()
	t.flush(ctx, batch)
}

func (t *Toucher) flush(ctx context.Context, batch []touch) {
	var objectRows, mapRows []string
	var objectLate, mapLate []bool
	writing := make([]touch, 0, len(batch))
	for _, tc := range batch {
		late := t.now().Sub(tc.queued) > t.lateAfter
		if tc.mapRow {
			mapRows = append(mapRows, tc.row)
			mapLate = append(mapLate, late)
			if late {
				t.mapRows.failed.Add(1)
			}
			continue
		}
		objectRows = append(objectRows, tc.row)
		objectLate = append(objectLate, late)
		// The collector counts on a touch landing within TouchDrainBound.
		if late {
			t.objects.failed.Add(1)
		} else {
			writing = append(writing, tc)
			t.slot(&t.writing, tc).Add(1)
		}
		t.slot(&t.pending, tc).Add(-1)
	}
	objectTouch := bigtable.NewMutation()
	objectTouch.Set(TouchFamily, TouchColumn, bigtable.ServerTime, nil)
	mapTouch := bigtable.NewMutation()
	mapTouch.Set(mapsFamily, "", bigtable.ServerTime, []byte("1"))
	t.write(ctx, t.gcTable, TableName, objectRows, objectLate, objectTouch, &t.objects)
	t.write(ctx, t.mapsTable, mapsTableName, mapRows, mapLate, mapTouch, &t.mapRows)
	for _, tc := range writing {
		t.slot(&t.writing, tc).Add(-1)
	}
}

// write retries the rows that fail with a transient error, which the client
// library does not do for server timestamps. A retry only moves a touch later.
func (t *Toucher) write(ctx context.Context, table *bigtable.Table, name string, rows []string, late []bool, mut *bigtable.Mutation, c *touchCounts) {
	wait := t.retryWait
	var failures int
	fail := func(i int) {
		failures++
		if !late[i] {
			c.failed.Add(1)
		}
	}
	var lastErr error
	for attempt := 1; len(rows) > 0; attempt++ {
		muts := make([]*bigtable.Mutation, len(rows))
		for i := range muts {
			muts[i] = mut
		}
		errs, err := table.ApplyBulk(ctx, rows, muts)
		var retry []string
		var retryLate []bool
		switch {
		case err != nil:
			lastErr = err
			if retryable(ctx, err) {
				retry, retryLate = rows, late
			} else {
				for i := range rows {
					fail(i)
				}
			}
		case errs == nil:
			c.written.Add(int64(len(rows)))
		default:
			for i, e := range errs {
				switch {
				case e == nil:
					c.written.Add(1)
				case retryable(ctx, e):
					retry, retryLate = append(retry, rows[i]), append(retryLate, late[i])
					lastErr = e
				default:
					fail(i)
					lastErr = e
				}
			}
		}
		rows, late = retry, retryLate
		if len(rows) > 0 && attempt == writeAttempts {
			for i := range rows {
				fail(i)
			}
			rows = nil
		}
		if len(rows) > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				for i := range rows {
					fail(i)
				}
				rows = nil
			}
			wait = min(2*wait, maxRetryWait)
		}
	}
	if failures > 0 {
		log.Printf("Warning: failed to write %d touches to Bigtable table %s: %v", failures, name, lastErr)
	}
}

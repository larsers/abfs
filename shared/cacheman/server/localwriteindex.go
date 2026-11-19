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

package server

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/google/abfs/shared/entities"
)

const (
	clearRetryWait    = 100 * time.Millisecond
	maxClearRetryWait = 5 * time.Second
)

var errEvictionHeld = errors.New("eviction is held until the server confirms the cache")

// localWriteIndex is implemented by indexes that keep local writes out of eviction until the
// server confirms them.
type localWriteIndex interface {
	enableLocalWriteCheck()
	clearRemote(hash entities.Hash, entry *IndexEntry, stop <-chan struct{}) (prevEnt *IndexEntry, cleared bool, retries int, err error)
	confirmRemote(hash entities.Hash) (bool, error)
	hasCheckMarker(hash entities.Hash) bool
	checkMarkers() ([]entities.Hash, error)
	evictionHeldSince() int64
	setEvictionWatermark(timestamp int64)
	releaseEviction()
	evictSkippedUnconfirmed() int64
	evictedUnchecked() int64
	tracksUse() bool
	park(hash entities.Hash, stop <-chan struct{}) (parked bool, retries int, err error)
	parkUnconfirmed(hash entities.Hash, stop <-chan struct{}) (parked bool, retries int, err error)
	unpark(hash entities.Hash, timestamp int64) (bool, error)
	isParked(hash entities.Hash) bool
	parkedSize() int64
	releaseParked(before int64) (int, error)
	evictedParked() int64
}

var _ localWriteIndex = (*badgerIndex)(nil)

// evictGate holds eviction to the entries the start pass has checked.
type evictGate struct {
	held atomic.Bool
	// Entries last used at or before watermark may be evicted while held.
	watermark atomic.Int64
	since     atomic.Int64
	unchecked atomic.Int64
	// parked counts the parked entries evicted.
	parked atomic.Int64
}

// enableLocalWriteCheck turns the check on and holds eviction until releaseEviction.
func (i *badgerIndex) enableLocalWriteCheck() {
	if i.localWriteCheck.Swap(true) {
		return
	}
	i.indexStore.SetLocalWriteCheck()
	i.gate.watermark.Store(0)
	i.gate.since.Store(time.Now().Unix())
	i.gate.held.Store(true)
}

// clearRemote retries ClearRemote until it commits, the index closes or stop is closed: a
// clear that gave up would leave the entry Remote and evictable.
func (i *badgerIndex) clearRemote(hash entities.Hash, entry *IndexEntry, stop <-chan struct{}) (prevEnt *IndexEntry, cleared bool, retries int, err error) {
	retries, err = i.retryConflicts(stop, func() error {
		var err error
		prevEnt, cleared, err = i.indexStore.ClearRemote(hash, entry)
		return err
	})
	return prevEnt, cleared, retries, err
}

// park retries Park until it commits, the index closes or stop is closed: a park that gave up
// would leave the entry in the LRU, which the start pass then lets eviction take.
func (i *badgerIndex) park(hash entities.Hash, stop <-chan struct{}) (parked bool, retries int, err error) {
	retries, err = i.retryConflicts(stop, func() error {
		var err error
		parked, err = i.indexStore.Park(hash)
		return err
	})
	return parked, retries, err
}

// parkUnconfirmed retries ParkUnconfirmed as park does.
func (i *badgerIndex) parkUnconfirmed(hash entities.Hash, stop <-chan struct{}) (parked bool, retries int, err error) {
	retries, err = i.retryConflicts(stop, func() error {
		var err error
		parked, err = i.indexStore.ParkUnconfirmed(hash)
		return err
	})
	return parked, retries, err
}

// retryConflicts runs fn until it returns an error other than ErrTxnConflict, the index closes
// or stop is closed, waiting longer after each conflict.
func (i *badgerIndex) retryConflicts(stop <-chan struct{}, fn func() error) (retries int, err error) {
	wait := clearRetryWait
	for {
		i.evictRWLock.RLock()
		err = fn()
		i.evictRWLock.RUnlock()
		if !errors.Is(err, ErrTxnConflict) || i.closed.Load() {
			return retries, err
		}
		retries++
		select {
		case <-stop:
			return retries, err
		case <-time.After(wait):
		}
		wait = min(2*wait, maxClearRetryWait)
	}
}

func (i *badgerIndex) unpark(hash entities.Hash, timestamp int64) (bool, error) {
	i.evictRWLock.RLock()
	defer i.evictRWLock.RUnlock()

	return i.indexStore.Unpark(hash, timestamp)
}

func (i *badgerIndex) isParked(hash entities.Hash) bool {
	return i.indexStore.IsParked(hash)
}

func (i *badgerIndex) parkedSize() int64 {
	return i.indexStore.ParkedSize()
}

// releaseParked returns the entries parked at or before the time before to the LRU, in batches.
func (i *badgerIndex) releaseParked(before int64) (int, error) {
	var released int
	for {
		i.evictRWLock.RLock()
		n, done, err := i.indexStore.ReleaseParked(before, MaxEvictBatchSize)
		i.evictRWLock.RUnlock()
		released += n
		if err != nil || done {
			return released, err
		}
	}
}

func (i *badgerIndex) evictedParked() int64 {
	return i.gate.parked.Load()
}

// evictableSize is the size the eviction threshold applies to: the LRU and the parked entries.
func (i *badgerIndex) evictableSize() int64 {
	return i.indexStore.LRUSize() + i.indexStore.ParkedSize()
}

func (i *badgerIndex) confirmRemote(hash entities.Hash) (bool, error) {
	i.evictRWLock.RLock()
	defer i.evictRWLock.RUnlock()

	return i.indexStore.ConfirmRemote(hash)
}

func (i *badgerIndex) hasCheckMarker(hash entities.Hash) bool {
	return i.indexStore.HasCheckMarker(hash)
}

func (i *badgerIndex) checkMarkers() ([]entities.Hash, error) {
	return i.indexStore.CheckMarkers()
}

// evictionHeldSince returns the time eviction was held, in Unix seconds.
func (i *badgerIndex) evictionHeldSince() int64 {
	return i.gate.since.Load()
}

// setEvictionWatermark lets eviction take entries last used at or before timestamp. Entries
// used since eviction was held stay until releaseEviction, since the start pass checked only
// the uses before.
func (i *badgerIndex) setEvictionWatermark(timestamp int64) {
	i.gate.watermark.Store(min(timestamp, i.gate.since.Load()-1))
}

func (i *badgerIndex) releaseEviction() {
	if !i.gate.held.Swap(false) {
		return
	}
	i.maybeTriggerEviction(i.indexStore.LRUSize())
}

func (i *badgerIndex) evictSkippedUnconfirmed() int64 {
	return i.indexStore.EvictSkippedUnconfirmed()
}

func (i *badgerIndex) evictedUnchecked() int64 {
	return i.gate.unchecked.Load()
}

// tracksUse reports whether the index keeps the LRU, without which nothing is evicted.
func (i *badgerIndex) tracksUse() bool {
	store, ok := i.indexStore.(*BadgerIndexStore)
	return !ok || store.trackUse
}

// evictionHeld reports whether eviction may take nothing now, so that a run started by a put
// ends before it logs: the cache is below its threshold, and either eviction is held or the
// LRU is empty and the parked entries fit in the eviction target.
func (i *badgerIndex) evictionHeld() bool {
	if i.evictThreshold > 0 && i.evictableSize() >= int64(i.evictThreshold) {
		return false
	}
	oldest, err := i.indexStore.PeekOldest()
	if err != nil {
		return i.indexStore.ParkedSize() == 0 || i.evictableSize() <= evictTarget(int64(i.evictThreshold))
	}
	return i.gate.held.Load() && oldest.Timestamp > i.gate.watermark.Load()
}

// evictBatch evicts one batch from the index store. While eviction is held it takes only
// entries at or below the watermark, unless the cache has reached threshold, in which case it
// evicts as usual and counts the victims as evicted unchecked. Once the LRU is empty, parked
// entries go, oldest park first, while the cache is above the eviction target of threshold.
// currSz and the size it returns are those of evictableSize.
func (i *badgerIndex) evictBatch(currSz, threshold int64) ([]*IndexKeyEntry, int64, error) {
	i.evictRWLock.Lock()
	defer i.evictRWLock.Unlock()

	victims, err := i.evictLRU(currSz, threshold)
	if (errors.Is(err, errNothingToEvict) || errors.Is(err, errEvictionHeld)) &&
		threshold > 0 && currSz > evictTarget(threshold) && i.lruEmpty() {
		victims, _, _, err = i.indexStore.EvictParked()
		if err == nil {
			i.gate.parked.Add(int64(len(victims)))
		}
	}
	return victims, i.evictableSize(), err
}

// lruEmpty reports whether the LRU holds no entry.
func (i *badgerIndex) lruEmpty() bool {
	_, err := i.indexStore.PeekOldest()
	return err != nil
}

func (i *badgerIndex) evictLRU(currSz, threshold int64) ([]*IndexKeyEntry, error) {
	if !i.gate.held.Load() {
		victims, _, _, err := i.indexStore.Evict()
		return victims, err
	}
	victims, _, _, err := i.indexStore.EvictUpTo(i.gate.watermark.Load())
	if !errors.Is(err, errNothingToEvict) {
		return victims, err
	}
	if threshold <= 0 || currSz < threshold {
		return nil, errEvictionHeld
	}
	victims, _, _, err = i.indexStore.Evict()
	if err == nil {
		i.gate.unchecked.Add(int64(len(victims)))
	}
	return victims, err
}

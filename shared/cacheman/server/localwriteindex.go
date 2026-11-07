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
}

var _ localWriteIndex = (*badgerIndex)(nil)

// evictGate holds eviction to the entries the start pass has checked.
type evictGate struct {
	held atomic.Bool
	// Entries last used at or before watermark may be evicted while held.
	watermark atomic.Int64
	since     atomic.Int64
	unchecked atomic.Int64
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
	wait := clearRetryWait
	for {
		i.evictRWLock.RLock()
		prevEnt, cleared, err = i.indexStore.ClearRemote(hash, entry)
		i.evictRWLock.RUnlock()
		if !errors.Is(err, ErrTxnConflict) || i.closed.Load() {
			return prevEnt, cleared, retries, err
		}
		retries++
		select {
		case <-stop:
			return nil, false, retries, err
		case <-time.After(wait):
		}
		wait = min(2*wait, maxClearRetryWait)
	}
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

// evictionHeld reports whether eviction is held and may take nothing now, so that a run
// started by a put ends before it logs.
func (i *badgerIndex) evictionHeld() bool {
	if !i.gate.held.Load() || (i.evictThreshold > 0 && i.indexStore.LRUSize() >= int64(i.evictThreshold)) {
		return false
	}
	oldest, err := i.indexStore.PeekOldest()
	return err != nil || oldest.Timestamp > i.gate.watermark.Load()
}

// evictBatch evicts one batch from the index store. While eviction is held it takes only
// entries at or below the watermark, unless the LRU has reached threshold, in which case it
// evicts as usual and counts the victims as evicted unchecked.
func (i *badgerIndex) evictBatch(currLRUSz, threshold int64) ([]*IndexKeyEntry, int64, error) {
	i.evictRWLock.Lock()
	defer i.evictRWLock.Unlock()

	if !i.gate.held.Load() {
		victims, _, lruSz, err := i.indexStore.Evict()
		return victims, lruSz, err
	}
	victims, _, lruSz, err := i.indexStore.EvictUpTo(i.gate.watermark.Load())
	if !errors.Is(err, errNothingToEvict) {
		return victims, lruSz, err
	}
	if threshold <= 0 || currLRUSz < threshold {
		return nil, currLRUSz, errEvictionHeld
	}
	victims, _, lruSz, err = i.indexStore.Evict()
	if err == nil {
		i.gate.unchecked.Add(int64(len(victims)))
	}
	return victims, lruSz, err
}

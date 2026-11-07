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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/google/abfs/shared/entities"
)

// check markers are stored with key = check::<hash> value = nil
// A marker means a local write of the hash waits for the server to confirm that it holds
// the object, and keeps the entry out of eviction until then.
const checkPrefix = "check::"

var errNothingToEvict = errors.New("there is nothing to evict")

func checkKey(hash entities.Hash) []byte {
	return addPrefix(checkPrefix, hash)
}

// hasCheckMarker should only be called from the context of an open transaction.
func hasCheckMarker(txn *badger.Txn, hash entities.Hash) (bool, error) {
	_, err := txn.Get(checkKey(hash))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// localWriteChecked reports whether local writes of entries of type t are checked with the
// server. ARIs come from an ARI server, not from the ABFS server.
func localWriteChecked(t EntryType) bool {
	return t == EntryTypeBlob || t == EntryTypeTree || t == EntryTypeObject
}

// SetLocalWriteCheck implements IndexStore.
func (b *BadgerIndexStore) SetLocalWriteCheck() {
	b.localWriteCheck.Store(true)
}

// EvictSkippedUnconfirmed implements IndexStore.
func (b *BadgerIndexStore) EvictSkippedUnconfirmed() int64 {
	return b.evictSkippedUnconfirmed.Load()
}

// ClearRemote implements IndexStore.
// It holds the timestamp lock of hash from before it reads the LRU timestamp until the
// transaction commits, so that no Use or Put moves the LRU key in between.
func (b *BadgerIndexStore) ClearRemote(hash entities.Hash, entry *IndexEntry) (prevEnt *IndexEntry, cleared bool, err error) {
	var sizeDelta, localCountDelta, indexCountDelta, lruSizeDelta, prevTimestamp int64

	b.lockTimestamps(hash)
	defer b.unlockTimestamps(hash)

	err = b.db.Update(func(txn *badger.Txn) error {
		var hasActiveTTL bool
		prevEnt, hasActiveTTL = b.get(txn, hash)
		if b.clearRemoteHook != nil {
			b.clearRemoteHook(hash)
		}

		var newEnt *IndexEntry
		if entry == nil {
			if prevEnt == nil || !prevEnt.Local || !prevEnt.Remote {
				return nil
			}
			e := prevEnt.ShallowCopy()
			newEnt = &e
		} else {
			newEnt = maybeMergeMetadata(prevEnt, entry)
		}
		cleared = prevEnt != nil && prevEnt.Remote
		if cleared {
			newEnt.Remote = false
		} else if prevEnt != nil && prevEnt.Equal(newEnt) {
			return nil
		}

		data, err := json.Marshal(newEnt)
		if err != nil {
			return err
		}
		if err := txn.Set(entryKey(hash), data); err != nil {
			return err
		}
		if cleared {
			if err := txn.Set(checkKey(hash), nil); err != nil {
				return err
			}
			if prevTimestamp = b.readTimestampLocked(hash); prevTimestamp != 0 {
				if err := txn.Delete(lruKey(prevTimestamp, hash)); err != nil {
					return err
				}
				lruSizeDelta = -int64(prevEnt.LocalSize())
			}
		}

		sizeDelta = int64(newEnt.LocalSize() - prevEnt.LocalSize())
		localCountDelta = newEnt.LocalCount() - prevEnt.LocalCount()
		if prevEnt == nil || hasActiveTTL {
			indexCountDelta = 1
		}
		return nil
	})
	if errors.Is(err, badger.ErrConflict) {
		return nil, false, ErrTxnConflict
	}
	if err != nil {
		return nil, false, fmt.Errorf("ClearRemote(%v) failed: %w", hash.String(), err)
	}

	b.totalSize.Add(sizeDelta)
	b.localCount.Add(localCountDelta)
	b.indexCount.Add(indexCountDelta)
	if prevTimestamp != 0 {
		b.deleteTimestampLocked(hash)
		b.lruSize.Add(lruSizeDelta)
	}
	return prevEnt, cleared, nil
}

// ConfirmRemote implements IndexStore.
func (b *BadgerIndexStore) ConfirmRemote(hash entities.Hash) (bool, error) {
	var (
		marked    bool
		lruTime   int64
		localSize uint64
	)

	b.lockTimestamps(hash)
	defer b.unlockTimestamps(hash)

	err := b.retryUpdateOnConflict(func(txn *badger.Txn) error {
		lruTime = 0
		localSize = 0

		var err error
		marked, err = hasCheckMarker(txn, hash)
		if err != nil || !marked {
			return err
		}
		if err := txn.Delete(checkKey(hash)); err != nil {
			return err
		}

		curr, _ := b.get(txn, hash)
		// An entry that is not local has an eviction TTL, which a write would drop.
		if curr == nil || !curr.Local {
			return nil
		}
		if !curr.Remote {
			curr.Remote = true
			data, err := json.Marshal(curr)
			if err != nil {
				return err
			}
			if err := txn.Set(entryKey(hash), data); err != nil {
				return err
			}
		}
		if b.isLRUCandidate(hash) && b.readTimestampLocked(hash) == 0 {
			now := time.Now().Unix()
			if err := txn.Set(lruKey(now, hash), nil); err != nil {
				return err
			}
			lruTime = now
			localSize = curr.LocalSize()
		}
		return nil
	}, defaultNumTries)
	if err != nil {
		return false, fmt.Errorf("ConfirmRemote(%v) failed: %w", hash.String(), err)
	}

	if lruTime != 0 {
		b.writeTimestampLocked(hash, lruTime)
		b.lruSize.Add(int64(localSize))
	}
	return marked, nil
}

// HasCheckMarker implements IndexStore.
func (b *BadgerIndexStore) HasCheckMarker(hash entities.Hash) bool {
	var marked bool
	err := b.db.View(func(txn *badger.Txn) error {
		var err error
		marked, err = hasCheckMarker(txn, hash)
		return err
	})
	return err == nil && marked
}

// CheckMarkers implements IndexStore.
func (b *BadgerIndexStore) CheckMarkers() ([]entities.Hash, error) {
	var hashes []entities.Hash
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(keyOnlyIterationOpts())
		defer it.Close()
		prefix := []byte(checkPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			hashes = append(hashes, stripPrefix(checkPrefix, it.Item().Key()))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("CheckMarkers() failed: %w", err)
	}
	return hashes, nil
}

// keepUnconfirmedLocked keeps an eviction candidate whose entry is not Remote or holds a check
// marker: it deletes only the LRU key, so the entry stays local, and returns the size the LRU
// no longer tracks.
// Caller should be holding the timestamps lock for the hash.
// Should only be called from the context of an open transaction.
func (b *BadgerIndexStore) keepUnconfirmedLocked(txn *badger.Txn, hash entities.Hash, timestamp int64) (bool, int64, error) {
	if !b.localWriteCheck.Load() {
		return false, 0, nil
	}
	entry, _ := b.get(txn, hash)
	if entry == nil {
		return false, 0, nil
	}
	if entry.Remote {
		marked, err := hasCheckMarker(txn, hash)
		if err != nil || !marked {
			return false, 0, err
		}
	}
	if err := txn.Delete(lruKey(timestamp, hash)); err != nil {
		return false, 0, err
	}
	return true, int64(entry.LocalSize()), nil
}

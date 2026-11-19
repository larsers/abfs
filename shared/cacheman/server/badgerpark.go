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
	"math"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/google/abfs/shared/entities"
)

const (
	// parked entries are stored with key = park::<padded park time>::<hash> value = the LRU
	// timestamp of the entry before the park, 0 if it had none.
	// A parked entry is Local and Remote, but out of the LRU: the server lacked it, and it
	// waits for a mount to name it or for its release.
	parkPrefix = "park::"

	// release times are stored with key = released::<hash> value = the time a parked entry
	// went back to the LRU. Until the entry is used again, Park does not park it but moves it
	// to the young end of the LRU and records the move here. A use deletes the key.
	releasedPrefix = "released::"

	// restoreBatchSize is the number of keys restoreParked deletes per transaction.
	restoreBatchSize = 10000
)

// parkedEntry is the park state of an entry in memory.
type parkedEntry struct {
	// time is the park time and lruTime the LRU timestamp before the park, 0 if none.
	time    int64
	lruTime int64
	// size is the local size the entry adds to parkedSize.
	size int64
}

func parkKey(timestamp int64, hash entities.Hash) []byte {
	return []byte(fmt.Sprintf("%s%0*d::%s", parkPrefix, lruKeyPadding, timestamp, hash.String()))
}

func releasedKey(hash entities.Hash) []byte {
	return addPrefix(releasedPrefix, hash)
}

// isParkedLocked reports whether hash is parked.
// Caller should be holding the timestamps lock for the hash.
func (b *BadgerIndexStore) isParkedLocked(hash entities.Hash) bool {
	_, ok := b.parked[shardForHash(hash)][hash]
	return ok
}

// deleteParkLocked deletes the park key and the release time of hash. After the transaction
// commits, the caller calls forgetParkLocked.
// Caller should be holding the timestamps lock for the hash.
// Should only be called from the context of an open transaction.
func (b *BadgerIndexStore) deleteParkLocked(txn *badger.Txn, hash entities.Hash) error {
	shard := shardForHash(hash)
	var err error
	if p, ok := b.parked[shard][hash]; ok {
		err = txn.Delete(parkKey(p.time, hash))
	}
	if _, ok := b.released[shard][hash]; ok {
		err = errors.Join(err, txn.Delete(releasedKey(hash)))
	}
	return err
}

// forgetParkLocked drops the park state of hash from memory.
// Caller should be holding the timestamps lock for the hash.
func (b *BadgerIndexStore) forgetParkLocked(hash entities.Hash) {
	shard := shardForHash(hash)
	if p, ok := b.parked[shard][hash]; ok {
		b.parkedSize.Add(-p.size)
		delete(b.parked[shard], hash)
	}
	delete(b.released[shard], hash)
}

// writeParkLocked takes hash out of the LRU and writes its park key.
// Caller should be holding the timestamps lock for the hash.
// Should only be called from the context of an open transaction.
func (b *BadgerIndexStore) writeParkLocked(txn *badger.Txn, hash entities.Hash, p parkedEntry) error {
	if p.lruTime != 0 {
		if err := txn.Delete(lruKey(p.lruTime, hash)); err != nil {
			return err
		}
	}
	if _, ok := b.released[shardForHash(hash)][hash]; ok {
		if err := txn.Delete(releasedKey(hash)); err != nil {
			return err
		}
	}
	return txn.Set(parkKey(p.time, hash), []byte(strconv.FormatInt(p.lruTime, 10)))
}

// parkedLocked records in memory a park that writeParkLocked wrote.
// Caller should be holding the timestamps lock for the hash.
func (b *BadgerIndexStore) parkedLocked(hash entities.Hash, p parkedEntry) {
	shard := shardForHash(hash)
	if p.lruTime != 0 {
		b.deleteTimestampLocked(hash)
		b.lruSize.Add(-p.size)
	}
	delete(b.released[shard], hash)
	b.parked[shard][hash] = p
	b.parkedSize.Add(p.size)
}

// Park implements IndexStore.
func (b *BadgerIndexStore) Park(hash entities.Hash) (bool, error) {
	b.lockTimestamps(hash)
	defer b.unlockTimestamps(hash)

	if b.isParkedLocked(hash) {
		return true, nil
	}
	lruTime := b.readTimestampLocked(hash)
	if released, ok := b.released[shardForHash(hash)][hash]; ok && lruTime != 0 && lruTime <= released {
		return false, b.renewReleasedLocked(hash, lruTime)
	}
	var p *parkedEntry
	err := b.db.Update(func(txn *badger.Txn) error {
		p = nil
		if b.parkHook != nil {
			if err := b.parkHook(hash); err != nil {
				return err
			}
		}
		entry, _ := b.get(txn, hash)
		if entry == nil || !entry.Local || !entry.Remote {
			return nil
		}
		if marked, err := hasCheckMarker(txn, hash); err != nil || marked {
			return err
		}
		next := parkedEntry{time: time.Now().Unix(), lruTime: lruTime, size: int64(entry.LocalSize())}
		if err := b.writeParkLocked(txn, hash, next); err != nil {
			return err
		}
		p = &next
		return nil
	})
	if errors.Is(err, badger.ErrConflict) {
		return false, ErrTxnConflict
	}
	if err != nil {
		return false, fmt.Errorf("Park(%v) failed: %w", hash.String(), err)
	}
	if p == nil {
		return false, nil
	}
	b.parkedLocked(hash, *p)
	return true, nil
}

// renewReleasedLocked moves a released entry that was not used since its release to the young
// end of the LRU, as a push does, so that a mount can name it before eviction takes it. The
// move counts as its release time, so the entry stays unparked until it is used.
// Caller should be holding the timestamps lock for the hash.
func (b *BadgerIndexStore) renewReleasedLocked(hash entities.Hash, lruTime int64) error {
	now := time.Now().Unix()
	if now <= lruTime {
		return nil
	}
	var moved bool
	err := b.db.Update(func(txn *badger.Txn) error {
		moved = false
		entry, _ := b.get(txn, hash)
		if entry == nil || !entry.Local || !entry.Remote {
			return nil
		}
		if err := txn.Delete(lruKey(lruTime, hash)); err != nil {
			return err
		}
		if err := txn.Set(lruKey(now, hash), nil); err != nil {
			return err
		}
		if err := txn.Set(releasedKey(hash), []byte(strconv.FormatInt(now, 10))); err != nil {
			return err
		}
		moved = true
		return nil
	})
	if errors.Is(err, badger.ErrConflict) {
		return ErrTxnConflict
	}
	if err != nil {
		return fmt.Errorf("Park(%v) failed: %w", hash.String(), err)
	}
	if moved {
		b.writeTimestampLocked(hash, now)
		b.released[shardForHash(hash)][hash] = now
	}
	return nil
}

// ParkUnconfirmed implements IndexStore.
func (b *BadgerIndexStore) ParkUnconfirmed(hash entities.Hash) (bool, error) {
	b.lockTimestamps(hash)
	defer b.unlockTimestamps(hash)

	if b.isParkedLocked(hash) {
		return true, nil
	}
	// A fetch after the local write can have put the entry back in the LRU.
	lruTime := b.readTimestampLocked(hash)
	var p *parkedEntry
	err := b.db.Update(func(txn *badger.Txn) error {
		p = nil
		entry, _ := b.get(txn, hash)
		if entry == nil || !entry.Local {
			return nil
		}
		if marked, err := hasCheckMarker(txn, hash); err != nil || !marked {
			return err
		}
		if err := txn.Delete(checkKey(hash)); err != nil {
			return err
		}
		if !entry.Remote {
			entry.Remote = true
			data, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if err := txn.Set(entryKey(hash), data); err != nil {
				return err
			}
		}
		next := parkedEntry{time: time.Now().Unix(), lruTime: lruTime, size: int64(entry.LocalSize())}
		if err := b.writeParkLocked(txn, hash, next); err != nil {
			return err
		}
		p = &next
		return nil
	})
	if errors.Is(err, badger.ErrConflict) {
		return false, ErrTxnConflict
	}
	if err != nil {
		return false, fmt.Errorf("ParkUnconfirmed(%v) failed: %w", hash.String(), err)
	}
	if p == nil {
		return false, nil
	}
	b.parkedLocked(hash, *p)
	return true, nil
}

// Unpark implements IndexStore.
func (b *BadgerIndexStore) Unpark(hash entities.Hash, timestamp int64) (bool, error) {
	b.lockTimestamps(hash)
	defer b.unlockTimestamps(hash)

	p, ok := b.parked[shardForHash(hash)][hash]
	if !ok {
		return false, nil
	}
	return b.unparkLocked(hash, p, timestamp, 0)
}

// unparkLocked deletes the park key of hash and puts the entry in the LRU at timestamp. A
// release time other than 0 is recorded for Park.
// Caller should be holding the timestamps lock for the hash.
func (b *BadgerIndexStore) unparkLocked(hash entities.Hash, p parkedEntry, timestamp, released int64) (bool, error) {
	var u unparkWrite
	err := b.retryUpdateOnConflict(func(txn *badger.Txn) error {
		var err error
		u, err = b.writeUnparkLocked(txn, hash, p, timestamp, released)
		return err
	}, defaultNumTries)
	if err != nil {
		return false, fmt.Errorf("Unpark(%v) failed: %w", hash.String(), err)
	}
	return b.unparkedLocked(hash, p, timestamp, released, u), nil
}

// unparkWrite is what writeUnparkLocked wrote, for unparkedLocked.
type unparkWrite struct {
	stale   bool
	inLRU   bool
	lruSize int64
}

// writeUnparkLocked writes the unpark of hash for unparkLocked.
// Caller should be holding the timestamps lock for the hash.
// Should only be called from the context of an open transaction.
func (b *BadgerIndexStore) writeUnparkLocked(txn *badger.Txn, hash entities.Hash, p parkedEntry, timestamp, released int64) (unparkWrite, error) {
	key := parkKey(p.time, hash)
	if _, err := txn.Get(key); errors.Is(err, badger.ErrKeyNotFound) {
		return unparkWrite{stale: true}, nil
	} else if err != nil {
		return unparkWrite{}, err
	}
	if err := txn.Delete(key); err != nil {
		return unparkWrite{}, err
	}
	if released != 0 {
		if err := txn.Set(releasedKey(hash), []byte(strconv.FormatInt(released, 10))); err != nil {
			return unparkWrite{}, err
		}
	}
	entry, _ := b.get(txn, hash)
	if entry == nil || !entry.Local || !entry.Remote || !b.isLRUCandidate(hash) || b.readTimestampLocked(hash) != 0 {
		return unparkWrite{}, nil
	}
	if err := txn.Set(lruKey(timestamp, hash), nil); err != nil {
		return unparkWrite{}, err
	}
	return unparkWrite{inLRU: true, lruSize: int64(entry.LocalSize())}, nil
}

// unparkedLocked records in memory an unpark that writeUnparkLocked wrote and reports whether
// the park key was there.
// Caller should be holding the timestamps lock for the hash.
func (b *BadgerIndexStore) unparkedLocked(hash entities.Hash, p parkedEntry, timestamp, released int64, u unparkWrite) bool {
	shard := shardForHash(hash)
	b.parkedSize.Add(-p.size)
	delete(b.parked[shard], hash)
	if u.stale {
		return false
	}
	if released != 0 {
		b.released[shard][hash] = released
	}
	if u.inLRU {
		b.writeTimestampLocked(hash, timestamp)
		b.lruSize.Add(u.lruSize)
	}
	return true
}

// IsParked implements IndexStore.
func (b *BadgerIndexStore) IsParked(hash entities.Hash) bool {
	b.lockTimestamps(hash)
	defer b.unlockTimestamps(hash)
	return b.isParkedLocked(hash)
}

// ParkedSize implements IndexStore.
func (b *BadgerIndexStore) ParkedSize() int64 {
	return b.parkedSize.Load()
}

// ReleaseParked implements IndexStore.
func (b *BadgerIndexStore) ReleaseParked(before int64, limit int) (int, bool, error) {
	var candidates []KeyTimestamp
	err := b.db.View(func(txn *badger.Txn) error {
		var err error
		candidates, err = oldestParked(txn, before, limit)
		return err
	})
	if err != nil {
		return 0, false, fmt.Errorf("ReleaseParked() failed: %w", err)
	}
	if len(candidates) == 0 {
		return 0, true, nil
	}
	hashes := make([]entities.Hash, len(candidates))
	for i, kt := range candidates {
		hashes[i] = kt.Key
	}
	shards := shardsForHashes(hashes)
	b.lockTimestampShards(shards)
	defer b.unlockTimestampShards(shards)

	type release struct {
		hash      entities.Hash
		p         parkedEntry
		timestamp int64
		u         unparkWrite
	}
	now := time.Now().Unix()
	var releases []release
	err = b.retryUpdateOnConflict(func(txn *badger.Txn) error {
		releases = releases[:0]
		for _, kt := range candidates {
			p, ok := b.parked[shardForHash(kt.Key)][kt.Key]
			if !ok || p.time != kt.Timestamp {
				if err := txn.Delete(parkKey(kt.Timestamp, kt.Key)); err != nil {
					return err
				}
				continue
			}
			// An entry without an LRU time before the park goes back at its park time.
			timestamp := p.lruTime
			if timestamp == 0 {
				timestamp = p.time
			}
			u, err := b.writeUnparkLocked(txn, kt.Key, p, timestamp, now)
			if err != nil {
				return err
			}
			releases = append(releases, release{hash: kt.Key, p: p, timestamp: timestamp, u: u})
		}
		return nil
	}, defaultNumTries)
	if err != nil {
		return 0, false, fmt.Errorf("ReleaseParked() failed: %w", err)
	}
	var released int
	for _, r := range releases {
		if b.unparkedLocked(r.hash, r.p, r.timestamp, now, r.u) {
			released++
		}
	}
	return released, len(candidates) < limit, nil
}

// oldestParked returns up to limit park keys, oldest first, parked at or before maxTimestamp.
// Should only be called from the context of an open transaction.
func oldestParked(txn *badger.Txn, maxTimestamp int64, limit int) ([]KeyTimestamp, error) {
	var keys []KeyTimestamp
	it := txn.NewIterator(keyOnlyIterationOpts())
	defer it.Close()
	prefix := []byte(parkPrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keys) < limit; it.Next() {
		kt, err := keyTimestampFromKey(it.Item().Key())
		if err != nil {
			return nil, err
		}
		if kt.Timestamp > maxTimestamp {
			break
		}
		keys = append(keys, kt)
	}
	return keys, nil
}

// EvictParked implements IndexStore.
func (b *BadgerIndexStore) EvictParked() ([]*IndexKeyEntry, int64, int64, error) {
	var locked bool
	var lockedShards []int
	var keyEntries []*IndexKeyEntry
	var evicted []entities.Hash
	var sizeDelta, localCountDelta, indexCountDelta int64

	defer func() {
		if locked {
			b.unlockTimestampShards(lockedShards)
		}
	}()

	err := b.db.Update(func(txn *badger.Txn) error {
		candidates, err := oldestParked(txn, math.MaxInt64, int(b.evictBatchSize))
		if err != nil {
			return fmt.Errorf("failed to find parked entries to evict: %w", err)
		}
		if len(candidates) == 0 {
			return errNothingToEvict
		}
		var hashes []entities.Hash
		for _, c := range candidates {
			hashes = append(hashes, c.Key)
		}
		lockedShards = shardsForHashes(hashes)
		b.lockTimestampShards(lockedShards)
		locked = true

		for _, c := range candidates {
			if p, ok := b.parked[shardForHash(c.Key)][c.Key]; !ok || p.time != c.Timestamp {
				if err := txn.Delete(parkKey(c.Timestamp, c.Key)); err != nil {
					return err
				}
				continue
			}
			ke, err := b.deleteWithTimestampLocked(txn, c.Key, 0, holdFlag)
			if err != nil {
				return err
			}
			evicted = append(evicted, c.Key)
			if ke == nil {
				continue
			}
			keyEntries = append(keyEntries, ke)
			sizeDelta -= int64(ke.Size)
			localCountDelta--
			indexCountDelta--
		}
		return nil
	})
	if err != nil {
		return nil, 0, 0, fmt.Errorf("EvictParked() failed: %w", err)
	}
	for _, h := range evicted {
		b.forgetParkLocked(h)
	}
	newSz := b.totalSize.Add(sizeDelta)
	b.localCount.Add(localCountDelta)
	b.indexCount.Add(indexCountDelta)
	return keyEntries, newSz, b.lruSize.Load(), nil
}

// restoreParked re-populates the park state in memory and parkedSize from the store. It deletes
// park keys whose entry is no longer Local and Remote, and release times of deleted entries.
func (b *BadgerIndexStore) restoreParked() error {
	parked := make(map[entities.Hash]parkedEntry)
	released := make(map[entities.Hash]int64)
	var size int64
	w := batchDeleter{db: b.db}
	defer w.discard()
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(parkPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			kt, err := keyTimestampFromKey(key)
			var p parkedEntry
			var entry *IndexEntry
			if err == nil {
				err = item.Value(func(val []byte) error {
					p.lruTime, err = strconv.ParseInt(string(val), 10, 64)
					return err
				})
				entry, _ = b.get(txn, kt.Key)
			}
			_, dup := parked[kt.Key]
			if err != nil || entry == nil || !entry.Local || !entry.Remote || dup {
				if err := w.delete(key); err != nil {
					return err
				}
				continue
			}
			p.time = kt.Timestamp
			p.size = int64(entry.LocalSize())
			parked[kt.Key] = p
			size += p.size
		}

		prefix = []byte(releasedPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			hash := stripPrefix(releasedPrefix, key)
			var t int64
			err := item.Value(func(val []byte) error {
				var err error
				t, err = strconv.ParseInt(string(val), 10, 64)
				return err
			})
			if entry, _ := b.get(txn, hash); err != nil || entry == nil {
				if err := w.delete(key); err != nil {
					return err
				}
				continue
			}
			released[hash] = t
		}
		return w.commit()
	})
	if err != nil {
		return fmt.Errorf("restoreParked() failed: %w", err)
	}
	for h, p := range parked {
		b.parked[shardForHash(h)][h] = p
	}
	for h, t := range released {
		b.released[shardForHash(h)][h] = t
	}
	b.parkedSize.Store(size)
	return nil
}

// batchDeleter deletes keys in write transactions of at most restoreBatchSize keys, since one
// transaction cannot hold the deletes of a large index.
type batchDeleter struct {
	db    *badger.DB
	txn   *badger.Txn
	count int
}

func (w *batchDeleter) delete(key []byte) error {
	if w.txn == nil {
		w.txn = w.db.NewTransaction(true)
	}
	if err := w.txn.Delete(key); err != nil {
		return err
	}
	if w.count++; w.count >= restoreBatchSize {
		return w.commit()
	}
	return nil
}

func (w *batchDeleter) commit() error {
	if w.txn == nil {
		return nil
	}
	err := w.txn.Commit()
	w.txn, w.count = nil, 0
	return err
}

func (w *batchDeleter) discard() {
	if w.txn != nil {
		w.txn.Discard()
		w.txn = nil
	}
}

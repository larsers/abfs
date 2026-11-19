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
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
)

func openParkStore(t *testing.T, dir string) *BadgerIndexStore {
	t.Helper()
	store := NewBadgerIndexStore(dir, nil, MaxEvictBatchSize, true)
	require.NoError(t, store.Open())
	return store
}

func putParked(t *testing.T, store *BadgerIndexStore, content string) entities.Hash {
	t.Helper()
	h := objects.HashBlobBytes([]byte(content))
	_, _, _, _, err := store.Put(h, &IndexEntry{EntryType: EntryTypeBlob, Local: true, Remote: true, Size: 10})
	require.NoError(t, err)
	parked, err := store.Park(h)
	require.NoError(t, err)
	require.True(t, parked)
	return h
}

func countKeys(t *testing.T, store *BadgerIndexStore, prefix string) int {
	t.Helper()
	n := 0
	require.NoError(t, store.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(keyOnlyIterationOpts())
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			n++
		}
		return nil
	}))
	return n
}

// More stale park keys and release times than one transaction can delete do not fail the open.
func TestBadgerIndexStore_RestoreParkedDeletesManyStaleKeys(t *testing.T) {
	dir := t.TempDir()
	store := openParkStore(t, dir)
	kept := putParked(t, store, "kept")
	const stale = 12 * restoreBatchSize
	for start := 0; start < stale; start += restoreBatchSize {
		require.NoError(t, store.db.Update(func(txn *badger.Txn) error {
			for i := start; i < start+restoreBatchSize; i++ {
				h := objects.HashBlobBytes(fmt.Appendf(nil, "deleted %d", i))
				if err := txn.Set(parkKey(int64(i+1), h), []byte("0")); err != nil {
					return err
				}
				if i%10 == 0 {
					if err := txn.Set(releasedKey(h), []byte("1")); err != nil {
						return err
					}
				}
			}
			return nil
		}))
	}
	require.NoError(t, store.Close())

	store = openParkStore(t, dir)
	defer func() { require.NoError(t, store.Close()) }()
	require.True(t, store.IsParked(kept))
	require.Equal(t, int64(10), store.ParkedSize())
	require.Equal(t, 1, countKeys(t, store, parkPrefix))
	require.Zero(t, countKeys(t, store, releasedPrefix))
}

// ReleaseParked releases a batch per call in one transaction and deletes park keys that name
// no parked entry.
func TestBadgerIndexStore_ReleaseParkedBatches(t *testing.T) {
	store := openParkStore(t, t.TempDir())
	defer func() { require.NoError(t, store.Close()) }()
	var hashes []entities.Hash
	for i := range 3 {
		hashes = append(hashes, putParked(t, store, fmt.Sprintf("parked %d", i)))
	}
	orphan := objects.HashBlobBytes([]byte("orphan"))
	require.NoError(t, store.db.Update(func(txn *badger.Txn) error {
		return txn.Set(parkKey(1, orphan), []byte("0"))
	}))
	before := time.Now().Unix()

	released, done, err := store.ReleaseParked(before, 2)
	require.NoError(t, err)
	require.Equal(t, 1, released)
	require.False(t, done)
	released, done, err = store.ReleaseParked(before, 2)
	require.NoError(t, err)
	require.Equal(t, 2, released)
	require.False(t, done)
	released, done, err = store.ReleaseParked(before, 2)
	require.NoError(t, err)
	require.Zero(t, released)
	require.True(t, done)

	require.Zero(t, countKeys(t, store, parkPrefix))
	require.Equal(t, 3, countKeys(t, store, releasedPrefix))
	require.Zero(t, store.ParkedSize())
	require.Equal(t, int64(30), store.LRUSize())
	for _, h := range hashes {
		require.False(t, store.IsParked(h))
		require.NotZero(t, store.readTimestampLocked(h))
	}
}

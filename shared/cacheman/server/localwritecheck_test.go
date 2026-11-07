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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cacheman/protocol"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/inmemory"
)

// checkRemote is a server whose Headers calls can be held, failed or made to miss keys.
type checkRemote struct {
	ABFSClient

	mu       sync.Mutex
	hold     chan struct{}
	holdKeys map[entities.Hash]chan struct{}
	hidden   map[entities.Hash]bool
	fail     bool
	putHold  chan struct{}

	calls atomic.Int64
	keys  atomic.Int64
}

func newCheckRemote() *checkRemote {
	return &checkRemote{
		ABFSClient: inmemory.New(),
		holdKeys:   make(map[entities.Hash]chan struct{}),
		hidden:     make(map[entities.Hash]bool),
	}
}

// holdHeaders makes Headers calls wait until the returned function is called.
func (r *checkRemote) holdHeaders() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan struct{})
	r.hold = ch
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.hold == ch {
			r.hold = nil
		}
		close(ch)
	}
}

// holdKey makes Headers calls that ask for h wait until the returned function is called.
func (r *checkRemote) holdKey(h entities.Hash) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan struct{})
	r.holdKeys[h] = ch
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.holdKeys, h)
		close(ch)
	}
}

func (r *checkRemote) holdPuts() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan struct{})
	r.putHold = ch
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.putHold = nil
		close(ch)
	}
}

func (r *checkRemote) setFail(fail bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = fail
}

// Headers implements remote.RemoteProvider.
func (r *checkRemote) Headers(ctx context.Context, keys []remote.Hash) ([]entities.ObjectHeader, error) {
	r.calls.Add(1)
	r.keys.Add(int64(len(keys)))
	r.mu.Lock()
	waits := []chan struct{}{r.hold}
	for _, k := range keys {
		waits = append(waits, r.holdKeys[k])
	}
	fail := r.fail
	r.mu.Unlock()
	for _, ch := range waits {
		if ch == nil {
			continue
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if fail {
		return nil, errors.New("server unavailable")
	}
	hdrs, err := r.ABFSClient.Headers(ctx, keys)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, k := range keys {
		if r.hidden[k] {
			hdrs[i] = entities.ObjectHeader{}
		}
	}
	return hdrs, nil
}

// PutObject implements remote.RemoteProvider.
func (r *checkRemote) PutObject(ctx context.Context, key remote.Hash, reader remote.ContentReadCloser) error {
	r.mu.Lock()
	hold := r.putHold
	r.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := r.ABFSClient.PutObject(ctx, key, reader); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.hidden, key)
	return nil
}

func (r *checkRemote) has(t *testing.T, h entities.Hash) bool {
	hdrs, err := r.ABFSClient.Headers(context.Background(), []entities.Hash{h})
	require.NoError(t, err)
	return !hdrs[0].IsEmpty()
}

type checkOpts struct {
	gcKeepAlive      bool
	period           time.Duration
	offlineThreshold uint64
	// dir reopens the cache of an earlier cacheman.
	dir string
}

// newCheckCacheman returns a cacheman on remote with the index on and eviction off, as
// New leaves it after applying the config, so that a test controls each eviction.
func newCheckCacheman(t *testing.T, r *checkRemote, opts checkOpts) *CacheMan {
	t.Helper()
	period := opts.period
	if period == 0 {
		period = 10 * time.Millisecond
	}
	oldPeriod := localWriteCheckPeriod
	localWriteCheckPeriod = period
	t.Cleanup(func() { localWriteCheckPeriod = oldPeriod })

	dir := opts.dir
	if dir == "" {
		dir = t.TempDir()
	}
	cacheDir := filepath.Join(dir, "cache")
	logDir := filepath.Join(dir, "log")
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "tmp"), 0755))
	require.NoError(t, os.MkdirAll(logDir, 0755))
	trueVal := true
	inst := instance.NewInstance("default", dir, dir, "abfs", &instance.ConfigOptional{
		CacheDir:    &cacheDir,
		LogDir:      &logDir,
		LocalOnly:   &trueVal,
		EnableIndex: &trueVal,
	}, instance.DefaultFlavor)
	t.Cleanup(func() { require.NoError(t, inst.Close()) })

	c, err := New(inst, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		select {
		case <-c.closeChan:
		default:
			require.NoError(t, c.Stop())
		}
	})
	c.config.LocalOnly = false
	c.config.GCKeepAlive = opts.gcKeepAlive
	c.config.OfflineThreshold = opts.offlineThreshold
	c.abfsClient = r
	c.setupDAGSync(context.Background(), c.config, true)
	c.applyLocalWriteCheck(c.config)
	return c
}

func writeBlob(t *testing.T, c *CacheMan, content string) entities.Hash {
	t.Helper()
	data := []byte(content)
	h := objects.HashBlobBytes(data)
	require.NoError(t, c.store.WriteBlob(context.Background(), h, bytes.NewReader(data)))
	return h
}

// fetchedBlob puts a blob into the cache as a fetch does.
func fetchedBlob(t *testing.T, c *CacheMan, content string) entities.Hash {
	t.Helper()
	h := writeBlob(t, c, content)
	_, err := c.addFetchedObjectToIndex(h, EntryTypeBlob)
	require.NoError(t, err)
	return h
}

func putOnServer(t *testing.T, r *checkRemote, content string) {
	t.Helper()
	data := []byte(content)
	h := objects.HashBlobBytes(data)
	var enc bytes.Buffer
	require.NoError(t, entities.WriteObject("blob", data, &enc))
	require.NoError(t, r.ABFSClient.PutObject(context.Background(), h, remote.NewReaderFromBytes(enc.Bytes())))
}

// evictAll evicts everything eviction takes now and returns the evicted hashes.
func evictAll(t *testing.T, c *CacheMan) []entities.Hash {
	t.Helper()
	bi := c.index.(*badgerIndex)
	var evicted []entities.Hash
	for {
		victims, _, err := bi.evictBatch(bi.LRUSize(), 0)
		if errors.Is(err, errNothingToEvict) || errors.Is(err, errEvictionHeld) {
			return evicted
		}
		require.NoError(t, err)
		for _, v := range victims {
			require.NoError(t, bi.removeCachedObject(v))
			evicted = append(evicted, v.Key)
		}
	}
}

// setLRUTime moves the LRU key of h to timestamp.
func setLRUTime(t *testing.T, store *BadgerIndexStore, h entities.Hash, timestamp int64) {
	t.Helper()
	store.lockTimestamps(h)
	defer store.unlockTimestamps(h)
	prev := store.readTimestampLocked(h)
	require.NotZero(t, prev)
	require.NoError(t, store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(lruKey(prev, h)); err != nil {
			return err
		}
		return txn.Set(lruKey(timestamp, h), nil)
	}))
	store.writeTimestampLocked(h, timestamp)
}

func inLRU(t *testing.T, c *CacheMan, h entities.Hash) bool {
	t.Helper()
	lru, err := c.index.ReadLRU()
	require.NoError(t, err)
	return slices.ContainsFunc(lru, func(kt KeyTimestamp) bool { return kt.Key == h })
}

func hasMarker(c *CacheMan, h entities.Hash) bool {
	return c.index.(*badgerIndex).hasCheckMarker(h)
}

func hasBlob(t *testing.T, c *CacheMan, h entities.Hash) bool {
	t.Helper()
	has, err := c.store.HasBlob(context.Background(), h)
	require.NoError(t, err)
	return has
}

// requireUnconfirmed checks that h is held locally, out of the LRU, and survives eviction.
func requireUnconfirmed(t *testing.T, c *CacheMan, h entities.Hash) {
	t.Helper()
	entry := c.index.Get(h)
	require.NotNil(t, entry)
	require.True(t, entry.Local)
	require.False(t, entry.Remote)
	require.True(t, hasMarker(c, h))
	require.False(t, inLRU(t, c, h))
	require.NotContains(t, evictAll(t, c), h)
	require.True(t, hasBlob(t, c, h))
}

func confirmed(c *CacheMan, h entities.Hash) bool {
	entry := c.index.Get(h)
	return entry != nil && entry.Remote && !hasMarker(c, h)
}

func TestCacheman_LocalWriteOfRemoteContent(t *testing.T) {
	ctx := context.Background()

	t.Run("present", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "fetched months ago")
		putOnServer(t, r, "fetched months ago")
		require.True(t, inLRU(t, c, h))

		release := r.holdHeaders()
		c.NewObject(h, protocol.TypeBlob)
		requireUnconfirmed(t, c, h)
		require.Zero(t, c.index.LRUSize())

		release()
		require.Eventually(t, func() bool { return confirmed(c, h) && inLRU(t, c, h) }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, []entities.Hash{h}, evictAll(t, c))
		require.Equal(t, int64(1), c.Stats().RemoteChecks.Present)
	})

	// The server deleted the object: the check pushes the local copy, the entry stays until
	// the push is done, and after an eviction the content is read from the pushed copy.
	t.Run("missing", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "deleted on the server")

		release := r.holdPuts()
		c.NewObject(h, protocol.TypeBlob)
		require.Eventually(t, func() bool { return c.Stats().RemoteChecks.Pushed == 1 }, 5*time.Second, 10*time.Millisecond)
		requireUnconfirmed(t, c, h)

		release()
		require.Eventually(t, func() bool { return confirmed(c, h) }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, []entities.Hash{h}, evictAll(t, c))
		require.False(t, hasBlob(t, c, h))
		_, err := c.fetchBlob(ctx, h)
		require.NoError(t, err)
		rc, err := c.store.OpenBlob(ctx, h)
		require.NoError(t, err)
		content, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		require.Equal(t, "deleted on the server", string(content))
	})

	t.Run("gc-keepalive off", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: false})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "fetched months ago")
		c.NewObject(h, protocol.TypeBlob)
		require.True(t, c.index.Get(h).Remote)
		require.False(t, hasMarker(c, h))
		require.Equal(t, []entities.Hash{h}, evictAll(t, c))
		// A NewObject that finds no file sends nothing either.
		c.NewObject(h, protocol.TypeBlob)
		time.Sleep(50 * time.Millisecond)
		require.Zero(t, r.calls.Load())
		require.Nil(t, c.Stats().RemoteChecks)
	})

	// A conflicting transaction on the entry, as an eviction of the same hash writes,
	// must not drop the clear: Put gives up after two conflicts and leaves the entry Remote.
	t.Run("clear conflicts", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "evicted concurrently")
		store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
		var conflicts atomic.Int64
		store.clearRemoteHook = func(hash entities.Hash) {
			if conflicts.Add(1) > 3 {
				return
			}
			require.NoError(t, store.db.Update(func(txn *badger.Txn) error {
				item, err := txn.Get(entryKey(hash))
				if err != nil {
					return err
				}
				val, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				return txn.Set(entryKey(hash), val)
			}))
		}

		release := r.holdHeaders()
		defer release()
		c.NewObject(h, protocol.TypeBlob)
		requireUnconfirmed(t, c, h)
		require.Equal(t, int64(3), c.Stats().RemoteChecks.ClearRetries)
	})

	// A fetch of the same content that completes after the clear puts the entry back in the
	// LRU as Remote; eviction still keeps it while it holds a marker.
	t.Run("fetch after the clear", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "fetched again")
		release := r.holdHeaders()
		defer release()
		c.NewObject(h, protocol.TypeBlob)
		_, err := c.addFetchedObjectToIndex(h, EntryTypeBlob)
		require.NoError(t, err)
		require.True(t, inLRU(t, c, h))
		require.Empty(t, evictAll(t, c))
		require.True(t, hasBlob(t, c, h))
		require.False(t, inLRU(t, c, h))
		require.Equal(t, int64(1), c.Stats().RemoteChecks.EvictSkippedUnconfirmed)
	})

	// A notification lost in a restart, for a blob the store already held: the start pass
	// holds eviction until it has asked the server, and pushes what the server lacks.
	t.Run("restart with a lost notification", func(t *testing.T) {
		dir := t.TempDir()
		r := newCheckRemote()
		c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
		h := fetchedBlob(t, c1, "written while cacheman restarted")
		require.NoError(t, c1.Stop())

		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
		release := r.holdHeaders()
		c.startLocalWriteCheck(ctx)
		require.Empty(t, evictAll(t, c))
		release()
		require.Eventually(t, func() bool { return c.Stats().RemoteChecks.Pushed == 1 }, 5*time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool { return confirmed(c, h) && r.has(t, h) }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, int64(1), c.Stats().RemoteChecks.StartPass)
	})

	// While the start pass runs, an answered entry can be evicted, an entry not answered yet
	// cannot, and neither can one used after the start.
	t.Run("start pass watermark", func(t *testing.T) {
		old := localWriteStartPassBatch
		localWriteStartPassBatch = 1
		defer func() { localWriteStartPassBatch = old }()

		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
		answered := fetchedBlob(t, c, "answered first")
		waiting := fetchedBlob(t, c, "answered later")
		used := fetchedBlob(t, c, "used after the start")
		for _, content := range []string{"answered first", "answered later", "used after the start"} {
			putOnServer(t, r, content)
		}
		setLRUTime(t, store, used, 50)
		setLRUTime(t, store, answered, 100)
		setLRUTime(t, store, waiting, 200)

		releaseWaiting := r.holdKey(waiting)
		c.startLocalWriteCheck(ctx)
		require.NoError(t, store.Use(used))
		require.Eventually(t, func() bool { return c.Stats().RemoteChecks.StartPass == 2 }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, []entities.Hash{answered}, evictAll(t, c))

		releaseWaiting()
		require.Eventually(t, func() bool { return !c.index.(*badgerIndex).gate.held.Load() }, 5*time.Second, 10*time.Millisecond)
		require.ElementsMatch(t, []entities.Hash{waiting, used}, evictAll(t, c))
	})

	// The server cannot be reached and the cache is full: eviction runs without the check
	// and says so.
	t.Run("start pass at the threshold", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: ""})
		h := fetchedBlob(t, c, "never answered")
		r.setFail(true)
		c.startLocalWriteCheck(ctx)
		bi := c.index.(*badgerIndex)
		_, _, err := bi.evictBatch(bi.LRUSize(), bi.LRUSize()+1)
		require.ErrorIs(t, err, errEvictionHeld)
		victims, _, err := bi.evictBatch(bi.LRUSize(), bi.LRUSize())
		require.NoError(t, err)
		require.Len(t, victims, 1)
		require.Equal(t, h, victims[0].Key)
		require.Equal(t, int64(1), c.Stats().RemoteChecks.EvictedUnchecked)
	})

	// An evicted entry whose file is on disk again: maintenance treats it as a local write.
	t.Run("maintenance finds the file again", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "written after its eviction")
		require.Equal(t, []entities.Hash{h}, evictAll(t, c))
		require.False(t, c.index.Get(h).Local)
		writeBlob(t, c, "written after its eviction")

		release := r.holdHeaders()
		defer release()
		c.reconcileDiskObject(h, protocol.TypeBlob, map[entities.Hash]struct{}{})
		requireUnconfirmed(t, c, h)
	})

	// A cleared entry whose check had no answer before a restart keeps its marker, and the
	// restarted cacheman checks it again.
	t.Run("restart with a marker", func(t *testing.T) {
		dir := t.TempDir()
		r := newCheckRemote()
		putOnServer(t, r, "checked after the restart")
		c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
		c1.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c1, "checked after the restart")
		release := r.holdHeaders()
		c1.NewObject(h, protocol.TypeBlob)
		require.NoError(t, c1.Stop())

		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
		c.startLocalWriteCheck(ctx)
		requireUnconfirmed(t, c, h)
		release()
		require.Eventually(t, func() bool { return confirmed(c, h) && inLRU(t, c, h) }, 5*time.Second, 10*time.Millisecond)
	})

	// Eviction removed the store copy before cacheman indexed the write.
	t.Run("evicted before indexing", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "evicted before indexing")
		require.Equal(t, []entities.Hash{h}, evictAll(t, c))
		c.NewObject(h, protocol.TypeBlob)
		require.Eventually(t, func() bool { return c.Stats().RemoteChecks.LostBeforeIndex == 1 }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, int64(1), c.Stats().RemoteChecks.Sent)
	})

	t.Run("over the offline threshold", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, offlineThreshold: 4})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "larger than the offline threshold")
		c.NewObject(h, protocol.TypeBlob)
		require.Eventually(t, func() bool { return c.Stats().RemoteChecks.Unpushable == 1 }, 5*time.Second, 10*time.Millisecond)
		requireUnconfirmed(t, c, h)
	})

	t.Run("phony", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, period: time.Hour})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "verified by a build")
		putOnServer(t, r, "verified by a build")
		c.NewObject(h, protocol.TypeBlob)

		r.setFail(true)
		failed := make(chan error, 1)
		c.Phony(0, []entities.Hash{h}, func(err error) {
			select {
			case failed <- err:
			default:
			}
		})
		require.Error(t, <-failed)
		requireUnconfirmed(t, c, h)
		require.Zero(t, c.Stats().RemoteChecks.PhonyConfirmed)

		r.setFail(false)
		done := make(chan error, 1)
		c.Phony(0, []entities.Hash{h}, func(err error) {
			if err == nil {
				done <- err
			}
		})
		require.NoError(t, <-done)
		require.True(t, confirmed(c, h))
		require.True(t, inLRU(t, c, h))
		require.Equal(t, int64(1), c.Stats().RemoteChecks.PhonyConfirmed)
	})
}

func TestCacheman_RemoteHeadersFixesFlags(t *testing.T) {
	ctx := context.Background()
	post := func(t *testing.T, c *CacheMan, keys []entities.Hash) []bool {
		b, err := json.Marshal(keys)
		require.NoError(t, err)
		req := httptest.NewRequest("POST", "http://localhost/remote-headers", bytes.NewReader(b))
		w := httptest.NewRecorder()
		c.httpHandler().ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		var found []bool
		require.NoError(t, json.NewDecoder(w.Body).Decode(&found))
		return found
	}

	for _, gcKeepAlive := range []bool{true, false} {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: gcKeepAlive, period: time.Hour})
		c.startLocalWriteCheck(ctx)

		staleRemote := fetchedBlob(t, c, "held as remote, deleted on the server")
		localOnServer := writeBlob(t, c, "local only, on the server")
		putOnServer(t, r, "local only, on the server")
		_, err := c.addLocalObjectToIndex(localOnServer, EntryTypeBlob)
		require.NoError(t, err)
		marked := fetchedBlob(t, c, "cleared, deleted on the server")
		c.NewObject(marked, protocol.TypeBlob)
		localOnly := writeBlob(t, c, "local only, not on the server")
		_, err = c.addLocalObjectToIndex(localOnly, EntryTypeBlob)
		require.NoError(t, err)

		found := post(t, c, []entities.Hash{staleRemote, localOnServer, marked, localOnly})
		require.Equal(t, []bool{false, true, false, false}, found)

		if !gcKeepAlive {
			time.Sleep(50 * time.Millisecond)
			require.True(t, c.index.Get(staleRemote).Remote)
			require.False(t, c.index.Get(localOnServer).Remote)
			require.Zero(t, c.pushBlobsStat.Request.Load())
			continue
		}
		require.True(t, c.index.Get(localOnServer).Remote)
		require.True(t, inLRU(t, c, localOnServer))
		require.Eventually(t, func() bool {
			return r.has(t, staleRemote) && r.has(t, marked) && confirmed(c, staleRemote) && confirmed(c, marked)
		}, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, int64(2), c.Stats().RemoteChecks.Pushed)
		require.False(t, r.has(t, localOnly))
		require.False(t, c.index.Get(localOnly).Remote)
	}
}

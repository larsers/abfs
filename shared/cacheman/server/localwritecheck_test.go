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
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cacheman/protocol"
	"github.com/google/abfs/shared/dagsync"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/objects/gitobjects"
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
	// answerHold holds the answers of Headers calls after the server read them.
	answerHold chan struct{}
	// asked counts the Headers calls that asked for each key, and askedNoTouch those with a
	// no-touch context.
	asked        map[entities.Hash]int
	askedNoTouch map[entities.Hash]int

	calls atomic.Int64
	keys  atomic.Int64
	// canceled counts the held Headers calls whose context ended.
	canceled atomic.Int64
}

func newCheckRemote() *checkRemote {
	return &checkRemote{
		ABFSClient:   inmemory.New(),
		holdKeys:     make(map[entities.Hash]chan struct{}),
		hidden:       make(map[entities.Hash]bool),
		asked:        make(map[entities.Hash]int),
		askedNoTouch: make(map[entities.Hash]int),
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

// holdAnswers makes Headers calls read the server and then wait with the answer until the
// returned function is called.
func (r *checkRemote) holdAnswers() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan struct{})
	r.answerHold = ch
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.answerHold = nil
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
		r.asked[k]++
		if remote.NoTouch(ctx) {
			r.askedNoTouch[k]++
		}
	}
	fail := r.fail
	answerHold := r.answerHold
	r.mu.Unlock()
	for _, ch := range waits {
		if ch == nil {
			continue
		}
		select {
		case <-ch:
		case <-ctx.Done():
			r.canceled.Add(1)
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
	if answerHold != nil {
		<-answerHold
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

func (r *checkRemote) askedFor(h entities.Hash) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.asked[h]
}

func (r *checkRemote) askedNoTouchFor(h entities.Hash) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.askedNoTouch[h]
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
	return entry != nil && entry.Remote && !hasMarker(c, h) && !c.isParked(h)
}

// requireParked checks that h is held locally as Remote, out of the LRU without a marker.
func requireParked(t *testing.T, c *CacheMan, h entities.Hash) {
	t.Helper()
	entry := c.index.Get(h)
	require.NotNil(t, entry)
	require.True(t, entry.Local)
	require.True(t, entry.Remote)
	require.True(t, c.isParked(h))
	require.False(t, hasMarker(c, h))
	require.False(t, inLRU(t, c, h))
	require.True(t, hasBlobOrObject(t, c, h))
}

func hasBlobOrObject(t *testing.T, c *CacheMan, h entities.Hash) bool {
	t.Helper()
	if hasBlob(t, c, h) {
		return true
	}
	has, err := c.store.HasObject(context.Background(), h)
	require.NoError(t, err)
	return has
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
		store.clearRemoteHook = func(hash entities.Hash) error {
			if conflicts.Add(1) > 3 {
				return nil
			}
			return store.db.Update(func(txn *badger.Txn) error {
				item, err := txn.Get(entryKey(hash))
				if err != nil {
					return err
				}
				val, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				return txn.Set(entryKey(hash), val)
			})
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
	// holds eviction until it has asked the server, and parks what the server lacks. The
	// mount's keep-alive names it and pushes it.
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
		waitForStartPass(t, c)
		requireParked(t, c, h)
		require.Equal(t, int64(1), c.Stats().RemoteChecks.StartPass)

		c.checkRemoteHeaders(ctx, []entities.Hash{h}, []bool{false})
		require.Eventually(t, func() bool { return confirmed(c, h) && r.has(t, h) && inLRU(t, c, h) }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, int64(1), c.Stats().RemoteChecks.Pushed)
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

	// An entry used after the start waits for the end of the pass even when its new LRU time
	// is below that of an answered batch, as with LRU times ahead of the clock.
	t.Run("start pass watermark below the start", func(t *testing.T) {
		old := localWriteStartPassBatch
		localWriteStartPassBatch = 1
		defer func() { localWriteStartPassBatch = old }()

		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		bi := c.index.(*badgerIndex)
		store := bi.indexStore.(*BadgerIndexStore)
		since := bi.gate.since.Load()
		used := fetchedBlob(t, c, "used after the start")
		answered := fetchedBlob(t, c, "answered")
		waiting := fetchedBlob(t, c, "not answered")
		for _, content := range []string{"used after the start", "answered", "not answered"} {
			putOnServer(t, r, content)
		}
		setLRUTime(t, store, used, since+50)
		setLRUTime(t, store, answered, since+100)
		setLRUTime(t, store, waiting, since+200)

		releaseWaiting := r.holdKey(waiting)
		c.startLocalWriteCheck(ctx)
		require.NoError(t, store.Use(used))
		require.Eventually(t, func() bool { return c.Stats().RemoteChecks.StartPass == 2 }, 5*time.Second, 10*time.Millisecond)
		require.Empty(t, evictAll(t, c))

		releaseWaiting()
		require.Eventually(t, func() bool { return !bi.gate.held.Load() }, 5*time.Second, 10*time.Millisecond)
		require.ElementsMatch(t, []entities.Hash{used, answered, waiting}, evictAll(t, c))
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

	// A Local and Remote entry outside the LRU, which the start pass did not check, can be a
	// local write whose notification was lost: maintenance holds it until the server answers.
	t.Run("maintenance finds an entry outside the LRU", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "not in the LRU")
		putOnServer(t, r, "not in the LRU")

		release := r.holdHeaders()
		c.reconcileDiskObject(h, protocol.TypeBlob, map[entities.Hash]struct{}{})
		requireUnconfirmed(t, c, h)
		release()
		require.Eventually(t, func() bool { return confirmed(c, h) && inLRU(t, c, h) }, 5*time.Second, 10*time.Millisecond)
	})

	// With eviction off the index tracks no use, so every entry is outside the LRU and none
	// can be evicted: maintenance leaves them as they are instead of checking the whole cache
	// on every run.
	t.Run("maintenance without eviction", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.index.(*badgerIndex).indexStore.(*BadgerIndexStore).trackUse = false
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "eviction off")
		putOnServer(t, r, "eviction off")
		c.reconcileDiskObject(h, protocol.TypeBlob, map[entities.Hash]struct{}{})
		require.True(t, confirmed(c, h))
		require.Zero(t, c.Stats().RemoteChecks.Sent)
	})

	// A clear that fails with an error other than a conflict leaves the old entry Remote; the
	// check finds the server lacks it and takes the entry out of the LRU.
	t.Run("clear fails", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "clear failed")
		store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
		var failed atomic.Bool
		store.clearRemoteHook = func(entities.Hash) error {
			if failed.Swap(true) {
				return nil
			}
			return errors.New("injected")
		}

		release := r.holdPuts()
		defer release()
		c.NewObject(h, protocol.TypeBlob)
		require.Equal(t, int64(1), c.Stats().RemoteChecks.ClearFailures)
		require.Eventually(t, func() bool { return c.Stats().RemoteChecks.Pushed == 1 }, 5*time.Second, 10*time.Millisecond)
		requireUnconfirmed(t, c, h)
	})

	// Sweep, the local GC of remote read-only mounts, keeps a local write that waits for the
	// server, since it is the only copy.
	t.Run("sweep", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		marked := fetchedBlob(t, c, "waits for the server")
		localOnly := writeBlob(t, c, "local only")
		_, err := c.addLocalObjectToIndex(localOnly, EntryTypeBlob)
		require.NoError(t, err)
		release := r.holdHeaders()
		defer release()
		c.NewObject(marked, protocol.TypeBlob)

		swept, err := c.index.Sweep(time.Now().Add(time.Hour).Unix())
		require.NoError(t, err)
		require.Equal(t, 1, swept)
		require.False(t, hasBlob(t, c, localOnly))
		requireUnconfirmed(t, c, marked)
	})

	// An entry that is not Remote is kept by eviction even without a marker, as when its LRU
	// key is stale.
	t.Run("not remote with an LRU key", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
		c.startLocalWriteCheck(ctx)
		h := fetchedBlob(t, c, "stale LRU key")
		store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
		require.NoError(t, store.db.Update(func(txn *badger.Txn) error {
			entry, _ := store.get(txn, h)
			entry.Remote = false
			data, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			return txn.Set(entryKey(h), data)
		}))
		require.True(t, inLRU(t, c, h))
		require.False(t, hasMarker(c, h))
		require.Empty(t, evictAll(t, c))
		require.True(t, hasBlob(t, c, h))
		require.Equal(t, int64(1), c.Stats().RemoteChecks.EvictSkippedUnconfirmed)
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

	// A local write while a phony request is in flight can come after the server answered it.
	// Neither that request nor an identical one that dagsync joins to it confirms the write.
	t.Run("phony in flight during a local write", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, period: time.Hour})
		c.startLocalWriteCheck(ctx)
		h := writeBlob(t, c, "written during a phony")
		_, err := c.addLocalObjectToIndex(h, EntryTypeBlob)
		require.NoError(t, err)
		putOnServer(t, r, "written during a phony")

		release := r.holdAnswers()
		calls := r.calls.Load()
		results := make(chan error, 2)
		c.Phony(0, []entities.Hash{h}, func(err error) { results <- err })
		require.Eventually(t, func() bool { return r.calls.Load() > calls }, 5*time.Second, 10*time.Millisecond)

		// The keep-alive found the entry on the server, then the content is written again.
		require.NoError(t, c.index.markObjectRemote(h))
		c.NewObject(h, protocol.TypeBlob)
		require.Eventually(t, func() bool { return hasMarker(c, h) }, 5*time.Second, 10*time.Millisecond)
		calls = r.calls.Load()
		// Phony sends from a goroutine; sending here puts the request into the dagsync
		// mailbox before the first request's answer is released.
		hashes := []entities.Hash{h}
		ds := c.getSync()
		ds.Phony(0, hashes, c.confirmAfterPhony(ds, hashes, func(err error) { results <- err }))

		release()
		require.NoError(t, <-results)
		require.NoError(t, <-results)
		require.Equal(t, calls, r.calls.Load(), "the second request was not joined to the first")
		requireUnconfirmed(t, c, h)
		require.Zero(t, c.Stats().RemoteChecks.PhonyConfirmed)
	})

	// With an offline threshold, dagsync skips a phony request whose link it skipped and calls
	// back without an error and without asking the server, so the phony request confirms
	// nothing. A later phony request for a small object runs and uploads it, and the upload
	// marks it remote.
	t.Run("phony with an offline threshold", func(t *testing.T) {
		r := newCheckRemote()
		c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, period: time.Hour, offlineThreshold: 4})
		c.startLocalWriteCheck(ctx)
		large := fetchedBlob(t, c, "larger than the offline threshold")
		small := fetchedBlob(t, c, "abc")
		c.NewObject(large, protocol.TypeBlob)
		c.NewObject(small, protocol.TypeBlob)

		for _, h := range []entities.Hash{large, small} {
			done := make(chan struct{})
			var once sync.Once
			c.Phony(0, []entities.Hash{h}, func(err error) {
				if err == nil {
					once.Do(func() { close(done) })
				}
			})
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("no successful callback for the phony request of %s", h)
			}
			if h == small {
				require.True(t, r.has(t, h))
				require.True(t, confirmed(c, h))
				continue
			}
			require.False(t, r.has(t, h))
			requireUnconfirmed(t, c, h)
		}
		require.Zero(t, c.Stats().RemoteChecks.PhonyConfirmed)
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

// A keep-alive answer can be older than a local write that cleared the entry while the call
// was in flight, so a marked entry is confirmed by a check sent after the answer.
func TestCacheman_RemoteHeadersLeavesMarkersToTheCheck(t *testing.T) {
	ctx := context.Background()
	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, period: time.Hour})
	c.startLocalWriteCheck(ctx)
	h := fetchedBlob(t, c, "cleared during the call")
	putOnServer(t, r, "cleared during the call")
	c.NewObject(h, protocol.TypeBlob)
	c.localWrites.take(localWriteCheckBatchSize)

	c.checkRemoteHeaders(ctx, []entities.Hash{h}, []bool{true})
	requireUnconfirmed(t, c, h)
	require.Equal(t, int64(1), c.Stats().RemoteChecks.Queued)
}

// A push of a cached tree the server lacks, whose child is in no store and missing on the
// server, is dropped after its wait. The entry is parked, and a later push succeeds once the
// child is in the cache.
func TestCacheman_DropsStuckPush(t *testing.T) {
	old := unconfirmedPushDropAfter
	unconfirmedPushDropAfter = 100 * time.Millisecond
	t.Cleanup(func() { unconfirmedPushDropAfter = old })

	ctx := context.Background()
	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, period: time.Hour})
	c.startLocalWriteCheck(ctx)

	childContent := "never read by the mount, deleted on the server"
	child := objects.HashBlobBytes([]byte(childContent))
	content, err := gitobjects.EncodeTree([]entities.FileEntry{
		{Name: "file", Mode: entities.FileModeRegular, Hash: child},
	})
	require.NoError(t, err)
	tree := entities.Hash(sha1.Sum(content))
	require.NoError(t, c.store.PutTree(ctx, tree, content, entities.TreeMeta{Sizes: []int64{int64(len(childContent))}}))
	_, err = c.addFetchedObjectToIndex(tree, EntryTypeTree)
	require.NoError(t, err)

	busy := func() int64 {
		var n int64
		for state, counts := range c.getSync().Stats().States {
			if state != dagsync.HolderState.String() {
				for _, v := range counts {
					n += v
				}
			}
		}
		return n
	}
	c.checkRemoteHeaders(ctx, []entities.Hash{tree}, []bool{false})
	require.Eventually(t, func() bool {
		s, _ := c.getSync().State(child.String())
		return s == dagsync.FailedLinkBuildState
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return c.Stats().RemoteChecks.Dropped == 1 }, 15*time.Second, 10*time.Millisecond)
	require.Zero(t, busy())
	require.Eventually(t, func() bool { return c.isParked(tree) }, 5*time.Second, 10*time.Millisecond)
	requireParked(t, c, tree)
	require.False(t, r.has(t, tree))

	writeBlob(t, c, childContent)
	c.checkRemoteHeaders(ctx, []entities.Hash{tree}, []bool{false})
	require.Eventually(t, func() bool { return r.has(t, tree) && confirmed(c, tree) }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), c.Stats().RemoteChecks.Pushed)
	require.Equal(t, int64(1), c.Stats().RemoteChecks.Dropped)
}

// fetchARIAndCASBlob fetches an ARI and an RBE CAS blob as a build does and returns their index
// keys once both are indexed.
func fetchARIAndCASBlob(t *testing.T, c *CacheMan) (ariKey, casKey entities.Hash) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	casPayload := []byte("output of a remote action")
	digest := sha256.Sum256(casPayload)
	sha256Hex := hex.EncodeToString(digest[:])
	ari := "rbe:" + sha256Hex + ":0"
	c.ariClient = &mockARIBlobClient{blobs: map[string][]byte{
		ari: []byte("an ARI"),
		fmt.Sprintf("rbe:%s:%d", sha256Hex, len(casPayload)): casPayload,
	}}
	go c.fetchObjectsWorker(ctx)
	go c.indexObjectsWorker(ctx)

	done := make(chan error, 2)
	c.FetchARI(ari, defaultFetchPriority, func(err error) { done <- err })
	c.FetchCASBlob(sha256Hex, int64(len(casPayload)), defaultFetchPriority, func(err error) { done <- err })
	require.NoError(t, <-done)
	require.NoError(t, <-done)

	ariKey = entities.Hash(sha1.Sum([]byte(ari)))
	casKey = entities.Hash(sha1.Sum([]byte(fmt.Sprintf("cas:%s:%d", sha256Hex, len(casPayload)))))
	require.Eventually(t, func() bool {
		a, b := c.index.Get(ariKey), c.index.Get(casKey)
		return a != nil && a.Local && b != nil && b.Local
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, EntryTypeARI, c.index.Get(ariKey).EntryType)
	require.Equal(t, EntryTypeBlob, c.index.Get(casKey).EntryType)
	return ariKey, casKey
}

// requireEvictable checks that h is a Remote entry in the LRU without a marker.
func requireEvictable(t *testing.T, c *CacheMan, h entities.Hash) {
	t.Helper()
	entry := c.index.Get(h)
	require.NotNil(t, entry)
	require.True(t, entry.Local)
	require.True(t, entry.Remote)
	require.False(t, hasMarker(c, h))
	require.True(t, inLRU(t, c, h))
}

func waitForStartPass(t *testing.T, c *CacheMan) {
	t.Helper()
	require.Eventually(t, func() bool { return !c.index.(*badgerIndex).gate.held.Load() }, 5*time.Second, 10*time.Millisecond)
}

// fakeClock is a clock for a keyPacer whose sleeps return at once, after onSleep if set.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	sleeps  []time.Duration
	onSleep func()
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if f.onSleep != nil {
		f.onSleep()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	f.sleeps = append(f.sleeps, d)
	return ctx.Err()
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func (f *fakeClock) slept() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sleeps)
}

func (f *fakeClock) pacer(rate int) *keyPacer {
	return &keyPacer{rate: rate, now: f.Now, sleep: f.Sleep}
}

func TestKeyPacer(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	p := clock.pacer(10000)

	require.NoError(t, p.wait(ctx, 10000))
	require.NoError(t, p.wait(ctx, 5000))
	require.NoError(t, p.wait(ctx, 5000))
	require.Equal(t, []time.Duration{time.Second, 500 * time.Millisecond}, clock.slept())

	// Time spent elsewhere counts toward the next wait, but only up to the time of one call:
	// after a pause the first call goes at once and the next waits its full time.
	clock.advance(200 * time.Millisecond)
	require.NoError(t, p.wait(ctx, 10000))
	clock.advance(10 * time.Second)
	require.NoError(t, p.wait(ctx, 10000))
	// A call with no keys neither waits nor counts.
	require.NoError(t, p.wait(ctx, 0))
	require.NoError(t, p.wait(ctx, 1))
	require.Equal(t, []time.Duration{time.Second, 500 * time.Millisecond, 300 * time.Millisecond, time.Second}, clock.slept())
	require.Equal(t, time.Unix(1013, 0), clock.Now())

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, p.wait(canceled, 1), context.Canceled)
}

// The start pass sends the LRU at the keep-alive's rate at most.
func TestCacheman_StartPassIsPaced(t *testing.T) {
	ctx := context.Background()
	oldBatch, oldPacer := localWriteStartPassBatch, newStartPassPacer
	localWriteStartPassBatch = 2
	clock := &fakeClock{now: time.Unix(1000, 0)}
	newStartPassPacer = func() *keyPacer { return clock.pacer(localWriteStartPassRate) }
	defer func() { localWriteStartPassBatch, newStartPassPacer = oldBatch, oldPacer }()

	dir := t.TempDir()
	r := newCheckRemote()
	c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	for _, content := range []string{"a", "b", "c", "d", "e"} {
		fetchedBlob(t, c1, content)
		putOnServer(t, r, content)
	}
	require.NoError(t, c1.Stop())

	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	c.startLocalWriteCheck(ctx)
	waitForStartPass(t, c)
	require.Equal(t, int64(5), c.Stats().RemoteChecks.StartPass)
	require.Equal(t, int64(3), r.calls.Load())
	// Two keys at 10,000 per second each time.
	require.Equal(t, []time.Duration{200 * time.Microsecond, 200 * time.Microsecond}, clock.slept())
}

// The start pass paces the keys it sends, not the entries the check leaves out.
func TestCacheman_StartPassPacesSentKeys(t *testing.T) {
	oldBatch, oldPacer := localWriteStartPassBatch, newStartPassPacer
	localWriteStartPassBatch = 2
	clock := &fakeClock{now: time.Unix(1000, 0)}
	newStartPassPacer = func() *keyPacer { return clock.pacer(localWriteStartPassRate) }
	defer func() { localWriteStartPassBatch, newStartPassPacer = oldBatch, oldPacer }()

	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
	store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	ariKey, casKey := fetchARIAndCASBlob(t, c)
	lru := []entities.Hash{ariKey, casKey}
	for _, content := range []string{"a", "b", "c"} {
		lru = append(lru, fetchedBlob(t, c, content))
		putOnServer(t, r, content)
	}
	for i, h := range lru {
		setLRUTime(t, store, h, int64(100*(i+1)))
	}

	c.startLocalWriteCheck(context.Background())
	waitForStartPass(t, c)
	require.Equal(t, int64(3), c.Stats().RemoteChecks.StartPass)
	require.Equal(t, []time.Duration{200 * time.Microsecond}, clock.slept())
}

// A config reload that reopens the index stops the start pass on the old index.
func TestCacheman_ReloadStopsStartPass(t *testing.T) {
	dir := t.TempDir()
	r := newCheckRemote()
	c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	h := fetchedBlob(t, c1, "cached")
	putOnServer(t, r, "cached")
	require.NoError(t, c1.Stop())

	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	release := r.holdHeaders()
	c.startLocalWriteCheck(context.Background())
	require.Eventually(t, func() bool { return r.calls.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
	oldIndex := c.index

	config := *c.config
	config.FsyncObjects = !config.FsyncObjects
	require.NoError(t, c.applyLocalConfig(&config, false))
	require.NotSame(t, oldIndex, c.index)
	require.Eventually(t, func() bool { return r.canceled.Load() == 1 && r.calls.Load() == 2 }, 5*time.Second, 10*time.Millisecond)
	release()
	waitForStartPass(t, c)
	require.True(t, confirmed(c, h))
	require.Equal(t, int64(1), r.canceled.Load())
}

// The keep-alive's Headers calls and the check read the ABFS client while a config reload
// replaces it.
func TestCacheman_RemoteClientReplacedDuringChecks(t *testing.T) {
	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
	h := fetchedBlob(t, c, "cached")
	putOnServer(t, r, "cached")
	body, err := json.Marshal([]entities.Hash{h})
	require.NoError(t, err)
	localOnly := *c.config
	localOnly.LocalOnly = true

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			w := httptest.NewRecorder()
			c.httpHandler().ServeHTTP(w, httptest.NewRequest("POST", "http://localhost/remote-headers", bytes.NewReader(body)))
		}
	}()
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			callCtx, callCancel := context.WithTimeout(ctx, time.Millisecond)
			_, _ = c.localWriteHeaders(callCtx, []entities.Hash{h})
			callCancel()
		}
	}()
	for range 100 {
		require.NoError(t, c.setupABFSClient(&localOnly, true))
		c.setABFSClient(r)
	}
	cancel()
	wg.Wait()
}

// A cacheman that stops during the start pass keeps eviction held at the batches answered so
// far.
func TestCacheman_StartPassStopped(t *testing.T) {
	oldBatch, oldPacer := localWriteStartPassBatch, newStartPassPacer
	localWriteStartPassBatch = 1
	proceed := make(chan struct{})
	clock := &fakeClock{now: time.Unix(1000, 0), onSleep: func() { <-proceed }}
	newStartPassPacer = func() *keyPacer { return clock.pacer(localWriteStartPassRate) }
	defer func() { localWriteStartPassBatch, newStartPassPacer = oldBatch, oldPacer }()

	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
	store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	var hashes []entities.Hash
	for i, content := range []string{"answered", "not sent", "not sent either"} {
		h := fetchedBlob(t, c, content)
		putOnServer(t, r, content)
		setLRUTime(t, store, h, int64(100*(i+1)))
		hashes = append(hashes, h)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.startLocalWriteCheck(ctx)
	require.Eventually(t, func() bool { return c.Stats().RemoteChecks.StartPass == 1 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	close(proceed)
	c.activeWorkers.Wait()
	require.True(t, c.index.(*badgerIndex).gate.held.Load())
	require.Equal(t, int64(1), r.calls.Load())
	require.Equal(t, hashes[:1], evictAll(t, c))
}

// The start pass checks trees and objects as it checks blobs: it parks those the server lacks,
// without a push.
func TestCacheman_StartPassChecksTreesAndObjects(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	r := newCheckRemote()
	c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	blob := fetchedBlob(t, c1, "file content")
	putOnServer(t, r, "file content")

	var treeRaw bytes.Buffer
	fmt.Fprintf(&treeRaw, "%o file.txt\x00", 0100644)
	treeRaw.Write(blob[:])
	var treeData bytes.Buffer
	require.NoError(t, objects.WriteHeader(&treeData, "tree", int64(treeRaw.Len())))
	treeData.Write(treeRaw.Bytes())
	tree := objects.HashBytes(treeData.Bytes())
	require.NoError(t, c1.store.PutTree(ctx, tree, treeData.Bytes(), entities.TreeMeta{Sizes: []int64{int64(len("file content"))}}))
	_, err := c1.addFetchedObjectToIndex(tree, EntryTypeTree)
	require.NoError(t, err)

	commitRaw := fmt.Appendf(nil, "tree %s\nauthor A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\nmessage\n", tree)
	var commitData bytes.Buffer
	require.NoError(t, objects.WriteHeader(&commitData, "commit", int64(len(commitRaw))))
	commitData.Write(commitRaw)
	commit := objects.HashBytes(commitData.Bytes())
	require.NoError(t, c1.store.PutObject(ctx, commit, commitData.Bytes()))
	_, err = c1.addFetchedObjectToIndex(commit, EntryTypeObject)
	require.NoError(t, err)
	require.NoError(t, c1.Stop())

	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	c.startLocalWriteCheck(ctx)
	waitForStartPass(t, c)
	for _, h := range []entities.Hash{tree, commit} {
		requireParked(t, c, h)
		require.False(t, r.has(t, h))
	}
	stats := c.Stats().RemoteChecks
	require.Equal(t, int64(3), stats.StartPass)
	require.Equal(t, int64(2), stats.Parked)
	require.Zero(t, stats.Pushed)
	require.Zero(t, stats.Skipped)
	require.Equal(t, EntryTypeTree, c.index.Get(tree).EntryType)
	require.Equal(t, EntryTypeObject, c.index.Get(commit).EntryType)
}

// A batch of only ARI entries and RBE CAS blobs sends no call.
func TestCacheman_StartPassSendsNoEmptyBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	r := newCheckRemote()
	c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	fetchARIAndCASBlob(t, c1)
	require.NoError(t, c1.Stop())

	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	calls := r.calls.Load()
	c.startLocalWriteCheck(ctx)
	waitForStartPass(t, c)
	require.Equal(t, int64(2), c.Stats().RemoteChecks.Skipped)
	require.Equal(t, calls, r.calls.Load())
}

// Batches the checks leave out count as answered, so the watermark moves past them.
func TestCacheman_StartPassWatermarkPassesSkippedBatches(t *testing.T) {
	old := localWriteStartPassBatch
	localWriteStartPassBatch = 1
	defer func() { localWriteStartPassBatch = old }()

	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
	store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	ariKey, casKey := fetchARIAndCASBlob(t, c)
	answered := fetchedBlob(t, c, "answered")
	waiting := fetchedBlob(t, c, "not answered")
	putOnServer(t, r, "answered")
	putOnServer(t, r, "not answered")
	for i, h := range []entities.Hash{answered, ariKey, casKey, waiting} {
		setLRUTime(t, store, h, int64(100*(i+1)))
	}

	releaseWaiting := r.holdKey(waiting)
	c.startLocalWriteCheck(context.Background())
	var evicted []entities.Hash
	require.Eventually(t, func() bool {
		evicted = append(evicted, evictAll(t, c)...)
		return len(evicted) == 3
	}, 5*time.Second, 10*time.Millisecond)
	require.ElementsMatch(t, []entities.Hash{answered, ariKey, casKey}, evicted)
	require.True(t, inLRU(t, c, waiting))

	releaseWaiting()
	waitForStartPass(t, c)
	require.Equal(t, []entities.Hash{waiting}, evictAll(t, c))
}

// An entry whose file cannot be read for another reason than its absence is checked.
func TestCacheman_StartPassChecksUnreadableEntries(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads files without permission")
	}
	dir := t.TempDir()
	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	h := fetchedBlob(t, c, "unreadable")
	shard := filepath.Join(dir, "cache", "blobs", h.String()[:3])
	require.DirExists(t, shard)
	require.NoError(t, os.Chmod(shard, 0))
	t.Cleanup(func() { require.NoError(t, os.Chmod(shard, 0755)) })

	c.startLocalWriteCheck(context.Background())
	waitForStartPass(t, c)
	require.Equal(t, 1, r.askedFor(h))
	require.Zero(t, c.Stats().RemoteChecks.Skipped)
	require.True(t, c.isParked(h))
	require.False(t, inLRU(t, c, h))
}

// ARI entries and RBE CAS blobs hold no object of the ABFS server: no check asks the server
// about them, and they stay in the LRU, evictable.
func TestCacheman_LocalWriteCheckLeavesARIAndCASBlobs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	r := newCheckRemote()
	c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	ariKey, casKey := fetchARIAndCASBlob(t, c1)
	present := fetchedBlob(t, c1, "on the server")
	putOnServer(t, r, "on the server")
	deleted := fetchedBlob(t, c1, "deleted on the server")
	require.NoError(t, c1.Stop())

	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	c.startLocalWriteCheck(ctx)
	waitForStartPass(t, c)
	stats := c.Stats().RemoteChecks
	require.Equal(t, int64(2), stats.StartPass)
	require.Equal(t, int64(2), stats.Skipped)
	require.Zero(t, stats.LostBeforeIndex)
	require.Zero(t, r.askedFor(ariKey))
	require.Zero(t, r.askedFor(casKey))
	requireEvictable(t, c, ariKey)
	requireEvictable(t, c, casKey)
	requireEvictable(t, c, present)
	require.True(t, c.isParked(deleted))

	// A missing answer from another check leaves them as well.
	c.checkRemoteHeaders(ctx, []entities.Hash{ariKey, casKey}, []bool{false, false})
	c.recheckCachedObject(ctx, casKey, EntryTypeBlob)
	require.Equal(t, int64(4), c.Stats().RemoteChecks.Skipped)
	requireEvictable(t, c, ariKey)
	requireEvictable(t, c, casKey)

	require.Subset(t, evictAll(t, c), []entities.Hash{ariKey, casKey, present})
	requireParked(t, c, deleted)
	has, err := c.store.HasARI(ctx, ariKey)
	require.NoError(t, err)
	require.False(t, has)
}

// An ARI entry or RBE CAS blob that a check took out of the LRU goes back at the next start,
// once; a marked ABFS blob is checked as before.
func TestCacheman_StartRestoresMarkedARIAndCASBlobs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	r := newCheckRemote()
	c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	ariKey, casKey := fetchARIAndCASBlob(t, c1)
	blob := fetchedBlob(t, c1, "marked, on the server")
	putOnServer(t, r, "marked, on the server")
	idx := c1.index.(*badgerIndex)
	for _, h := range []entities.Hash{ariKey, casKey, blob} {
		_, cleared, _, err := idx.clearRemote(h, nil, nil)
		require.NoError(t, err)
		require.True(t, cleared)
		require.False(t, inLRU(t, c1, h))
	}
	require.NoError(t, c1.Stop())

	c2 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	release := r.holdHeaders()
	c2.startLocalWriteCheck(ctx)
	require.Equal(t, int64(2), c2.Stats().RemoteChecks.Restored)
	requireEvictable(t, c2, ariKey)
	requireEvictable(t, c2, casKey)
	require.True(t, hasMarker(c2, blob))
	require.False(t, inLRU(t, c2, blob))
	release()
	require.Eventually(t, func() bool { return confirmed(c2, blob) && inLRU(t, c2, blob) }, 5*time.Second, 10*time.Millisecond)
	waitForStartPass(t, c2)
	require.Zero(t, r.askedFor(ariKey))
	require.Zero(t, r.askedFor(casKey))
	require.NoError(t, c2.Stop())

	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	c.startLocalWriteCheck(ctx)
	waitForStartPass(t, c)
	require.Zero(t, c.Stats().RemoteChecks.Restored)
	requireEvictable(t, c, ariKey)
	requireEvictable(t, c, casKey)
	require.Subset(t, evictAll(t, c), []entities.Hash{ariKey, casKey, blob})
}

// restartWithMissing caches the contents in a cacheman, none of them on the server, at LRU
// times 100, 200 and so on, and returns a restarted cacheman whose start pass has run.
func restartWithMissing(t *testing.T, r *checkRemote, contents ...string) (*CacheMan, []entities.Hash) {
	t.Helper()
	dir := t.TempDir()
	c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	store := c1.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	var hashes []entities.Hash
	for i, content := range contents {
		h := fetchedBlob(t, c1, content)
		setLRUTime(t, store, h, int64(100*(i+1)))
		hashes = append(hashes, h)
	}
	require.NoError(t, c1.Stop())

	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	c.startLocalWriteCheck(context.Background())
	waitForStartPass(t, c)
	return c, hashes
}

// lruTime returns the LRU timestamp of h, 0 when it is not in the LRU.
func lruTime(t *testing.T, c *CacheMan, h entities.Hash) int64 {
	t.Helper()
	lru, err := c.index.ReadLRU()
	require.NoError(t, err)
	for _, kt := range lru {
		if kt.Key == h {
			return kt.Timestamp
		}
	}
	return 0
}

// parkTimes returns the park times of the parked entries as stored.
func parkTimes(t *testing.T, c *CacheMan) map[entities.Hash]int64 {
	t.Helper()
	store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	times := make(map[entities.Hash]int64)
	require.NoError(t, store.db.View(func(txn *badger.Txn) error {
		keys, err := oldestParked(txn, math.MaxInt64, 1000)
		for _, kt := range keys {
			times[kt.Key] = kt.Timestamp
		}
		return err
	}))
	return times
}

// setParkTime moves the park of h to timestamp.
func setParkTime(t *testing.T, c *CacheMan, h entities.Hash, timestamp int64) {
	t.Helper()
	store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	store.lockTimestamps(h)
	defer store.unlockTimestamps(h)
	p, ok := store.parked[shardForHash(h)][h]
	require.True(t, ok)
	require.NoError(t, store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(parkKey(p.time, h)); err != nil {
			return err
		}
		return txn.Set(parkKey(timestamp, h), []byte(strconv.FormatInt(p.lruTime, 10)))
	}))
	p.time = timestamp
	store.parked[shardForHash(h)][h] = p
}

// The start pass parks a cached entry the server lacks: no push, out of the LRU, and kept
// while eviction has other entries to take.
func TestCacheman_StartPassParksMissing(t *testing.T) {
	r := newCheckRemote()
	putOnServer(t, r, "on the server")
	c, hashes := restartWithMissing(t, r, "deleted on the server", "on the server")
	missing, present := hashes[0], hashes[1]

	requireParked(t, c, missing)
	require.False(t, r.has(t, missing))
	stats := c.Stats().RemoteChecks
	require.Equal(t, int64(1), stats.Parked)
	require.Equal(t, int64(c.index.Get(missing).Size), stats.ParkedSize)
	require.Zero(t, stats.Pushed)
	require.Zero(t, c.pushBlobsStat.Request.Load())

	// Over the threshold eviction takes the LRU entries before any parked one.
	bi := c.index.(*badgerIndex)
	size := bi.evictableSize()
	victims, _, err := bi.evictBatch(size, size)
	require.NoError(t, err)
	require.Len(t, victims, 1)
	require.Equal(t, present, victims[0].Key)
	_, _, err = bi.evictBatch(bi.evictableSize(), size)
	require.ErrorIs(t, err, errNothingToEvict)
	requireParked(t, c, missing)
}

// Every Headers call of the start pass asks the server to record no use; the calls of the
// check worker and of the keep-alive do not.
func TestCacheman_StartPassNoTouch(t *testing.T) {
	ctx := context.Background()
	r := newCheckRemote()
	putOnServer(t, r, "present")
	c, hashes := restartWithMissing(t, r, "missing", "present")
	for _, h := range hashes {
		require.Equal(t, 1, r.askedFor(h))
		require.Equal(t, 1, r.askedNoTouchFor(h))
	}

	written := fetchedBlob(t, c, "written over a fetched blob")
	c.NewObject(written, protocol.TypeBlob)
	require.Eventually(t, func() bool { return r.askedFor(written) == 1 }, 5*time.Second, 10*time.Millisecond)
	c.checkRemoteHeaders(ctx, hashes, []bool{false, true})
	b, err := json.Marshal(hashes)
	require.NoError(t, err)
	w := httptest.NewRecorder()
	c.httpHandler().ServeHTTP(w, httptest.NewRequest("POST", "http://localhost/remote-headers", bytes.NewReader(b)))
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, 2, r.askedFor(hashes[1]))
	for _, h := range append(hashes, written) {
		require.LessOrEqual(t, r.askedNoTouchFor(h), 1, h)
	}
	require.Zero(t, r.askedNoTouchFor(written))
}

// Past the park time maintenance returns a parked entry to the LRU at its LRU time before the
// park, Remote and without a marker, so that eviction takes it first.
func TestCacheman_ParkedReleasedAfterParkTime(t *testing.T) {
	r := newCheckRemote()
	putOnServer(t, r, "used later")
	c, hashes := restartWithMissing(t, r, "garbage", "used later")
	garbage, used := hashes[0], hashes[1]
	require.True(t, c.isParked(garbage))

	require.NoError(t, c.maintenanceWork())
	require.True(t, c.isParked(garbage), "released before its park time")

	old := localWriteParkTime
	localWriteParkTime = 0
	defer func() { localWriteParkTime = old }()
	require.NoError(t, c.maintenanceWork())
	require.False(t, c.isParked(garbage))
	requireEvictable(t, c, garbage)
	require.Equal(t, int64(100), lruTime(t, c, garbage))
	stats := c.Stats().RemoteChecks
	require.Equal(t, int64(1), stats.Released)
	require.Zero(t, stats.ParkedSize)
	require.Empty(t, parkTimes(t, c))
	require.Equal(t, []entities.Hash{garbage, used}, evictAll(t, c))
}

// releasedKeys returns the hashes that have a release time, in memory and as stored.
func releasedKeys(t *testing.T, c *CacheMan) (inMemory, stored []entities.Hash) {
	t.Helper()
	store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	for _, shard := range store.released {
		for h := range shard {
			inMemory = append(inMemory, h)
		}
	}
	require.NoError(t, store.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(keyOnlyIterationOpts())
		defer it.Close()
		prefix := []byte(releasedPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			stored = append(stored, stripPrefix(releasedPrefix, it.Item().KeyCopy(nil)))
		}
		return nil
	}))
	return inMemory, stored
}

// An entry released after its park time is not parked again by the next start until it is
// used, so that a machine that restarts often does not keep garbage out of eviction. A use
// deletes its release time.
func TestCacheman_ReleasedNotParkedAgain(t *testing.T) {
	r := newCheckRemote()
	c, hashes := restartWithMissing(t, r, "garbage")
	h := hashes[0]
	old := localWriteParkTime
	localWriteParkTime = 0
	defer func() { localWriteParkTime = old }()
	c.releaseParked()
	require.False(t, c.isParked(h))
	dir := filepath.Dir(c.config.CacheDir)
	require.NoError(t, c.Stop())

	c = newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	c.startLocalWriteCheck(context.Background())
	waitForStartPass(t, c)
	require.Equal(t, 2, r.askedFor(h))
	require.False(t, c.isParked(h))
	requireEvictable(t, c, h)

	// A use since the release makes the entry count as held again.
	store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	require.NoError(t, store.useWithTimestamp(h, time.Now().Unix()+useCooldownSec))
	inMemory, stored := releasedKeys(t, c)
	require.Empty(t, inMemory)
	require.Empty(t, stored)
	require.NoError(t, c.Stop())
	c = newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	c.startLocalWriteCheck(context.Background())
	waitForStartPass(t, c)
	requireParked(t, c, h)
}

// A start that finds a released entry still missing, and not used since, moves it to the young
// end of the LRU: it can be a write cacheman never indexed, which a mount names in its next
// keep-alive, so eviction takes the other entries first.
func TestCacheman_ReleasedMovesToYoungEnd(t *testing.T) {
	r := newCheckRemote()
	putOnServer(t, r, "held by the server")
	putOnServer(t, r, "held by the server too")
	c, hashes := restartWithMissing(t, r, "released, then written again", "held by the server", "held by the server too")
	h := hashes[0]
	old := localWriteParkTime
	localWriteParkTime = 0
	defer func() { localWriteParkTime = old }()
	c.releaseParked()
	require.Equal(t, int64(100), lruTime(t, c, h))
	dir := filepath.Dir(c.config.CacheDir)
	require.NoError(t, c.Stop())

	// A build writes the same content, and cacheman restarts before it indexes the write.
	start := time.Now().Unix()
	c = newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	c.startLocalWriteCheck(context.Background())
	waitForStartPass(t, c)
	require.False(t, c.isParked(h))
	requireEvictable(t, c, h)
	require.GreaterOrEqual(t, lruTime(t, c, h), start)
	require.Equal(t, []entities.Hash{hashes[1], hashes[2], h}, evictAll(t, c))
}

// A keep-alive answer that names a parked entry: missing, it is pushed and confirmed; found,
// it goes back to the LRU as used now.
func TestCacheman_KeepAliveNamesParked(t *testing.T) {
	ctx := context.Background()
	r := newCheckRemote()
	c, hashes := restartWithMissing(t, r, "written by the mount", "written by another cache")
	written, other := hashes[0], hashes[1]
	requireParked(t, c, written)
	requireParked(t, c, other)

	c.checkRemoteHeaders(ctx, []entities.Hash{written}, []bool{false})
	require.Eventually(t, func() bool { return confirmed(c, written) && r.has(t, written) && inLRU(t, c, written) }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(1), c.Stats().RemoteChecks.Pushed)

	putOnServer(t, r, "written by another cache")
	before := time.Now().Unix()
	c.checkRemoteHeaders(ctx, []entities.Hash{other}, []bool{true})
	require.False(t, c.isParked(other))
	requireEvictable(t, c, other)
	require.GreaterOrEqual(t, lruTime(t, c, other), before)
	require.Zero(t, c.Stats().RemoteChecks.ParkedSize)
}

// A local write over a parked entry ends the park: the entry gets a check marker and is pushed.
func TestCacheman_LocalWriteOverParked(t *testing.T) {
	r := newCheckRemote()
	c, hashes := restartWithMissing(t, r, "written again")
	h := hashes[0]
	requireParked(t, c, h)

	release := r.holdPuts()
	c.NewObject(h, protocol.TypeBlob)
	require.False(t, c.isParked(h))
	require.Zero(t, c.Stats().RemoteChecks.ParkedSize)
	require.Eventually(t, func() bool { return c.Stats().RemoteChecks.Pushed == 1 }, 5*time.Second, 10*time.Millisecond)
	requireUnconfirmed(t, c, h)
	require.Empty(t, parkTimes(t, c))

	release()
	require.Eventually(t, func() bool { return confirmed(c, h) && r.has(t, h) && inLRU(t, c, h) }, 5*time.Second, 10*time.Millisecond)
}

// A parked entry stays parked through a use and through maintenance, which neither clears nor
// pushes it, and its bytes count once.
func TestCacheman_ParkedStaysParked(t *testing.T) {
	r := newCheckRemote()
	c, hashes := restartWithMissing(t, r, "parked")
	h := hashes[0]
	size := int64(c.index.Get(h).Size)
	asked := r.askedFor(h)

	require.NoError(t, c.index.Use(h))
	require.NoError(t, c.index.UseMany([]entities.Hash{h}))
	_, err := c.addFetchedObjectToIndex(h, EntryTypeBlob)
	require.NoError(t, err)
	requireParked(t, c, h)
	require.Zero(t, c.index.LRUSize())
	require.Equal(t, size, c.Stats().RemoteChecks.ParkedSize)

	require.NoError(t, c.maintenanceWork())
	time.Sleep(50 * time.Millisecond)
	requireParked(t, c, h)
	require.Equal(t, asked, r.askedFor(h))
	require.Zero(t, c.pushBlobsStat.Request.Load())
	require.Zero(t, c.index.LRUSize())
	require.Equal(t, size, c.Stats().RemoteChecks.ParkedSize)
}

// The park state survives a restart: parks keep their time and size, and a start during the
// pass checks again only the entries still in the LRU.
func TestCacheman_ParkSurvivesRestart(t *testing.T) {
	old := localWriteStartPassBatch
	localWriteStartPassBatch = 1
	defer func() { localWriteStartPassBatch = old }()

	dir := t.TempDir()
	r := newCheckRemote()
	c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	store := c1.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	var hashes []entities.Hash
	for i, content := range []string{"first", "second", "third", "fourth"} {
		h := fetchedBlob(t, c1, content)
		setLRUTime(t, store, h, int64(100*(i+1)))
		hashes = append(hashes, h)
	}
	require.NoError(t, c1.Stop())

	c2 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	release2, release3 := r.holdKey(hashes[2]), r.holdKey(hashes[3])
	c2.startLocalWriteCheck(context.Background())
	require.Eventually(t, func() bool { return c2.Stats().RemoteChecks.Parked == 2 }, 5*time.Second, 10*time.Millisecond)
	parked := parkTimes(t, c2)
	require.Len(t, parked, 2)
	require.NoError(t, c2.Stop())
	release2()
	release3()

	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	require.Equal(t, int64(c.index.Get(hashes[0]).Size+c.index.Get(hashes[1]).Size), c.index.(*badgerIndex).parkedSize())
	for h, parkTime := range parked {
		require.True(t, c.isParked(h))
		require.False(t, inLRU(t, c, h))
		require.Equal(t, parkTime, parkTimes(t, c)[h])
	}
	asked := r.askedFor(hashes[0])
	c.startLocalWriteCheck(context.Background())
	waitForStartPass(t, c)
	require.Equal(t, asked, r.askedFor(hashes[0]))
	require.Equal(t, asked, r.askedFor(hashes[1]))
	for _, h := range hashes {
		requireParked(t, c, h)
	}
	for h, parkTime := range parked {
		require.Equal(t, parkTime, parkTimes(t, c)[h])
	}
}

// A deleted entry leaves no park behind, and a reopened index counts only the entries still
// parked and puts none of them in the LRU.
func TestCacheman_ParkBookkeeping(t *testing.T) {
	r := newCheckRemote()
	c, hashes := restartWithMissing(t, r, "deleted", "kept")
	deleted, kept := hashes[0], hashes[1]
	keptSize := int64(c.index.Get(kept).Size)
	require.Equal(t, int64(c.index.Get(deleted).Size)+keptSize, c.Stats().RemoteChecks.ParkedSize)

	require.NoError(t, c.index.Delete(deleted))
	require.False(t, c.isParked(deleted))
	require.NotContains(t, parkTimes(t, c), deleted)
	require.Equal(t, keptSize, c.Stats().RemoteChecks.ParkedSize)

	dir := filepath.Dir(c.config.CacheDir)
	require.NoError(t, c.Stop())
	c = newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	require.Equal(t, keptSize, c.Stats().RemoteChecks.ParkedSize)
	require.Zero(t, c.index.LRUSize())
	requireParked(t, c, kept)
	require.NoError(t, c.index.Use(kept))
	require.False(t, inLRU(t, c, kept))
}

// A park that conflicts with another transaction on the entry, such as a keep-alive time
// written by a mount, is retried until it commits; the entry never becomes evictable.
func TestCacheman_StartPassParkRetriesConflicts(t *testing.T) {
	dir := t.TempDir()
	r := newCheckRemote()
	c1 := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	h := fetchedBlob(t, c1, "parked after conflicts")
	require.NoError(t, c1.Stop())

	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir})
	store := c.index.(*badgerIndex).indexStore.(*BadgerIndexStore)
	var conflicts atomic.Int64
	store.parkHook = func(hash entities.Hash) error {
		if conflicts.Add(1) > 3 {
			return nil
		}
		return store.KeepAlive(hash)
	}
	c.startLocalWriteCheck(context.Background())
	require.Empty(t, evictAll(t, c))
	waitForStartPass(t, c)
	requireParked(t, c, h)
	require.Equal(t, int64(3), c.Stats().RemoteChecks.ClearRetries)
}

// At the threshold, with no other entry left, eviction takes parked entries, oldest park first,
// and counts them; below the threshold it takes none.
func TestCacheman_EvictsParkedAtThreshold(t *testing.T) {
	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
	bi := c.index.(*badgerIndex)
	first := fetchedBlob(t, c, "parked first")
	last := fetchedBlob(t, c, "parked last")
	lru := fetchedBlob(t, c, "in the LRU")
	for _, h := range []entities.Hash{last, first} {
		parked, _, err := bi.park(h, nil)
		require.NoError(t, err)
		require.True(t, parked)
	}
	setParkTime(t, c, first, 1000)
	setParkTime(t, c, last, 2000)
	bi.releaseEviction()
	bi.evictThreshold = uint64(bi.parkedSize())
	bi.indexStore.(*BadgerIndexStore).evictBatchSize = 1

	require.NoError(t, bi.evictIndexAndCache())
	require.False(t, hasBlob(t, c, lru))
	require.False(t, hasBlob(t, c, first))
	require.False(t, c.isParked(first))
	require.False(t, c.index.Get(first).Local)
	requireParked(t, c, last)
	require.Equal(t, int64(1), c.Stats().RemoteChecks.EvictedParked)
	require.Equal(t, int64(c.index.Get(last).Size), c.Stats().RemoteChecks.ParkedSize)

	// Below the threshold the last one stays, and the run ends without an error.
	require.NoError(t, bi.evictIndexAndCache())
	requireParked(t, c, last)
	require.Equal(t, int64(1), c.Stats().RemoteChecks.EvictedParked)
}

// With the LRU empty and the cache above its eviction target, eviction takes parked entries,
// oldest park first, down to the target, so that a later fetch below the background trigger
// stays.
func TestCacheman_EvictsParkedDownToTarget(t *testing.T) {
	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true})
	bi := c.index.(*badgerIndex)
	var parked []entities.Hash
	for i := range 4 {
		h := fetchedBlob(t, c, fmt.Sprintf("parked %d", i))
		ok, _, err := bi.park(h, nil)
		require.NoError(t, err)
		require.True(t, ok)
		setParkTime(t, c, h, int64(1000*(i+1)))
		parked = append(parked, h)
	}
	bi.releaseEviction()
	bi.indexStore.(*BadgerIndexStore).evictBatchSize = 1
	// Parked entries fill 85 % of the threshold; two fetches take the cache over 90 %.
	threshold := uint64(bi.parkedSize() * 100 / 85)
	fetched := []entities.Hash{fetchedBlob(t, c, "fetched first"), fetchedBlob(t, c, "fetched second")}
	require.Greater(t, float64(bi.evictableSize()), float64(threshold)*evictBackgroundTrigger)

	bi.evictThreshold = threshold
	require.NoError(t, bi.evictIndexAndCache())
	bi.evictThreshold = 0
	for _, h := range fetched {
		require.False(t, hasBlob(t, c, h))
	}
	require.False(t, c.isParked(parked[0]))
	require.False(t, hasBlob(t, c, parked[0]))
	for _, h := range parked[1:] {
		requireParked(t, c, h)
	}
	require.Equal(t, int64(1), c.Stats().RemoteChecks.EvictedParked)
	require.LessOrEqual(t, bi.evictableSize(), evictTarget(int64(threshold)))

	// The next fetch stays below the background trigger, so no eviction starts.
	bi.evictThreshold = threshold
	later := fetchedBlob(t, c, "fetched later")
	unlock := bi.LockEviction()
	bi.evictThreshold = 0
	unlock()
	require.True(t, hasBlob(t, c, later))
	requireEvictable(t, c, later)
}

// A phony request that finds a parked entry on the server returns it to the LRU as used now,
// without a push.
func TestCacheman_PhonyUnparksFound(t *testing.T) {
	r := newCheckRemote()
	c, hashes := restartWithMissing(t, r, "pushed again by another cache")
	h := hashes[0]
	requireParked(t, c, h)
	putOnServer(t, r, "pushed again by another cache")

	before := time.Now().Unix()
	done := make(chan error, 1)
	c.Phony(0, []entities.Hash{h}, func(err error) {
		if err == nil {
			done <- err
		}
	})
	require.NoError(t, <-done)
	require.False(t, c.isParked(h))
	requireEvictable(t, c, h)
	require.GreaterOrEqual(t, lruTime(t, c, h), before)
	require.Zero(t, c.Stats().RemoteChecks.ParkedSize)
	require.Zero(t, c.pushBlobsStat.Request.Load())
}

// The server lacks a parked entry, so WaitFor and /is-object-remote treat it as not Remote: a phony
// request pushes it, which returns it to the LRU.
func TestCacheman_ParkedIsNotRemote(t *testing.T) {
	r := newCheckRemote()
	c, hashes := restartWithMissing(t, r, "an output a build waits for")
	h := hashes[0]

	b, err := json.Marshal(h)
	require.NoError(t, err)
	w := httptest.NewRecorder()
	c.httpHandler().ServeHTTP(w, httptest.NewRequest("POST", "http://localhost/is-object-remote", bytes.NewReader(b)))
	require.Equal(t, http.StatusOK, w.Code)
	var remoteFlag bool
	require.NoError(t, json.NewDecoder(w.Body).Decode(&remoteFlag))
	require.False(t, remoteFlag)

	done := make(chan error, 1)
	c.Phony(0, []entities.Hash{h}, func(err error) {
		if err == nil {
			done <- err
		}
	})
	require.NoError(t, <-done)
	require.True(t, r.has(t, h))
	require.Eventually(t, func() bool { return confirmed(c, h) && inLRU(t, c, h) }, 5*time.Second, 10*time.Millisecond)
}

// A push dropped because a child is in no store parks the entry in place of its marker, so a
// restart does not push it again.
func TestCacheman_DroppedPushParks(t *testing.T) {
	old := unconfirmedPushDropAfter
	unconfirmedPushDropAfter = 100 * time.Millisecond
	t.Cleanup(func() { unconfirmedPushDropAfter = old })

	ctx := context.Background()
	dir := t.TempDir()
	r := newCheckRemote()
	c := newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir, period: 10 * time.Millisecond})
	c.startLocalWriteCheck(ctx)
	child := objects.HashBlobBytes([]byte("never read by the mount"))
	content, err := gitobjects.EncodeTree([]entities.FileEntry{
		{Name: "file", Mode: entities.FileModeRegular, Hash: child},
	})
	require.NoError(t, err)
	tree := entities.Hash(sha1.Sum(content))
	require.NoError(t, c.store.PutTree(ctx, tree, content, entities.TreeMeta{Sizes: []int64{1}}))
	_, err = c.addFetchedObjectToIndex(tree, EntryTypeTree)
	require.NoError(t, err)

	c.NewObject(tree, protocol.TypeTree)
	require.Eventually(t, func() bool { return c.Stats().RemoteChecks.Dropped == 1 }, 15*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return c.isParked(tree) }, 5*time.Second, 10*time.Millisecond)
	requireParked(t, c, tree)
	require.Equal(t, int64(1), c.Stats().RemoteChecks.Parked)
	require.NoError(t, c.Stop())

	c = newCheckCacheman(t, r, checkOpts{gcKeepAlive: true, dir: dir, period: 10 * time.Millisecond})
	asked := r.askedFor(tree)
	c.startLocalWriteCheck(ctx)
	waitForStartPass(t, c)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, asked, r.askedFor(tree))
	require.Zero(t, c.Stats().RemoteChecks.Queued)
	requireParked(t, c, tree)
	require.False(t, r.has(t, tree))
}

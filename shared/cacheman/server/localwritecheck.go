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
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/shared/cacheman/protocol"
	"github.com/google/abfs/shared/dagsync"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
)

const (
	// The keep-alive's batch size, which is also the per-call key limit of the gRPC client.
	localWriteCheckBatchSize = 50000
	// Headers calls the start pass has in flight.
	localWriteStartPassCalls = 4
	localWriteRetryWait      = time.Second
	maxLocalWriteRetryWait   = time.Minute
)

var (
	// localWriteCheckPeriod is the longest a queued check waits before it is sent.
	localWriteCheckPeriod = time.Second
	// localWriteStartPassBatch is the number of keys per Headers call of the start pass.
	localWriteStartPassBatch = localWriteCheckBatchSize
)

// localWriteCheck keeps content written locally in the cache until the server has confirmed
// that it holds the object. A local write can equal an object the cache fetched or pushed
// long ago and that the server GC has deleted since; evicting it under the old remote flag
// would leave the bytes nowhere. It runs only while the applied config has gc-keepalive on,
// and once on it stays on for the life of the process, since mount details record it.
type localWriteCheck struct {
	on atomic.Bool

	mu     sync.Mutex
	ctx    context.Context
	index  localWriteIndex
	worker bool
	queue  map[entities.Hash]EntryType
	full   chan struct{}

	// phonySync is the dagsync whose phony callbacks report an answer of the server. A
	// dagsync with an offline threshold skips requests and then calls back without an
	// error and without a server call, so phony requests sent to it confirm nothing.
	phonySync atomic.Pointer[dagsync.DAGSync]

	sent            atomic.Int64
	present         atomic.Int64
	missing         atomic.Int64
	pushed          atomic.Int64
	unpushable      atomic.Int64
	clearRetries    atomic.Int64
	clearFailures   atomic.Int64
	startPass       atomic.Int64
	lostBeforeIndex atomic.Int64
	phonyConfirmed  atomic.Int64
}

func (lw *localWriteCheck) enqueue(hash entities.Hash, entryType EntryType) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.enqueueLocked(hash, entryType)
}

func (lw *localWriteCheck) enqueueLocked(hash entities.Hash, entryType EntryType) {
	if lw.queue == nil {
		lw.queue = make(map[entities.Hash]EntryType)
	}
	lw.queue[hash] = entryType
	if len(lw.queue) >= localWriteCheckBatchSize && lw.full != nil {
		select {
		case lw.full <- struct{}{}:
		default:
		}
	}
}

// take removes up to n queued hashes and returns them with their entry types.
func (lw *localWriteCheck) take(n int) ([]entities.Hash, []EntryType) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	var hashes []entities.Hash
	var types []EntryType
	for h, t := range lw.queue {
		if len(hashes) == n {
			break
		}
		hashes = append(hashes, h)
		types = append(types, t)
		delete(lw.queue, h)
	}
	return hashes, types
}

func (lw *localWriteCheck) queued() int {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return len(lw.queue)
}

// checkedIndex returns the index while the check is on.
func (c *CacheMan) checkedIndex() (localWriteIndex, bool) {
	if !c.localWrites.on.Load() {
		return nil, false
	}
	idx, ok := c.index.(localWriteIndex)
	return idx, ok
}

// applyLocalWriteCheck turns the check on once config has gc-keepalive on. A config that turns
// it off later takes effect at the next start.
func (c *CacheMan) applyLocalWriteCheck(config *instance.Config) {
	if config.GCKeepAlive && config.EnableIndex && !config.LocalOnly {
		c.localWrites.on.Store(true)
	}
	idx, ok := c.checkedIndex()
	if !ok {
		return
	}
	idx.enableLocalWriteCheck()

	c.localWrites.mu.Lock()
	defer c.localWrites.mu.Unlock()
	c.startLocalWriteCheckLocked()
}

// startLocalWriteCheck starts the check worker and the start pass. Serve calls it before
// it starts the new-object workers.
func (c *CacheMan) startLocalWriteCheck(ctx context.Context) {
	c.localWrites.mu.Lock()
	defer c.localWrites.mu.Unlock()
	c.localWrites.ctx = ctx
	c.startLocalWriteCheckLocked()
}

func (c *CacheMan) startLocalWriteCheckLocked() {
	lw := &c.localWrites
	idx, ok := c.checkedIndex()
	if !ok || lw.ctx == nil || idx == lw.index {
		return
	}
	lw.index = idx

	// An entry cleared before a restart is not Remote, so nothing else would check it.
	markers, err := idx.checkMarkers()
	if err != nil {
		c.printer.Printf("Failed to read the check markers: %v\n", err)
	}
	for _, h := range markers {
		entryType := EntryTypeBlob
		if entry := c.index.Get(h); entry != nil {
			entryType = entry.EntryType
		}
		lw.enqueueLocked(h, entryType)
	}

	// A local write whose notification was lost leaves no trace when the store already held
	// the blob, so every cached entry is checked before eviction may take it.
	lru, err := c.index.ReadLRU()
	if err != nil {
		c.printer.Printf("Failed to read the LRU, eviction waits for the cache to fill: %v\n", err)
	} else {
		c.activeWorkers.Add(1)
		go c.localWriteStartPass(lw.ctx, idx, slices.Collect(slices.Chunk(lru, localWriteStartPassBatch)))
	}

	if !lw.worker {
		lw.worker = true
		lw.full = make(chan struct{}, 1)
		c.activeWorkers.Add(1)
		go c.localWriteCheckWorker(lw.ctx, lw.full, localWriteCheckPeriod)
	}
}

// stopContext returns a context that is canceled when cacheman stops.
func (c *CacheMan) stopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-c.closeChan:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// localWriteHeaders asks the server whether it has keys, retrying with backoff until it
// answers or ctx is done.
func (c *CacheMan) localWriteHeaders(ctx context.Context, keys []entities.Hash) ([]bool, error) {
	wait := localWriteRetryWait
	for {
		hdrs, err := c.abfsClient.Headers(ctx, keys)
		if err == nil && len(hdrs) != len(keys) {
			err = fmt.Errorf("server answered %d headers for %d keys", len(hdrs), len(keys))
		}
		if err == nil {
			found := make([]bool, len(hdrs))
			for i, hdr := range hdrs {
				found[i] = !hdr.IsEmpty()
			}
			c.localWrites.sent.Add(int64(len(keys)))
			return found, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		c.printer.Printf("Failed to check %d cached objects with the server, retrying in %s: %v\n", len(keys), wait, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		wait = min(2*wait, maxLocalWriteRetryWait)
	}
}

// localWriteCheckWorker sends the queued checks at least once per period.
func (c *CacheMan) localWriteCheckWorker(ctx context.Context, full <-chan struct{}, period time.Duration) {
	defer c.activeWorkers.Done()
	defer c.logPanic()
	ctx, cancel := c.stopContext(ctx)
	defer cancel()

	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-full:
		}
		for {
			hashes, types := c.localWrites.take(localWriteCheckBatchSize)
			if len(hashes) == 0 {
				break
			}
			found, err := c.localWriteHeaders(ctx, hashes)
			if err != nil {
				return
			}
			for i, h := range hashes {
				switch {
				case found[i]:
					c.confirmLocalWrite(h, types[i])
				case c.isCachedRemote(h):
					c.recheckCachedObject(ctx, h, types[i])
				default:
					c.pushUnconfirmed(ctx, h, types[i])
				}
			}
		}
	}
}

func (c *CacheMan) confirmLocalWrite(hash entities.Hash, entryType EntryType) {
	c.localWrites.present.Add(1)
	idx, ok := c.checkedIndex()
	if !ok {
		return
	}
	if _, err := idx.confirmRemote(hash); err != nil {
		c.printer.Printf("Failed to mark %s remote after the server confirmed it: %v\n", hash, err)
		c.localWrites.enqueue(hash, entryType)
	}
}

// pushUnconfirmed handles a hash the server lacks: it pushes the local copy, after which
// onPush marks the entry remote. The entry stays local and out of the LRU until then.
func (c *CacheMan) pushUnconfirmed(ctx context.Context, hash entities.Hash, entryType EntryType) {
	lw := &c.localWrites
	lw.missing.Add(1)
	entry := c.index.Get(hash)
	if entry != nil {
		entryType = entry.EntryType
	}
	if !c.hasLocalCopy(ctx, hash, entryType) {
		lw.lostBeforeIndex.Add(1)
		c.printer.Printf("Warning: %s %s was written locally, but the cache no longer holds it and the server does not have it\n", entryType, hash)
		return
	}
	// dagsync skips objects over the offline threshold and calls no onPush for them, and a
	// remote read-only cacheman pushes nothing: such entries stay local and keep their marker.
	if c.noRemotePush() || (entry != nil && entry.Offline) || c.overOfflineThreshold(ctx, hash) {
		lw.unpushable.Add(1)
		return
	}
	sync := c.getSync()
	if sync == nil {
		lw.enqueue(hash, entryType)
		return
	}
	lw.pushed.Add(1)
	switch entryType {
	case EntryTypeBlob:
		c.pushBlobsStat.Request.Add(1)
		go sync.PutBlob(hash)
	case EntryTypeTree:
		c.pushTreesStat.Request.Add(1)
		go sync.PutTree(hash)
	default:
		c.pushObjectsStat.Request.Add(1)
		go sync.PutObject(hash)
	}
}

func (c *CacheMan) hasLocalCopy(ctx context.Context, hash entities.Hash, entryType EntryType) bool {
	var has bool
	var err error
	if entryType == EntryTypeBlob {
		has, err = c.store.HasBlob(ctx, hash)
	} else {
		has, err = c.store.HasObject(ctx, hash)
	}
	return err == nil && has
}

func (c *CacheMan) overOfflineThreshold(ctx context.Context, hash entities.Hash) bool {
	threshold := c.config.OfflineThreshold
	if threshold == 0 {
		return false
	}
	hdr, err := c.store.ObjectHeader(ctx, hash)
	return err == nil && uint64(hdr.Size) > threshold
}

// isCachedRemote reports whether hash has a Local and Remote entry, which eviction may take.
func (c *CacheMan) isCachedRemote(hash entities.Hash) bool {
	entry := c.index.Get(hash)
	return entry != nil && entry.Local && entry.Remote
}

// recheckCachedObject handles a cached entry the server lacks: it takes the entry out of the
// LRU with a check marker and pushes it.
func (c *CacheMan) recheckCachedObject(ctx context.Context, hash entities.Hash, entryType EntryType) {
	idx, ok := c.checkedIndex()
	if !ok {
		return
	}
	_, cleared, retries, err := idx.clearRemote(hash, nil, c.closeChan)
	c.localWrites.clearRetries.Add(int64(retries))
	if err != nil {
		c.printer.Printf("Failed to keep %s, which the server lacks, in the cache: %v\n", hash, err)
		return
	}
	if cleared {
		c.pushUnconfirmed(ctx, hash, entryType)
	}
}

// localWriteStartPass checks every LRU entry with the server, in batches oldest first, and
// lets eviction take the entries checked so far.
func (c *CacheMan) localWriteStartPass(ctx context.Context, idx localWriteIndex, batches [][]KeyTimestamp) {
	defer c.activeWorkers.Done()
	defer c.logPanic()
	ctx, cancel := c.stopContext(ctx)
	defer cancel()

	start := time.Now()
	var mu sync.Mutex
	answered := make([]bool, len(batches))
	next := 0
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(localWriteStartPassCalls)
	for i, batch := range batches {
		eg.Go(func() error {
			keys := make([]entities.Hash, len(batch))
			for j, kt := range batch {
				keys[j] = kt.Key
			}
			found, err := c.localWriteHeaders(ctx, keys)
			if err != nil {
				return err
			}
			c.localWrites.startPass.Add(int64(len(keys)))
			for j, ok := range found {
				if ok {
					c.localWrites.present.Add(1)
				} else {
					c.recheckCachedObject(ctx, keys[j], EntryTypeBlob)
				}
			}

			mu.Lock()
			defer mu.Unlock()
			answered[i] = true
			for next < len(batches) && answered[next] {
				next++
			}
			// Entries of a later batch can share the last timestamp of an answered one.
			if next > 0 && next < len(batches) {
				last := batches[next-1][len(batches[next-1])-1].Timestamp
				idx.setEvictionWatermark(min(last, batches[next][0].Timestamp-1))
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return
	}
	idx.releaseEviction()
	c.printer.Printf("Checked the cached objects with the server in %s\n", time.Since(start).Round(time.Millisecond))
}

// putIndexEntry adds an entry to the index. With the check on, a local write goes through
// clearRemote, which never drops the write on a conflict and keeps content the cache fetched
// or pushed before out of the LRU until the server confirms it.
func (c *CacheMan) putIndexEntry(ke *IndexKeyEntry, remote bool) (*IndexEntry, error) {
	idx, ok := c.checkedIndex()
	if remote || !ok || !localWriteChecked(ke.EntryType) {
		_, _, prevEnt, _, err := c.index.Put(ke.Key, ke.IndexEntry)
		return prevEnt, err
	}
	prevEnt, cleared, retries, err := idx.clearRemote(ke.Key, ke.IndexEntry, c.closeChan)
	c.localWrites.clearRetries.Add(int64(retries))
	if err != nil {
		// The old entry can still be Remote and evictable: the check takes it out of the LRU
		// when the server lacks it.
		c.localWrites.clearFailures.Add(1)
		c.localWrites.enqueue(ke.Key, ke.EntryType)
		return nil, err
	}
	if cleared {
		c.localWrites.enqueue(ke.Key, ke.EntryType)
	}
	return prevEnt, nil
}

// checkLostLocalWrite checks a locally written object that eviction removed before cacheman
// indexed it: the server either has it, or the bytes are gone.
func (c *CacheMan) checkLostLocalWrite(hash entities.Hash, objType protocol.RequestObjectType) {
	if _, ok := c.checkedIndex(); !ok {
		return
	}
	switch objType {
	case protocol.TypeBlob:
		c.localWrites.enqueue(hash, EntryTypeBlob)
	case protocol.TypeTree:
		c.localWrites.enqueue(hash, EntryTypeTree)
	case protocol.TypeObject:
		c.localWrites.enqueue(hash, EntryTypeObject)
	}
}

// reindexLocalWrite re-indexes an evicted entry whose file is on disk again as a local write,
// since the file can be a write whose notification never reached cacheman.
func (c *CacheMan) reindexLocalWrite(hash entities.Hash, entry *IndexEntry) bool {
	if _, ok := c.checkedIndex(); !ok || entry == nil || entry.Local || !entry.Remote || !localWriteChecked(entry.EntryType) {
		return false
	}
	if _, err := c.addLocalObjectToIndex(hash, entry.EntryType); err != nil && !entities.IsNotExist(err) {
		c.printer.Printf("maintenance: failed to restore local entry for %q: %v\n", hash.String(), err)
	}
	return true
}

// useSync records the dagsync that setupDAGSync created for offlineThreshold.
func (lw *localWriteCheck) useSync(sync *dagsync.DAGSync, offlineThreshold uint64) {
	if offlineThreshold > 0 {
		sync = nil
	}
	lw.phonySync.Store(sync)
}

// checkUntrackedEntry handles a Local and Remote entry that maintenance found outside the
// LRU. The start pass checked only the LRU entries, and the entry can be a local write whose
// notification was lost, so it stays out of eviction until the server confirms it. An index
// that does not track use evicts nothing and holds every entry outside the LRU, so it needs no
// check.
func (c *CacheMan) checkUntrackedEntry(hash entities.Hash, entry *IndexEntry) bool {
	idx, ok := c.checkedIndex()
	if !ok || !localWriteChecked(entry.EntryType) || !idx.tracksUse() {
		return false
	}
	_, cleared, retries, err := idx.clearRemote(hash, nil, c.closeChan)
	c.localWrites.clearRetries.Add(int64(retries))
	if err != nil {
		c.printer.Printf("maintenance: failed to keep %s in the cache until the server confirms it: %v\n", hash, err)
		return false
	}
	if cleared {
		c.localWrites.enqueue(hash, entry.EntryType)
	}
	return true
}

// confirmAfterPhony wraps the callback of a phony request sent to sync: on success every hash
// it sent is on the server, so entries waiting for a check are confirmed.
func (c *CacheMan) confirmAfterPhony(sync *dagsync.DAGSync, hashes []entities.Hash, callback dagsync.Callback) dagsync.Callback {
	idx, ok := c.checkedIndex()
	if !ok || c.localWrites.phonySync.Load() != sync {
		return callback
	}
	return func(err error) {
		if err == nil {
			for _, h := range hashes {
				confirmed, cerr := idx.confirmRemote(h)
				if cerr != nil {
					c.printer.Printf("Failed to mark %s remote after a phony request: %v\n", h, cerr)
				} else if confirmed {
					c.localWrites.phonyConfirmed.Add(1)
				}
			}
		}
		callback(err)
	}
}

// checkRemoteHeaders updates the index from the answer to a keep-alive Headers call. Content
// the cache holds and the server lacks is pushed; local content the server has becomes
// evictable.
func (c *CacheMan) checkRemoteHeaders(ctx context.Context, keys []entities.Hash, found []bool) {
	idx, ok := c.checkedIndex()
	if !ok {
		return
	}
	for i, key := range keys {
		entry := c.index.Get(key)
		if entry == nil || !entry.Local || !localWriteChecked(entry.EntryType) {
			continue
		}
		switch {
		case found[i] && idx.hasCheckMarker(key):
			// The answer can be older than a local write that cleared the entry while the
			// call was in flight, so the check worker asks again.
			c.localWrites.enqueue(key, entry.EntryType)
		case found[i] && !entry.Remote:
			if err := c.index.markObjectRemote(key); err != nil {
				c.printer.Printf("Failed to mark %s remote after the server confirmed it: %v\n", key, err)
			}
		case found[i]:
		case entry.Remote:
			c.recheckCachedObject(ctx, key, entry.EntryType)
		case idx.hasCheckMarker(key):
			c.pushUnconfirmed(ctx, key, entry.EntryType)
		}
	}
}

func (c *CacheMan) localWriteStats() *protocol.RemoteCheckStats {
	idx, ok := c.checkedIndex()
	if !ok {
		return nil
	}
	lw := &c.localWrites
	return &protocol.RemoteCheckStats{
		Queued:                  int64(lw.queued()),
		Sent:                    lw.sent.Load(),
		Present:                 lw.present.Load(),
		Missing:                 lw.missing.Load(),
		Pushed:                  lw.pushed.Load(),
		Unpushable:              lw.unpushable.Load(),
		ClearRetries:            lw.clearRetries.Load(),
		ClearFailures:           lw.clearFailures.Load(),
		EvictSkippedUnconfirmed: idx.evictSkippedUnconfirmed(),
		StartPass:               lw.startPass.Load(),
		EvictedUnchecked:        idx.evictedUnchecked(),
		LostBeforeIndex:         lw.lostBeforeIndex.Load(),
		PhonyConfirmed:          lw.phonyConfirmed.Load(),
	}
}

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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/shared/cacheman/protocol"
	"github.com/google/abfs/shared/dagsync"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/objects/gitobjects"
)

const (
	// The keep-alive's batch size, which is also the per-call key limit of the gRPC client.
	localWriteCheckBatchSize = 50000
	// Headers calls the start pass has in flight.
	localWriteStartPassCalls = 4
	localWriteRetryWait      = time.Second
	maxLocalWriteRetryWait   = time.Minute
	// Restored entries whose hash the start logs.
	localWriteRestoredLogged = 10

	// Keys the start pass sends per second at most, the rate of a mount's keep-alive. Each key
	// the server finds can queue a touch, and the server drops touches when its buffer is full.
	localWriteStartPassRate = 10000
)

var (
	// localWriteCheckPeriod is the longest a queued check waits before it is sent.
	localWriteCheckPeriod = time.Second
	// localWriteStartPassBatch is the number of keys per Headers call of the start pass.
	localWriteStartPassBatch = localWriteStartPassRate
	newStartPassPacer        = func() *keyPacer { return newKeyPacer(localWriteStartPassRate) }
	// unconfirmedPushDropAfter is how long a push of content the server lacks may wait for an
	// object that is in no local store. dagsync retries such an object at waits that grow by
	// 5 s per try, about 27 tries in this time, while content a mount writes reaches the store
	// within seconds. The next keep-alive or start pushes a dropped entry again.
	unconfirmedPushDropAfter = 30 * time.Minute
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
	// phonies counts the phony requests in flight per list of hashes. dagsync joins a phony
	// request to a running identical one, whose answer can predate the joining request.
	phonies map[string]int

	sent            atomic.Int64
	present         atomic.Int64
	missing         atomic.Int64
	pushed          atomic.Int64
	unpushable      atomic.Int64
	dropped         atomic.Int64
	clearRetries    atomic.Int64
	clearFailures   atomic.Int64
	startPass       atomic.Int64
	lostBeforeIndex atomic.Int64
	phonyConfirmed  atomic.Int64
	skipped         atomic.Int64
	restored        atomic.Int64
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
	// A few restored hashes go to the log, so that an operator can look them up.
	var restored []entities.Hash
	var restoredCount int
	for _, h := range markers {
		entry := c.index.Get(h)
		if entry != nil && entry.Local && !c.checksEntry(lw.ctx, h, entry) {
			// No server can confirm such an entry, so it goes back to the LRU as it was.
			if marked, err := idx.confirmRemote(h); err != nil {
				c.printer.Printf("Failed to return %s to the LRU: %v\n", h, err)
			} else if marked {
				lw.restored.Add(1)
				restoredCount++
				if len(restored) < localWriteRestoredLogged {
					restored = append(restored, h)
				}
			}
			continue
		}
		entryType := EntryTypeBlob
		if entry != nil {
			entryType = entry.EntryType
		}
		lw.enqueueLocked(h, entryType)
	}
	if restoredCount > 0 {
		c.printer.Printf("Returned %d marked entries without a blob, tree or object under their key to the LRU, among them %v\n", restoredCount, restored)
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
	var objType entities.ObjectType
	switch entryType {
	case EntryTypeBlob:
		c.pushBlobsStat.Request.Add(1)
		objType = entities.ObjectTypeBlob
	case EntryTypeTree:
		c.pushTreesStat.Request.Add(1)
		objType = gitobjects.ObjectTypeDir
	default:
		c.pushObjectsStat.Request.Add(1)
	}
	// A child that is in no store and that the server lacks would hold the push in dagsync
	// for good. The entry keeps its marker when the push is dropped.
	go sync.PutObjectOrDrop(hash, objType, unconfirmedPushDropAfter, func(err error) {
		if errors.Is(err, dagsync.ErrDropped) {
			lw.dropped.Add(1)
			c.printer.Printf("Dropped the push of %s %s, which the server lacks: %v\n", entryType, hash, err)
		}
	})
}

func (c *CacheMan) hasLocalCopy(ctx context.Context, hash entities.Hash, entryType EntryType) bool {
	has, err := c.storeHas(ctx, hash, entryType)
	return err == nil && has
}

// lacksLocalCopy reports whether the store confirms that it holds no file under hash. An error
// other than not found counts as a copy, so that a failing disk keeps an entry checked.
func (c *CacheMan) lacksLocalCopy(ctx context.Context, hash entities.Hash, entryType EntryType) bool {
	has, err := c.storeHas(ctx, hash, entryType)
	return err == nil && !has
}

func (c *CacheMan) storeHas(ctx context.Context, hash entities.Hash, entryType EntryType) (bool, error) {
	if entryType == EntryTypeBlob {
		return c.store.HasBlob(ctx, hash)
	}
	return c.store.HasObject(ctx, hash)
}

func (c *CacheMan) overOfflineThreshold(ctx context.Context, hash entities.Hash) bool {
	threshold := c.config.OfflineThreshold
	if threshold == 0 {
		return false
	}
	hdr, err := c.store.ObjectHeader(ctx, hash)
	return err == nil && uint64(hdr.Size) > threshold
}

// checksEntry reports whether the check covers a cached entry: a blob, tree or object whose
// content the cache holds. ARI entries come from an ARI server. An RBE CAS blob is indexed as a
// blob under a key derived from its digest, while its content is stored under the digest, so
// neither the store nor the ABFS server holds a blob under its key.
func (c *CacheMan) checksEntry(ctx context.Context, hash entities.Hash, entry *IndexEntry) bool {
	return entry != nil && entry.Local && localWriteChecked(entry.EntryType) && !c.lacksLocalCopy(ctx, hash, entry.EntryType)
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
	if !c.checksEntry(ctx, hash, c.index.Get(hash)) {
		c.localWrites.skipped.Add(1)
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

// localWriteStartPass checks every LRU entry with the server, in batches oldest first and at
// most localWriteStartPassRate entries per second, and lets eviction take the entries checked
// so far. The calls look up the entries of their batch, which reads the disk.
func (c *CacheMan) localWriteStartPass(ctx context.Context, idx localWriteIndex, batches [][]KeyTimestamp) {
	defer c.activeWorkers.Done()
	defer c.logPanic()
	ctx, cancel := c.stopContext(ctx)
	defer cancel()

	start := time.Now()
	pacer := newStartPassPacer()
	var mu sync.Mutex
	answered := make([]bool, len(batches))
	next := 0
	stopped := false
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(localWriteStartPassCalls)
	for i, batch := range batches {
		if err := pacer.wait(ctx, len(batch)); err != nil {
			stopped = true
			break
		}
		eg.Go(func() error {
			keys, types := c.startPassKeys(ctx, batch)
			var found []bool
			if len(keys) > 0 {
				var err error
				if found, err = c.localWriteHeaders(ctx, keys); err != nil {
					return err
				}
			}
			c.localWrites.startPass.Add(int64(len(keys)))
			for j, ok := range found {
				if ok {
					c.localWrites.present.Add(1)
				} else {
					c.recheckCachedObject(ctx, keys[j], types[j])
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
	if err := eg.Wait(); err != nil || stopped {
		return
	}
	idx.releaseEviction()
	c.printer.Printf("Checked the cached objects with the server in %s\n", time.Since(start).Round(time.Millisecond))
}

// startPassKeys returns the keys of batch that the check covers, with their entry types.
func (c *CacheMan) startPassKeys(ctx context.Context, batch []KeyTimestamp) ([]entities.Hash, []EntryType) {
	keys := make([]entities.Hash, 0, len(batch))
	types := make([]EntryType, 0, len(batch))
	for _, kt := range batch {
		entry := c.index.Get(kt.Key)
		if !c.checksEntry(ctx, kt.Key, entry) {
			c.localWrites.skipped.Add(1)
			continue
		}
		keys = append(keys, kt.Key)
		types = append(types, entry.EntryType)
	}
	return keys, types
}

// keyPacer spaces calls so that a call of n keys starts at least n/rate seconds after the
// previous one. A caller that was slow gets no credit for the time it lost, so after a pause
// keys still go out at rate.
type keyPacer struct {
	rate  int
	next  time.Time
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

func newKeyPacer(rate int) *keyPacer {
	return &keyPacer{rate: rate, now: time.Now, sleep: sleepContext}
}

// wait returns when n more keys may be sent, or with an error once ctx is done.
func (p *keyPacer) wait(ctx context.Context, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if d := p.next.Sub(p.now()); d > 0 {
		if err := p.sleep(ctx, d); err != nil {
			return err
		}
	}
	p.next = p.now().Add(time.Duration(n) * time.Second / time.Duration(p.rate))
	return nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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

// confirmAfterPhony wraps the callback of a phony request sent to ds: on success every hash
// it sent is on the server, so entries waiting for a check are confirmed. Only the entries that
// waited when the request was sent are confirmed, since the answer can predate a later local
// write, and a request that joins an identical one in flight confirms nothing.
func (c *CacheMan) confirmAfterPhony(ds *dagsync.DAGSync, hashes []entities.Hash, callback dagsync.Callback) dagsync.Callback {
	idx, ok := c.checkedIndex()
	if !ok || c.localWrites.phonySync.Load() != ds {
		return callback
	}
	var marked []entities.Hash
	key, joined := c.localWrites.startPhony(hashes)
	if !joined {
		for _, h := range hashes {
			if idx.hasCheckMarker(h) {
				marked = append(marked, h)
			}
		}
	}
	var once sync.Once
	return func(err error) {
		once.Do(func() { c.localWrites.endPhony(key) })
		if err == nil {
			for _, h := range marked {
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

// startPhony records a phony request for hashes in flight and reports whether an identical one
// was in flight already.
func (lw *localWriteCheck) startPhony(hashes []entities.Hash) (string, bool) {
	var b strings.Builder
	for _, h := range hashes {
		b.Write(h[:])
	}
	key := b.String()
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if lw.phonies == nil {
		lw.phonies = make(map[string]int)
	}
	lw.phonies[key]++
	return key, lw.phonies[key] > 1
}

func (lw *localWriteCheck) endPhony(key string) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if lw.phonies[key]--; lw.phonies[key] <= 0 {
		delete(lw.phonies, key)
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
		Dropped:                 lw.dropped.Load(),
		ClearRetries:            lw.clearRetries.Load(),
		ClearFailures:           lw.clearFailures.Load(),
		EvictSkippedUnconfirmed: idx.evictSkippedUnconfirmed(),
		StartPass:               lw.startPass.Load(),
		EvictedUnchecked:        idx.evictedUnchecked(),
		LostBeforeIndex:         lw.lostBeforeIndex.Load(),
		PhonyConfirmed:          lw.phonyConfirmed.Load(),
		Skipped:                 lw.skipped.Load(),
		Restored:                lw.restored.Load(),
	}
}

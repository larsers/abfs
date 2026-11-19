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
	"github.com/google/abfs/shared/remote"
)

const (
	// The keep-alive's batch size, also the per-call key limit of the gRPC client.
	localWriteCheckBatchSize = 50000
	localWriteStartPassCalls = 4
	localWriteRetryWait      = time.Second
	maxLocalWriteRetryWait   = time.Minute
	localWriteRestoredLogged = 10

	// Keys the start pass sends per second at most, the rate of a mount's keep-alive.
	localWriteStartPassRate = 10000
)

var (
	localWriteCheckPeriod    = time.Second
	localWriteStartPassBatch = localWriteStartPassRate
	newStartPassPacer        = func() *keyPacer { return newKeyPacer(localWriteStartPassRate) }
	// How long a push waits for an object in no local store. Content a mount writes reaches
	// the store within seconds.
	unconfirmedPushDropAfter = 30 * time.Minute
	// Longer than the time the server GC lets a mount's keep-alive pause at its default
	// settings, about 82 hours.
	localWriteParkTime = 96 * time.Hour
)

// localWriteCheck keeps content written locally in the cache until the server has confirmed
// that it holds the object. Once on it stays on for the life of the process, since mount
// details record it.
type localWriteCheck struct {
	on atomic.Bool

	mu         sync.Mutex
	ctx        context.Context
	index      localWriteIndex
	worker     bool
	cancelPass context.CancelFunc
	queue      map[entities.Hash]EntryType
	full       chan struct{}

	// phonySync is the dagsync whose phony callbacks report an answer of the server. A dagsync
	// with an offline threshold calls back without a server call.
	phonySync atomic.Pointer[dagsync.DAGSync]
	// phonies counts the phony requests in flight per list of hashes, since dagsync joins a
	// request to a running identical one.
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
	parked          atomic.Int64
	released        atomic.Int64
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

// applyLocalWriteCheck turns the check on once config has gc-keepalive on. Turning it off
// takes effect at the next start.
func (c *CacheMan) applyLocalWriteCheck(config *instance.Config) {
	if config.GCKeepAlive && config.EnableIndex && !config.LocalOnly {
		c.localWrites.on.Store(true)
	}
	if idx, ok := c.checkedIndex(); ok {
		idx.enableLocalWriteCheck()
	}

	c.localWrites.mu.Lock()
	defer c.localWrites.mu.Unlock()
	c.startLocalWriteCheckLocked()
}

// startLocalWriteCheck starts the check worker and the start pass.
func (c *CacheMan) startLocalWriteCheck(ctx context.Context) {
	c.localWrites.mu.Lock()
	defer c.localWrites.mu.Unlock()
	c.localWrites.ctx = ctx
	c.startLocalWriteCheckLocked()
}

func (c *CacheMan) startLocalWriteCheckLocked() {
	lw := &c.localWrites
	idx, ok := c.checkedIndex()
	if lw.ctx == nil || (ok && idx == lw.index) {
		return
	}
	// A config reload closed the index of the running pass.
	if lw.cancelPass != nil {
		lw.cancelPass()
		lw.cancelPass = nil
	}
	lw.index = idx
	if !ok {
		return
	}

	// An entry cleared before a restart is not Remote, so nothing else would check it.
	markers, err := idx.checkMarkers()
	if err != nil {
		c.printer.Printf("Failed to read the check markers: %v\n", err)
	}
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
		var passCtx context.Context
		passCtx, lw.cancelPass = context.WithCancel(lw.ctx)
		c.activeWorkers.Add(1)
		go c.localWriteStartPass(passCtx, idx, slices.Collect(slices.Chunk(lru, localWriteStartPassBatch)))
	}

	if !lw.worker {
		lw.worker = true
		lw.full = make(chan struct{}, 1)
		c.activeWorkers.Add(1)
		go c.localWriteCheckWorker(lw.ctx, lw.full, localWriteCheckPeriod)
	}
}

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

// localWriteHeaders asks the server whether it has keys, retrying until it answers or ctx is done.
func (c *CacheMan) localWriteHeaders(ctx context.Context, keys []entities.Hash) ([]bool, error) {
	wait := localWriteRetryWait
	for {
		hdrs, err := c.remoteClient().Headers(ctx, keys)
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

// pushUnconfirmed pushes the local copy of a hash the server lacks; onPush then marks it remote.
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
	// dagsync calls no onPush for objects over the offline threshold, and a remote read-only
	// cacheman pushes nothing.
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
	// A child that is in no store and that the server lacks would hold the push for good.
	go sync.PutObjectOrDrop(hash, objType, unconfirmedPushDropAfter, func(err error) {
		if errors.Is(err, dagsync.ErrDropped) {
			lw.dropped.Add(1)
			c.printer.Printf("Dropped the push of %s %s, which the server lacks: %v\n", entryType, hash, err)
			c.parkDropped(hash)
		}
	})
}

// parkDropped parks an entry whose push was dropped in place of its check marker, so that a
// start does not push it again.
func (c *CacheMan) parkDropped(hash entities.Hash) {
	idx, ok := c.checkedIndex()
	if !ok {
		return
	}
	parked, retries, err := idx.parkUnconfirmed(hash, c.closeChan)
	c.localWrites.clearRetries.Add(int64(retries))
	if err != nil {
		c.printer.Printf("Failed to park %s after its push was dropped: %v\n", hash, err)
		return
	}
	if parked {
		c.localWrites.parked.Add(1)
	}
}

func (c *CacheMan) hasLocalCopy(ctx context.Context, hash entities.Hash, entryType EntryType) bool {
	has, err := c.storeHas(ctx, hash, entryType)
	return err == nil && has
}

// lacksLocalCopy reports whether the store confirms that it holds no file under hash. Other
// errors count as a copy, so that a failing disk keeps an entry checked.
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

// checksEntry reports whether the check covers a cached entry. Neither the store nor the ABFS
// server holds ARI entries or RBE CAS blobs under their key.
func (c *CacheMan) checksEntry(ctx context.Context, hash entities.Hash, entry *IndexEntry) bool {
	return entry != nil && entry.Local && localWriteChecked(entry.EntryType) && !c.lacksLocalCopy(ctx, hash, entry.EntryType)
}

func (c *CacheMan) isCachedRemote(hash entities.Hash) bool {
	entry := c.index.Get(hash)
	return entry != nil && entry.Local && entry.Remote
}

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

// parkCachedObject parks a cached entry the start pass found missing on the server, out of
// eviction and without a push, until a mount names it or localWriteParkTime ends.
func (c *CacheMan) parkCachedObject(ctx context.Context, idx localWriteIndex, hash entities.Hash) error {
	if !c.checksEntry(ctx, hash, c.index.Get(hash)) {
		c.localWrites.skipped.Add(1)
		return nil
	}
	c.localWrites.missing.Add(1)
	parked, retries, err := idx.park(hash, c.closeChan)
	c.localWrites.clearRetries.Add(int64(retries))
	if err != nil {
		c.localWrites.clearFailures.Add(1)
		return fmt.Errorf("failed to park %s, which the server lacks: %w", hash, err)
	}
	if parked {
		c.localWrites.parked.Add(1)
	}
	return nil
}

func (c *CacheMan) releaseParked() {
	idx, ok := c.index.(localWriteIndex)
	if !ok {
		return
	}
	released, err := idx.releaseParked(time.Now().Add(-localWriteParkTime).Unix())
	c.localWrites.released.Add(int64(released))
	if err != nil {
		c.printer.Printf("Failed to return parked entries to the LRU: %v\n", err)
	}
	if released > 0 {
		c.printer.Printf("Returned %d entries parked for %s to the LRU\n", released, localWriteParkTime)
	}
}

func (c *CacheMan) isParked(hash entities.Hash) bool {
	idx, ok := c.index.(localWriteIndex)
	return ok && idx.isParked(hash)
}

// localWriteStartPass checks every LRU entry with the server, oldest first and paced, and lets
// eviction take the entries checked so far. A start does not use the content, so its calls ask
// the server to record no use.
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
	stopCtx := ctx
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(localWriteStartPassCalls)
	for i, batch := range batches {
		keys := c.startPassKeys(ctx, batch)
		if err := pacer.wait(ctx, len(keys)); err != nil {
			stopped = true
			break
		}
		eg.Go(func() error {
			var found []bool
			if len(keys) > 0 {
				var err error
				if found, err = c.localWriteHeaders(remote.WithNoTouch(ctx), keys); err != nil {
					return err
				}
			}
			c.localWrites.startPass.Add(int64(len(keys)))
			for j, ok := range found {
				if ok {
					c.localWrites.present.Add(1)
				} else if err := c.parkCachedObject(ctx, idx, keys[j]); err != nil {
					// The batch stays unanswered, so eviction stays held below it.
					return err
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
		if err != nil && stopCtx.Err() == nil {
			c.printer.Printf("Stopped the check of the cached objects with the server, eviction stays held: %v\n", err)
		}
		return
	}
	idx.releaseEviction()
	c.printer.Printf("Checked the cached objects with the server in %s\n", time.Since(start).Round(time.Millisecond))
}

func (c *CacheMan) startPassKeys(ctx context.Context, batch []KeyTimestamp) []entities.Hash {
	keys := make([]entities.Hash, 0, len(batch))
	for _, kt := range batch {
		if !c.checksEntry(ctx, kt.Key, c.index.Get(kt.Key)) {
			c.localWrites.skipped.Add(1)
			continue
		}
		keys = append(keys, kt.Key)
	}
	return keys
}

// keyPacer spaces calls so that the call after one of n keys starts at least n/rate seconds
// after it. A caller that was slow gets no credit for the time it lost.
type keyPacer struct {
	rate  int
	next  time.Time
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

func newKeyPacer(rate int) *keyPacer {
	return &keyPacer{rate: rate, now: time.Now, sleep: sleepContext}
}

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
// clearRemote, which keeps the entry out of the LRU until the server confirms it.
func (c *CacheMan) putIndexEntry(ke *IndexKeyEntry, remote bool) (*IndexEntry, error) {
	idx, ok := c.checkedIndex()
	if remote || !ok || !localWriteChecked(ke.EntryType) {
		_, _, prevEnt, _, err := c.index.Put(ke.Key, ke.IndexEntry)
		return prevEnt, err
	}
	prevEnt, cleared, retries, err := idx.clearRemote(ke.Key, ke.IndexEntry, c.closeChan)
	c.localWrites.clearRetries.Add(int64(retries))
	if err != nil {
		// The check takes the old entry out of the LRU if the server lacks it.
		c.localWrites.clearFailures.Add(1)
		c.localWrites.enqueue(ke.Key, ke.EntryType)
		return nil, err
	}
	if cleared {
		c.localWrites.enqueue(ke.Key, ke.EntryType)
	}
	return prevEnt, nil
}

// checkLostLocalWrite checks a local write that eviction removed before cacheman indexed it.
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

// reindexLocalWrite re-indexes an evicted entry whose file is on disk again as a local write.
func (c *CacheMan) reindexLocalWrite(hash entities.Hash, entry *IndexEntry) bool {
	if _, ok := c.checkedIndex(); !ok || entry == nil || entry.Local || !entry.Remote || !localWriteChecked(entry.EntryType) {
		return false
	}
	if _, err := c.addLocalObjectToIndex(hash, entry.EntryType); err != nil && !entities.IsNotExist(err) {
		c.printer.Printf("maintenance: failed to restore local entry for %q: %v\n", hash.String(), err)
	}
	return true
}

func (lw *localWriteCheck) useSync(sync *dagsync.DAGSync, offlineThreshold uint64) {
	if offlineThreshold > 0 {
		sync = nil
	}
	lw.phonySync.Store(sync)
}

// checkUntrackedEntry keeps a Local and Remote entry that maintenance found outside the LRU
// out of eviction until the server confirms it, since the start pass checked only the LRU.
func (c *CacheMan) checkUntrackedEntry(hash entities.Hash, entry *IndexEntry) bool {
	if c.isParked(hash) {
		return true
	}
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

// confirmAfterPhony wraps the callback of a phony request sent to ds: on success it confirms
// the entries that waited for a check when the request was sent and unparks parked ones. A
// request that joins an identical one in flight confirms nothing.
func (c *CacheMan) confirmAfterPhony(ds *dagsync.DAGSync, hashes []entities.Hash, callback dagsync.Callback) dagsync.Callback {
	idx, ok := c.checkedIndex()
	if !ok || c.localWrites.phonySync.Load() != ds {
		return callback
	}
	var marked, parked []entities.Hash
	key, joined := c.localWrites.startPhony(hashes)
	for _, h := range hashes {
		if !joined && idx.hasCheckMarker(h) {
			marked = append(marked, h)
		}
		if idx.isParked(h) {
			parked = append(parked, h)
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
			now := time.Now().Unix()
			for _, h := range parked {
				if _, uerr := idx.unpark(h, now); uerr != nil {
					c.printer.Printf("Failed to return %s to the LRU after a phony request: %v\n", h, uerr)
				}
			}
		}
		callback(err)
	}
}

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

// checkRemoteHeaders updates the index from the answer to a keep-alive Headers call.
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
			// The answer can predate a local write that cleared the entry during the call.
			c.localWrites.enqueue(key, entry.EntryType)
		case found[i] && !entry.Remote:
			if err := c.index.markObjectRemote(key); err != nil {
				c.printer.Printf("Failed to mark %s remote after the server confirmed it: %v\n", key, err)
			}
		case found[i]:
			// A parked entry goes back to the LRU as used now, as the keep-alive touched it.
			if _, err := idx.unpark(key, time.Now().Unix()); err != nil {
				c.printer.Printf("Failed to return %s to the LRU after the server confirmed it: %v\n", key, err)
			}
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
		Parked:                  lw.parked.Load(),
		ParkedSize:              idx.parkedSize(),
		Released:                lw.released.Load(),
		EvictedParked:           idx.evictedParked(),
	}
}

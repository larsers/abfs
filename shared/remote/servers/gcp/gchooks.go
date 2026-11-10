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

package gcp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	lru "github.com/hashicorp/golang-lru/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
	"github.com/google/abfs/shared/remote/servers/gcp/depot"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// The server side of the garbage collector, on with NewServerOpts.GC: reads
// rescue condemned objects, hits touch objects and map rows, cache entries
// expire, and each server writes a heartbeat, without which it does not serve.
const (
	DefaultCacheTTL            = gc.DefaultCacheTTL
	DefaultGCTouchInterval     = 36 * time.Hour
	DefaultGCHeartbeatInterval = 5 * time.Minute
	DefaultGCTouchedMapRows    = 4 * 1000 * 1000
	heartbeatTimeout           = 30 * time.Second
	firstHeartbeatRetry        = 5 * time.Second
	rescueWorkers              = 32
	metricsScope               = "github.com/google/abfs/shared/remote/servers/gcp"
)

// rescuePath is the server call that found an object, condemned or not.
type rescuePath int

const (
	pathHeaders rescuePath = iota
	pathValidation
	pathPutObject
	pathGetObject
	pathMapResults
	numRescuePaths
)

var rescuePathNames = [numRescuePaths]string{"headers", "validation", "put_object", "get_object", "map_results"}

// firstHeartbeatTimeout is how long a server tries to write its first
// heartbeat before it fails to start, and writeServer writes a heartbeat;
// tests change both.
var (
	firstHeartbeatTimeout = time.Minute
	writeServer           = gc.WriteServer
)

// contentEntry is an entry of the s.content LRU cache.
type contentEntry struct {
	content *ObjectContent
	loaded  uint32
}

type mapRowKey struct {
	from, to entities.Hash
}

// toucher is the part of gc.Toucher that the hooks use.
type toucher interface {
	TouchObject(key gc.Hash) bool
	TryTouchObject(key gc.Hash) bool
	TouchMapRow(row string) bool
	Stats() gc.TouchStats
	Late() bool
	Close()
}

type gcHooks struct {
	// on is NewServerOpts.GC. Off, none of the hooks below run.
	on bool
	// Zero disables the TTL and the touches.
	cacheTTL      uint32
	touchInterval uint32
	rescue        func(ctx context.Context, key entities.Hash, condemned []byte) (bool, error)
	toucher       toucher
	// touchedMapRows holds the last touch of map rows, sharded by the first
	// byte of from.
	touchedMapRows []*lru.Cache[mapRowKey, uint32]
	stopHeartbeat  func()
	// server is this process's heartbeat row in GcServers, which exists once
	// hasRow is set. reportedLosses and reportedMapRowLosses count the
	// object and map row touches lost when the last heartbeat was written.
	server               gc.Server
	hasRow               bool
	reportedLosses       int64
	reportedMapRowLosses int64
	// heartbeatStale is the age in seconds of the last successful heartbeat,
	// at lastHeartbeat on the cacheNow clock, beyond which the server stops
	// serving; zero without a heartbeat.
	heartbeatStale uint32
	lastHeartbeat  atomic.Uint32
	// pause and pausePending record a pause of the process that the next
	// heartbeat records as a touch loss.
	pause        pauseDetector
	pausePending bool
	// lastAttempt is the start of the last heartbeat attempt on the cacheNow
	// clock.
	lastAttempt uint32
	metrics     metric.Registration
	rescueLog   *rescueLog
	stopped     bool

	// rescues counts per path and index into rescueTypes.
	rescues           [numRescuePaths][]atomic.Int64
	rescuesLost       [numRescuePaths]atomic.Int64
	rescueFailures    [numRescuePaths]atomic.Int64
	headerExpiries    atomic.Int64
	contentExpiries   atomic.Int64
	mapResultsDropped atomic.Int64
	heartbeatFailures atomic.Int64
}

func seconds(d time.Duration) uint32 {
	if d <= 0 {
		return 0
	}
	return uint32(max(1, (d+time.Second-1)/time.Second))
}

// startGCHooks starts the garbage collector hooks configured in opts. It does
// nothing unless opts.GC is set.
func (s *GCPServer) startGCHooks(ctx context.Context, opts *NewServerOpts) error {
	if !opts.GC {
		return nil
	}
	if opts.CacheTTL < 0 || opts.GCTouchInterval < 0 || opts.GCHeartbeatInterval < 0 {
		return fmt.Errorf("invalid negative cache TTL %s, touch interval %s or heartbeat interval %s", opts.CacheTTL, opts.GCTouchInterval, opts.GCHeartbeatInterval)
	}
	if opts.GCTouchedMapRows < 0 {
		return fmt.Errorf("invalid negative number of touched map rows %d", opts.GCTouchedMapRows)
	}
	// The collector sees the touches a server lost only through its
	// heartbeat, so a server that rescues and touches must write one.
	if !opts.GCReadOnly && (opts.GCHeartbeatInterval == 0 || opts.GCHeartbeatInterval > gc.MaxHeartbeatInterval) {
		return fmt.Errorf("invalid GC heartbeat interval %s: with GC it must be above 0 and at most %s", opts.GCHeartbeatInterval, gc.MaxHeartbeatInterval)
	}
	if err := s.checkGCResources(ctx); err != nil {
		return fmt.Errorf("garbage collector resources missing, run 'abfs server gcp init --gc' to create them: %w", err)
	}
	// Read-only, the server rescues nothing and needs no conditional writes.
	if !opts.GCReadOnly {
		if err := s.checkSingleRowTransactions(ctx, opts); err != nil {
			return err
		}
	}
	return s.enableGCHooks(opts)
}

// enableGCHooks turns the hooks on and starts the toucher and the heartbeat.
// It fails when it cannot write the first heartbeat.
func (s *GCPServer) enableGCHooks(opts *NewServerOpts) error {
	s.casdb.EnableGC()
	s.gcEdges = gc.NewEdgeWriter(s.casdb.BTClient())
	g := &s.gcHooks
	g.on = true
	g.stopped = false
	g.cacheTTL = seconds(opts.CacheTTL)
	for p := range g.rescues {
		g.rescues[p] = make([]atomic.Int64, len(rescueTypes))
	}
	g.rescueLog = newRescueLog()
	if !opts.GCReadOnly {
		g.rescue = gc.NewRescuer(s.casdb.BTClient()).Rescue
	}
	if opts.GCTouchInterval > 0 && !opts.GCReadOnly {
		g.touchInterval = seconds(opts.GCTouchInterval)
		rows := opts.GCTouchedMapRows
		if rows == 0 {
			rows = DefaultGCTouchedMapRows
		}
		g.touchedMapRows = make([]*lru.Cache[mapRowKey, uint32], cacheShards)
		for i := range g.touchedMapRows {
			var err error
			if g.touchedMapRows[i], err = lru.New[mapRowKey, uint32](max(1, (rows+cacheShards-1)/cacheShards)); err != nil {
				return err
			}
		}
		g.toucher = gc.NewToucher(s.casdb.BTClient(), gc.TouchBufferSize)
	}
	reg, err := s.registerGCMetrics()
	if err != nil {
		s.stopGCHooks()
		return fmt.Errorf("failed to register the GC metrics: %w", err)
	}
	g.metrics = reg
	if opts.GCHeartbeatInterval > 0 && !opts.GCReadOnly {
		g.server = gc.Server{
			ID:                gc.NewReplicaID(),
			Version:           gc.ExtractorVersion,
			GC:                true,
			TouchInterval:     time.Duration(g.touchInterval) * time.Second,
			CacheTTL:          time.Duration(g.cacheTTL) * time.Second,
			HeartbeatInterval: time.Duration(seconds(opts.GCHeartbeatInterval)) * time.Second,
			LinkValidation:    s.validateLinks,
		}
		if err := s.startHeartbeat(opts.GCHeartbeatInterval); err != nil {
			s.stopGCHooks()
			return err
		}
	}
	return nil
}

// stopGCHooks stops the heartbeat, writes the buffered touches and records in
// the heartbeat row of this server that it stopped, and whether it lost
// touches or was paused since its last heartbeat. The row stays: a row
// without the stop, also after a failed write of the stop, tells the
// collector that the process crashed and its object and map row touches may
// be lost.
func (s *GCPServer) stopGCHooks() {
	g := &s.gcHooks
	if g.stopped {
		return
	}
	g.stopped = true
	if g.metrics != nil {
		_ = g.metrics.Unregister()
		g.metrics = nil
	}
	if g.stopHeartbeat != nil {
		g.stopHeartbeat()
		g.stopHeartbeat = nil
	}
	if g.toucher != nil {
		before := g.toucher.Stats()
		g.toucher.Close()
		after := g.toucher.Stats()
		// The metrics are gone by now, so the log has the counts of the stop.
		dropped, failed := after.Dropped-before.Dropped, after.Failed-before.Failed
		log.Printf("GC touches at stop: %d written, %d dropped and %d failed while stopping; %d written, %d dropped and %d failed since the start; map rows %d written, %d dropped and %d failed since the start",
			after.Written-before.Written, dropped, failed, after.Written, after.Dropped, after.Failed, after.MapRowsWritten, after.MapRowsDropped, after.MapRowsFailed)
	}
	if g.rescueLog != nil {
		g.rescueLog.flush()
	}
	if !g.hasRow {
		return
	}
	g.hasRow = false
	// A pause after the last heartbeat, or one whose heartbeat failed, is
	// recorded by the stop.
	if g.pausedSince() {
		g.pausePending = true
	}
	lost := g.pausePending || g.losses() > g.reportedLosses
	if lost {
		log.Printf("Warning: recorded lost object touches in the GC heartbeat row %s", g.server.ID)
	}
	mapRowsLost := g.mapRowLosses() > g.reportedMapRowLosses
	if mapRowsLost {
		log.Printf("Warning: recorded lost map row touches in the GC heartbeat row %s", g.server.ID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancel()
	if err := gc.StopServer(ctx, s.casdb.SpannerClient(), g.server.ID, lost, mapRowsLost); err != nil {
		log.Printf("Warning: %v", err)
	}
}

// losses counts the object touches this process lost so far: every one that
// was dropped or failed.
func (g *gcHooks) losses() int64 {
	if g.toucher == nil {
		return 0
	}
	st := g.toucher.Stats()
	return st.Dropped + st.Failed
}

// mapRowLosses counts the map row touches this process lost so far. They are
// recorded apart from the object touches: a lost one only shortens the
// retention of an eval map row, and the collector then requires mounts to
// touch their objects often enough that the touch of the result, an object
// touch of MapResults, keeps the outputs until a mount's keep-alive names
// them.
func (g *gcHooks) mapRowLosses() int64 {
	if g.toucher == nil {
		return 0
	}
	st := g.toucher.Stats()
	return st.MapRowsDropped + st.MapRowsFailed
}

// touchesLate reports whether the toucher holds touches past the time at
// which they count as lost, whose loss it has not counted yet.
func (g *gcHooks) touchesLate() bool {
	return g.toucher != nil && g.toucher.Late()
}

// checkHeartbeat fails with Unavailable once the last successful heartbeat
// of this server is older than two heartbeat intervals plus the write
// timeout, until the next one succeeds. While its heartbeats fail, the
// collector cannot learn of touches the server loses, so it must not serve
// the uses they record.
func (g *gcHooks) checkHeartbeat() error {
	if g.heartbeatStale == 0 {
		return nil
	}
	now, last := cacheNow(), g.lastHeartbeat.Load()
	if now > last && now-last > g.heartbeatStale {
		return status.Errorf(codes.Unavailable, "GC heartbeat stale: none written for %d s", now-last)
	}
	return nil
}

// CheckGCHeartbeat fails while the server does not serve because its GC
// heartbeat is stale, for a health probe. It does no I/O.
func (s *GCPServer) CheckGCHeartbeat(context.Context) error {
	return s.gcHooks.checkHeartbeat()
}

func (g *gcHooks) expired(loaded uint32) bool {
	return g.cacheTTL > 0 && loaded != 0 && cacheNow()-loaded >= g.cacheTTL
}

// lookupHeader returns the cached header of key unless it has expired.
func (s *GCPServer) lookupHeader(key entities.Hash) (CachedHeader, bool) {
	h, ok := s.headers[key[0]].Get(key)
	if ok && s.gcHooks.expired(h.Loaded) {
		s.gcHooks.headerExpiries.Add(1)
		return CachedHeader{}, false
	}
	return h, ok
}

// storeHeader caches hdr, read from Bigtable by a read that started at
// loaded on the cacheNow clock, so that the entry ages from the start of the
// read. With GC it replaces an expired entry and keeps the touch mark of the
// entry it replaces when that touch is of the current period; without GC it
// keeps an existing entry.
func (s *GCPServer) storeHeader(hdr *ObjectHeader, loaded uint32) CachedHeader {
	h := newCachedHeader(hdr)
	h.Loaded = loaded
	shard := s.headers[hdr.Id[0]]
	if !s.gcHooks.on {
		shard.PeekOrAdd(hdr.Id, h)
		return h
	}
	if old, ok := shard.Peek(hdr.Id); ok && s.gcHooks.touchInterval > 0 && s.gcHooks.touchedThisPeriod(hdr.Id, old, h.Loaded) {
		h.Touched = old.Touched
	}
	shard.Add(hdr.Id, h)
	return h
}

// lookupContent returns the cached content of key unless it has expired.
func (s *GCPServer) lookupContent(key entities.Hash) (*ObjectContent, bool) {
	c, ok := s.content[key[0]].Get(key)
	if ok && s.gcHooks.expired(c.loaded) {
		s.gcHooks.contentExpiries.Add(1)
		return nil, false
	}
	return c.content, ok
}

// storeContent caches content; see storeHeader.
func (s *GCPServer) storeContent(key entities.Hash, content *ObjectContent, loaded uint32) {
	entry := contentEntry{content: content, loaded: loaded}
	if !s.gcHooks.on {
		s.content[key[0]].PeekOrAdd(key, entry)
		return
	}
	s.content[key[0]].Add(key, entry)
}

// forget removes key from the caches.
func (s *GCPServer) forget(key entities.Hash) {
	s.headers[key[0]].Remove(key)
	s.content[key[0]].Remove(key)
}

// forgetMissing removes key, which a read found missing, from the caches, so
// that an expired entry is not kept. Without GC entries do not expire.
func (s *GCPServer) forgetMissing(key entities.Hash) {
	if s.gcHooks.on {
		s.forget(key)
	}
}

// getObjectDataGC is getObjectData with GC on. A read touches the object, from
// the cache or not, as any other call that finds it.
func (s *GCPServer) getObjectDataGC(ctx context.Context, key entities.Hash) (CachedHeader, *ObjectContent, error) {
	cachedHdr, hdrOk := s.lookupHeader(key)
	content, contentOk := s.lookupContent(key)
	if hdrOk && contentOk {
		s.touchHit(pathGetObject, key, cachedHdr)
		return cachedHdr, content, nil
	}
	// A read of the whole row sees the condemn marker with the content.
	start := cacheNow()
	header, content, err := s.casdb.GetObjectData(ctx, key)
	if err == nil && header != nil {
		if err = s.gcHooks.checkReadAge(start); err == nil {
			err = s.rescueCondemned(ctx, pathGetObject, header)
		}
	}
	if errors.Is(err, casdb.ErrNotFound) || (err == nil && header == nil) {
		s.forget(key)
		return CachedHeader{}, nil, entities.ErrNotExist
	}
	if err != nil {
		return CachedHeader{}, nil, err
	}
	cachedHdr = s.storeHeader(header, start)
	s.storeContent(key, content, start)
	s.touchHit(pathGetObject, key, cachedHdr)
	return cachedHdr, content, nil
}

// rescueCondemned rescues hdr if the collector condemned it. It returns
// casdb.ErrNotFound when the collector deleted the object first.
func (s *GCPServer) rescueCondemned(ctx context.Context, path rescuePath, hdr *ObjectHeader) error {
	if hdr.Condemned == nil || s.gcHooks.rescue == nil {
		return nil
	}
	req := newRequestID(ctx)
	err := s.rescueOne(ctx, path, req, hdr)
	if err == nil {
		s.gcHooks.rescueLog.call(ctx, path, req, 1, 1, 0)
	} else if !errors.Is(err, casdb.ErrNotFound) {
		s.gcHooks.rescueLog.call(ctx, path, req, 1, 0, 1)
	}
	return err
}

// rescueOne rescues hdr, which the collector condemned, and counts the result.
// The log lines name the request req of the call.
func (s *GCPServer) rescueOne(ctx context.Context, path rescuePath, req string, hdr *ObjectHeader) error {
	g := &s.gcHooks
	condemned := hdr.Condemned
	live, err := g.rescue(ctx, hdr.Id, condemned)
	if err != nil {
		g.rescueFailures[path].Add(1)
		return err
	}
	if !live {
		g.rescuesLost[path].Add(1)
		g.rescueLog.lost(ctx, path, req, hdr)
		return casdb.ErrNotFound
	}
	g.rescues[path][rescueType(hdr.Type)].Add(1)
	g.rescueLog.object(path, req, hdr, condemned)
	hdr.Condemned = nil
	return nil
}

// rescueHeaders rescues the condemned objects of hdrs and returns hdrs without
// the ones the collector deleted first. A batch can hold many condemned
// objects, for example a keep-alive touch right after a cycle condemned what
// the mount holds, so the rescues run in parallel. A failed rescue does not
// stop the others: the call fails with the first error once all have run, so
// a retry finds only the objects whose rescue failed still condemned.
func (s *GCPServer) rescueHeaders(ctx context.Context, path rescuePath, hdrs []*ObjectHeader) ([]*ObjectHeader, error) {
	if s.gcHooks.rescue == nil {
		return hdrs, nil
	}
	gone := make([]bool, len(hdrs))
	var rescued, failed atomic.Int64
	var g errgroup.Group
	g.SetLimit(rescueWorkers)
	var req string
	for i, h := range hdrs {
		if h.Condemned == nil {
			continue
		}
		if req == "" {
			req = newRequestID(ctx)
		}
		g.Go(func() error {
			err := s.rescueOne(ctx, path, req, h)
			switch {
			case err == nil:
				rescued.Add(1)
			case errors.Is(err, casdb.ErrNotFound):
				gone[i] = true
				return nil
			default:
				failed.Add(1)
			}
			return err
		})
	}
	err := g.Wait()
	s.gcHooks.rescueLog.call(ctx, path, req, len(hdrs), int(rescued.Load()), int(failed.Load()))
	if err != nil {
		return nil, err
	}
	live := hdrs[:0]
	for i, h := range hdrs {
		if !gone[i] {
			live = append(live, h)
		}
	}
	return live, nil
}

// touchMark returns the CachedHeader.Touched value of a touch period. The
// value repeats every 255 periods, never zero.
func touchMark(period uint64) uint8 {
	return uint8(period%255) + 1
}

// touchPeriod returns the touch period of the object key at now. Periods are
// --gc-touch-interval long and counted from the process start, shifted by an
// offset taken from the key, so that the periods of different objects end at
// different times and their next touches do not all queue at once.
func (g *gcHooks) touchPeriod(key entities.Hash, now uint32) uint64 {
	interval := uint64(g.touchInterval)
	offset := uint64(binary.BigEndian.Uint32(key[len(key)-4:])) % interval
	return (uint64(now) + offset) / interval
}

// touchedThisPeriod reports whether this server touched the object key, whose
// entry is h, in the current touch period. A touch is never older than the
// load of its entry, so for entries loaded less than 255 periods ago the mark
// cannot have wrapped.
func (g *gcHooks) touchedThisPeriod(key entities.Hash, h CachedHeader, now uint32) bool {
	period := g.touchPeriod(key, now)
	return h.Touched != 0 && period-g.touchPeriod(key, h.Loaded) < 255 && h.Touched == touchMark(period)
}

// touchHit touches the object key, which the call path found, unless this
// server touched it in the current touch period. So an object that is used
// keeps a touch younger than the touch interval, and each server writes at
// most one touch per object and period.
func (s *GCPServer) touchHit(path rescuePath, key entities.Hash, h CachedHeader) {
	g := &s.gcHooks
	if g.toucher == nil {
		return
	}
	now := cacheNow()
	if g.touchedThisPeriod(key, h, now) {
		return
	}
	if !g.touch(path, key) {
		return
	}
	// A concurrent forget can be undone here; the entry keeps its load time,
	// so the TTL still bounds it.
	shard := s.headers[key[0]]
	if cur, ok := shard.Peek(key); ok && cur.Loaded == h.Loaded {
		cur.Touched = touchMark(g.touchPeriod(key, now))
		if g.cacheTTL == 0 {
			// Without a TTL the load time means nothing else; moving it keeps
			// entries older than 255 periods from being touched on every hit.
			cur.Loaded = now
		}
		shard.Add(key, cur)
	}
}

// touch queues a touch of key. Headers and MapResults, the calls of the client
// keep-alive and of builds, wait for room in a full buffer, so that a burst of
// them slows down instead of losing touches. The other calls do not wait, so
// that reads and uploads do not slow down when the gc table does.
func (g *gcHooks) touch(path rescuePath, key entities.Hash) bool {
	if path == pathHeaders || path == pathMapResults {
		return g.toucher.TouchObject(key)
	}
	return g.toucher.TryTouchObject(key)
}

// touchMapRow rewrites the cell of the map row from#to, so that its timestamp
// is the last use, unless this replica did so within the touch interval. A
// touch the toucher dropped is tried again on the next use.
func (s *GCPServer) touchMapRow(from, to entities.Hash) {
	g := &s.gcHooks
	if g.toucher == nil {
		return
	}
	key := mapRowKey{from: from, to: to}
	shard := g.touchedMapRows[from[0]]
	now := cacheNow()
	if last, ok := shard.Get(key); ok && now-last < g.touchInterval {
		return
	}
	if g.toucher.TouchMapRow(casdb.MapRowKey(from, to)) {
		shard.Add(key, now)
	}
}

// liveMapResults returns the results of from whose objects exist, and touches
// their map rows. The collector removes results nobody used for a while, and a
// client cannot use a result whose object is gone.
func (s *GCPServer) liveMapResults(ctx context.Context, from entities.Hash, results []entities.Hash) ([]entities.Hash, error) {
	if len(results) == 0 {
		return results, nil
	}
	hdrs, err := s.getHeaders(ctx, pathMapResults, results)
	if err != nil {
		return nil, err
	}
	live := make([]entities.Hash, 0, len(results))
	for i, h := range hdrs {
		if !h.Valid {
			s.gcHooks.mapResultsDropped.Add(1)
			continue
		}
		live = append(live, results[i])
		s.touchMapRow(from, results[i])
	}
	return live, nil
}

// missingChunkReader reads the content of the object key and handles a chunk
// that is missing when it is read.
type missingChunkReader struct {
	io.ReadCloser
	ctx     context.Context
	s       *GCPServer
	key     entities.Hash
	content string
}

// Read implements io.Reader.
func (r *missingChunkReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil && (errors.Is(err, depot.ErrNotFound) || errors.Is(err, storage.ErrObjectNotExist)) {
		err = r.s.chunkMissing(r.ctx, r.key, r.content, err)
	}
	return n, err
}

// chunkMissing handles a chunk of the object key that was missing while its
// content, described by content, was read. When the collector deleted the
// object during the read, the object no longer exists. When the row still
// lists the chunk, the chunk is lost, which is not a missing object.
func (s *GCPServer) chunkMissing(ctx context.Context, key entities.Hash, content string, chunkErr error) error {
	s.forget(key)
	_, current, err := s.casdb.GetObjectData(ctx, key)
	switch {
	case errors.Is(err, casdb.ErrNotFound):
		return fmt.Errorf("object %s was removed while it was read (%v): %w", key, chunkErr, entities.ErrNotExist) //nolint:errorlint // only the missing object is the cause
	case err != nil:
		return fmt.Errorf("failed to read %s: %v, and then failed to read its object row: %v", key, chunkErr, err) //nolint:errorlint // neither error means the object is missing
	case current.Content != content:
		return fmt.Errorf("object %s was stored again while it was read, read it again: %v", key, chunkErr) //nolint:errorlint // the object exists
	default:
		log.Printf("Warning: object %s lists a chunk that is missing: %v", key, chunkErr)
		return fmt.Errorf("failed to read %s: %v", key, chunkErr) //nolint:errorlint // the object exists, its content is lost
	}
}

// startHeartbeat writes the first heartbeat of this server, retrying for up
// to firstHeartbeatTimeout, so that the server has a row before it serves,
// and then one every interval until stopGCHooks.
func (s *GCPServer) startHeartbeat(interval time.Duration) error {
	g := &s.gcHooks
	log.Printf("GC server ID %s, heartbeat every %s", g.server.ID, interval)
	g.lastAttempt = 0
	first, cancelFirst := context.WithTimeout(context.Background(), firstHeartbeatTimeout)
	defer cancelFirst()
	for {
		err := s.heartbeat(first)
		if err == nil {
			break
		}
		select {
		case <-first.Done():
			return fmt.Errorf("failed to write the first GC heartbeat within %s: %w", firstHeartbeatTimeout, err)
		case <-time.After(firstHeartbeatRetry):
		}
	}
	g.heartbeatStale = seconds(2*interval + heartbeatTimeout)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			_ = s.heartbeat(ctx)
		}
	}()
	g.stopHeartbeat = func() {
		cancel()
		<-done
	}
	return nil
}

// heartbeat writes one heartbeat of this server. The touch stats are read
// after the Spanner read that gives StatsTime, so every loss counted before
// that time is in the row.
func (s *GCPServer) heartbeat(ctx context.Context) error {
	g := &s.gcHooks
	start := cacheNow()
	if g.heartbeatLate(start) {
		log.Printf("Warning: no GC heartbeat attempt for over the heartbeat interval plus %s; recording the gap as a touch loss", gc.TouchDrainBound)
		g.pausePending = true
	}
	hctx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()
	sp := s.casdb.SpannerClient()
	statsTime, err := gc.ReadServerTime(hctx, sp, g.server.ID)
	if err == nil {
		if g.pausedSince() {
			log.Printf("Warning: the wall clock advanced more than the monotonic clock by over %s since the last GC heartbeat; recording the pause as a touch loss", maxPause)
			g.pausePending = true
		}
		// A late touch leaves the toucher's slots only after its loss is
		// counted, so the slots are read first.
		late := g.touchesLate()
		losses, mapRowLosses := g.losses(), g.mapRowLosses()
		if err = writeServer(hctx, sp, &g.server, statsTime, g.pausePending || late || losses > g.reportedLosses, mapRowLosses > g.reportedMapRowLosses); err == nil {
			g.reportedLosses, g.reportedMapRowLosses = losses, mapRowLosses
			g.pausePending = false
			g.hasRow = true
			g.lastHeartbeat.Store(start)
			return nil
		}
	}
	if ctx.Err() == nil {
		g.heartbeatFailures.Add(1)
		log.Printf("Warning: %v", err)
	}
	return err
}

func (s *GCPServer) registerGCMetrics() (metric.Registration, error) {
	meter := otel.Meter(metricsScope)
	rescues, err := meter.Int64ObservableCounter("abfs.gcp.gc.rescues",
		metric.WithDescription("Condemned objects that a server call rescued, by call and object type"))
	if err != nil {
		return nil, err
	}
	lost, err := meter.Int64ObservableCounter("abfs.gcp.gc.rescues_lost",
		metric.WithDescription("Condemned objects that the collector deleted before a server call could rescue them, by call"))
	if err != nil {
		return nil, err
	}
	failures, err := meter.Int64ObservableCounter("abfs.gcp.gc.rescue_failures",
		metric.WithDescription("Rescues of condemned objects that failed, so that the server call failed, by call"))
	if err != nil {
		return nil, err
	}
	touches, err := meter.Int64ObservableCounter("abfs.gcp.gc.touches",
		metric.WithDescription("Touches of objects and map rows, by kind and result: written, dropped because the buffer was full (about half full for map rows), or failed"))
	if err != nil {
		return nil, err
	}
	expiries, err := meter.Int64ObservableCounter("abfs.gcp.cache.ttl_expiries",
		metric.WithDescription("Cache lookups that found an entry older than the cache TTL, by cache"))
	if err != nil {
		return nil, err
	}
	dropped, err := meter.Int64ObservableCounter("abfs.gcp.gc.map_results_dropped",
		metric.WithDescription("Map results not returned because their object is missing"))
	if err != nil {
		return nil, err
	}
	heartbeats, err := meter.Int64ObservableCounter("abfs.gcp.gc.heartbeat_failures",
		metric.WithDescription("Failed writes of the server heartbeat"))
	if err != nil {
		return nil, err
	}
	g := &s.gcHooks
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for p := range numRescuePaths {
			callName := attribute.String("call", rescuePathNames[p])
			for t, typ := range rescueTypes {
				o.ObserveInt64(rescues, g.rescues[p][t].Load(), metric.WithAttributes(callName, attribute.String("type", typ)))
			}
			call := metric.WithAttributes(callName)
			o.ObserveInt64(lost, g.rescuesLost[p].Load(), call)
			o.ObserveInt64(failures, g.rescueFailures[p].Load(), call)
		}
		if g.toucher != nil {
			st := g.toucher.Stats()
			for _, c := range []struct {
				kind, result string
				n            int64
			}{
				{"object", "written", st.Written}, {"object", "dropped", st.Dropped}, {"object", "failed", st.Failed},
				{"map_row", "written", st.MapRowsWritten}, {"map_row", "dropped", st.MapRowsDropped}, {"map_row", "failed", st.MapRowsFailed},
			} {
				o.ObserveInt64(touches, c.n, metric.WithAttributes(attribute.String("kind", c.kind), attribute.String("result", c.result)))
			}
		}
		o.ObserveInt64(expiries, g.headerExpiries.Load(), metric.WithAttributes(attribute.String("cache", "headers")))
		o.ObserveInt64(expiries, g.contentExpiries.Load(), metric.WithAttributes(attribute.String("cache", "content")))
		o.ObserveInt64(dropped, g.mapResultsDropped.Load())
		o.ObserveInt64(heartbeats, g.heartbeatFailures.Load())
		return nil
	}, rescues, lost, failures, touches, expiries, dropped, heartbeats)
}

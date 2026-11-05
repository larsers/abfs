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

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
	"github.com/google/abfs/shared/remote/servers/gcp/depot"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// The server side of the garbage collector, on with NewServerOpts.GC: reads
// rescue condemned objects, hits touch objects and map rows, cache entries
// expire, and each replica writes a heartbeat.
const (
	DefaultCacheTTL            = 6 * time.Hour
	DefaultGCTouchInterval     = 36 * time.Hour
	DefaultGCHeartbeatInterval = 5 * time.Minute
	touchBufferSize            = 100 * 1000
	maxTouchedMapRows          = 256 * 1024
	heartbeatTimeout           = 30 * time.Second
	metricsScope               = "github.com/google/abfs/shared/remote/servers/gcp"
)

// rescuePath is the server call that found an object condemned.
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

// cacheEpoch and cacheNow give cache entries their load and touch times in
// seconds since the process started, plus one so that zero means unset.
var (
	cacheEpoch = time.Now()
	cacheNow   = func() uint32 { return uint32(time.Since(cacheEpoch)/time.Second) + 1 }
)

// contentEntry is an entry of the s.content LRU cache.
type contentEntry struct {
	content *ObjectContent
	loaded  uint32
}

type mapRowKey struct {
	from, to entities.Hash
}

type gcHooks struct {
	// on is NewServerOpts.GC. Off, none of the hooks below run.
	on bool
	// Zero disables the TTL and the touches.
	cacheTTL      uint32
	touchInterval uint32
	rescue        func(ctx context.Context, key entities.Hash, condemned []byte) (bool, error)
	toucher       *gc.Toucher
	// touchedMapRows holds the last touch of map rows, sharded by the first
	// byte of from.
	touchedMapRows []*lru.Cache[mapRowKey, uint32]
	stopHeartbeat  func()
	metrics        metric.Registration

	rescues           [numRescuePaths]atomic.Int64
	rescuesLost       [numRescuePaths]atomic.Int64
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
	if err := s.checkGCResources(ctx); err != nil {
		return fmt.Errorf("garbage collector resources missing, run 'abfs server gcp init --gc' to create them: %w", err)
	}
	return s.enableGCHooks(opts)
}

// enableGCHooks turns the hooks on and starts the toucher and the heartbeat.
func (s *GCPServer) enableGCHooks(opts *NewServerOpts) error {
	s.casdb.EnableGC()
	s.gcEdges = gc.NewEdgeWriter(s.casdb.BTClient())
	g := &s.gcHooks
	g.on = true
	g.cacheTTL = seconds(opts.CacheTTL)
	g.rescue = gc.NewRescuer(s.casdb.BTClient()).Rescue
	if opts.GCTouchInterval > 0 {
		g.touchInterval = seconds(opts.GCTouchInterval)
		g.touchedMapRows = make([]*lru.Cache[mapRowKey, uint32], cacheShards)
		for i := range g.touchedMapRows {
			var err error
			if g.touchedMapRows[i], err = lru.New[mapRowKey, uint32](maxTouchedMapRows / cacheShards); err != nil {
				return err
			}
		}
		g.toucher = gc.NewToucher(s.casdb.BTClient(), touchBufferSize)
	}
	reg, err := s.registerGCMetrics()
	if err != nil {
		s.stopGCHooks()
		return fmt.Errorf("failed to register the GC metrics: %w", err)
	}
	g.metrics = reg
	if opts.GCHeartbeatInterval > 0 {
		s.startHeartbeat(opts.GCHeartbeatInterval, gc.ExtractorVersion)
	}
	return nil
}

// stopGCHooks stops the heartbeat and writes the buffered touches.
func (s *GCPServer) stopGCHooks() {
	g := &s.gcHooks
	if g.metrics != nil {
		_ = g.metrics.Unregister()
		g.metrics = nil
	}
	if g.stopHeartbeat != nil {
		g.stopHeartbeat()
		g.stopHeartbeat = nil
	}
	if g.toucher != nil {
		g.toucher.Close()
	}
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

// storeHeader caches hdr, read from Bigtable. With GC it replaces an expired
// entry and keeps the touch mark of the entry it replaces when that touch is
// of the current period; without GC it keeps an existing entry.
func (s *GCPServer) storeHeader(hdr *ObjectHeader) CachedHeader {
	h := newCachedHeader(hdr)
	shard := s.headers[hdr.Id[0]]
	if !s.gcHooks.on {
		shard.PeekOrAdd(hdr.Id, h)
		return h
	}
	if old, ok := shard.Peek(hdr.Id); ok && s.gcHooks.touchInterval > 0 && s.gcHooks.touchedThisPeriod(old, h.Loaded) {
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
func (s *GCPServer) storeContent(key entities.Hash, content *ObjectContent) {
	entry := contentEntry{content: content, loaded: cacheNow()}
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
		s.touchHit(key, cachedHdr)
		return cachedHdr, content, nil
	}
	// A read of the whole row sees the condemn marker with the content.
	header, content, err := s.casdb.GetObjectData(ctx, key)
	if err == nil && header != nil {
		err = s.rescueCondemned(ctx, pathGetObject, header)
	}
	if errors.Is(err, casdb.ErrNotFound) || (err == nil && header == nil) {
		s.forget(key)
		return CachedHeader{}, nil, entities.ErrNotExist
	}
	if err != nil {
		return CachedHeader{}, nil, err
	}
	cachedHdr = s.storeHeader(header)
	s.storeContent(key, content)
	s.touchHit(key, cachedHdr)
	return cachedHdr, content, nil
}

// rescueCondemned rescues hdr if the collector condemned it. It returns
// casdb.ErrNotFound when the collector deleted the object first.
func (s *GCPServer) rescueCondemned(ctx context.Context, path rescuePath, hdr *ObjectHeader) error {
	g := &s.gcHooks
	if hdr.Condemned == nil || g.rescue == nil {
		return nil
	}
	live, err := g.rescue(ctx, hdr.Id, hdr.Condemned)
	if err != nil {
		return err
	}
	if !live {
		g.rescuesLost[path].Add(1)
		log.Printf("Condemned object %s was deleted before it could be rescued", hdr.Id)
		return casdb.ErrNotFound
	}
	g.rescues[path].Add(1)
	hdr.Condemned = nil
	return nil
}

// rescueHeaders rescues the condemned objects of hdrs and returns hdrs without
// the ones the collector deleted first.
func (s *GCPServer) rescueHeaders(ctx context.Context, path rescuePath, hdrs []*ObjectHeader) ([]*ObjectHeader, error) {
	live := hdrs[:0]
	for _, h := range hdrs {
		err := s.rescueCondemned(ctx, path, h)
		if errors.Is(err, casdb.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		live = append(live, h)
	}
	return live, nil
}

// touchMark returns the CachedHeader.Touched value of a touch period. Periods
// are --gc-touch-interval long and counted from the process start; the value
// repeats every 255 periods, never zero.
func touchMark(period uint32) uint8 {
	return uint8(period%255) + 1
}

// touchedThisPeriod reports whether this server touched the object of h in the
// current touch period. A touch is never older than the load of its entry, so
// for entries loaded less than 255 periods ago the mark cannot have wrapped.
func (g *gcHooks) touchedThisPeriod(h CachedHeader, now uint32) bool {
	period := now / g.touchInterval
	return h.Touched != 0 && period-h.Loaded/g.touchInterval < 255 && h.Touched == touchMark(period)
}

// touchHit touches the object key, which a call found, unless this server
// touched it in the current touch period. So an object that is used keeps a
// touch younger than the touch interval, and each server writes at most one
// touch per object and period.
func (s *GCPServer) touchHit(key entities.Hash, h CachedHeader) {
	g := &s.gcHooks
	if g.toucher == nil {
		return
	}
	now := cacheNow()
	if g.touchedThisPeriod(h, now) {
		return
	}
	if !g.toucher.TouchObject(key) {
		return
	}
	// A concurrent forget can be undone here; the entry keeps its load time,
	// so the TTL still bounds it.
	shard := s.headers[key[0]]
	if cur, ok := shard.Peek(key); ok && cur.Loaded == h.Loaded {
		cur.Touched = touchMark(now / g.touchInterval)
		if g.cacheTTL == 0 {
			// Without a TTL the load time means nothing else; moving it keeps
			// entries older than 255 periods from being touched on every hit.
			cur.Loaded = now
		}
		shard.Add(key, cur)
	}
}

// touchMapRow rewrites the cell of the map row from#to, so that its timestamp
// is the last use, unless this replica did so within the touch interval.
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

// startHeartbeat writes the heartbeat of this replica now and then every
// interval until stopGCHooks.
func (s *GCPServer) startHeartbeat(interval time.Duration, version int64) {
	id := gc.NewReplicaID()
	log.Printf("GC replica ID %s, heartbeat every %s", id, interval)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			s.heartbeat(ctx, id, version)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	s.gcHooks.stopHeartbeat = func() {
		cancel()
		<-done
	}
}

func (s *GCPServer) heartbeat(ctx context.Context, id string, version int64) {
	hctx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()
	if err := gc.WriteHeartbeat(hctx, s.casdb.SpannerClient(), id, version); err != nil && ctx.Err() == nil {
		s.gcHooks.heartbeatFailures.Add(1)
		log.Printf("Warning: %v", err)
	}
}

func (s *GCPServer) registerGCMetrics() (metric.Registration, error) {
	meter := otel.Meter(metricsScope)
	rescues, err := meter.Int64ObservableCounter("abfs.gcp.gc.rescues",
		metric.WithDescription("Condemned objects that a server call rescued, by call"))
	if err != nil {
		return nil, err
	}
	lost, err := meter.Int64ObservableCounter("abfs.gcp.gc.rescues_lost",
		metric.WithDescription("Condemned objects that the collector deleted before a server call could rescue them, by call"))
	if err != nil {
		return nil, err
	}
	touches, err := meter.Int64ObservableCounter("abfs.gcp.gc.touches",
		metric.WithDescription("Touches of objects and map rows, by result: written, dropped because the buffer was full, or failed"))
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
		metric.WithDescription("Failed writes of the replica heartbeat"))
	if err != nil {
		return nil, err
	}
	g := &s.gcHooks
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for p := range numRescuePaths {
			call := metric.WithAttributes(attribute.String("call", rescuePathNames[p]))
			o.ObserveInt64(rescues, g.rescues[p].Load(), call)
			o.ObserveInt64(lost, g.rescuesLost[p].Load(), call)
		}
		if g.toucher != nil {
			st := g.toucher.Stats()
			o.ObserveInt64(touches, st.Written, metric.WithAttributes(attribute.String("result", "written")))
			o.ObserveInt64(touches, st.Dropped, metric.WithAttributes(attribute.String("result", "dropped")))
			o.ObserveInt64(touches, st.Failed, metric.WithAttributes(attribute.String("result", "failed")))
		}
		o.ObserveInt64(expiries, g.headerExpiries.Load(), metric.WithAttributes(attribute.String("cache", "headers")))
		o.ObserveInt64(expiries, g.contentExpiries.Load(), metric.WithAttributes(attribute.String("cache", "content")))
		o.ObserveInt64(dropped, g.mapResultsDropped.Load())
		o.ObserveInt64(heartbeats, g.heartbeatFailures.Load())
		return nil
	}, rescues, lost, touches, expiries, dropped, heartbeats)
}

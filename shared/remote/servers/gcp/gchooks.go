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

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
	"github.com/google/abfs/shared/remote/servers/gcp/depot"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

const (
	DefaultCacheTTL            = 6 * time.Hour
	DefaultGCTouchInterval     = 36 * time.Hour
	DefaultGCHeartbeatInterval = 5 * time.Minute
	heartbeatTimeout           = gc.HeartbeatTimeout
	firstHeartbeatRetry        = 5 * time.Second
	rescueWorkers              = 32
	metricsScope               = "github.com/google/abfs/shared/remote/servers/gcp"
)

// maxTouchedMapRows costs about 225 bytes per row.
const maxTouchedMapRows = 4 * 1000 * 1000

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

// Tests change these.
var (
	firstHeartbeatTimeout = time.Minute
	writeServer           = gc.WriteServer
)

type contentEntry struct {
	content *ObjectContent
	loaded  uint32
}

type mapRowKey struct {
	from, to entities.Hash
}

type toucher interface {
	TouchObject(key gc.Hash) bool
	TryTouchObject(key gc.Hash) bool
	TouchMapRow(row string) bool
	Stats() gc.TouchStats
	Late() bool
	Close()
}

type gcHooks struct {
	on bool
	// Zero disables the TTL and the touches.
	cacheTTL      uint32
	touchInterval uint32
	rescue        func(ctx context.Context, key entities.Hash, condemned []byte) (bool, error)
	toucher       toucher
	// touchedMapRows is sharded by the first byte of from.
	touchedMapRows []*lru.Cache[mapRowKey, uint32]
	stopHeartbeat  func()
	// server is this process's heartbeat row, which exists once hasRow is set.
	server               gc.Server
	hasRow               bool
	reportedLosses       int64
	reportedMapRowLosses int64
	// heartbeatStale is the heartbeat age in seconds at which serving stops.
	heartbeatStale uint32
	lastHeartbeat  atomic.Uint32
	// pausePending is a pause the next heartbeat records as a touch loss.
	pause        pauseDetector
	pausePending bool
	// lastAttempt is on the cacheNow clock.
	lastAttempt uint32
	metrics     metric.Registration
	rescueLog   *rescueLog
	stopped     bool
	// answerBound is off on a read-only server, which answers no client.
	answerBound   bool
	answerSeconds metric.Float64Histogram

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

func (s *GCPServer) startGCHooks(ctx context.Context, opts *NewServerOpts) error {
	if !opts.GC {
		return nil
	}
	o, err := gcHookOpts(opts)
	if err != nil {
		return err
	}
	if err := s.checkGCResources(ctx, o.GCReadOnly); err != nil {
		return fmt.Errorf("garbage collector resources missing, run 'abfs server gcp init --gc' to create them: %w", err)
	}
	// Read-only, the server rescues nothing and needs no conditional writes.
	if !o.GCReadOnly {
		if err := s.checkSingleRowTransactions(ctx, &o); err != nil {
			return err
		}
	}
	return s.enableGCHooks(ctx, &o)
}

// gcHookOpts requires a cache TTL, touch interval and heartbeat: without them
// the collector refuses every condemn and delete.
func gcHookOpts(opts *NewServerOpts) (NewServerOpts, error) {
	o := *opts
	if o.CacheTTL < 0 || o.GCTouchInterval < 0 || o.GCHeartbeatInterval < 0 {
		return o, fmt.Errorf("invalid negative cache TTL %s, touch interval %s or heartbeat interval %s", o.CacheTTL, o.GCTouchInterval, o.GCHeartbeatInterval)
	}
	if o.GCReadOnly {
		return o, nil
	}
	if o.GCHeartbeatInterval == 0 {
		o.GCHeartbeatInterval = DefaultGCHeartbeatInterval
	}
	switch {
	case o.CacheTTL == 0:
		return o, errors.New("invalid cache TTL 0: with GC it must be above 0")
	case o.GCTouchInterval == 0:
		return o, errors.New("invalid GC touch interval 0: with GC it must be above 0")
	case o.GCHeartbeatInterval > gc.MaxHeartbeatInterval:
		return o, fmt.Errorf("invalid GC heartbeat interval %s: with GC it must be at most %s", o.GCHeartbeatInterval, gc.MaxHeartbeatInterval)
	}
	return o, nil
}

func (s *GCPServer) enableGCHooks(ctx context.Context, opts *NewServerOpts) error {
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
		g.answerBound = true
	}
	if opts.GCTouchInterval > 0 && !opts.GCReadOnly {
		g.touchInterval = seconds(opts.GCTouchInterval)
		g.touchedMapRows = make([]*lru.Cache[mapRowKey, uint32], cacheShards)
		for i := range g.touchedMapRows {
			var err error
			if g.touchedMapRows[i], err = lru.New[mapRowKey, uint32](maxTouchedMapRows / cacheShards); err != nil {
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
	if g.answerSeconds, err = otel.Meter(metricsScope).Float64Histogram("abfs.gcp.gc.answer_seconds",
		metric.WithDescription("Time from the start of a call that can answer that objects are present to its answer, by call; a call that takes 2 minutes or more fails"),
		metric.WithUnit("s")); err != nil {
		s.stopGCHooks()
		return fmt.Errorf("failed to register the GC metrics: %w", err)
	}
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
		if err := s.startHeartbeat(ctx, opts.GCHeartbeatInterval); err != nil {
			s.stopGCHooks()
			return err
		}
	}
	return nil
}

// stopGCHooks marks the heartbeat row stopped but keeps it, so the collector
// sees the process in every cycle window it may have served in.
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
	// The stop records a pause that no heartbeat recorded.
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

func (g *gcHooks) losses() int64 {
	if g.toucher == nil {
		return 0
	}
	st := g.toucher.Stats()
	return st.Dropped + st.Failed
}

// A lost map row touch only shortens the retention of an eval map row.
func (g *gcHooks) mapRowLosses() int64 {
	if g.toucher == nil {
		return 0
	}
	st := g.toucher.Stats()
	return st.MapRowsDropped + st.MapRowsFailed
}

func (g *gcHooks) touchesLate() bool {
	return g.toucher != nil && g.toucher.Late()
}

// checkHeartbeat fails once the heartbeat is stale: the collector counts a
// process in a cycle window only while its heartbeat is recent.
func (g *gcHooks) checkHeartbeat() error {
	if g.heartbeatStale == 0 {
		return nil
	}
	now, last := cacheNow(), g.lastHeartbeat.Load()
	if now > last && now-last > g.heartbeatStale {
		return &remote.ErrUnavailable{Reason: fmt.Sprintf("GC heartbeat stale: none written for %d s", now-last)}
	}
	return nil
}

// CheckGCHeartbeat fails while the GC heartbeat is stale. It does no I/O.
func (s *GCPServer) CheckGCHeartbeat(context.Context) error {
	return s.gcHooks.checkHeartbeat()
}

func (g *gcHooks) expired(loaded uint32) bool {
	return g.cacheTTL > 0 && loaded != 0 && cacheNow()-loaded >= g.cacheTTL
}

func (s *GCPServer) lookupHeader(key entities.Hash) (CachedHeader, bool) {
	h, ok := s.headers[key[0]].Get(key)
	if ok && s.gcHooks.expired(h.Loaded) {
		s.gcHooks.headerExpiries.Add(1)
		return CachedHeader{}, false
	}
	return h, ok
}

// storeHeader ages the entry from loaded, the start of the read. touched means
// the object got its own touch after loaded, by new edges or a rescue, so a
// hit in that period queues none.
func (s *GCPServer) storeHeader(hdr *ObjectHeader, loaded uint32, touched bool) CachedHeader {
	h := newCachedHeader(hdr)
	h.Loaded = loaded
	shard := s.headers[hdr.Id[0]]
	if !s.gcHooks.on {
		shard.PeekOrAdd(hdr.Id, h)
		return h
	}
	if g := &s.gcHooks; g.touchInterval > 0 {
		if touched {
			h.Touched = touchMark(g.touchPeriod(hdr.Id, loaded))
		} else if old, ok := shard.Peek(hdr.Id); ok && g.touchedThisPeriod(hdr.Id, old, h.Loaded) {
			h.Touched = old.Touched
		}
	}
	shard.Add(hdr.Id, h)
	return h
}

func (s *GCPServer) lookupContent(key entities.Hash) (*ObjectContent, bool) {
	c, ok := s.content[key[0]].Get(key)
	if ok && s.gcHooks.expired(c.loaded) {
		s.gcHooks.contentExpiries.Add(1)
		return nil, false
	}
	return c.content, ok
}

func (s *GCPServer) storeContent(key entities.Hash, content *ObjectContent, loaded uint32) {
	entry := contentEntry{content: content, loaded: loaded}
	if !s.gcHooks.on {
		s.content[key[0]].PeekOrAdd(key, entry)
		return
	}
	s.content[key[0]].Add(key, entry)
}

func (s *GCPServer) forget(key entities.Hash) {
	s.headers[key[0]].Remove(key)
	s.content[key[0]].Remove(key)
}

// forgetMissing drops an expired entry; without GC entries do not expire.
func (s *GCPServer) forgetMissing(key entities.Hash) {
	if s.gcHooks.on {
		s.forget(key)
	}
}

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
	rescued := false
	if err == nil && header != nil {
		if err = s.gcHooks.checkReadAge(start); err == nil {
			rescued, err = s.rescueCondemned(ctx, pathGetObject, header)
		}
	}
	if errors.Is(err, casdb.ErrNotFound) || (err == nil && header == nil) {
		s.forget(key)
		return CachedHeader{}, nil, entities.ErrNotExist
	}
	if err != nil {
		return CachedHeader{}, nil, err
	}
	cachedHdr = s.storeHeader(header, start, rescued)
	s.storeContent(key, content, start)
	s.touchHit(pathGetObject, key, cachedHdr)
	return cachedHdr, content, nil
}

// rescueCondemned returns casdb.ErrNotFound when the collector deleted hdr.
func (s *GCPServer) rescueCondemned(ctx context.Context, path rescuePath, hdr *ObjectHeader) (bool, error) {
	if hdr.Condemned == nil || s.gcHooks.rescue == nil {
		return false, nil
	}
	req := newRequestID(ctx)
	err := s.rescueOne(ctx, path, req, hdr)
	if err == nil {
		s.gcHooks.rescueLog.call(ctx, path, req, 1, 1, 0)
	} else if !errors.Is(err, casdb.ErrNotFound) {
		s.gcHooks.rescueLog.call(ctx, path, req, 1, 0, 1)
	}
	return err == nil, err
}

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

// rescueHeaders runs every rescue even after a failure, so that a retry finds
// only the failed ones still condemned.
func (s *GCPServer) rescueHeaders(ctx context.Context, path rescuePath, hdrs []*ObjectHeader) ([]*ObjectHeader, map[entities.Hash]bool, error) {
	if s.gcHooks.rescue == nil {
		return hdrs, nil, nil
	}
	gone := make([]bool, len(hdrs))
	done := make([]bool, len(hdrs))
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
				done[i] = true
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
		return nil, nil, err
	}
	var keys map[entities.Hash]bool
	live := hdrs[:0]
	for i, h := range hdrs {
		if done[i] {
			if keys == nil {
				keys = make(map[entities.Hash]bool)
			}
			keys[h.Id] = true
		}
		if !gone[i] {
			live = append(live, h)
		}
	}
	return live, keys, nil
}

// touchMark repeats every 255 periods and is never zero.
func touchMark(period uint64) uint8 {
	return uint8(period%255) + 1
}

// touchPeriod shifts the periods by an offset from the key, so that the next
// touches of different objects do not all queue at once.
func (g *gcHooks) touchPeriod(key entities.Hash, now uint32) uint64 {
	interval := uint64(g.touchInterval)
	offset := uint64(binary.BigEndian.Uint32(key[len(key)-4:])) % interval
	return (uint64(now) + offset) / interval
}

// The mark cannot have wrapped for entries loaded less than 255 periods ago.
func (g *gcHooks) touchedThisPeriod(key entities.Hash, h CachedHeader, now uint32) bool {
	period := g.touchPeriod(key, now)
	return h.Touched != 0 && period-g.touchPeriod(key, h.Loaded) < 255 && h.Touched == touchMark(period)
}

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
	// A concurrent forget can be undone here; the TTL still bounds the entry.
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

// The keep-alive and build calls wait for buffer room, so a burst slows down
// instead of losing touches; other calls do not wait.
func (g *gcHooks) touch(path rescuePath, key entities.Hash) bool {
	if path == pathHeaders || path == pathMapResults {
		return g.toucher.TouchObject(key)
	}
	return g.toucher.TryTouchObject(key)
}

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

// liveMapResults drops results whose object is gone; a client cannot use them.
func (s *GCPServer) liveMapResults(ctx context.Context, from entities.Hash, results []entities.Hash) ([]entities.Hash, error) {
	if len(results) == 0 {
		return results, nil
	}
	hdrs, err := s.getHeaders(ctx, pathMapResults, results, false)
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

// chunkMissing reports a missing object only when the row is gone; a chunk the
// row still lists is lost, not missing.
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

// startHeartbeat writes the first heartbeat before the server serves.
func (s *GCPServer) startHeartbeat(ctx context.Context, interval time.Duration) error {
	g := &s.gcHooks
	log.Printf("GC server ID %s, heartbeat every %s", g.server.ID, interval)
	g.lastAttempt = 0
	first, cancelFirst := context.WithTimeout(ctx, firstHeartbeatTimeout)
	defer cancelFirst()
	for {
		err := s.heartbeat(first)
		if err == nil {
			break
		}
		select {
		case <-first.Done():
			if ctx.Err() != nil {
				return fmt.Errorf("failed to write the first GC heartbeat: %w", context.Cause(ctx))
			}
			return fmt.Errorf("failed to write the first GC heartbeat within %s: %w", firstHeartbeatTimeout, err)
		case <-time.After(firstHeartbeatRetry):
		}
	}
	g.heartbeatStale = seconds(2*interval + heartbeatTimeout)
	// Not tied to ctx, so that a server draining its calls keeps its row fresh.
	loop, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-loop.Done():
				return
			case <-ticker.C:
			}
			_ = s.heartbeat(loop)
		}
	}()
	g.stopHeartbeat = func() {
		cancel()
		<-done
	}
	return nil
}

func (s *GCPServer) heartbeat(ctx context.Context) error {
	g := &s.gcHooks
	start := cacheNow()
	if g.heartbeatLate(start) {
		log.Printf("Warning: no GC heartbeat attempt for over the heartbeat interval plus %s; recording the gap as a touch loss", gc.TouchDrainBound)
		g.pausePending = true
	}
	hctx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()
	if g.pausedSince() {
		log.Printf("Warning: the wall clock advanced more than the monotonic clock by over %s since the last GC heartbeat; recording the pause as a touch loss", maxPause)
		g.pausePending = true
	}
	// Read the slots first: a late touch leaves them after its loss is counted.
	late := g.touchesLate()
	losses, mapRowLosses := g.losses(), g.mapRowLosses()
	err := writeServer(hctx, s.casdb.SpannerClient(), &g.server, g.pausePending || late || losses > g.reportedLosses, mapRowLosses > g.reportedMapRowLosses)
	if err == nil {
		g.reportedLosses, g.reportedMapRowLosses = losses, mapRowLosses
		g.pausePending = false
		g.hasRow = true
		g.lastHeartbeat.Store(start)
		return nil
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

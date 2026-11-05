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
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
	"github.com/google/abfs/shared/remote/servers/gcp/depot"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// newGCHooksServer returns a server on an in-process Bigtable that has the
// tables of the garbage collector hooks, with the hooks started with opts.
func newGCHooksServer(t *testing.T, opts *NewServerOpts) *GCPServer {
	t.Helper()
	ctx := context.Background()
	s, _ := createTestServerWithCASDB(t)
	createGCTable(t)
	adm, err := bigtable.NewAdminClient(ctx, "test-proj", "test-bt")
	require.NoError(t, err)
	defer adm.Close()
	require.NoError(t, adm.CreateColumnFamily(ctx, "objects", gc.ObjectsFamily))
	require.NoError(t, adm.CreateTable(ctx, "maps"))
	require.NoError(t, adm.CreateColumnFamily(ctx, "maps", "cf"))
	require.NoError(t, s.enableGCHooks(opts))
	t.Cleanup(s.stopGCHooks)
	return s
}

// setCacheNow makes cacheNow return *now for the rest of the test.
func setCacheNow(t *testing.T, now *uint32) {
	t.Helper()
	orig := cacheNow
	cacheNow = func() uint32 { return *now }
	t.Cleanup(func() { cacheNow = orig })
}

func putBlob(t *testing.T, s *GCPServer, content string) entities.Hash {
	t.Helper()
	data := fmt.Appendf(nil, "blob %d\x00%s", len(content), content)
	key := objects.HashBlobBytes([]byte(content))
	require.NoError(t, putRaw(s, key, data))
	return key
}

func condemnObject(t *testing.T, s *GCPServer, key entities.Hash, cycle byte) {
	t.Helper()
	mut := bigtable.NewMutation()
	mut.Set(gc.ObjectsFamily, gc.CondemnedColumn, bigtable.Now(), []byte{0, 0, 0, 0, 0, 0, 0, cycle})
	require.NoError(t, s.casdb.BTClient().Open("objects").Apply(context.Background(), key.String(), mut))
	s.forget(key)
}

func deleteObjectRow(t *testing.T, s *GCPServer, key entities.Hash) {
	t.Helper()
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	require.NoError(t, s.casdb.BTClient().Open("objects").Apply(context.Background(), key.String(), mut))
}

func isCondemned(t *testing.T, s *GCPServer, key entities.Hash) bool {
	t.Helper()
	hdr, err := s.casdb.GetHeader(context.Background(), key)
	require.NoError(t, err)
	return hdr.Condemned != nil
}

func touched(t *testing.T, s *GCPServer, key string, table, family, column string) (bigtable.Timestamp, bool) {
	t.Helper()
	row, err := s.casdb.BTClient().Open(table).ReadRow(context.Background(), key, bigtable.RowFilter(bigtable.LatestNFilter(1)))
	require.NoError(t, err)
	for _, item := range row[family] {
		if item.Column == family+":"+column {
			return item.Timestamp, true
		}
	}
	return 0, false
}

func objectTouched(t *testing.T, s *GCPServer, key entities.Hash) bool {
	t.Helper()
	_, ok := touched(t, s, key.String(), gc.TableName, gc.TouchFamily, gc.TouchColumn)
	return ok
}

func readObject(s *GCPServer, key entities.Hash) error {
	r, err := s.GetObject(context.Background(), key)
	if err != nil {
		return err
	}
	_, err = io.ReadAll(r)
	return err
}

func TestGCHooks_RescueOnEachPath(t *testing.T) {
	tests := []struct {
		path rescuePath
		use  func(s *GCPServer, key entities.Hash) error
	}{
		{pathHeaders, func(s *GCPServer, key entities.Hash) error {
			hdrs, err := s.Headers(context.Background(), []entities.Hash{key})
			if err == nil && hdrs[0].Type == "" {
				err = fmt.Errorf("%s reported missing", key)
			}
			return err
		}},
		{pathValidation, func(s *GCPServer, key entities.Hash) error {
			tree, data := treeOf(key)
			return putRaw(s, tree, data)
		}},
		{pathValidation, func(s *GCPServer, key entities.Hash) error {
			return s.AddMapResult(context.Background(), key, key)
		}},
		{pathPutObject, func(s *GCPServer, key entities.Hash) error {
			return putRaw(s, key, []byte("blob 5\x00hello"))
		}},
		{pathGetObject, func(s *GCPServer, key entities.Hash) error {
			return readObject(s, key)
		}},
		{pathMapResults, func(s *GCPServer, key entities.Hash) error {
			from := entities.EmptyDirHash
			require.NoError(t, s.AddMapResult(context.Background(), from, key))
			condemnObject(t, s, key, 3)
			results, err := s.MapResults(context.Background(), from)
			if err == nil && len(results) != 1 {
				err = fmt.Errorf("got %d map results", len(results))
			}
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(rescuePathNames[tt.path], func(t *testing.T) {
			require := require.New(t)
			s := newGCHooksServer(t, &NewServerOpts{})
			require.NoError(putRaw(s, entities.EmptyDirHash, []byte("tree 0\x00")))
			key := putBlob(t, s, "hello")
			condemnObject(t, s, key, 3)

			require.NoError(tt.use(s, key))
			require.False(isCondemned(t, s, key))
			require.True(objectTouched(t, s, key), "the rescue wrote no touch")
			require.Equal(int64(1), s.gcHooks.rescues[tt.path].Load())
		})
	}
}

func TestGCHooks_RescueLosesToDelete(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s := newGCHooksServer(t, &NewServerOpts{})
	key := putBlob(t, s, "hello")
	rescue := s.gcHooks.rescue
	// The collector deletes the row between the read and the rescue.
	s.gcHooks.rescue = func(ctx context.Context, key entities.Hash, condemned []byte) (bool, error) {
		deleteObjectRow(t, s, key)
		return rescue(ctx, key, condemned)
	}

	condemnObject(t, s, key, 3)
	hdrs, err := s.Headers(ctx, []entities.Hash{key})
	require.NoError(err)
	require.Empty(hdrs[0].Type)
	require.Equal(int64(1), s.gcHooks.rescuesLost[pathHeaders].Load())

	key2 := putBlob(t, s, "world")
	condemnObject(t, s, key2, 3)
	require.ErrorIs(readObject(s, key2), entities.ErrNotExist)
	require.Equal(int64(1), s.gcHooks.rescuesLost[pathGetObject].Load())

	// A writer whose object was deleted stores it again.
	key3 := putBlob(t, s, "again")
	condemnObject(t, s, key3, 3)
	require.NoError(putRaw(s, key3, []byte("blob 5\x00again")))
	require.Equal(int64(1), s.gcHooks.rescuesLost[pathPutObject].Load())
	_, err = s.casdb.GetHeader(ctx, key3)
	require.NoError(err)
	require.False(objectTouched(t, s, key), "a lost rescue wrote a touch")
}

// A keep-alive batch right after a condemn cycle can hold many condemned
// objects; their rescues run in parallel, and one the collector deleted first
// is reported missing.
func TestGCHooks_RescuesABatchInParallel(t *testing.T) {
	require := require.New(t)
	s := newGCHooksServer(t, &NewServerOpts{})
	keys := make([]entities.Hash, 100)
	for i := range keys {
		keys[i] = putBlob(t, s, fmt.Sprintf("condemned %d", i))
		condemnObject(t, s, keys[i], 3)
	}
	gone := keys[7]
	rescue := s.gcHooks.rescue
	var mu sync.Mutex
	var running, most int
	s.gcHooks.rescue = func(ctx context.Context, key entities.Hash, condemned []byte) (bool, error) {
		mu.Lock()
		running++
		most = max(most, running)
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
		if key == gone {
			deleteObjectRow(t, s, key)
		}
		return rescue(ctx, key, condemned)
	}

	hdrs, err := s.Headers(context.Background(), keys)
	require.NoError(err)
	for i, h := range hdrs {
		require.Equal(keys[i] == gone, h.IsEmpty(), i)
	}
	for _, key := range keys {
		if key != gone {
			require.False(isCondemned(t, s, key))
		}
	}
	require.Equal(int64(99), s.gcHooks.rescues[pathHeaders].Load())
	require.Equal(int64(1), s.gcHooks.rescuesLost[pathHeaders].Load())
	require.Greater(most, 1)
	require.LessOrEqual(most, rescueWorkers)
}

func TestGCHooks_TouchThrottle(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	now := uint32(1)
	setCacheNow(t, &now)
	s := newGCHooksServer(t, &NewServerOpts{CacheTTL: 10 * time.Minute, GCTouchInterval: time.Hour})
	key := putBlob(t, s, "hello")
	written := func() int64 {
		time.Sleep(10 * time.Millisecond)
		s.gcHooks.toucher.Close()
		st := s.gcHooks.toucher.Stats()
		s.gcHooks.toucher = gc.NewToucher(s.casdb.BTClient(), touchBufferSize)
		return st.Written
	}

	for range 3 {
		_, err := s.Headers(ctx, []entities.Hash{key})
		require.NoError(err)
	}
	require.NoError(putRaw(s, key, []byte("blob 5\x00hello")))
	tree, data := treeOf(key)
	require.NoError(putRaw(s, tree, data))
	require.Equal(int64(1), written(), "more than one touch in a period")
	require.True(objectTouched(t, s, key))

	// The mark survives a reload of the expired header in the same period.
	now += 601
	_, err := s.Headers(ctx, []entities.Hash{key})
	require.NoError(err)
	require.Equal(int64(1), s.gcHooks.headerExpiries.Load())
	require.Equal(int64(0), written())

	now += 3600
	_, err = s.Headers(ctx, []entities.Hash{key})
	require.NoError(err)
	require.Equal(int64(1), written(), "no touch in the next period")
}

// The keep-alive of a large mount sends batches of 50,000 cached keys back to
// back. Every hit gets its touch.
func TestGCHooks_HeadersBurstDropsNoTouches(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s := newGCHooksServer(t, &NewServerOpts{GCTouchInterval: time.Hour})
	const n = 200000
	var err error
	s.headers, err = newShardedCache[CachedHeader](2 * n)
	require.NoError(err)
	keys := make([]entities.Hash, n)
	for i := range keys {
		keys[i] = entities.Hash(sha1.Sum(fmt.Appendf(nil, "burst %d", i)))
		s.headers[keys[i][0]].Add(keys[i], newCachedHeader(&ObjectHeader{Id: keys[i], Type: "blob", Size: 1}))
	}

	for batch := range slices.Chunk(keys, 50000) {
		hdrs, err := s.Headers(ctx, batch)
		require.NoError(err)
		for _, h := range hdrs {
			require.False(h.IsEmpty())
		}
	}
	s.gcHooks.toucher.Close()
	require.Equal(gc.TouchStats{Written: n}, s.gcHooks.toucher.Stats())
}

func TestGCHooks_GetObjectTouches(t *testing.T) {
	require := require.New(t)
	now := uint32(1)
	setCacheNow(t, &now)
	s := newGCHooksServer(t, &NewServerOpts{CacheTTL: 10 * time.Minute, GCTouchInterval: time.Hour})
	cached := putBlob(t, s, "cached")
	uncached := putBlob(t, s, "uncached")
	s.forget(uncached)
	_, hdrOk := s.headers[cached[0]].Peek(cached)
	_, contentOk := s.content[cached[0]].Peek(cached)
	require.True(hdrOk && contentOk)

	for range 2 {
		require.NoError(readObject(s, cached))
		require.NoError(readObject(s, uncached))
	}
	s.gcHooks.toucher.Close()
	require.Equal(int64(2), s.gcHooks.toucher.Stats().Written, "one touch per object and period")
	require.True(objectTouched(t, s, cached))
	require.True(objectTouched(t, s, uncached))
}

func TestGCHooks_TouchesOff(t *testing.T) {
	require := require.New(t)
	s := newGCHooksServer(t, &NewServerOpts{})
	key := putBlob(t, s, "hello")
	_, err := s.Headers(context.Background(), []entities.Hash{key})
	require.NoError(err)
	s.stopGCHooks()
	require.Nil(s.gcHooks.toucher)
	require.False(objectTouched(t, s, key))
}

func TestGCHooks_CacheTTL(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	now := uint32(1)
	setCacheNow(t, &now)
	s := newGCHooksServer(t, &NewServerOpts{CacheTTL: time.Hour})
	key := putBlob(t, s, "hello")
	require.NoError(readObject(s, key))
	deleteObjectRow(t, s, key)

	// Within the TTL the cached entries answer.
	now += 3599
	hdrs, err := s.Headers(ctx, []entities.Hash{key})
	require.NoError(err)
	require.NotEmpty(hdrs[0].Type)
	require.NoError(readObject(s, key))

	now++
	hdrs, err = s.Headers(ctx, []entities.Hash{key})
	require.NoError(err)
	require.Empty(hdrs[0].Type)
	require.ErrorIs(readObject(s, key), entities.ErrNotExist)
	require.Equal(int64(1), s.gcHooks.headerExpiries.Load())
	_, ok := s.content[key[0]].Peek(key)
	require.False(ok, "content of a missing object stays cached")
}

func TestGCHooks_NoCacheTTL(t *testing.T) {
	require := require.New(t)
	now := uint32(1)
	setCacheNow(t, &now)
	s := newGCHooksServer(t, &NewServerOpts{})
	key := putBlob(t, s, "hello")
	deleteObjectRow(t, s, key)
	now += 1000 * 3600
	hdrs, err := s.Headers(context.Background(), []entities.Hash{key})
	require.NoError(err)
	require.NotEmpty(hdrs[0].Type)
}

func TestGCHooks_MapResults(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s := newGCHooksServer(t, &NewServerOpts{GCTouchInterval: time.Hour})
	from := putBlob(t, s, "request")
	kept := putBlob(t, s, "kept")
	gone := putBlob(t, s, "gone")
	require.NoError(s.AddMapResult(ctx, from, kept))
	require.NoError(s.AddMapResult(ctx, from, gone))
	added, ok := touched(t, s, casdb.MapRowKey(from, kept), "maps", "cf", "")
	require.True(ok)
	deleteObjectRow(t, s, gone)
	s.forget(gone)
	time.Sleep(2 * time.Millisecond)

	results, err := s.MapResults(ctx, from)
	require.NoError(err)
	require.Equal([]entities.Hash{kept}, results)
	require.Equal(int64(1), s.gcHooks.mapResultsDropped.Load())

	s.gcHooks.toucher.Close()
	used, ok := touched(t, s, casdb.MapRowKey(from, kept), "maps", "cf", "")
	require.True(ok)
	require.Greater(used, added, "the map row was not touched")
	// The validation of AddMapResult touched the three objects, MapResults the
	// map row of the result it returned.
	require.Equal(int64(4), s.gcHooks.toucher.Stats().Written)
}

func TestGCHooks_ChunkMissingDuringRead(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s := newGCHooksServer(t, &NewServerOpts{})
	key := putBlob(t, s, "hello")
	require.NoError(readObject(s, key))
	content, err := s.casdb.GetContent(ctx, key)
	require.NoError(err)

	// The row still lists the chunk: the content is lost, the object is not
	// missing.
	require.NoError(s.depot.Remove(ctx, content.Content))
	err = readObject(s, key)
	require.Error(err)
	require.False(entities.IsNotExist(err), err)

	// The collector deleted the row during the read.
	require.NoError(readObject(s, putBlob(t, s, "other")))
	deleteObjectRow(t, s, key)
	s.storeHeader(&ObjectHeader{Id: key, Type: "blob", Size: 5})
	s.storeContent(key, content)
	err = readObject(s, key)
	require.ErrorIs(err, entities.ErrNotExist)
	_, ok := s.headers[key[0]].Peek(key)
	require.False(ok, "header of a missing object stays cached")
	_, ok = s.content[key[0]].Peek(key)
	require.False(ok, "content of a missing object stays cached")
}

func TestGCHooks_ChunkMissingInBucket(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s := newEmulatorServer(t, &NewServerOpts{GC: true, CacheTTL: time.Hour, GCTouchInterval: time.Hour})
	content := make([]byte, 1500*1024)
	for i := range content {
		content[i] = byte(i * 7)
	}
	data := append(fmt.Appendf(nil, "blob %d\x00", len(content)), content...)
	key := objects.HashBlobBytes(content)
	require.NoError(putRaw(s, key, data))
	require.NoError(readObject(s, key))

	desc, err := s.casdb.GetContent(ctx, key)
	require.NoError(err)
	var chunks []depot.Chunk
	require.NoError(json.Unmarshal([]byte(desc.Content), &chunks))
	require.Len(chunks, 2)
	require.Equal("bucket", chunks[1].Depot)
	deleteObjectRow(t, s, key)
	require.NoError(s.depot.RemoveChunks(ctx, chunks[1:]))

	err = readObject(s, key)
	require.ErrorIs(err, entities.ErrNotExist)
	_, ok := s.headers[key[0]].Peek(key)
	require.False(ok)
}

func TestGCHooks_Heartbeat(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s := newEmulatorCASDBServer(t)
	require.NoError(s.enableGCHooks(&NewServerOpts{GCHeartbeatInterval: time.Hour}))
	t.Cleanup(s.stopGCHooks)

	var version int64
	var lastSeen time.Time
	require.Eventually(func() bool {
		rows := 0
		err := s.casdb.SpannerClient().Single().Query(ctx, spanner.Statement{SQL: "SELECT Version, LastSeen FROM GcReplicas"}).Do(func(r *spanner.Row) error {
			rows++
			return r.Columns(&version, &lastSeen)
		})
		require.NoError(err)
		return rows == 1
	}, 10*time.Second, 50*time.Millisecond)
	require.Equal(int64(gc.ExtractorVersion), version)
	require.WithinDuration(time.Now(), lastSeen, time.Minute)
	require.Zero(s.gcHooks.heartbeatFailures.Load())
}

func TestGCHooks_RejectsNegativeDurations(t *testing.T) {
	s, _ := createTestServerWithCASDB(t)
	require.Error(t, s.startGCHooks(context.Background(), &NewServerOpts{GC: true, CacheTTL: -time.Second}))
}

func TestGCHooks_Metrics(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	s := newGCHooksServer(t, &NewServerOpts{GCTouchInterval: time.Hour})
	key := putBlob(t, s, "hello")
	condemnObject(t, s, key, 3)
	_, err := s.Headers(ctx, []entities.Hash{key})
	require.NoError(err)

	var rm metricdata.ResourceMetrics
	require.NoError(reader.Collect(ctx, &rm))
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != metricsScope {
			continue
		}
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(ok, m.Name)
			for _, dp := range sum.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					got[m.Name+"/"+kv.Value.AsString()] += dp.Value
				}
				if dp.Attributes.Len() == 0 {
					got[m.Name] += dp.Value
				}
			}
		}
	}
	require.Equal(int64(1), got["abfs.gcp.gc.rescues/headers"])
	require.Contains(got, "abfs.gcp.gc.touches/dropped")
	require.Contains(got, "abfs.gcp.cache.ttl_expiries/content")
	require.Contains(got, "abfs.gcp.gc.map_results_dropped")
	require.Contains(got, "abfs.gcp.gc.heartbeat_failures")
}

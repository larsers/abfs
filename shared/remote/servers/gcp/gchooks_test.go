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
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote"
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

// rescuesOf returns the rescues of path, of all types.
func rescuesOf(s *GCPServer, path rescuePath) int64 {
	var n int64
	for i := range s.gcHooks.rescues[path] {
		n += s.gcHooks.rescues[path][i].Load()
	}
	return n
}

// captureLog sends the standard logger to a buffer for the rest of the test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return &buf
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
			require.Equal(int64(1), rescuesOf(s, tt.path))
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
	// The rescue touches before it clears, so a lost one leaves its touch.
	require.True(objectTouched(t, s, key))

	// A tree that links an object the collector deleted fails validation.
	key4 := putBlob(t, s, "linked")
	condemnObject(t, s, key4, 3)
	tree, data := treeOf(key4)
	var linkErr *remote.ErrObjectLinkValidation
	require.ErrorAs(putRaw(s, tree, data), &linkErr)
	require.Equal([]entities.Hash{key4}, linkErr.MissingLinks)
	require.Equal(int64(1), s.gcHooks.rescuesLost[pathValidation].Load())

	// MapResults leaves out a result the collector deleted.
	from := putBlob(t, s, "request")
	key5 := putBlob(t, s, "result")
	require.NoError(s.AddMapResult(ctx, from, key5))
	condemnObject(t, s, key5, 3)
	results, err := s.MapResults(ctx, from)
	require.NoError(err)
	require.Empty(results)
	require.Equal(int64(1), s.gcHooks.rescuesLost[pathMapResults].Load())
	require.Equal(int64(1), s.gcHooks.mapResultsDropped.Load())
}

// lockedBuffer is a log output that goroutines a call leaves behind may still
// write to while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A rescue that loses to a delete in link validation or in MapResults
// answers missing, and its log line names the call, the client and the
// request: the trace ID of a traced call, else a random id.
func TestGCHooks_LostRescueLogsTheCall(t *testing.T) {
	require := require.New(t)
	s := newGCHooksServer(t, &NewServerOpts{})
	rescue := s.gcHooks.rescue
	s.gcHooks.rescue = func(ctx context.Context, key entities.Hash, condemned []byte) (bool, error) {
		deleteObjectRow(t, s, key)
		return rescue(ctx, key, condemned)
	}
	logged := &lockedBuffer{}
	log.SetOutput(logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	client := `, peer 10.0.0.9:4711, x-forwarded-for "10.1.2.3, 10.0.0.9", client-version "0.1.23"`

	linked := putBlob(t, s, "linked")
	condemnObject(t, s, linked, 3)
	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	ctx := trace.ContextWithSpanContext(rescueClientContext(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: trace.SpanID{1}}))
	tree, data := treeOf(linked)
	var linkErr *remote.ErrObjectLinkValidation
	require.ErrorAs(s.PutObject(ctx, tree, remote.NewContentReader(io.NopCloser(bytes.NewReader(data)), remote.EncodingRaw)), &linkErr)
	require.Equal([]entities.Hash{linked}, linkErr.MissingLinks)
	// Each call has its own request id, also when the calls share a trace.
	linked2 := putBlob(t, s, "linked 2")
	condemnObject(t, s, linked2, 3)
	tree2, data2 := treeOf(linked2)
	require.ErrorAs(s.PutObject(ctx, tree2, remote.NewContentReader(io.NopCloser(bytes.NewReader(data2)), remote.EncodingRaw)), &linkErr)
	var reqs []string
	for _, key := range []entities.Hash{linked, linked2} {
		lostLine := regexp.MustCompile(fmt.Sprintf(`Condemned object %s was deleted before it could be rescued in validation: request ([0-9a-f]{16}), trace %s`, key, traceID) + regexp.QuoteMeta(client))
		m := lostLine.FindStringSubmatch(logged.String())
		require.NotNil(m, key)
		reqs = append(reqs, m[1])
	}
	require.NotEqual(reqs[0], reqs[1])

	from := putBlob(t, s, "request")
	result := putBlob(t, s, "result")
	require.NoError(s.AddMapResult(context.Background(), from, result))
	condemnObject(t, s, result, 3)
	results, err := s.MapResults(rescueClientContext(), from)
	require.NoError(err)
	require.Empty(results)
	require.Regexp(fmt.Sprintf(`(?m)Condemned object %s was deleted before it could be rescued in map_results: request [0-9a-f]{16}`, result)+regexp.QuoteMeta(client)+`$`, logged.String())
	require.Equal(int64(1), s.gcHooks.mapResultsDropped.Load())
}

// A keep-alive batch right after a condemn cycle can hold many condemned
// objects; their rescues run in parallel, one the collector deleted first is
// reported missing, and the log lines of the batch share one request id.
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

	logged := captureLog(t)
	hdrs, err := s.Headers(context.Background(), keys)
	require.NoError(err)
	for i, h := range hdrs {
		require.Equal(keys[i] == gone, h.IsEmpty(), i)
	}
	// Every line of the call names the same request.
	reqs := requestIDPattern.FindAllStringSubmatch(logged.String(), -1)
	require.Len(reqs, 101)
	for _, r := range reqs {
		require.Equal(reqs[0][1], r[1])
	}
	for _, key := range keys {
		if key != gone {
			require.False(isCondemned(t, s, key))
		}
	}
	require.Equal(int64(99), rescuesOf(s, pathHeaders))
	require.Equal(int64(1), s.gcHooks.rescuesLost[pathHeaders].Load())
	require.Greater(most, 1)
	require.LessOrEqual(most, rescueWorkers)
}

// A failed rescue does not stop the other rescues of the batch, so that a
// retry of the call finds only that object still condemned.
func TestGCHooks_FailedRescueDoesNotStopTheBatch(t *testing.T) {
	require := require.New(t)
	s := newGCHooksServer(t, &NewServerOpts{})
	keys := make([]entities.Hash, 100)
	for i := range keys {
		keys[i] = putBlob(t, s, fmt.Sprintf("condemned %d", i))
		condemnObject(t, s, keys[i], 3)
	}
	failing := keys[7]
	rescueErr := errors.New("unavailable")
	rescue := s.gcHooks.rescue
	var calls atomic.Int64
	s.gcHooks.rescue = func(ctx context.Context, key entities.Hash, condemned []byte) (bool, error) {
		calls.Add(1)
		if key == failing {
			return false, rescueErr
		}
		return rescue(ctx, key, condemned)
	}

	_, err := s.Headers(context.Background(), keys)
	require.ErrorIs(err, rescueErr)
	require.Equal(int64(len(keys)), calls.Load())
	for _, key := range keys {
		require.Equal(key == failing, isCondemned(t, s, key), key)
	}
	require.Equal(int64(99), rescuesOf(s, pathHeaders))
	require.Equal(int64(1), s.gcHooks.rescueFailures[pathHeaders].Load())

	// The retry rescues the one left.
	s.gcHooks.rescue = rescue
	hdrs, err := s.Headers(context.Background(), keys)
	require.NoError(err)
	for _, h := range hdrs {
		require.False(h.IsEmpty())
	}
	require.False(isCondemned(t, s, failing))
}

// fakeToucher records the touches of objects by whether the call waited for
// room in a full buffer, and drops the map row touches when dropMapRows is
// set.
type fakeToucher struct {
	mu          sync.Mutex
	waited      []entities.Hash
	tried       []entities.Hash
	mapRows     []string
	dropMapRows bool
}

func (f *fakeToucher) TouchObject(key gc.Hash) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waited = append(f.waited, key)
	return true
}

func (f *fakeToucher) TryTouchObject(key gc.Hash) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tried = append(f.tried, key)
	return true
}

func (f *fakeToucher) TouchMapRow(row string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mapRows = append(f.mapRows, row)
	return !f.dropMapRows
}

func (f *fakeToucher) Stats() gc.TouchStats { return gc.TouchStats{} }

func (f *fakeToucher) Late() bool { return false }

func (f *fakeToucher) Close() {}

// Headers and MapResults, the calls of the client keep-alive and of builds,
// wait for room in a full touch buffer; reads, uploads and link checks do not.
func TestGCHooks_OnlyKeepAliveCallsWaitForTouches(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	now := uint32(1)
	setCacheNow(t, &now)
	s := newGCHooksServer(t, &NewServerOpts{GCTouchInterval: time.Hour})
	headersKey := putBlob(t, s, "headers")
	readKey := putBlob(t, s, "read")
	putKey := putBlob(t, s, "put")
	linkKey := putBlob(t, s, "link")
	from := putBlob(t, s, "request")
	result := putBlob(t, s, "result")
	require.NoError(s.AddMapResult(ctx, from, result))
	s.gcHooks.toucher.Close()
	fake := &fakeToucher{}
	s.gcHooks.toucher = fake
	// A new touch period, so that every object is touched again.
	now += 3600

	_, err := s.Headers(ctx, []entities.Hash{headersKey})
	require.NoError(err)
	require.NoError(readObject(s, readKey))
	require.NoError(putRaw(s, putKey, []byte("blob 3\x00put")))
	tree, data := treeOf(linkKey)
	require.NoError(putRaw(s, tree, data))
	results, err := s.MapResults(ctx, from)
	require.NoError(err)
	require.Equal([]entities.Hash{result}, results)

	require.ElementsMatch([]entities.Hash{headersKey, result}, fake.waited)
	require.ElementsMatch([]entities.Hash{readKey, putKey, linkKey}, fake.tried)
	require.Equal([]string{casdb.MapRowKey(from, result)}, fake.mapRows)
}

// The collector's own server reads condemned objects as live and leaves the
// marker and the touches as they are.
func TestGCHooks_ReadOnly(t *testing.T) {
	require := require.New(t)
	s := newGCHooksServer(t, &NewServerOpts{GCReadOnly: true, CacheTTL: time.Hour, GCTouchInterval: time.Hour, GCHeartbeatInterval: time.Hour})
	require.Nil(s.gcHooks.toucher)
	require.Nil(s.gcHooks.stopHeartbeat)
	key := putBlob(t, s, "hello")
	condemnObject(t, s, key, 3)

	hdrs, err := s.Headers(context.Background(), []entities.Hash{key})
	require.NoError(err)
	require.False(hdrs[0].IsEmpty())
	s.forget(key)
	require.NoError(readObject(s, key))
	s.forget(key)
	require.NoError(putRaw(s, key, []byte("blob 5\x00hello")))
	tree, data := treeOf(key)
	require.NoError(putRaw(s, tree, data))

	require.True(isCondemned(t, s, key))
	require.False(objectTouched(t, s, key))
	for p := range numRescuePaths {
		require.Zero(rescuesOf(s, p))
	}
}

func TestGCHooks_TouchThrottle(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	now := uint32(1)
	setCacheNow(t, &now)
	s := newGCHooksServer(t, &NewServerOpts{CacheTTL: 10 * time.Minute, GCTouchInterval: time.Hour})
	key := putBlob(t, s, "hello")
	// Start at the start of the object's touch period.
	now = periodEnd(&s.gcHooks, key, now)
	written := func() int64 {
		time.Sleep(10 * time.Millisecond)
		s.gcHooks.toucher.Close()
		st := s.gcHooks.toucher.Stats()
		s.gcHooks.toucher = gc.NewToucher(s.casdb.BTClient(), gc.TouchBufferSize)
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
	st := s.gcHooks.toucher.Stats()
	require.Equal(int64(3), st.Written)
	require.Equal(int64(1), st.MapRowsWritten)
}

// GCTouchedMapRows sizes the map row throttle, which the shards hold
// together: a row forgotten by a full shard is touched again on its next use.
func TestGCHooks_TouchedMapRows(t *testing.T) {
	var to entities.Hash
	rows := []entities.Hash{offsetKey(1), offsetKey(2)}
	for _, tt := range []struct {
		size     int
		perShard int
		touches  int
	}{
		{0, DefaultGCTouchedMapRows / cacheShards, 2},
		{cacheShards, 1, 3},
		{2 * cacheShards, 2, 2},
		{1, 1, 3},
	} {
		t.Run(fmt.Sprint(tt.size), func(t *testing.T) {
			require := require.New(t)
			s := newGCHooksServer(t, &NewServerOpts{GCTouchInterval: time.Hour, GCTouchedMapRows: tt.size})
			g := &s.gcHooks
			g.toucher.Close()
			fake := &fakeToucher{}
			g.toucher = fake
			// Both rows are in the shard of the first byte 0.
			s.touchMapRow(rows[0], to)
			s.touchMapRow(rows[1], to)
			s.touchMapRow(rows[0], to)
			require.Len(fake.mapRows, tt.touches)

			shard := g.touchedMapRows[0]
			for i := range tt.perShard + 1 {
				var from entities.Hash
				binary.BigEndian.PutUint32(from[1:], uint32(i))
				shard.Add(mapRowKey{from: from}, 1)
			}
			require.Equal(tt.perShard, shard.Len())
		})
	}
	s, _ := createTestServerWithCASDB(t)
	require.ErrorContains(t, s.startGCHooks(context.Background(), &NewServerOpts{GC: true, GCTouchedMapRows: -1}), "invalid negative number of touched map rows")
}

// A map row touch that the toucher dropped is tried again on the next use
// of the row, and one it queued is not tried again within the period.
func TestGCHooks_RetriesADroppedMapRowTouch(t *testing.T) {
	require := require.New(t)
	now := uint32(1)
	setCacheNow(t, &now)
	s := newGCHooksServer(t, &NewServerOpts{GCTouchInterval: time.Hour})
	g := &s.gcHooks
	g.toucher.Close()
	fake := &fakeToucher{dropMapRows: true}
	g.toucher = fake
	from, to := offsetKey(1), offsetKey(2)
	s.touchMapRow(from, to)
	s.touchMapRow(from, to)
	require.Len(fake.mapRows, 2)
	fake.dropMapRows = false
	s.touchMapRow(from, to)
	s.touchMapRow(from, to)
	require.Len(fake.mapRows, 3)
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
	s.storeHeader(&ObjectHeader{Id: key, Type: "blob", Size: 5}, cacheNow())
	s.storeContent(key, content, cacheNow())
	err = readObject(s, key)
	require.ErrorIs(err, entities.ErrNotExist)
	_, ok := s.headers[key[0]].Peek(key)
	require.False(ok, "header of a missing object stays cached")
	_, ok = s.content[key[0]].Peek(key)
	require.False(ok, "content of a missing object stays cached")
}

// An object stored again with new content while a server read the old one
// is not missing: the read fails and tells to read again.
func TestGCHooks_ChunkMissingObjectStoredAgain(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s := newGCHooksServer(t, &NewServerOpts{})
	key := putBlob(t, s, "hello")
	require.NoError(readObject(s, key))
	old, err := s.casdb.GetContent(ctx, key)
	require.NoError(err)

	content, err := s.depot.Store(ctx, key.String()+"-again", bytes.NewReader([]byte("blob 5\x00hello")))
	require.NoError(err)
	require.NoError(s.casdb.AddObject(ctx, &casdb.ObjectHeader{Id: key, Type: "blob", Size: 5}, &casdb.ObjectContent{Encoding: old.Encoding, Content: content}))
	require.NoError(s.depot.Remove(ctx, old.Content))

	err = readObject(s, key)
	require.ErrorContains(err, "stored again while it was read")
	require.False(entities.IsNotExist(err), err)
	require.NoError(readObject(s, key))
}

// offsetKey returns a key whose touch periods start offset seconds before
// those of a key with offset zero, for touch intervals above offset.
func offsetKey(offset uint32) entities.Hash {
	var key entities.Hash
	binary.BigEndian.PutUint32(key[len(key)-4:], offset)
	return key
}

// periodEnd returns the first time after now at which the touch period of key
// is not the one at now.
func periodEnd(g *gcHooks, key entities.Hash, now uint32) uint32 {
	p := g.touchPeriod(key, now)
	lo, hi := now, now+g.touchInterval
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		if g.touchPeriod(key, mid) == p {
			lo = mid
		} else {
			hi = mid
		}
	}
	return hi
}

// The touch mark of a period repeats every 255 periods; an entry loaded 255
// or more periods ago is touched again even when its mark matches.
func TestGCHooks_TouchMarkWraps(t *testing.T) {
	require := require.New(t)
	g := &gcHooks{touchInterval: 10}
	for _, off := range []uint32{0, 4} {
		key := offsetKey(off)
		mark := func(now uint32) CachedHeader {
			return CachedHeader{Loaded: now, Touched: touchMark(g.touchPeriod(key, now))}
		}
		h := mark(5)
		require.Equal(touchMark(g.touchPeriod(key, 5)), touchMark(g.touchPeriod(key, 5+2550)))
		require.False(g.touchedThisPeriod(key, h, 5+2550), "a mark 255 periods old counts as this period")
		h = mark(5 + 2550)
		require.True(g.touchedThisPeriod(key, h, 5+2550))
		require.False(g.touchedThisPeriod(key, CachedHeader{Loaded: 5}, 5), "an entry without a touch")
	}

	// The guard counts the periods of the object, shifted by its offset. An
	// entry loaded at 6 is in period 1 with an offset of 4, and in period 0
	// without it. A mark set in period 255 is 254 periods after the load and
	// counts; one set at the load counts again in period 256, exactly 255
	// periods later, and must not.
	key := offsetKey(4)
	require.Equal(uint64(1), g.touchPeriod(key, 6))
	require.Equal(uint64(255), g.touchPeriod(key, 2546))
	require.Equal(uint64(256), g.touchPeriod(key, 2556))
	require.True(g.touchedThisPeriod(key, CachedHeader{Loaded: 6, Touched: touchMark(255)}, 2546))
	require.Equal(touchMark(1), touchMark(256))
	require.False(g.touchedThisPeriod(key, CachedHeader{Loaded: 6, Touched: touchMark(1)}, 2556))
}

// Each object's touch periods are shifted by an offset taken from its key, so
// that marks set at the same time expire at different times. The periods keep
// the touch interval as their length: a mark is current only within the
// interval after its touch.
func TestGCHooks_TouchPeriodOffset(t *testing.T) {
	require := require.New(t)
	g := &gcHooks{touchInterval: 10}
	a, b := offsetKey(0), offsetKey(4)
	ha := CachedHeader{Loaded: 5, Touched: touchMark(g.touchPeriod(a, 5))}
	hb := CachedHeader{Loaded: 5, Touched: touchMark(g.touchPeriod(b, 5))}
	require.True(g.touchedThisPeriod(a, ha, 5))
	require.True(g.touchedThisPeriod(b, hb, 5))
	require.True(g.touchedThisPeriod(a, ha, 9))
	require.False(g.touchedThisPeriod(b, hb, 6), "b's period ends at 6")
	require.True(g.touchedThisPeriod(a, ha, 9))
	require.False(g.touchedThisPeriod(a, ha, 10), "a's period ends at 10")

	g.touchInterval = 36 * 3600
	ends := make(map[uint32]bool)
	for i := range 1000 {
		key := entities.Hash(sha1.Sum(fmt.Appendf(nil, "offset %d", i)))
		for _, at := range []uint32{1, 12345, 1 << 30} {
			end := periodEnd(g, key, at)
			require.Equal(g.touchPeriod(key, at)+1, g.touchPeriod(key, end))
			require.Equal(g.touchPeriod(key, end), g.touchPeriod(key, end+g.touchInterval-1), "a period shorter than the interval")
			require.Equal(g.touchPeriod(key, end)+1, g.touchPeriod(key, end+g.touchInterval), "a period longer than the interval")
			h := CachedHeader{Loaded: at, Touched: touchMark(g.touchPeriod(key, at))}
			require.True(g.touchedThisPeriod(key, h, end-1))
			require.False(g.touchedThisPeriod(key, h, end))
		}
		ends[periodEnd(g, key, 1)] = true
	}
	require.Greater(len(ends), 900, "the periods of most objects end at different times")
}

// A server touches again at the end of each object's own period, so two
// objects first used at the same time get their next touches at different
// times.
func TestGCHooks_TouchPeriodsEndPerObject(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	now := uint32(1)
	setCacheNow(t, &now)
	s := newGCHooksServer(t, &NewServerOpts{GCTouchInterval: time.Hour})
	a, b := putBlob(t, s, "first"), putBlob(t, s, "second")
	g := &s.gcHooks
	g.toucher.Close()
	fake := &fakeToucher{}
	g.toucher = fake
	_, err := s.Headers(ctx, []entities.Hash{a, b})
	require.NoError(err)
	require.ElementsMatch([]entities.Hash{a, b}, fake.waited)

	first, second := a, b
	if periodEnd(g, b, now) < periodEnd(g, a, now) {
		first, second = b, a
	}
	require.Less(periodEnd(g, first, now), periodEnd(g, second, now))
	now = periodEnd(g, first, now)
	_, err = s.Headers(ctx, []entities.Hash{a, b})
	require.NoError(err)
	require.ElementsMatch([]entities.Hash{a, b, first}, fake.waited)
	now = periodEnd(g, second, now)
	_, err = s.Headers(ctx, []entities.Hash{a, b})
	require.NoError(err)
	require.ElementsMatch([]entities.Hash{a, b, first, second}, fake.waited)
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

// rescueClientContext returns the context of a call from a client behind a
// load balancer.
func rescueClientContext() context.Context {
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 9), Port: 4711}})
	return metadata.NewIncomingContext(ctx, metadata.Pairs("x-forwarded-for", "10.1.2.3, 10.0.0.9", "client-version", "0.1.23"))
}

var requestIDPattern = regexp.MustCompile(`request ([0-9a-f]+)`)

// A rescue is counted by call and type and logged with the client, as the
// load balancer forwards it, the cycle of the marker and a request id that
// the object line and the call line share and the next call does not.
func TestGCHooks_Metrics(t *testing.T) {
	require := require.New(t)
	ctx := rescueClientContext()
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	s := newGCHooksServer(t, &NewServerOpts{GCTouchInterval: time.Hour})
	key := putBlob(t, s, "hello")
	condemnObject(t, s, key, 3)
	logged := captureLog(t)
	_, err := s.Headers(ctx, []entities.Hash{key})
	require.NoError(err)
	lines := strings.Split(strings.TrimSpace(logged.String()), "\n")
	require.Len(lines, 2, logged.String())
	require.Contains(lines[0], fmt.Sprintf("Rescued blob %s in headers, condemned by GC cycle 3: request ", key))
	req := requestIDPattern.FindStringSubmatch(lines[0])
	require.NotNil(req, lines[0])
	require.Len(req[1], 16)
	require.Contains(lines[1], `Rescued 1 condemned objects of 1 read in headers, 0 failed: request `+req[1]+`, peer 10.0.0.9:4711, x-forwarded-for "10.1.2.3, 10.0.0.9", client-version "0.1.23"`)

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
				if call, ok := dp.Attributes.Value("call"); ok {
					if typ, ok := dp.Attributes.Value("type"); ok {
						got[m.Name+"/"+call.AsString()+"/"+typ.AsString()] = dp.Value
					}
				}
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
	require.Equal(int64(1), got["abfs.gcp.gc.rescues/headers/blob"])
	require.Equal(int64(0), got["abfs.gcp.gc.rescues/headers/tree"])
	require.Contains(got, "abfs.gcp.gc.rescues/get_object/other")
	require.Equal(int64(0), got["abfs.gcp.gc.rescue_failures/headers"])
	require.Contains(got, "abfs.gcp.gc.rescue_failures/headers")
	require.Contains(got, "abfs.gcp.gc.touches/dropped")
	require.Contains(got, "abfs.gcp.gc.touches/object")
	require.Contains(got, "abfs.gcp.gc.touches/map_row")
	require.Contains(got, "abfs.gcp.cache.ttl_expiries/content")
	require.Contains(got, "abfs.gcp.gc.map_results_dropped")
	require.Contains(got, "abfs.gcp.gc.heartbeat_failures")

	condemnObject(t, s, key, 4)
	logged.Reset()
	_, err = s.Headers(ctx, []entities.Hash{key})
	require.NoError(err)
	next := requestIDPattern.FindStringSubmatch(logged.String())
	require.NotNil(next, logged.String())
	require.NotEqual(req[1], next[1])
}

// The object lines and the call lines of the rescue log are limited per
// minute, and the next window says how many the limits left out.
func TestRescueLog_LimitsLines(t *testing.T) {
	require := require.New(t)
	logged := captureLog(t)
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	l := newRescueLog()
	l.now = func() time.Time { return now }
	hdr := &ObjectHeader{Id: entities.EmptyDirHash, Type: "tree"}
	for range rescueKeyLines + 5 {
		l.object(pathHeaders, "r1", hdr, []byte{0, 0, 0, 0, 0, 0, 0, 2})
	}
	require.Equal(rescueKeyLines, strings.Count(logged.String(), "condemned by GC cycle 2"))
	// GetObject logs one call per object.
	for range rescueCallLines + 3 {
		l.call(context.Background(), pathGetObject, "r1", 1, 1, 0)
	}
	require.Equal(rescueCallLines, strings.Count(logged.String(), "Rescued 1 condemned objects of 1 read in get_object"))
	now = now.Add(rescueLogWindow)
	l.object(pathHeaders, "r1", hdr, []byte{0, 0, 0, 0, 0, 0, 0, 2})
	require.Contains(logged.String(), "Left out the lines of 5 rescued objects in the minute from 2026-11-07T12:00:00Z")
	require.Contains(logged.String(), "Left out the lines of 3 calls with rescues in the minute from 2026-11-07T12:00:00Z")
	require.Equal(rescueKeyLines+1, strings.Count(logged.String(), "condemned by GC cycle 2"))
	l.call(context.Background(), pathGetObject, "r1", 1, 1, 0)
	require.Equal(rescueCallLines+1, strings.Count(logged.String(), "Rescued 1 condemned objects of 1 read in get_object"))
}

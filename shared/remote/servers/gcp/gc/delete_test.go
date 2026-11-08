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

package gc

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/google/abfs/shared/entities"
)

// stepFixture holds objects in an in-process Bigtable, every one condemned
// by the list of cycle 4, for a delete step of cycle 5 over a mark without
// roots, in which every object is unreachable.
type stepFixture struct {
	t      *testing.T
	client *bigtable.Client
	bucket *storage.BucketHandle
	now    time.Time
	keys   []Hash
	shards int
}

const (
	stepListCycle = 4
	stepCycle     = 5
)

func newStepFixture(t *testing.T, shards int, opts ...grpc.DialOption) *stepFixture {
	t.Helper()
	return &stepFixture{t: t, client: newHooksTestBigtable(t, opts...), bucket: newTestBucket(t), now: time.Now(), shards: shards}
}

// object writes the gc row, first written long ago, and the objects row of
// key, with the marker of the list unless marker says otherwise.
func (f *stepFixture) object(key Hash, typ string, edges []Hash) {
	f.objectWith(key, typ, edges, markerValue(stepListCycle), true)
}

func (f *stepFixture) objectWith(key Hash, typ string, edges []Hash, marker []byte, row bool) {
	f.t.Helper()
	ctx := context.Background()
	old := bigtable.Time(f.now.Add(-1000 * time.Hour))
	gcRow := bigtable.NewMutation()
	gcRow.Set(EdgesFamily, LinksColumn, old, PackEdges(edges))
	gcRow.Set(EdgesFamily, TypeColumn, old, []byte(typ))
	gcRow.Set(EdgesFamily, VersionColumn, old, versionBytes(ExtractorVersion))
	require.NoError(f.t, f.client.Open(TableName).Apply(ctx, key.String(), gcRow))
	if row {
		obj := bigtable.NewMutation()
		obj.Set(objectsMetaFamily, "type", old, []byte(typ))
		obj.Set(objectsMetaFamily, "size", old, versionBytes(7))
		obj.Set(objectsDataFamily, objectsContentColumn, old, fmt.Appendf(nil, `[{"size":7,"depot":"bucket","id":"%s-u-0"}]`, key))
		if marker != nil {
			obj.Set(ObjectsFamily, CondemnedColumn, bigtable.Now(), marker)
		}
		require.NoError(f.t, f.client.Open(objectsTable).Apply(ctx, key.String(), obj))
	}
	f.keys = append(f.keys, key)
}

func (f *stepFixture) mapRow(from, to Hash, at time.Time) {
	f.t.Helper()
	mut := bigtable.NewMutation()
	mut.Set(mapsFamily, "", bigtable.Time(at), []byte("1"))
	require.NoError(f.t, f.client.Open(mapsTableName).Apply(context.Background(), from.String()+"#"+to.String(), mut))
}

func (f *stepFixture) bounds() []string {
	f.t.Helper()
	bounds, err := splitRange("", "", f.shards)
	require.NoError(f.t, err)
	return prefixBounds(bounds)
}

// mark scans the tables as a cycle does, with no root, and plans a delete
// from a list of every key written, for the types allows accepts.
func (f *stepFixture) mark(allows func(string) bool) *Result {
	f.t.Helper()
	ctx := context.Background()
	bounds := f.bounds()
	g, _, err := scanNodes(ctx, f.client.Open(TableName), bounds, f.now.Add(-DefaultGrace))
	require.NoError(f.t, err)
	_, _, err = scanEdges(ctx, f.client.Open(TableName), bounds, g)
	require.NoError(f.t, err)
	maps, err := scanMaps(ctx, f.client.Open(mapsTableName), bounds, g)
	require.NoError(f.t, err)
	live := newBitmap(g.size())
	scan, err := checkConsistency(ctx, f.client.Open(objectsTable), bounds, g, live, f.now.Add(-time.Minute), true)
	require.NoError(f.t, err)
	res := &Result{
		Report: &Report{Cycle: stepCycle, SnapshotTime: f.now},
		graph:  g, live: live, seen: scan.seen, unreachable: scan.unreachable, bounds: bounds, maps: maps,
	}
	keys := slices.Clone(f.keys)
	slices.SortFunc(keys, compareHash)
	l := &condemnedList{header: &listHeader{cycle: stepListCycle}, keys: keys}
	if allows == nil {
		allows = func(string) bool { return true }
	}
	res.plan = planDeletes(res, l, f.now.Add(-DefaultMapRetention).UnixMicro(), allows)
	return res
}

// deleter returns a deleter of cycle 5 whose writes check lease.
func (f *stepFixture) deleter(lease *Lease, change func(o *DeleteOpts)) *deleter {
	o := DefaultDeleteOpts()
	o.StateName, o.Holder, o.Shards, o.DeleteRate = DefaultStateName, "collector", f.shards, 0
	o.BatchSize = DefaultCondemnBatch
	if change != nil {
		change(&o)
	}
	return &deleter{opts: &o, bt: f.client, bucket: f.bucket, budget: writeBudget{deadline: o.WriteDeadline, lease: lease}, rep: &DeleteReport{}}
}

// farLease is a lease that does not expire during a test.
func farLease() *Lease {
	l := &Lease{Epoch: 3}
	l.expiry.Store(time.Now().Add(time.Hour).UnixNano())
	return l
}

func (f *stepFixture) rowExists(table string, key string) bool {
	f.t.Helper()
	row, err := f.client.Open(table).ReadRow(context.Background(), key)
	require.NoError(f.t, err)
	return len(row) > 0
}

// The plan puts each candidate below its deepest candidate parent, blocks a
// candidate with an unreachable parent that has an objects row and is not
// a candidate, keeps a candidate on a cycle of edges, and fails every key
// below one the cycle may not delete. A parent without an objects row
// blocks nothing: nothing can read it.
func TestPlanDeletes(t *testing.T) {
	require := require.New(t)
	f := newStepFixture(t, 4)
	h := testHashes(12)
	blob, tree, commit, deep := h[0], h[1], h[2], h[3]
	f.object(blob, "blob", nil)
	f.object(tree, "tree", []Hash{blob})
	f.object(commit, "commit", []Hash{tree, blob})
	// deep links blob too, and is itself below commit only through tree.
	f.object(deep, "tree", []Hash{blob})
	f.object(h[4], "tree", []Hash{deep})
	f.object(h[5], "commit", []Hash{h[4]})
	// A parent outside the list with an objects row blocks its child.
	outside, child := h[6], h[7]
	f.object(child, "blob", nil)
	f.objectWith(outside, "tree", []Hash{child}, nil, true)
	f.keys = slices.DeleteFunc(f.keys, func(k Hash) bool { return k == outside })
	// A parent in the list without an objects row is no candidate and blocks
	// nothing.
	ghost, ghostChild := h[8], h[9]
	f.objectWith(ghost, "tree", []Hash{ghostChild}, nil, false)
	f.object(ghostChild, "blob", nil)
	// Two keys that link each other.
	f.object(h[10], "tree", []Hash{h[11]})
	f.object(h[11], "tree", []Hash{h[10]})

	res := f.mark(nil)
	p := res.plan
	level := func(k Hash) int {
		i := p.index(res.graph.find(k))
		for l, ids := range p.levels {
			if slices.Contains(ids, int32(i)) {
				return l
			}
		}
		return -1
	}
	require.Equal(-1, p.index(res.graph.find(ghost)))
	require.Equal(-1, p.index(res.graph.find(outside)))
	require.Equal(1, level(commit))
	require.Equal(2, level(tree))
	require.Equal(1, level(h[5]))
	require.Equal(2, level(h[4]))
	require.Equal(3, level(deep))
	require.Equal(4, level(blob))
	require.Equal(1, level(ghostChild))
	outcome := func(k Hash) keyOutcome { return p.outcome[p.index(res.graph.find(k))] }
	require.Equal(outcomeBlocked, outcome(child))
	require.Equal(outcomePending, outcome(ghostChild))
	require.Equal(outcomeUnplaced, outcome(h[10]))
	require.Equal(outcomeUnplaced, outcome(h[11]))
	rep := p.report
	require.Equal(int64(10), rep.Candidates)
	require.Equal(int64(1), rep.BlockedByNoncandidateParent)
	require.Equal([]string{outside.String() + ">" + child.String()}, rep.BlockedSamples)
	require.Equal(int64(2), rep.Unplaced)
	require.Equal(int64(7), rep.WouldDelete)
	require.Equal(int64(7*7), rep.WouldDeleteBytes)
	require.Equal(4, rep.Levels)
	require.Equal(&PlanTypeCount{Candidates: 3, WouldDelete: 2, WouldDeleteBytes: 14}, rep.Types["blob"])

	// With only blobs allowed, every blob below a tree or commit stays.
	p = f.mark(func(t string) bool { return t == "blob" }).plan
	require.Equal(int64(1), p.report.WouldDelete)
	require.Equal(int64(5), p.report.TypeNotAllowed)
	require.Equal(outcomeTypeNotAllowed, p.outcome[p.index(res.graph.find(commit))])
	require.Equal(outcomeBlocked, p.outcome[p.index(res.graph.find(blob))])
	require.Equal(outcomePending, p.outcome[p.index(res.graph.find(ghostChild))])
}

// A map row goes only when the cycle may delete the types of both its ends,
// and one that names a candidate and stays keeps that candidate. An eval row
// past the map retention whose ends are live goes only when the cycle may
// delete eval requests and responses.
func TestPlanDeletes_MapRowTypes(t *testing.T) {
	require := require.New(t)
	f := newStepFixture(t, 4)
	h := testHashes(6)
	old := f.now.Add(-1000 * time.Hour)
	// Both ends are candidates.
	req, resp := h[0], h[1]
	f.object(req, "eval-request", nil)
	f.object(resp, "eval-response", nil)
	f.mapRow(req, resp, old)
	// The request is outside the list, the response a candidate.
	outReq, outResp := h[2], h[3]
	f.objectWith(outReq, "eval-request", nil, nil, true)
	f.object(outResp, "eval-response", nil)
	f.keys = slices.DeleteFunc(f.keys, func(k Hash) bool { return k == outReq })
	f.mapRow(outReq, outResp, old)
	// Both ends live.
	liveReq, liveResp := h[4], h[5]
	f.object(liveReq, "eval-request", nil)
	f.object(liveResp, "eval-response", nil)
	f.mapRow(liveReq, liveResp, old)

	res := f.mark(nil)
	for _, k := range []Hash{liveReq, liveResp} {
		res.live.set(res.graph.find(k))
	}
	keys := slices.Clone(f.keys)
	slices.SortFunc(keys, compareHash)
	l := &condemnedList{header: &listHeader{cycle: stepListCycle}, keys: keys}
	plan := func(allows func(string) bool) *deletePlan {
		return planDeletes(res, l, f.now.Add(-DefaultMapRetention).UnixMicro(), allows)
	}
	rows := func(p *deletePlan) []string {
		var keys []string
		for _, r := range p.mapRows {
			keys = append(keys, r.key)
		}
		return keys
	}
	row := func(from, to Hash) string { return from.String() + "#" + to.String() }

	p := plan(func(string) bool { return true })
	require.ElementsMatch([]string{row(req, resp), row(outReq, outResp), row(liveReq, liveResp)}, rows(p))
	require.Equal(int64(2), p.report.MapRows)
	require.Equal(int64(1), p.report.ExpiredEvalMapRows)
	require.Zero(p.report.MapRowsKeptForTypes)
	require.Equal(int64(3), p.report.WouldDelete)

	p = plan(func(t string) bool { return t == "eval-request" })
	require.Empty(rows(p))
	require.Equal(int64(2), p.report.MapRowsKeptForTypes)
	require.Zero(p.report.ExpiredEvalMapRows)
	outcome := func(k Hash) keyOutcome { return p.outcome[p.index(res.graph.find(k))] }
	require.Equal(outcomeBlocked, outcome(req))
	require.Equal(outcomeTypeNotAllowed, outcome(resp))
	require.Equal(outcomeTypeNotAllowed, outcome(outResp))
	require.Zero(p.report.WouldDelete)
}

// A delete record reads back as written, and keeps the content descriptor
// only of an object with a chunk outside the inline depot.
func TestDeleteRecord_WriteRead(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	bucket := newTestBucket(t)
	h := testHashes(3)
	entries := []recordEntry{
		{kind: recordMapRow, key: h[0], mapTo: h[1]},
		{kind: recordObject, key: h[1], content: []byte(`[{"size":40000,"depot":"bigtable","id":"x-u-0"}]`)},
		{kind: recordObject, key: h[2]},
	}
	hdr := &recordHeader{cycle: 5, runID: "collector#3", gcRowsAfter: time.UnixMicro(1000), listComplete: time.UnixMicro(2000)}
	name := recordObjectName("default", 5, 1, 3, 0)
	require.Equal("gc/deleted/default/5/1-3-0", name)
	require.NoError(writeRecord(ctx, writeBudget{deadline: DefaultWriteDeadline}, bucket, name, hdr, entries))
	got, gotEntries, err := readRecord(ctx, bucket, name)
	require.NoError(err)
	require.Equal(hdr, got)
	require.Equal(entries, gotEntries)
	require.ErrorContains(writeRecord(ctx, writeBudget{deadline: DefaultWriteDeadline}, bucket, name, &recordHeader{runID: "other#1"}, nil), `written by the GC run "collector#3"`)

	require.Nil(recordedContent([]byte(`[{"size":4,"depot":"inline","id":"abcd"}]`)))
	two := []byte(`[{"size":1048576,"depot":"bigtable","id":"x-u-0"},{"size":5,"depot":"bucket","id":"x-u-1"}]`)
	require.Equal(two, recordedContent(two))
	require.Equal([]byte("not json"), recordedContent([]byte("not json")))
}

// call is a conditional write the delete step sent.
type call struct {
	table, key string
	start, end time.Time
}

// callLog records the conditional writes of a Bigtable client and the row
// counts of its reads of the objects table; slow delays the writes to the
// keys it matches.
type callLog struct {
	mu        sync.Mutex
	calls     []call
	inFlight  map[int]int
	maxFlight map[int]int
	maxRead   int
	rangeOf   func(key string) int
	slow      func(key string) bool
	slowRead  func(keys [][]byte) time.Duration
	after     func(c call)
}

func (l *callLog) options() []grpc.DialOption {
	l.inFlight, l.maxFlight = make(map[int]int), make(map[int]int)
	unary := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		m, ok := req.(*bigtablepb.CheckAndMutateRowRequest)
		if !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		c := call{table: m.GetTableName()[strings.LastIndex(m.GetTableName(), "/")+1:], key: string(m.GetRowKey()), start: time.Now()}
		rng := l.rangeOf(c.key)
		if c.table != TableName {
			l.mu.Lock()
			l.inFlight[rng]++
			l.maxFlight[rng] = max(l.maxFlight[rng], l.inFlight[rng])
			l.mu.Unlock()
		}
		if l.slow != nil && l.slow(c.key) {
			time.Sleep(20 * time.Millisecond)
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		c.end = time.Now()
		l.mu.Lock()
		if c.table != TableName {
			l.inFlight[rng]--
		}
		l.calls = append(l.calls, c)
		l.mu.Unlock()
		if l.after != nil {
			l.after(c)
		}
		return err
	}
	stream := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || !strings.HasSuffix(method, "/ReadRows") {
			return s, err
		}
		return &readLog{ClientStream: s, l: l}, nil
	}
	return []grpc.DialOption{grpc.WithUnaryInterceptor(unary), grpc.WithStreamInterceptor(stream)}
}

type readLog struct {
	grpc.ClientStream
	l *callLog
}

func (r *readLog) SendMsg(m any) error {
	if req, ok := m.(*bigtablepb.ReadRowsRequest); ok && strings.HasSuffix(req.GetTableName(), "/"+objectsTable) {
		keys := req.GetRows().GetRowKeys()
		r.l.mu.Lock()
		r.l.maxRead = max(r.l.maxRead, len(keys))
		r.l.mu.Unlock()
		if r.l.slowRead != nil {
			time.Sleep(r.l.slowRead(keys))
		}
	}
	return r.ClientStream.SendMsg(m)
}

// rangeTree is a fixture of two object levels and map rows in every range:
// trees on level 1, each linking blobs on level 2, and map rows from trees
// to blobs.
type rangeTree struct {
	trees, blobs []Hash
	level        map[string]int
}

func newRangeTree(f *stepFixture, rangeOf func(string) int) *rangeTree {
	rt := &rangeTree{level: make(map[string]int)}
	keys := testHashes(2000)
	perRange := make(map[int]int)
	var rest []Hash
	for _, k := range keys {
		if r := rangeOf(k.String()); perRange[r] < 3 {
			perRange[r]++
			rt.trees = append(rt.trees, k)
			continue
		}
		rest = append(rest, k)
	}
	children := make([][]Hash, len(rt.trees))
	for i, b := range rest[:40*len(rt.trees)] {
		children[i%len(rt.trees)] = append(children[i%len(rt.trees)], b)
		rt.blobs = append(rt.blobs, b)
		f.object(b, "blob", nil)
		rt.level[b.String()] = 2
	}
	for i, tr := range rt.trees {
		f.object(tr, "tree", children[i])
		rt.level[tr.String()] = 1
		if i%3 != 2 {
			f.mapRow(tr, children[i][0], f.now.Add(-1000*time.Hour))
			rt.level[tr.String()+"#"+children[i][0].String()] = 0
		}
	}
	return rt
}

// The delete step deletes the map rows first, then each level in every
// range before the next level starts, with one worker per range that sends
// its keys in key order with its share of the calls in flight, and reads at
// most 100 rows per request. A range whose record cannot be written stops
// the step: no later level starts, the calls already sent end with an
// outcome, and every key not deleted stays to be condemned again.
func TestDeleteLevel_RangeWorkers(t *testing.T) {
	// With two calls in flight per range, the order in which the server sees
	// two of them is up to the scheduler, so the key order is checked with
	// one.
	for _, workers := range []int{DefaultDeleteWorkers, 16} {
		t.Run(fmt.Sprintf("all levels, %d workers", workers), func(t *testing.T) { testRangeWorkers(t, workers) })
	}
	t.Run("record write fails", testRangeWorkersRecordFails)
}

func testRangeWorkers(t *testing.T, workers int) {
	require := require.New(t)
	ctx := context.Background()
	log := &callLog{}
	f := &stepFixture{t: t, now: time.Now(), shards: 16}
	s := &step{bounds: f.bounds()}
	log.rangeOf = s.rangeOf
	log.slow = func(key string) bool { return s.rangeOf(key) == 5 }
	f.client, f.bucket = newHooksTestBigtable(t, log.options()...), newTestBucket(t)
	rt := newRangeTree(f, s.rangeOf)
	res := f.mark(nil)
	require.Equal(int64(len(rt.trees)+len(rt.blobs)), res.plan.report.WouldDelete)
	require.Equal(2, res.plan.report.Levels)

	d := f.deleter(farLease(), func(o *DeleteOpts) { o.DeleteWorkers = workers })
	deleted, err := d.deleteStep(ctx, farLease(), res)
	require.NoError(err)
	rep := d.rep.Step
	require.Empty(rep.Error)
	require.Equal(int64(len(rt.trees)+len(rt.blobs)), rep.Deleted)
	require.Equal(deleted.count(), rep.Deleted)
	require.Equal(int64(len(rt.trees)*2/3), rep.MapRowsDeleted)
	require.Equal(rep.Deleted, rep.GCRowsDeleted)
	require.Equal(int64(3), int64(len(rep.Levels)))
	for _, k := range append(slices.Clone(rt.trees), rt.blobs...) {
		require.False(f.rowExists(objectsTable, k.String()), k)
		require.False(f.rowExists(TableName, k.String()), k)
	}

	levelOf := func(c call) int {
		if c.table == mapsTableName {
			return 0
		}
		return rt.level[c.key]
	}
	end := make(map[int]time.Time)
	for _, c := range log.calls {
		end[levelOf(c)] = maxTime(end[levelOf(c)], c.end)
	}
	for _, c := range log.calls {
		if l := levelOf(c); l > 0 {
			require.False(c.start.Before(end[l-1]), "a call of level %d started before level %d ended", l, l-1)
		}
	}
	share := workers / 16
	for rng, n := range log.maxFlight {
		require.LessOrEqual(n, share, "range %d", rng)
	}
	require.Equal(share, log.maxFlight[5])
	if share == 1 {
		byRange := make(map[[2]int][]call)
		for _, c := range log.calls {
			if c.table != TableName {
				id := [2]int{levelOf(c), s.rangeOf(c.key)}
				byRange[id] = append(byRange[id], c)
			}
		}
		for id, calls := range byRange {
			slices.SortFunc(calls, func(a, b call) int { return a.start.Compare(b.start) })
			require.True(slices.IsSortedFunc(calls, func(a, b call) int { return strings.Compare(a.key, b.key) }), "level %d range %d", id[0], id[1])
		}
	}
	require.Positive(log.maxRead)
	require.LessOrEqual(log.maxRead, deleteReadBatch)
	require.Greater(len(rt.blobs)/16, deleteReadBatch, "a range must hold more keys than one read")
}

func testRangeWorkersRecordFails(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	log := &callLog{}
	f := &stepFixture{t: t, now: time.Now(), shards: 16}
	s := &step{bounds: f.bounds()}
	log.rangeOf = s.rangeOf
	var slow atomic.Bool
	log.slowRead = func(keys [][]byte) time.Duration {
		if slow.Load() && len(keys) > 0 && s.rangeOf(string(keys[0])) == 3 {
			return 300 * time.Millisecond
		}
		return 0
	}
	f.client, f.bucket = newHooksTestBigtable(t, log.options()...), newTestBucket(t)
	rt := newRangeTree(f, s.rangeOf)
	taken := f.bucket.Object(recordObjectName(DefaultStateName, stepCycle, 1, 3, 0)).NewWriter(ctx)
	taken.Metadata = map[string]string{runIDMetadata: "another-collector#9"}
	require.NoError(taken.Close())
	res := f.mark(nil)

	d := f.deleter(farLease(), nil)
	slow.Store(true)
	deleted, err := d.deleteStep(ctx, farLease(), res)
	slow.Store(false)
	require.NoError(err)
	rep := d.rep.Step
	require.Contains(rep.Error, `written by the GC run "another-collector#9"`)
	require.Zero(rep.Unknown)
	var deletedTrees int
	for _, c := range log.calls {
		require.NotEqual(2, rt.level[c.key], "a call of level 2 was sent")
	}
	p := res.plan
	for i, id := range p.cands {
		key := res.graph.key(id).String()
		gone := !f.rowExists(objectsTable, key)
		require.Equal(gone, deleted.has(id), key)
		require.Equal(gone, p.outcome[i] == outcomeDeleted, key)
		if gone {
			deletedTrees++
			require.Equal(1, rt.level[key])
		}
	}
	require.Positive(deletedTrees, "no other range matched a key before the failure")
	// No key of the range whose record failed was deleted.
	for _, tr := range rt.trees {
		if s.rangeOf(tr.String()) == 3 {
			require.True(f.rowExists(objectsTable, tr.String()), tr)
		}
	}
	for _, c := range log.calls {
		if c.table == objectsTable {
			require.True(deleted.has(res.graph.find(entities.NewHash(c.key))), "key %s was sent but is not deleted", c.key)
		}
	}
	for _, b := range rt.blobs {
		oc := p.outcome[p.index(res.graph.find(b))]
		require.True(oc == outcomeBlocked || oc == outcomeNotSent, "blob %s: %d", b, oc)
	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// No conditional write starts once the wall clock is within two write
// deadlines and the clock skew margin of the expiry of the lease, so that a
// collector that lost its lease stops deleting before another can take it
// over. The keys not reached stay to be condemned again.
func TestDeleteGC_StopsBeforeLeaseExpiry(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	start := time.Now()
	var clock atomic.Int64
	clock.Store(start.UnixNano())
	orig := wallNow
	wallNow = func() time.Time { return time.Unix(0, clock.Load()) }
	t.Cleanup(func() { wallNow = orig })
	lease := &Lease{Epoch: 3}
	expiry := start.Add(time.Hour)
	lease.expiry.Store(expiry.UnixNano())

	log := &callLog{rangeOf: func(string) int { return 0 }}
	var objectCalls atomic.Int32
	log.after = func(c call) {
		if c.table == objectsTable && objectCalls.Add(1) == 3 {
			clock.Store(expiry.Add(-2*DefaultWriteDeadline - clockSkewMargin + time.Second).UnixNano())
		}
	}
	f := newStepFixture(t, 1, log.options()...)
	for _, k := range testHashes(10) {
		f.object(k, "blob", nil)
	}
	res := f.mark(nil)
	d := f.deleter(lease, func(o *DeleteOpts) { o.DeleteWorkers = 1 })
	deleted, err := d.deleteStep(ctx, lease, res)
	require.NoError(err)
	rep := d.rep.Step
	require.Equal(errLeaseExpiring.Error(), rep.Error)
	require.Equal(int32(3), objectCalls.Load())
	var gcCalls int
	for _, c := range log.calls {
		if c.table == TableName {
			gcCalls++
		}
	}
	require.Equal(2, gcCalls)
	require.Equal(int64(3), rep.Deleted)
	require.Equal(int64(3), deleted.count())
	require.Equal(int64(7), rep.NotSent)
	require.Equal(int64(1), rep.GCRowErrors)
	require.Zero(rep.Unknown)
}

// A map row whose result has no gc row still names its request: the scan
// keeps its key, and the delete step removes it before the request.
func TestMapScan_KeepsRawKeys(t *testing.T) {
	require := require.New(t)
	f := newStepFixture(t, 4)
	h := testHashes(2)
	req, noNode := h[0], h[1]
	f.object(req, "blob", nil)
	f.mapRow(req, noNode, f.now.Add(-1000*time.Hour))
	res := f.mark(nil)
	require.Equal(int64(1), res.plan.report.MapRows)

	d := f.deleter(farLease(), nil)
	_, err := d.deleteStep(context.Background(), farLease(), res)
	require.NoError(err)
	require.Equal(int64(1), d.rep.Step.MapRowsDeleted)
	require.Equal(int64(1), d.rep.Step.Deleted)
	require.False(f.rowExists(mapsTableName, req.String()+"#"+noNode.String()))
}

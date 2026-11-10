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
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/objects/gitobjects"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// deleteFixture is a condemnFixture whose cycles may delete: one server
// writes a live GcServers heartbeat whose touch stats it read after every
// snapshot of the test, and fakeChecks answers for the Bigtable instance.
type deleteFixture struct {
	*condemnFixture
	checks *fakeChecks
}

func newDeleteFixture(t *testing.T) *deleteFixture {
	t.Helper()
	f := &deleteFixture{condemnFixture: newCondemnFixture(t), checks: &fakeChecks{clusters: []string{"c1"}}}
	f.server("srv-0-aa", false)
	return f
}

// server writes the heartbeat of a server process; lost records a lost touch.
func (f *deleteFixture) server(id string, lost bool) {
	f.t.Helper()
	s := &gc.Server{ID: id, Version: gc.ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: time.Hour, HeartbeatInterval: 5 * time.Minute, LinkValidation: true}
	require.NoError(f.t, gc.WriteServer(context.Background(), f.s.casdb.SpannerClient(), s, time.Now().Add(time.Hour), lost))
}

// fakeChecks answers the checks of the Bigtable instance. hook runs once, at
// the next probe: after the mark of a cycle and before its delete step.
type fakeChecks struct {
	mu         sync.Mutex
	clusters   []string
	probeErr   error
	backups    []gc.Backup
	backupsErr error
	hook       func()
	probes     int
}

var _ gc.InstanceChecks = (*fakeChecks)(nil)

func (c *fakeChecks) Clusters(context.Context) ([]string, error) { return c.clusters, nil }

func (c *fakeChecks) Backups(context.Context) ([]gc.Backup, error) { return c.backups, c.backupsErr }

func (c *fakeChecks) ProbeConditionalWrite(context.Context) error {
	c.mu.Lock()
	c.probes++
	hook := c.hook
	c.hook = nil
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	return c.probeErr
}

func (f *deleteFixture) deleteOpts() *gc.DeleteOpts {
	o := gc.DefaultDeleteOpts()
	o.Shards, o.ExpectedReplicas, o.Rate, o.BatchSize, o.DeleteRate, o.NoBackupCheck = 3, 1, 0, 2, 0, true
	return &o
}

// delete runs a delete cycle and returns its result.
func (f *deleteFixture) delete(o *gc.DeleteOpts) *gc.Result {
	f.t.Helper()
	res, err := f.deleteWith(f.s.casdb.BTClient(), o)
	require.NoError(f.t, err)
	return res
}

// deleteWith runs a delete cycle whose collector uses the Bigtable client bt.
func (f *deleteFixture) deleteWith(bt *bigtable.Client, o *gc.DeleteOpts) (*gc.Result, error) {
	if o == nil {
		o = f.deleteOpts()
	}
	return gc.Delete(context.Background(), f.s.casdb.SpannerClient(), bt, f.s.storage.Bucket(f.bucket), f.s, f.checks, o)
}

// deletes runs a delete cycle that must not refuse.
func (f *deleteFixture) deletes(o *gc.DeleteOpts) *gc.Result {
	f.t.Helper()
	res := f.delete(o)
	require.Empty(f.t, res.Report.Delete.Refused)
	require.NotNil(f.t, res.Report.Delete.Step)
	return res
}

// age moves the times of the list in GcState back, as if it had been taken
// 8 h ago and completed 7 h ago, and GcReplicas had been empty since long
// before the window of the next cycle, so that the timing bounds of a delete
// from it hold now.
func (f *deleteFixture) age() {
	f.t.Helper()
	now := time.Now()
	_, err := f.s.casdb.SpannerClient().Apply(context.Background(), []*spanner.Mutation{spanner.Update("GcState",
		[]string{"Name", "ListSnapshotTime", "ListCompleteTime", "LegacyServersGoneTime"},
		[]any{gc.DefaultStateName, now.Add(-8 * time.Hour), now.Add(-7 * time.Hour), now.Add(-100 * time.Hour)})})
	require.NoError(f.t, err)
}

// row returns the cells of the row key of table.
func (f *deleteFixture) row(table string, key entities.Hash) bigtable.Row {
	f.t.Helper()
	row, err := f.s.casdb.BTClient().Open(table).ReadRow(context.Background(), key.String())
	require.NoError(f.t, err)
	return row
}

func (f *deleteFixture) requireGone(keys ...entities.Hash) {
	f.t.Helper()
	for _, k := range keys {
		require.Empty(f.t, f.row("objects", k), "objects row of %s", k)
		f.s.forget(k)
		require.True(f.t, entities.IsNotExist(readObject(f.s, k)), "object %s", k)
	}
}

func (f *deleteFixture) requirePresent(keys ...entities.Hash) {
	f.t.Helper()
	for _, k := range keys {
		f.s.forget(k)
		require.NoError(f.t, readObject(f.s, k), "object %s", k)
	}
}

// newInterceptedClient returns a Bigtable client of the emulator whose calls
// pass through the interceptors.
func newInterceptedClient(t *testing.T, opts ...grpc.DialOption) *bigtable.Client {
	t.Helper()
	conn, err := grpc.NewClient(os.Getenv("BIGTABLE_EMULATOR_HOST"), append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	bt, err := bigtable.NewClient(context.Background(), emulatorProject, emulatorBigtable, option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bt.Close() })
	return bt
}

// condMutations intercepts the conditional mutations of a client: before
// runs ahead of each one and may answer it with an error, after runs once
// it returned.
func condMutations(before func(table, key string) error, after func(table, key string)) grpc.DialOption {
	return grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		m, ok := req.(*bigtablepb.CheckAndMutateRowRequest)
		if !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		table, key := m.GetTableName()[strings.LastIndex(m.GetTableName(), "/")+1:], string(m.GetRowKey())
		if before != nil {
			if err := before(table, key); err != nil {
				return err
			}
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		if after != nil {
			after(table, key)
		}
		return err
	})
}

func (f *deleteFixture) setMarker(key entities.Hash, cells ...[]byte) {
	f.t.Helper()
	mut := bigtable.NewMutation()
	mut.DeleteCellsInColumn(gc.ObjectsFamily, gc.CondemnedColumn)
	for i, c := range cells {
		mut.Set(gc.ObjectsFamily, gc.CondemnedColumn, bigtable.Time(time.Now().Add(time.Duration(i)*time.Second)), c)
	}
	require.NoError(f.t, f.s.casdb.BTClient().Open("objects").Apply(context.Background(), key.String(), mut))
}

// A delete removes an object only while the newest cell of its marker holds
// exactly the number of the list it deletes from, read in the conditional
// write itself: a marker changed between the read before the delete and the
// delete keeps the object. A row deleted in between is counted deleted and
// gets no row back.
func TestDeleteGC_MatchesOnlyTheExactMarker(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	exact := f.blob("exact marker", f.old)
	older := f.blob("older marker", f.old)
	newer := f.blob("newer marker", f.old)
	none := f.blob("marker cleared", f.old)
	twoCells := f.blob("old cell matches, newest does not", f.old)
	gone := f.blob("row deleted", f.old)
	f.delete(nil)
	f.age()

	changes := map[string]func(){
		older.String():    func() { f.setMarker(older, marker(0)) },
		newer.String():    func() { f.setMarker(newer, marker(2)) },
		none.String():     func() { f.setMarker(none) },
		twoCells.String(): func() { f.setMarker(twoCells, marker(1), marker(0)) },
		gone.String(): func() {
			mut := bigtable.NewMutation()
			mut.DeleteRow()
			require.NoError(f.s.casdb.BTClient().Open("objects").Apply(context.Background(), gone.String(), mut))
		},
	}
	var mu sync.Mutex
	bt := newInterceptedClient(t, condMutations(func(table, key string) error {
		mu.Lock()
		change := changes[key]
		delete(changes, key)
		mu.Unlock()
		if table == "objects" && change != nil {
			change()
		}
		return nil
	}, nil))
	res, err := f.deleteWith(bt, nil)
	require.NoError(err)
	require.Empty(res.Report.Delete.Refused)
	step := res.Report.Delete.Step
	require.Equal(int64(2), step.Deleted)
	require.Equal(int64(4), step.NotMatched)
	f.requireGone(exact, gone)
	f.requirePresent(older, newer, none, twoCells)
	// The cycle condemns the kept keys again and writes nothing to the
	// deleted ones.
	for _, k := range []entities.Hash{older, newer, none, twoCells} {
		require.Equal([][]byte{marker(2)}, f.markerCells(k), k)
	}
	require.Equal(int64(4), res.Report.Condemn.Condemned)
}

// A key whose row is gone between the consistency scan and the read of the
// delete step is not condemned again, so that its marker does not write a
// row that holds only the marker.
func TestDeleteGC_GoneRowGetsNoMarker(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	gone := f.blob("row gone before the delete step", f.old)
	kept := f.blob("rescued before the delete step", f.old)
	f.delete(nil)
	f.age()

	f.checks.hook = func() {
		del := bigtable.NewMutation()
		del.DeleteRow()
		require.NoError(f.s.casdb.BTClient().Open("objects").Apply(context.Background(), gone.String(), del))
		f.setMarker(kept)
	}
	res := f.deletes(nil)
	step := res.Report.Delete.Step
	require.Equal(int64(1), step.Gone)
	require.Equal(int64(1), step.NotMatched)
	require.Zero(step.Deleted)
	require.Empty(f.row("objects", gone))
	require.Equal([][]byte{marker(2)}, f.markerCells(kept))
	require.Equal(int64(1), res.Report.Condemn.Condemned)
}

// A server that reads a condemned commit between the mark and the delete
// step rescues it, and the step then keeps the commit and everything below
// it, which the next cycle finds live through the rescue's touch.
func TestDeleteGC_RescuedParentKeepsItsChildren(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	blob := f.blob("file", f.old)
	tree := f.tree(f.old, blob)
	commit := f.commit(tree, "dropped", f.old)
	other := f.blob("not read", f.old)
	f.delete(nil)
	f.age()

	require.NoError(f.s.enableGCHooks(&NewServerOpts{GCTouchInterval: time.Hour}))
	t.Cleanup(f.s.stopGCHooks)
	f.checks.hook = func() {
		f.s.forget(commit)
		require.NoError(readObject(f.s, commit))
	}
	res := f.deletes(nil)
	step := res.Report.Delete.Step
	require.Equal(int64(1), step.NotMatched)
	require.Equal([]string{commit.String()}, step.NotMatchedSample)
	require.Equal(int64(2), step.Blocked)
	require.Equal(int64(1), step.Deleted)
	f.requireGone(other)
	f.requirePresent(commit, tree, blob)

	res = f.run(nil)
	requireReachable(t, res, true, commit, tree, blob)
	require.Zero(res.Report.Delete.Plan.WouldDelete)
}

// An object written again after its row was deleted keeps its new row and
// its gc row, whose links are newer than the grace start; the record of the
// batch holds the content descriptor of the deleted row.
func TestDeleteGC_RePutBetweenRowAndGCRowDelete(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newDeleteFixture(t)
	content := "written again"
	key := f.blob(content, f.old)
	oldDesc := f.row("objects", key)["data"]
	var done atomic.Bool
	bt := newInterceptedClient(t, condMutations(nil, func(table, k string) {
		if table == "objects" && k == key.String() && !done.Swap(true) {
			f.s.forget(key)
			f.blob(content, time.Now())
		}
	}))
	f.delete(nil)
	f.age()

	res, err := f.deleteWith(bt, nil)
	require.NoError(err)
	step := res.Report.Delete.Step
	require.Equal(int64(1), step.Deleted)
	require.Equal(int64(1), step.GCRowsKept)
	require.Zero(step.GCRowsDeleted)
	f.requirePresent(key)
	require.NotEmpty(f.row(gc.TableName, key)[gc.EdgesFamily])
	newDesc := f.row("objects", key)["data"]
	require.NotEqual(oldDesc, newDesc)

	var records [][]byte
	it := f.s.storage.Bucket(f.bucket).Objects(ctx, &storage.Query{Prefix: step.Records})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(err)
		r, err := f.s.storage.Bucket(f.bucket).Object(attrs.Name).NewReader(ctx)
		require.NoError(err)
		b, err := io.ReadAll(r)
		require.NoError(err)
		require.NoError(r.Close())
		records = append(records, b)
	}
	require.Len(records, 1)
	for _, item := range oldDesc {
		if item.Column == "data:content" {
			require.True(bytes.Contains(records[0], item.Value), "the record lacks the deleted descriptor")
		}
	}
	res = f.run(nil)
	requireReachable(t, res, true, key)
}

// The map rows that name a candidate go before any object: a row deleted,
// a row written or used since the grace start is kept and keeps both its
// ends, as does one whose delete fails; a row whose two ends are of types the
// cycle may not delete stays. An eval row past the map retention whose two
// ends stay is deleted, unless it was used since that threshold.
func TestDeleteGC_MapRows(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	old := f.old
	env := f.object(&binding.EnvVars{Vars: map[string]string{"PATH": "/bin"}}, old)
	f.ref("platform/a", "refs/heads/env", env, old)
	evalRow := func(name string, at time.Time) (req, resp entities.Hash) {
		req = f.object(&binding.EvalRequest{Version: binding.CurrentEvalRequestVersion, Command: name, EnvVars: env}, old)
		resp = f.object(&binding.EvalResponse{Version: binding.CurrentEvalResponseVersion, EvalRequest: req}, old)
		f.mapRow(req, resp, at)
		return req, resp
	}
	delReq, delResp := evalRow("deleted", old)
	usedReq, usedResp := evalRow("used after the scan", old)
	failReq, failResp := evalRow("delete fails", old)
	tree := f.tree(old, f.blob("tree of a dead deep tree", old))
	dtReq := f.object(&objects.DeepTreeRequest{Version: objects.CurrentDeepTreeRequestVersion, Tree: tree}, old)
	dt := f.object(&gitobjects.DeepTreeObject{DeepTree: entities.DeepTree{Entries: []*entities.DeepEntry{{Name: "d", Mode: entities.FileModeDir, Hash: tree}}}}, old)
	f.mapRow(dtReq, dt, old)
	// Eval rows whose ends a ref keeps.
	expReq, expResp := evalRow("expired", old)
	f.ref("platform/a", "refs/heads/exp", expResp, old)
	touchedReq, touchedResp := evalRow("expired, used after the scan", old)
	f.ref("platform/a", "refs/heads/touched", touchedResp, old)
	recentReq, recentResp := evalRow("recent", time.Now().Add(-time.Hour))
	f.ref("platform/a", "refs/heads/recent", recentResp, old)
	f.delete(nil)
	f.age()

	f.checks.hook = func() {
		f.mapRow(usedReq, usedResp, time.Now())
		f.mapRow(touchedReq, touchedResp, time.Now())
	}
	var mu sync.Mutex
	var order []string
	failRow := failReq.String() + "#" + failResp.String()
	bt := newInterceptedClient(t, condMutations(func(table, key string) error {
		mu.Lock()
		order = append(order, table)
		mu.Unlock()
		if key == failRow {
			return status.Error(codes.PermissionDenied, "refused by the test")
		}
		return nil
	}, nil))
	o := f.deleteOpts()
	for _, t := range gc.TypeNames() {
		if t != string(objects.ObjectTypeDeepTreeRequest) && t != string(gitobjects.ObjectTypeDeepTree) {
			o.Types = append(o.Types, t)
		}
	}
	res, err := f.deleteWith(bt, o)
	require.NoError(err)
	require.Empty(res.Report.Delete.Refused)
	plan, step := res.Report.Delete.Plan, res.Report.Delete.Step
	require.Equal(int64(3), plan.MapRows)
	require.Equal(int64(1), plan.MapRowsKeptForTypes)
	require.Equal(int64(2), plan.ExpiredEvalMapRows)
	require.Equal(int64(2), step.MapRowsDeleted)
	require.Equal(int64(2), step.MapRowsKept)
	require.Equal(int64(1), step.MapRowErrors)
	firstObject := slices.Index(order, "objects")
	require.Positive(firstObject)
	require.NotContains(order[firstObject:], "maps", "a map row delete came after an object delete")

	mapRowExists := func(from, to entities.Hash) bool {
		row, err := f.s.casdb.BTClient().Open("maps").ReadRow(context.Background(), from.String()+"#"+to.String())
		require.NoError(err)
		return len(row) > 0
	}
	require.False(mapRowExists(delReq, delResp))
	f.requireGone(delReq, delResp)
	require.True(mapRowExists(usedReq, usedResp))
	require.True(mapRowExists(failReq, failResp))
	f.requirePresent(usedReq, usedResp, failReq, failResp)
	require.True(mapRowExists(dtReq, dt))
	f.requirePresent(dtReq, dt, tree)
	require.Equal(int64(2), step.TypeNotAllowed)
	require.False(mapRowExists(expReq, expResp))
	require.True(mapRowExists(touchedReq, touchedResp))
	require.True(mapRowExists(recentReq, recentResp))
	f.requirePresent(expReq, expResp, touchedReq, touchedResp, recentReq, recentResp)
}

// A refused delete never stops the condemn: the cycle publishes and
// completes its list and marks what is unreachable, and the next cycle
// deletes from that list. A probe error that is not a refusal code, a
// second cluster and a backup listing the library cannot parse each refuse
// with a reason.
func TestDeleteGC_RefusedCycleCondemns(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	key := f.blob("unreachable file", f.old)
	o := f.deleteOpts()
	o.NoBackupCheck = false
	ready := func() {
		f.checks.clusters, f.checks.probeErr, f.checks.backupsErr = []string{"c1"}, nil, nil
		f.checks.backups = nil
		for _, table := range []string{"objects", gc.TableName, "maps", "chunks"} {
			f.checks.backups = append(f.checks.backups, gc.Backup{Name: table + "-b", Table: table, Start: time.Now(), Ready: true})
		}
	}
	tests := []struct {
		name   string
		change func()
		want   string
	}{
		{"no list", func() {}, "no condemn cycle published a list to delete from"},
		{"two clusters", func() { f.checks.clusters = []string{"c1", "c2"} }, "the Bigtable instance has 2 clusters"},
		{"probe fails", func() { f.checks.probeErr = status.Error(codes.Internal, "injected") }, "the conditional write probe failed: rpc error: code = Internal desc = injected"},
		{"backup being created", func() {
			f.checks.backupsErr = errors.New("failed to parse backup proto invalid endTime: nil Timestamp")
		}, "(a backup that is still being created fails the listing until it is ready)"},
		{"backup older than the list", func() { f.checks.backups[0].Start = time.Now().Add(-8 * time.Hour) }, "no ready backup of Bigtable table objects started after the list was completed"},
	}
	for i, tt := range tests {
		ready()
		tt.change()
		res := f.delete(o)
		r := res.Report
		// The first cycle also finds GcReplicas empty only now.
		if i > 0 {
			require.Len(r.Delete.Refused, 1, tt.name)
		}
		require.True(slices.ContainsFunc(r.Delete.Refused, func(s string) bool { return strings.Contains(s, tt.want) }), "%s: %q", tt.name, r.Delete.Refused)
		require.Nil(r.Delete.Step, tt.name)
		// The chunk step of older records runs in a refused cycle too.
		require.NotNil(r.Delete.Chunks, tt.name)
		require.Equal(int64(1), r.Condemn.Condemned, tt.name)
		require.Equal([][]byte{marker(byte(i + 1))}, f.markerCells(key), tt.name)
		location, complete := f.listState()
		require.Equal(r.Condemn.List, location.StringVal, tt.name)
		require.True(complete.Bool, tt.name)
		f.age()
	}
	ready()
	res := f.delete(o)
	require.Empty(res.Report.Delete.Refused)
	require.Equal(int64(1), res.Report.Delete.Step.Deleted)
	f.requireGone(key)
}

// A mount without the keep-alive that abfs unmount ended refuses no deletes
// of blobs; a live one does.
func TestDeleteGC_EndedMountWithoutKeepAlive(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	recent := time.Now().Add(-time.Hour)
	f.mount("refs/abfs/mount/user/u/1", nil, recent)
	f.ref(entities.ABFSMetaProject, "refs/abfs/unmount/user/u/1", f.blob("unmount details", f.old), recent)
	key := f.blob("unreachable file", f.old)
	_, err := f.condemn(nil)
	require.NoError(err)
	f.age()

	res := f.deletes(nil)
	require.Equal(int64(1), res.Report.Refs[gc.RefMount].Ended)
	require.Zero(res.Report.LiveMounts.WithoutKeepAlive)
	require.Empty(res.Report.Delete.Warnings)
	require.Equal(int64(1), res.Report.Delete.Step.Deleted)
	f.requireGone(key)

	f.mount("refs/abfs/mount/user/u/2", nil, recent)
	f.age()
	res = f.delete(nil)
	require.Equal(int64(1), res.Report.LiveMounts.WithoutKeepAlive)
	require.True(slices.ContainsFunc(res.Report.Delete.Refused, func(s string) bool {
		return strings.Contains(s, "1 live mounts run without the GC keep-alive")
	}), "%q", res.Report.Delete.Refused)
	require.Nil(res.Report.Delete.Step)
}

// The cycle runs its steps in order: a refusal of the condemn checks stops
// the cycle before any delete check; the checks of the instance run before
// the markers of live keys are cleared, the delete step after that and
// before the cycle's own markers; and the second read of the servers runs
// after the first checks, so that a touch lost in between refuses the
// deletes.
func TestDeleteGC_CycleOrder(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newDeleteFixture(t)
	live := f.blob("reached again", f.old)
	dead := f.blob("deleted", f.old)
	f.delete(nil)
	f.age()
	f.ref("platform/a", "refs/heads/main", f.tree(f.old, live), f.old)
	newer := f.blob("unreachable since the list", f.old)

	var mu sync.Mutex
	var events []string
	add := func(e string) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	f.checks.hook = func() { add("checks") }
	mutateRows := grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || !strings.HasSuffix(method, "/MutateRows") {
			return s, err
		}
		return &mutateLog{ClientStream: s, add: add}, nil
	})
	bt := newInterceptedClient(t, mutateRows, condMutations(func(table, key string) error {
		if table == "objects" {
			add("delete")
		}
		return nil
	}, nil))
	res, err := f.deleteWith(bt, nil)
	require.NoError(err)
	require.Equal(int64(1), res.Report.Delete.Step.Deleted)
	require.Equal([]string{"checks", "clear", "delete", "mark"}, slices.Compact(events))
	f.requireGone(dead)
	require.Equal([][]byte{marker(2)}, f.markerCells(newer))

	// A touch lost after the first read of the servers refuses at the second.
	other := f.blob("kept by a lost touch", f.old)
	f.condemn(nil)
	f.age()
	f.checks.hook = func() { f.server("srv-0-aa", true) }
	res = f.delete(nil)
	require.Len(res.Report.Delete.Refused, 1)
	require.Contains(res.Report.Delete.Refused[0], "server srv-0-aa lost touches at")
	require.Nil(res.Report.Delete.Step)
	f.requirePresent(other, newer)

	// With no server heartbeat the condemn checks refuse, and no delete
	// check runs.
	_, err = f.s.casdb.SpannerClient().Apply(ctx, []*spanner.Mutation{spanner.Delete("GcServers", spanner.AllKeys())})
	require.NoError(err)
	probes := f.checks.probes
	_, err = f.deleteWith(f.s.casdb.BTClient(), nil)
	var refused *gc.ErrCondemnRefused
	require.ErrorAs(err, &refused)
	require.Equal(probes, f.checks.probes)
}

// mutateLog records whether each bulk write of the objects table clears
// markers or sets them.
type mutateLog struct {
	grpc.ClientStream
	add func(string)
}

func (m *mutateLog) SendMsg(msg any) error {
	if req, ok := msg.(*bigtablepb.MutateRowsRequest); ok && strings.HasSuffix(req.GetTableName(), "/objects") {
		e := "clear"
		for _, entry := range req.GetEntries() {
			for _, mut := range entry.GetMutations() {
				if mut.GetSetCell() != nil {
					e = "mark"
				}
			}
		}
		m.add(e)
	}
	return m.ClientStream.SendMsg(msg)
}

// A delete whose outcome is unknown after its retries is read again at the
// end of the step: a key whose row stays is condemned again with its child,
// which it blocked; a key whose row is gone counts as deleted and gets no
// marker. A read that fails stops the cycle before its list, so that the
// next cycle does not delete from the list of the cycle before.
func TestDeleteGC_UnknownOutcome(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	kept := f.blob("child of the kept parent", f.old)
	keptParent := f.tree(f.old, kept)
	child := f.blob("child of the deleted parent", f.old)
	parent := f.tree(f.old, child)
	f.delete(nil)
	f.age()

	unavailable := status.Error(codes.Unavailable, "injected")
	bt := newInterceptedClient(t, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		m, ok := req.(*bigtablepb.CheckAndMutateRowRequest)
		switch {
		case !ok || !strings.HasSuffix(m.GetTableName(), "/objects"):
		case string(m.GetRowKey()) == keptParent.String():
			return unavailable
		case string(m.GetRowKey()) == parent.String():
			_ = invoker(ctx, method, req, reply, cc, opts...)
			return unavailable
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))
	res, err := f.deleteWith(bt, nil)
	require.NoError(err)
	step := res.Report.Delete.Step
	require.Equal(int64(2), step.Unknown)
	require.Equal(int64(1), step.UnknownPresent)
	require.Equal(int64(1), step.UnknownDeleted)
	require.Equal(int64(2), step.Blocked)
	require.Equal(int64(1), step.Deleted)
	f.requireGone(parent)
	f.requirePresent(keptParent, kept, child)
	for _, k := range []entities.Hash{keptParent, kept, child} {
		require.Equal([][]byte{marker(2)}, f.markerCells(k), k)
	}
	require.Equal(int64(3), res.Report.Condemn.Condemned)

	// The next cycle cannot read the kept parent again.
	f.age()
	var failReads atomic.Bool
	reads := grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || !strings.HasSuffix(method, "/ReadRows") {
			return s, err
		}
		return &failingRead{ClientStream: s, key: keptParent.String(), fail: &failReads}, nil
	})
	bt = newInterceptedClient(t, reads, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if m, ok := req.(*bigtablepb.CheckAndMutateRowRequest); ok && string(m.GetRowKey()) == keptParent.String() {
			failReads.Store(true)
			return unavailable
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))
	_, err = f.deleteWith(bt, nil)
	require.ErrorContains(err, "whose delete had an unknown outcome")
	location, _ := f.listState()
	require.Equal("gc/condemned/default/2", location.StringVal)
	f.requirePresent(keptParent, kept)

	f.age()
	res = f.delete(nil)
	require.Len(res.Report.Delete.Refused, 1)
	require.Contains(res.Report.Delete.Refused[0], "but cycle 3 was the last condemn or delete cycle and did not complete its own list")
}

// failingRead fails a read of the objects table that names key once fail is
// set.
type failingRead struct {
	grpc.ClientStream
	key    string
	fail   *atomic.Bool
	failed bool
}

func (r *failingRead) SendMsg(m any) error {
	if req, ok := m.(*bigtablepb.ReadRowsRequest); ok && r.fail.Load() && strings.HasSuffix(req.GetTableName(), "/objects") {
		for _, k := range req.GetRows().GetRowKeys() {
			if string(k) == r.key {
				r.failed = true
			}
		}
	}
	return r.ClientStream.SendMsg(m)
}

func (r *failingRead) RecvMsg(m any) error {
	if r.failed {
		return status.Error(codes.PermissionDenied, "read refused by the test")
	}
	return r.ClientStream.RecvMsg(m)
}

// Two condemn cycles and a delete cycle over objects of every retention
// class delete exactly the objects that were unreachable in both marks,
// with the map rows that name them, and leave every object a root reaches
// readable. The delete cycle writes no marker to a deleted key and deletes
// what the plan of the condemn cycle before it counted. A key of the list
// whose objects row was removed before the scan is no candidate and keeps
// none of its children.
func TestDeleteGC_EndToEnd(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	old, recentRef := f.old, time.Now().Add(-time.Hour)
	var live, dead []entities.Hash
	keep := func(k ...entities.Hash) { live = append(live, k...) }
	drop := func(k ...entities.Hash) { dead = append(dead, k...) }

	// A branch moved by a force push.
	oldBlob := f.blob("old file", old)
	oldTree := f.tree(old, oldBlob)
	oldCommit := f.commit(oldTree, "old", old)
	drop(oldBlob, oldTree, oldCommit)
	newBlob := f.blob("new file", old)
	newTree := f.tree(old, newBlob)
	newCommit := f.commit(newTree, "new", old)
	f.ref("platform/a", "refs/heads/main", newCommit, old)
	status := f.blob("status", old)
	f.ref("platform/a", binding.ProjectStatusRef, status, old)
	keep(newBlob, newTree, newCommit, status)

	// Builds and unmounts past their retention and within it.
	oldLog := f.blob("old ninja log", old)
	oldOutput := f.blob("old output", old)
	oldRootEnd := f.tree(old, oldOutput)
	oldBuild := f.object(&binding.BuildInfo{Version: binding.CurrentBuildInfoVersion, BuildStartData: binding.BuildStartData{BuildID: "b1"}, NinjaLog: oldLog.String(), RootEnd: oldRootEnd.String()}, old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/build/user/u/b1", oldBuild, old)
	drop(oldLog, oldOutput, oldRootEnd, oldBuild)
	newLog := f.blob("new ninja log", old)
	newBuild := f.object(&binding.BuildInfo{Version: binding.CurrentBuildInfoVersion, BuildStartData: binding.BuildStartData{BuildID: "b2"}, NinjaLog: newLog.String()}, old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/build/user/u/b2", newBuild, recentRef)
	keep(newLog, newBuild)
	oldUnmount := f.blob("old unmount details", old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/unmount/user/u/3", oldUnmount, old)
	drop(oldUnmount)
	newUnmount := f.blob("new unmount details", old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/unmount/user/u/4", newUnmount, recentRef)
	keep(newUnmount)

	// Eval maps unused past the map retention and used recently.
	env := f.object(&binding.EnvVars{Vars: map[string]string{"PATH": "/bin"}}, old)
	keep(env)
	evalRow := func(name string, at time.Time) []entities.Hash {
		out := f.blob(name+" output", old)
		req := f.object(&binding.EvalRequest{Version: binding.CurrentEvalRequestVersion, Command: name, EnvVars: env}, old)
		resp := f.object(&binding.EvalResponse{
			Version:     binding.CurrentEvalResponseVersion,
			EvalRequest: req,
			WritePatch:  entities.Patch{{Path: "out/" + name, Mode: entities.FileModeRegular, Hash: out}},
		}, old)
		f.mapRow(req, resp, at)
		return []entities.Hash{out, req, resp}
	}
	drop(evalRow("old step", old)...)
	keep(evalRow("new step", recentRef)...)

	// Pusher maps of a live tree and of a tree nothing reaches, and LFS.
	pusherRow := func(tree entities.Hash) []entities.Hash {
		req := f.object(&objects.DeepTreeRequest{Version: objects.CurrentDeepTreeRequestVersion, Tree: tree}, old)
		result := f.object(&gitobjects.DeepTreeObject{DeepTree: entities.DeepTree{Entries: []*entities.DeepEntry{{Name: "d", Mode: entities.FileModeDir, Hash: tree}}}}, old)
		f.mapRow(req, result, old)
		return []entities.Hash{req, result}
	}
	keep(pusherRow(newTree)...)
	drop(pusherRow(oldTree)...)
	lfsContent := f.blob("lfs content", old)
	lfsReq := f.object(&binding.LFSRequest{LFSOID: "oid"}, old)
	lfsRes := f.object(&binding.LFSResult{LFSRequest: lfsReq, ABFSOID: lfsContent}, old)
	f.mapRow(lfsReq, lfsRes, old)
	keep(lfsContent, lfsReq, lfsRes)

	// A live mount, an idle one, a workspace, recent and touched objects.
	pinnedBlob := f.blob("pinned file", old)
	pinnedTree := f.tree(old, pinnedBlob)
	pinnedCommit := f.commit(pinnedTree, "pinned", old)
	liveDetails, livePinned := f.mount("refs/abfs/mount/user/u/1", []string{pinnedCommit.String()}, recentRef)
	keep(pinnedBlob, pinnedTree, pinnedCommit, liveDetails, livePinned)
	idleBlob := f.blob("idle file", old)
	idleTree := f.tree(old, idleBlob)
	idleCommit := f.commit(idleTree, "idle", old)
	idleDetails, idlePinned := f.mount("refs/abfs/mount/user/u/2", []string{idleCommit.String()}, old)
	drop(idleBlob, idleTree, idleCommit, idleDetails, idlePinned)
	wsBlob := f.blob("workspace file", old)
	wsTree := f.tree(old, wsBlob)
	ws := f.workspace("refs/abfs/ws/user/u/ws1", entities.Hash{}, wsTree)
	keep(wsBlob, wsTree, ws)
	recentBlob := f.blob("reached by a recent tree", old)
	keep(recentBlob, f.tree(time.Now(), recentBlob))
	touched := f.blob("touched", old)
	f.touch(touched, time.Now())
	keep(touched)

	// A gc row without an objects row, and a parent whose objects row goes
	// after the first list.
	missing := objects.HashBlobBytes([]byte("never pushed"))
	danglingTree, _ := treeOf(missing)
	f.gcRow(danglingTree, "tree", []entities.Hash{missing}, old)
	orphan := f.blob("child of a parent without a row", old)
	rowless := f.tree(old, orphan)
	drop(orphan)

	_, err := f.condemn(nil)
	require.NoError(err)
	del := bigtable.NewMutation()
	del.DeleteRow()
	require.NoError(f.s.casdb.BTClient().Open("objects").Apply(context.Background(), rowless.String(), del))
	res, err := f.condemn(nil)
	require.NoError(err)
	plan := res.Report.Delete.Plan
	require.Equal(int64(len(dead)), plan.WouldDelete)
	require.Equal(int64(len(dead)), plan.Candidates)
	require.Zero(plan.BlockedByNoncandidateParent)
	f.age()

	o := f.deleteOpts()
	o.AcceptMountsWithoutKeepAlive = true
	res = f.deletes(o)
	r := res.Report
	require.Len(r.Delete.Warnings, 1)
	require.Contains(r.Delete.Warnings[0], "1 live mounts run without the GC keep-alive")
	step := r.Delete.Step
	require.Equal(int64(len(dead)), step.Deleted)
	require.Equal(r.Delete.Plan.WouldDelete, step.Deleted)
	require.Zero(step.NotMatched + step.Blocked + step.Unknown + step.Gone)
	require.Equal(int64(2), step.MapRowsDeleted)
	require.Equal(step.Deleted, step.GCRowsDeleted)
	require.Zero(r.Condemn.Condemned)
	f.requireGone(dead...)
	for _, k := range dead {
		require.Empty(f.row(gc.TableName, k), "gc row of %s", k)
	}
	f.requirePresent(live...)
	require.NotEmpty(f.row(gc.TableName, danglingTree))
	require.NotEmpty(f.row(gc.TableName, rowless))

	// The next cycle finds every remaining object where it was.
	res = f.run(nil)
	requireReachable(t, res, true, live...)
	require.Equal(int64(2), res.Report.Objects.Unreachable)
}

// pointReads calls read with the table and the row keys of each point read
// of a client, and range with the table of each scan of a key range.
func pointReads(read func(table string, keys [][]byte), ranges func(table string)) grpc.DialOption {
	return grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || !strings.HasSuffix(method, "/ReadRows") {
			return s, err
		}
		return &readSeen{ClientStream: s, read: read, ranges: ranges}, nil
	})
}

type readSeen struct {
	grpc.ClientStream
	read   func(table string, keys [][]byte)
	ranges func(table string)
}

func (r *readSeen) SendMsg(m any) error {
	if req, ok := m.(*bigtablepb.ReadRowsRequest); ok {
		table := req.GetTableName()[strings.LastIndex(req.GetTableName(), "/")+1:]
		if keys := req.GetRows().GetRowKeys(); len(keys) > 0 {
			r.read(table, keys)
		}
		if len(req.GetRows().GetRowRanges()) > 0 {
			r.ranges(table)
		}
	}
	return r.ClientStream.SendMsg(m)
}

// Only a delete cycle checks the objects written during its consistency
// scan, and it deletes when each of them has a gc row by the time it is
// checked: then it reads only their gc rows. A condemn cycle and a dry run
// count them and read nothing more.
func TestDeleteGC_ChecksObjectsWrittenDuringTheScan(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newDeleteFixture(t)
	dead := f.blob("deleted", f.old)
	const n = 250
	var written []entities.Hash
	isWritten := make(map[string]bool)
	for i := range n {
		content := fmt.Sprintf("written during the scan %d", i)
		key := objects.HashBlobBytes([]byte(content))
		require.NoError(putRaw(f.s, key, fmt.Appendf(nil, "blob %d\x00%s", len(content), content)))
		written = append(written, key)
		isWritten[key.String()] = true
	}

	var mu sync.Mutex
	reads := make(map[string]int)
	var scanStarted func()
	bt := newInterceptedClient(t, pointReads(func(table string, keys [][]byte) {
		mu.Lock()
		defer mu.Unlock()
		for _, k := range keys {
			if isWritten[string(k)] {
				reads[table]++
			}
		}
	}, func(table string) {
		mu.Lock()
		started := scanStarted
		mu.Unlock()
		if table == "objects" && started != nil {
			started()
		}
	}))
	readsOf := func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		out := maps.Clone(reads)
		clear(reads)
		return out
	}

	res, err := gc.Condemn(ctx, f.s.casdb.SpannerClient(), bt, f.s.storage.Bucket(f.bucket), f.s, &f.deleteOpts().CondemnOpts)
	require.NoError(err)
	require.Equal(int64(n), res.Report.Consistency.TooNew)
	require.Nil(res.Report.Delete)
	require.Empty(readsOf())

	o := f.deleteOpts().DryRunOpts
	res, err = gc.DryRun(ctx, f.s.casdb.SpannerClient(), bt, f.s.storage.Bucket(f.bucket), f.s, &o)
	require.NoError(err)
	require.Equal(int64(n), res.Report.Consistency.TooNew)
	require.Nil(res.Report.Delete.TooNew)
	require.Empty(readsOf())

	// The gc rows land once the scan of the table gc is done, as those of
	// uploads during the cycle do; the scan of the objects waits for them.
	f.age()
	var once sync.Once
	mu.Lock()
	scanStarted = func() {
		once.Do(func() {
			for _, k := range written {
				f.gcRow(k, "blob", nil, time.Now())
			}
		})
	}
	mu.Unlock()
	res, err = f.deleteWith(bt, nil)
	require.NoError(err)
	require.Equal(int64(n), res.Report.Consistency.TooNew)
	require.Equal(&gc.TooNewReport{Keys: n}, res.Report.Delete.TooNew)
	require.Empty(res.Report.Delete.Refused)
	require.Equal(int64(1), res.Report.Delete.Step.Deleted)
	require.Equal(map[string]int{gc.TableName: n}, readsOf())
	f.requireGone(dead)
}

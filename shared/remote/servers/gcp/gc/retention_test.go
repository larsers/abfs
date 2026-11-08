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
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
)

const retentionTestProject = 7

// refCommits sees the commits of a Spanner client that delete refs: it counts
// the refs each one deletes, and fail, when set, may answer the commit with
// the given index with an error instead.
type refCommits struct {
	mu      sync.Mutex
	deletes []int
	fail    func(i int) error
}

func (c *refCommits) option() option.ClientOption {
	return option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		commit, _ := req.(*spannerpb.CommitRequest)
		n := 0
		for _, m := range commit.GetMutations() {
			if d := m.GetDelete(); d.GetTable() == refsTable {
				n += len(d.GetKeySet().GetKeys())
			}
		}
		if n == 0 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		c.mu.Lock()
		i := len(c.deletes)
		c.deletes = append(c.deletes, n)
		fail := c.fail
		c.mu.Unlock()
		if fail != nil {
			if err := fail(i); err != nil {
				return err
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))
}

// refsFixture holds refs in a Spanner database of the server schema, and a
// bucket for the ref records.
type refsFixture struct {
	t       *testing.T
	sp      *spanner.Client
	bucket  *storage.BucketHandle
	commits *refCommits
	old     time.Time
}

func newRefsFixture(t *testing.T) *refsFixture {
	t.Helper()
	commits := &refCommits{}
	sp := newTestSpannerWith(t, append(casdb.SchemaStatements(), casdb.GCSchemaStatements()...), commits.option())
	f := &refsFixture{t: t, sp: sp, bucket: newTestBucket(t), commits: commits, old: time.Now().Add(-1000 * time.Hour)}
	_, err := sp.Apply(context.Background(), []*spanner.Mutation{
		spanner.Insert("Projects", []string{"ProjectId", "Host", "Project"}, []any{int64(retentionTestProject), "gerrit.example.com", entities.ABFSMetaProject}),
	})
	require.NoError(t, err)
	return f
}

// write writes refs with the update time and target, a zero target as NULL,
// and returns the commit time.
func (f *refsFixture) write(refs []string, updated time.Time, target Hash) time.Time {
	f.t.Helper()
	t := spanner.NullString{StringVal: target.String(), Valid: !target.IsZero()}
	var muts []*spanner.Mutation
	for _, ref := range refs {
		muts = append(muts, spanner.InsertOrUpdate(refsTable, []string{"ProjectId", "Ref", "UpdateTime", "Target"}, []any{int64(retentionTestProject), ref, updated, t}))
	}
	commit, err := f.sp.Apply(context.Background(), muts)
	require.NoError(f.t, err)
	return commit
}

// refs returns the refs that exist, by name.
func (f *refsFixture) refs() map[string]expiredRef {
	f.t.Helper()
	refs := make(map[string]expiredRef)
	err := f.sp.Single().Read(context.Background(), refsTable, spanner.AllKeys(), []string{"Ref", "UpdateTime", "Target"}).Do(func(row *spanner.Row) error {
		var e expiredRef
		var target spanner.NullString
		if err := row.Columns(&e.ref, &e.updated, &target); err != nil {
			return err
		}
		e.target = target.StringVal
		refs[e.ref] = e
		return nil
	})
	require.NoError(f.t, err)
	return refs
}

// snapshot reads the refs as a cycle with the snapshot does and returns the
// cycle's result.
func (f *refsFixture) snapshot(cycle int64, snapshot time.Time) *Result {
	f.t.Helper()
	o := DefaultDryRunOpts()
	snap, err := readRefs(context.Background(), f.sp, &o, snapshot)
	require.NoError(f.t, err)
	return &Result{Report: &Report{Cycle: cycle, SnapshotTime: snapshot}, expiredRefs: snap.expired}
}

func (f *refsFixture) deleter() *deleter {
	o := DefaultDeleteOpts()
	o.StateName, o.Holder = DefaultStateName, "collector"
	return &deleter{opts: &o, sp: f.sp, bucket: f.bucket, budget: writeBudget{deadline: o.WriteDeadline, lease: farLease()}}
}

func buildRefs(prefix string, n int) []string {
	refs := make([]string, n)
	for i := range refs {
		refs[i] = fmt.Sprintf("refs/abfs/build/user/u/%s%04d", prefix, i)
	}
	return refs
}

// A delete cycle records the refs past their retention before it removes
// any: when the record cannot be written, as when another run wrote one
// under the cycle's name, or the lease is about to expire, every ref stays.
// The record names each ref with its project, update time and target, so
// that it can be put back.
func TestRemoveExpiredRefs_RecordsFirst(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newRefsFixture(t)
	build, mount := entities.NewHash("1111111111111111111111111111111111111111"), entities.NewHash("2222222222222222222222222222222222222222")
	f.write([]string{"refs/abfs/build/user/u/b1"}, f.old, build)
	f.write([]string{"refs/abfs/mount/user/u/m1"}, f.old, mount)
	snapshot := f.write([]string{"refs/abfs/build/user/u/recent"}, time.Now(), build)

	res := f.snapshot(4, snapshot)
	require.Len(res.expiredRefs, 2)
	w := f.bucket.Object(refRecordName(DefaultStateName, 4)).NewWriter(ctx)
	w.Metadata = map[string]string{runIDMetadata: "other#1"}
	_, err := w.Write([]byte("{}"))
	require.NoError(err)
	require.NoError(w.Close())
	d := f.deleter()
	rep, err := d.removeExpiredRefs(ctx, farLease(), res)
	require.ErrorContains(err, "written by the GC run")
	require.Empty(rep.Record)
	require.NotEmpty(rep.Error)
	require.Len(f.refs(), 3)
	require.Empty(f.commits.deletes)

	expiring := &Lease{Epoch: 3}
	expiring.expiry.Store(time.Now().UnixNano())
	d.budget.lease = expiring
	res = f.snapshot(5, snapshot)
	_, err = d.removeExpiredRefs(ctx, expiring, res)
	require.ErrorIs(err, errLeaseExpiring)
	require.True(stops(ctx, err))
	require.Len(f.refs(), 3)
	_, err = f.bucket.Object(refRecordName(DefaultStateName, 5)).Attrs(ctx)
	require.ErrorIs(err, storage.ErrObjectNotExist)

	d = f.deleter()
	res = f.snapshot(6, snapshot)
	rep, err = d.removeExpiredRefs(ctx, farLease(), res)
	require.NoError(err)
	require.Equal(refRecordName(DefaultStateName, 6), rep.Record)
	require.Equal(map[RefClass]*RefRemovalCounts{RefBuild: {Recorded: 1, Removed: 1}, RefMount: {Recorded: 1, Removed: 1}}, rep.Classes)
	require.Equal([]string{"refs/abfs/build/user/u/recent"}, slices.Collect(maps.Keys(f.refs())))
	r, err := f.bucket.Object(rep.Record).NewReader(ctx)
	require.NoError(err)
	content, err := io.ReadAll(r)
	require.NoError(err)
	require.NoError(r.Close())
	var rec refRecord
	require.NoError(json.Unmarshal(content, &rec))
	require.Equal(int64(6), rec.Cycle)
	require.True(rec.Snapshot.Equal(snapshot))
	require.Equal("collector#3", rec.Run)
	require.Len(rec.Refs, 2)
	byRef := make(map[string]refRecordEntry)
	for _, e := range rec.Refs {
		byRef[e.Ref] = e
	}
	b := byRef["refs/abfs/build/user/u/b1"]
	require.Equal(RefBuild, b.Class)
	require.Equal(int64(retentionTestProject), b.ProjectID)
	require.Equal("gerrit.example.com", b.Host)
	require.Equal(entities.ABFSMetaProject, b.Project)
	require.Equal(build.String(), b.Target)
	require.True(b.UpdateTime.Equal(f.old), "%s", b.UpdateTime)
	require.Equal(mount.String(), byRef["refs/abfs/mount/user/u/m1"].Target)
}

// A ref is removed only while its update time and its target are the ones
// of the snapshot. One written again since, with the same target or another
// one, or set to NULL, stays; one already gone is counted apart.
func TestRemoveExpiredRefs_OnlyUnchangedRefs(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newRefsFixture(t)
	target, other := entities.NewHash("1111111111111111111111111111111111111111"), entities.NewHash("3333333333333333333333333333333333333333")
	names := buildRefs("b", 5)
	snapshot := f.write(names, f.old, target)
	res := f.snapshot(4, snapshot)
	require.Len(res.expiredRefs, 5)

	f.write(names[0:1], time.Now(), target)
	f.write(names[1:2], f.old, other)
	f.write(names[2:3], f.old, Hash{})
	_, err := f.sp.Apply(ctx, []*spanner.Mutation{spanner.Delete(refsTable, spanner.Key{int64(retentionTestProject), names[3]})})
	require.NoError(err)

	rep, err := f.deleter().removeExpiredRefs(ctx, farLease(), res)
	require.NoError(err)
	require.Equal(map[RefClass]*RefRemovalCounts{RefBuild: {Recorded: 5, Removed: 1, Changed: 3, Gone: 1}}, rep.Classes)
	refs := f.refs()
	require.Len(refs, 3)
	require.Equal(target.String(), refs[names[0]].target)
	require.Equal(other.String(), refs[names[1]].target)
	require.Empty(refs[names[2]].target)
	require.NotContains(refs, names[4])
}

// Each transaction removes at most 500 refs, so that it holds the locks of
// few refs that servers may write at the same time.
func TestRemoveExpiredRefs_Batches(t *testing.T) {
	require := require.New(t)
	f := newRefsFixture(t)
	snapshot := f.write(buildRefs("b", 1001), f.old, entities.NewHash("1111111111111111111111111111111111111111"))
	rep, err := f.deleter().removeExpiredRefs(context.Background(), farLease(), f.snapshot(4, snapshot))
	require.NoError(err)
	require.Equal(int64(1001), rep.Classes[RefBuild].Removed)
	require.Equal([]int{500, 500, 1}, f.commits.deletes)
	require.Empty(f.refs())
}

// A failed transaction stops the removal with its error, which refuses the
// deletes of the cycle; the refs removed before it stay removed, and the
// rest stay for the next cycle.
func TestRemoveExpiredRefs_FailedTransaction(t *testing.T) {
	require := require.New(t)
	f := newRefsFixture(t)
	snapshot := f.write(buildRefs("b", 1001), f.old, entities.NewHash("1111111111111111111111111111111111111111"))
	f.commits.fail = func(i int) error {
		if i == 1 {
			return status.Error(codes.PermissionDenied, "injected")
		}
		return nil
	}
	rep, err := f.deleter().removeExpiredRefs(context.Background(), farLease(), f.snapshot(4, snapshot))
	require.Error(err)
	require.Equal(codes.PermissionDenied, status.Code(err))
	require.Contains(rep.Error, "injected")
	require.Equal(&RefRemovalCounts{Recorded: 1001, Removed: 500}, rep.Classes[RefBuild])
	require.Len(f.refs(), 501)
	require.Equal([]int{500, 500}, f.commits.deletes)
}

// The pruning keeps the ref records of the newest cycles and leaves other
// names and other states alone.
func TestPruneRefRecords(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	bucket := newTestBucket(t)
	names := []string{"gc/refs/default/1", "gc/refs/default/2", "gc/refs/default/10", "gc/refs/default/11", "gc/refs/default/notes", "gc/refs/other/1"}
	for _, name := range names {
		w := bucket.Object(name).NewWriter(ctx)
		_, err := w.Write([]byte("{}"))
		require.NoError(err)
		require.NoError(w.Close())
	}
	n, err := pruneRefRecords(ctx, writeBudget{deadline: DefaultWriteDeadline, lease: farLease()}, bucket, DefaultStateName, 2)
	require.NoError(err)
	require.Equal(int64(2), n)
	for i, name := range names {
		_, err := bucket.Object(name).Attrs(ctx)
		if i < 2 {
			require.ErrorIs(err, storage.ErrObjectNotExist, name)
		} else {
			require.NoError(err, name)
		}
	}
}

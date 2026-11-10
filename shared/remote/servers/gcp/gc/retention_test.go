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
	"sync/atomic"
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

// A mount ref with an unmount ref of the same name in the same project is an
// ended mount: a root of its mount details, but no live mount. A mount ref
// without one, also one whose name differs only in the start time, whose
// unmount ref is in another project or whose unmount ref is a tombstone,
// stays live.
func TestReadRefs_UnmountEndsMount(t *testing.T) {
	require := require.New(t)
	f := newRefsFixture(t)
	const otherProject = retentionTestProject + 1
	_, err := f.sp.Apply(context.Background(), []*spanner.Mutation{
		spanner.Insert("Projects", []string{"ProjectId", "Host", "Project"}, []any{int64(otherProject), "other.example.com", entities.ABFSMetaProject}),
	})
	require.NoError(err)
	ended, live, otherStart, otherHost := entities.NewHash("1111111111111111111111111111111111111111"), entities.NewHash("2222222222222222222222222222222222222222"), entities.NewHash("3333333333333333333333333333333333333333"), entities.NewHash("4444444444444444444444444444444444444444")
	unmount, withTombstone := entities.NewHash("5555555555555555555555555555555555555555"), entities.NewHash("6666666666666666666666666666666666666666")
	recent := time.Now().Add(-time.Hour)
	f.write([]string{"refs/abfs/mount/user/u/100"}, recent, ended)
	f.write([]string{"refs/abfs/mount/user/u/200"}, recent, live)
	f.write([]string{"refs/abfs/mount/user/u/300"}, recent, otherStart)
	f.write([]string{"refs/abfs/mount/user/u/400"}, recent, otherHost)
	f.write([]string{"refs/abfs/mount/user/u/500"}, recent, withTombstone)
	f.write([]string{"refs/abfs/unmount/user/u/500"}, recent, Hash{})
	f.write([]string{"refs/abfs/unmount/user/u/100", "refs/abfs/unmount/user/u/301", "refs/abfs/unmount/user/v/200"}, recent, unmount)
	snapshot, err := f.sp.Apply(context.Background(), []*spanner.Mutation{
		spanner.Insert(refsTable, []string{"ProjectId", "Ref", "UpdateTime", "Target"}, []any{int64(otherProject), "refs/abfs/unmount/user/u/400", recent, unmount.String()}),
	})
	require.NoError(err)

	o := DefaultDryRunOpts()
	snap, err := readRefs(context.Background(), f.sp, &o, snapshot)
	require.NoError(err)
	require.ElementsMatch([]Hash{live, otherStart, otherHost, withTombstone}, snap.mounts)
	require.Equal(&RefCounts{Refs: 5, Roots: 5, Ended: 1}, snap.counts[RefMount])
	require.Equal(&RefCounts{Refs: 5, Null: 1, Roots: 4}, snap.counts[RefUnmount])
	require.Empty(snap.expired)
	var mountRoots []Hash
	for _, r := range snap.roots {
		if r.class == RefMount {
			mountRoots = append(mountRoots, r.target)
		}
	}
	require.ElementsMatch([]Hash{ended, live, otherStart, otherHost, withTombstone}, mountRoots)
}

// An ended mount ref goes with its unmount ref when that passes the build
// retention, also when the mount ref was written again since the unmount.
// Within that retention it stays, and past the mount idle time it goes
// whether or not it has an unmount ref. Mount refs are removed before
// unmount refs, so that a removal that stops in between leaves no ended
// mount live again.
func TestRemoveExpiredRefs_EndedMountGoesWithUnmount(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newRefsFixture(t)
	details, unmount := entities.NewHash("1111111111111111111111111111111111111111"), entities.NewHash("2222222222222222222222222222222222222222")
	recent := time.Now().Add(-time.Hour)
	f.write([]string{"refs/abfs/unmount/user/u/1"}, f.old, unmount)
	f.write([]string{"refs/abfs/mount/user/u/1", "refs/abfs/mount/user/u/2"}, recent, details)
	f.write([]string{"refs/abfs/mount/user/u/3"}, f.old, details)
	snapshot := f.write([]string{"refs/abfs/unmount/user/u/2", "refs/abfs/unmount/user/u/3"}, recent, unmount)

	res := f.snapshot(4, snapshot)
	var expired []string
	for _, e := range res.expiredRefs {
		expired = append(expired, e.ref)
	}
	require.ElementsMatch([]string{"refs/abfs/unmount/user/u/1", "refs/abfs/mount/user/u/1", "refs/abfs/mount/user/u/3"}, expired)
	require.Equal("refs/abfs/unmount/user/u/1", expired[2])

	rep, err := f.deleter().removeExpiredRefs(ctx, farLease(), res)
	require.NoError(err)
	require.Equal(map[RefClass]*RefRemovalCounts{RefMount: {Recorded: 2, Removed: 2}, RefUnmount: {Recorded: 1, Removed: 1}}, rep.Classes)
	require.ElementsMatch([]string{"refs/abfs/mount/user/u/2", "refs/abfs/unmount/user/u/2", "refs/abfs/unmount/user/u/3"}, slices.Collect(maps.Keys(f.refs())))
}

// The pruning keeps the ref records of the newest cycles and those younger
// than seven days, and leaves other names and other states alone.
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
	n, err := pruneRefRecords(ctx, writeBudget{deadline: DefaultWriteDeadline, lease: leaseAt()}, bucket, DefaultStateName, 2)
	require.NoError(err)
	require.Zero(n)
	laterBy(t, undoWindow+time.Minute)
	n, err = pruneRefRecords(ctx, writeBudget{deadline: DefaultWriteDeadline, lease: leaseAt()}, bucket, DefaultStateName, 2)
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

// No ref removal transaction starts once the wall clock is within two write
// deadlines and the clock skew margin of the expiry of the lease, also after
// the record was written, and the cycle then stops.
func TestRemoveExpiredRefs_StopsBeforeLeaseExpiry(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newRefsFixture(t)
	snapshot := f.write(buildRefs("b", 3), f.old, entities.NewHash("1111111111111111111111111111111111111111"))
	res := f.snapshot(4, snapshot)
	d := f.deleter()
	lease := farLease()
	d.budget.lease = lease
	// The clock passes the lease after the check of the record write, the
	// first write of the removal.
	var calls atomic.Int64
	orig := wallNow
	wallNow = func() time.Time {
		if calls.Add(1) > 1 {
			return orig().Add(2 * time.Hour)
		}
		return orig()
	}
	t.Cleanup(func() { wallNow = orig })

	rep, err := d.removeExpiredRefs(ctx, lease, res)
	require.ErrorIs(err, errLeaseExpiring)
	require.Equal(refRecordName(DefaultStateName, 4), rep.Record)
	require.Empty(f.commits.deletes)
	require.Len(f.refs(), 3)
}

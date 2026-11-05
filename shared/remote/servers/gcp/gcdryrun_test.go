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
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/objects/gitobjects"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

const gcTestHost = "gerrit.example.com"

// gcFixture stores objects with gc rows of a chosen first write time, and refs
// and map rows of a chosen time, on the emulators.
type gcFixture struct {
	t   *testing.T
	s   *GCPServer
	now time.Time
	// old is older than every retention of the default options.
	old time.Time
}

func newGCFixture(t *testing.T) *gcFixture {
	t.Helper()
	s := newEmulatorCASDBServer(t)
	now := time.Now()
	return &gcFixture{t: t, s: s, now: now, old: now.Add(-1000 * time.Hour)}
}

// put stores an object and writes its gc row, first written at the given time.
func (f *gcFixture) put(key entities.Hash, data []byte, at time.Time) entities.Hash {
	f.t.Helper()
	require.NoError(f.t, putRaw(f.s, key, data))
	typ, obj := entities.ObjectTypeBlob, objects.Object(nil)
	if !bytes.HasPrefix(data, []byte("blob ")) {
		var err error
		typ, obj, err = objects.Factory.Decode(key, data)
		require.NoError(f.t, err)
	}
	edges, _, err := gc.Edges(string(typ), obj)
	require.NoError(f.t, err)
	f.gcRow(key, string(typ), edges, at)
	return key
}

func (f *gcFixture) gcRow(key entities.Hash, typ string, edges []entities.Hash, at time.Time) {
	f.t.Helper()
	ts := bigtable.Time(at)
	mut := bigtable.NewMutation()
	mut.Set(gc.EdgesFamily, gc.LinksColumn, ts, gc.PackEdges(edges))
	mut.Set(gc.EdgesFamily, gc.TypeColumn, ts, []byte(typ))
	mut.Set(gc.EdgesFamily, gc.VersionColumn, ts, binary.BigEndian.AppendUint64(nil, gc.ExtractorVersion))
	require.NoError(f.t, f.s.casdb.BTClient().Open(gc.TableName).Apply(context.Background(), key.String(), mut))
}

func (f *gcFixture) touch(key entities.Hash, at time.Time) {
	f.t.Helper()
	mut := bigtable.NewMutation()
	mut.Set(gc.TouchFamily, "touch", bigtable.Time(at), nil)
	require.NoError(f.t, f.s.casdb.BTClient().Open(gc.TableName).Apply(context.Background(), key.String(), mut))
}

func (f *gcFixture) blob(content string, at time.Time) entities.Hash {
	return f.put(objects.HashBlobBytes([]byte(content)), fmt.Appendf(nil, "blob %d\x00%s", len(content), content), at)
}

func (f *gcFixture) tree(at time.Time, blobs ...entities.Hash) entities.Hash {
	key, data := treeOf(blobs...)
	return f.put(key, data, at)
}

func (f *gcFixture) commit(tree entities.Hash, msg string, at time.Time, parents ...entities.Hash) entities.Hash {
	body := fmt.Appendf(nil, "tree %s\n", tree)
	for _, p := range parents {
		body = fmt.Appendf(body, "parent %s\n", p)
	}
	body = fmt.Appendf(body, "author A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\n%s\n", msg)
	data := append(fmt.Appendf(nil, "commit %d\x00", len(body)), body...)
	return f.put(entities.Hash(sha1.Sum(data)), data, at)
}

func (f *gcFixture) object(obj objects.Object, at time.Time) entities.Hash {
	f.t.Helper()
	require.NoError(f.t, obj.Encode())
	return f.put(obj.GetHash(), obj.GetEncoded(), at)
}

// ref writes a ref with the given update time; a zero target writes a
// tombstone.
func (f *gcFixture) ref(project, ref string, target entities.Hash, updated time.Time) {
	f.t.Helper()
	ctx := context.Background()
	id, err := f.s.casdb.GetOrCreateProject(ctx, gcTestHost, project)
	require.NoError(f.t, err)
	t := spanner.NullString{StringVal: target.String(), Valid: !target.IsZero()}
	_, err = f.s.casdb.SpannerClient().Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("Refs", []string{"ProjectId", "Ref", "UpdateTime", "Target"}, []any{id, ref, updated, t}),
	})
	require.NoError(f.t, err)
}

func (f *gcFixture) mapRow(from, to entities.Hash, at time.Time) {
	f.t.Helper()
	mut := bigtable.NewMutation()
	mut.Set("cf", "", bigtable.Time(at), []byte("1"))
	require.NoError(f.t, f.s.casdb.BTClient().Open("maps").Apply(context.Background(), from.String()+"#"+to.String(), mut))
}

func (f *gcFixture) run(opts *gc.DryRunOpts) *gc.Result {
	f.t.Helper()
	if opts == nil {
		o := gc.DefaultDryRunOpts()
		o.Shards = 3
		opts = &o
	}
	res, err := f.s.DryRunGC(context.Background(), opts)
	require.NoError(f.t, err)
	return res
}

func requireReachable(t *testing.T, res *gc.Result, want bool, hashes ...entities.Hash) {
	t.Helper()
	for i, h := range hashes {
		reachable, found := res.Reachable(h)
		require.True(t, found, "object %d (%s) has no gc row", i, h)
		require.Equal(t, want, reachable, "object %d (%s)", i, h)
	}
}

func pinnedManifest(revisions ...string) string {
	s := "<manifest>\n<remote name=\"origin\" fetch=\"https://example.com\"/>\n<default revision=\"main\" remote=\"origin\"/>\n"
	for i, r := range revisions {
		if r == "" {
			s += fmt.Sprintf("<project name=\"p%d\" path=\"p%d\"/>\n", i, i)
			continue
		}
		s += fmt.Sprintf("<project name=\"p%d\" path=\"p%d\" revision=\"%s\"/>\n", i, i, r)
	}
	return s + "</manifest>\n"
}

func (f *gcFixture) mount(ref string, revisions []string, updated time.Time) (details, pinned entities.Hash) {
	f.t.Helper()
	pinned = f.blob(pinnedManifest(revisions...), f.old)
	details = f.object(&binding.MountDetails{
		Version:            binding.CurrentMountDetailsVersion,
		MetaRefPrefix:      "user/u",
		PinnedManifestHash: pinned,
	}, f.old)
	f.ref(entities.ABFSMetaProject, ref, details, updated)
	return details, pinned
}

func TestDryRunGC_RetentionClasses(t *testing.T) {
	require := require.New(t)
	f := newGCFixture(t)
	old, recentRef := f.old, f.now.Add(-time.Hour)

	// A branch moved by a force push: the old commit and its tree go.
	oldBlob := f.blob("old file", old)
	oldTree := f.tree(old, oldBlob)
	oldCommit := f.commit(oldTree, "old", old)
	newBlob := f.blob("new file", old)
	newTree := f.tree(old, newBlob)
	newCommit := f.commit(newTree, "new", old)
	f.ref("platform/a", "refs/heads/main", newCommit, old)
	f.ref("platform/a", "refs/heads/deleted", entities.Hash{}, old)
	status := f.blob("status", old)
	f.ref("platform/a", binding.ProjectStatusRef, status, old)

	// Builds: one past the retention, one within it.
	oldLog := f.blob("old ninja log", old)
	oldRootEnd := f.tree(old, f.blob("old output", old))
	oldBuild := f.object(&binding.BuildInfo{Version: binding.CurrentBuildInfoVersion, BuildStartData: binding.BuildStartData{BuildID: "b1"}, NinjaLog: oldLog.String(), RootEnd: oldRootEnd.String()}, old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/build/user/u/b1", oldBuild, old)
	newLog := f.blob("new ninja log", old)
	newBuild := f.object(&binding.BuildInfo{Version: binding.CurrentBuildInfoVersion, BuildStartData: binding.BuildStartData{BuildID: "b2"}, NinjaLog: newLog.String()}, old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/build/user/u/b2", newBuild, recentRef)
	unmount := f.blob("unmount details", old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/unmount/user/u/1", unmount, old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/unmount/user/u/2", unmount, recentRef)

	// Eval maps: one unused past the map retention, one used recently.
	env := f.object(&binding.EnvVars{Vars: map[string]string{"PATH": "/bin"}}, old)
	evalRow := func(name string, at time.Time) (req, resp, out entities.Hash) {
		out = f.blob(name+" output", old)
		req = f.object(&binding.EvalRequest{Version: binding.CurrentEvalRequestVersion, Command: name, EnvVars: env}, old)
		resp = f.object(&binding.EvalResponse{
			Version:     binding.CurrentEvalResponseVersion,
			EvalRequest: req,
			WritePatch:  entities.Patch{{Path: "out/" + name, Mode: entities.FileModeRegular, Hash: out}},
		}, old)
		f.mapRow(req, resp, at)
		return req, resp, out
	}
	oldReq, oldResp, oldOut := evalRow("old step", old)
	newReq, newResp, newOut := evalRow("new step", recentRef)

	// Pusher maps of a live tree and of a tree nothing reaches.
	pusherRow := func(tree entities.Hash) (req, result entities.Hash) {
		req = f.object(&objects.DeepTreeRequest{Version: objects.CurrentDeepTreeRequestVersion, Tree: tree}, old)
		result = f.object(&gitobjects.DeepTreeObject{DeepTree: entities.DeepTree{Entries: []*entities.DeepEntry{
			{Name: "d", Mode: entities.FileModeDir, Hash: tree},
		}}}, old)
		f.mapRow(req, result, old)
		return req, result
	}
	liveDTReq, liveDT := pusherRow(newTree)
	deadDTReq, deadDT := pusherRow(oldTree)
	// A digest map of the live tree.
	dmReq := f.object(&binding.DigestMapRequest{Version: binding.CurrentDigestMapRequestVersion, Tree: newTree}, old)
	dm := f.object(&binding.DigestMap{Version: binding.CurrentDigestMapVersion}, old)
	f.mapRow(dmReq, dm, old)

	// An LFS map is kept however old.
	lfsContent := f.blob("lfs content", old)
	lfsReq := f.object(&binding.LFSRequest{LFSOID: "oid"}, old)
	lfsRes := f.object(&binding.LFSResult{LFSRequest: lfsReq, ABFSOID: lfsContent}, old)
	f.mapRow(lfsReq, lfsRes, old)

	// A live mount whose pinned manifest keeps an old commit, and an idle one.
	pinnedTree := f.tree(old, f.blob("pinned file", old))
	pinnedCommit := f.commit(pinnedTree, "pinned", old)
	liveDetails, livePinned := f.mount("refs/abfs/mount/user/u/1", []string{pinnedCommit.String(), ""}, recentRef)
	idleCommit := f.commit(f.tree(old, f.blob("idle file", old)), "idle", old)
	idleDetails, idlePinned := f.mount("refs/abfs/mount/user/u/2", []string{idleCommit.String()}, old)

	// A workspace is kept however old.
	wsBlob := f.blob("workspace file", old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/ws/user/u/ws1", f.tree(old, wsBlob), old)

	// Recent objects keep what they reach; so does a recent touch.
	recentTreeBlob := f.blob("reached by a recent tree", old)
	recentTree := f.tree(f.now, recentTreeBlob)
	touched := f.blob("touched", old)
	f.touch(touched, f.now)

	// A gc row with an edge to an object that has no gc row.
	missing := objects.HashBlobBytes([]byte("never pushed"))
	danglingTree, _ := treeOf(missing)
	f.gcRow(danglingTree, "tree", []entities.Hash{missing}, old)

	res := f.run(nil)
	r := res.Report

	requireReachable(t, res, true, newCommit, newTree, newBlob, status)
	requireReachable(t, res, false, oldCommit, oldTree, oldBlob)
	requireReachable(t, res, true, newBuild, newLog)
	requireReachable(t, res, false, oldBuild, oldLog, oldRootEnd, unmount)
	requireReachable(t, res, true, newReq, newResp, newOut, env)
	requireReachable(t, res, false, oldReq, oldResp, oldOut)
	requireReachable(t, res, true, liveDTReq, liveDT, dmReq, dm)
	requireReachable(t, res, false, deadDTReq, deadDT)
	requireReachable(t, res, true, lfsReq, lfsRes, lfsContent)
	requireReachable(t, res, true, liveDetails, livePinned, pinnedCommit, pinnedTree)
	requireReachable(t, res, false, idleDetails, idlePinned, idleCommit)
	requireReachable(t, res, true, wsBlob, recentTree, recentTreeBlob, touched)
	requireReachable(t, res, false, danglingTree)

	require.Equal(gc.PhaseDryRun, r.Phase)
	require.Equal(int64(1), r.Cycle)
	require.Equal(&gc.RefCounts{Refs: 2, Null: 1, Roots: 1}, r.Refs[gc.RefSource])
	require.Equal(&gc.RefCounts{Refs: 1, Roots: 1}, r.Refs[gc.RefProjectStatus])
	require.Equal(&gc.RefCounts{Refs: 2, Roots: 1, Expired: 1}, r.Refs[gc.RefBuild])
	require.Equal(&gc.RefCounts{Refs: 2, Expired: 1, Kept: 1}, r.Refs[gc.RefUnmount])
	require.Equal(&gc.RefCounts{Refs: 2, Roots: 1, Expired: 1}, r.Refs[gc.RefMount])
	require.Equal(&gc.RefCounts{Refs: 1, Roots: 1}, r.Refs[gc.RefWorkspace])
	require.Equal(int64(3), r.RefsToTombstone)
	require.Equal(gc.MountCounts{Live: 1, PinnedManifests: 1, Revisions: 1, IgnoredRevisions: 1}, r.LiveMounts)
	require.Equal(&gc.RootCounts{Roots: 1}, r.Roots[gc.RootPinnedRevisions])
	require.Equal(&gc.RootCounts{Roots: 2}, r.Roots[gc.RootRecent])

	require.Equal(&gc.MapCounts{Rows: 2, Live: 1, Expired: 1}, r.Maps[gc.MapEval])
	require.Equal(&gc.MapCounts{Rows: 3, Live: 2, Expired: 1}, r.Maps[gc.MapPusher])
	require.Equal(&gc.MapCounts{Rows: 1, Live: 1}, r.Maps[gc.MapLFS])

	require.Equal(int64(1), r.Edges.Dangling)
	require.Equal([]string{danglingTree.String() + ">" + missing.String()}, r.Edges.DanglingSample)
	require.Equal(r.Objects.Rows, r.Objects.Reachable+r.Objects.Unreachable)
	require.Equal(int64(2), r.Types["eval-request"].Objects)
	require.Equal(int64(1), r.Types["eval-request"].Unreachable)

	// Every object but the dangling tree has its object row, written just now.
	require.True(r.Consistency.Ran)
	require.Zero(r.Consistency.MissingGCRows)
	require.Equal(r.Objects.Rows-1, r.Consistency.Objects)
	require.Equal(int64(1), r.Consistency.GCRowsWithoutObject)
	require.NotNil(r.Objects.UnreachableBytes)
	require.Positive(*r.Objects.UnreachableBytes)
	require.Positive(r.Memory.PeakHeapBytes)

	// The dry run left the lease free and counted the cycle.
	res = f.run(nil)
	require.Equal(int64(2), res.Report.Cycle)
}

func TestDryRunGC_UnknownTypeFailsClosed(t *testing.T) {
	f := newGCFixture(t)
	f.blob("file", f.old)
	f.gcRow(objects.HashBlobBytes([]byte("from a newer server")), "future-type", nil, f.old)

	o := gc.DefaultDryRunOpts()
	_, err := f.s.DryRunGC(context.Background(), &o)
	require.ErrorContains(t, err, `unknown object type "future-type"`)
}

func TestDryRunGC_ConsistencyFindsMissingGCRow(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newGCFixture(t)
	f.blob("file", f.old)
	// An object row written before the scan without a gc row, as from a server
	// that ran with --gc-edges off.
	lost := objects.HashBlobBytes([]byte("no gc row"))
	mut := bigtable.NewMutation()
	ts := bigtable.Time(f.old)
	mut.Set("meta", "type", ts, []byte("blob"))
	mut.Set("meta", "size", ts, binary.BigEndian.AppendUint64(nil, 9))
	require.NoError(f.s.casdb.BTClient().Open("objects").Apply(ctx, lost.String(), mut))

	r := f.run(nil).Report
	require.True(r.Consistency.Ran)
	require.Equal(int64(1), r.Consistency.MissingGCRows)
	require.Equal([]string{lost.String()}, r.Consistency.FirstMissing)

	o := gc.DefaultDryRunOpts()
	o.Consistency = false
	r = f.run(&o).Report
	require.False(r.Consistency.Ran)
	require.Nil(r.Objects.UnreachableBytes)
}

func TestDryRunGC_LeaseRefusesSecondRun(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newGCFixture(t)
	sp := f.s.casdb.SpannerClient()

	lease, err := gc.AcquireLease(ctx, sp, gc.DefaultStateName, "first", gc.PhaseDryRun, time.Minute, gc.DefaultGrace)
	require.NoError(err)
	require.Equal(int64(1), lease.Cycle)

	o := gc.DefaultDryRunOpts()
	o.Holder = "second"
	_, err = f.s.DryRunGC(ctx, &o)
	var held *gc.ErrLeaseHeld
	require.ErrorAs(err, &held)
	require.Equal("first", held.Holder)

	lease.Release()
	res, err := f.s.DryRunGC(ctx, &o)
	require.NoError(err)
	require.Equal(int64(2), res.Report.Cycle)
	require.Equal("second", res.Report.Holder)
}

// The dry run warns about live replicas whose settings break the timing
// rules, ignores replicas that stopped, and deletes the rows of replicas not
// seen for a day.
func TestDryRunGC_ChecksReplicaSettings(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newGCFixture(t)
	sp := f.s.casdb.SpannerClient()
	good := gc.Replica{ID: "good", Version: gc.ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour}
	require.NoError(gc.WriteHeartbeat(ctx, sp, &good))
	bad := good
	bad.ID, bad.TouchInterval, bad.CacheTTL = "bad", 72*time.Hour, 30*time.Hour
	require.NoError(gc.WriteHeartbeat(ctx, sp, &bad))
	_, err := sp.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("GcReplicas",
		[]string{"ReplicaId", "Version", "GcEnabled", "TouchIntervalSeconds", "CacheTTLSeconds", "LastSeen"},
		[]any{"stopped", int64(0), true, int64(0), int64(0), f.now.Add(-2 * time.Hour)}),
		spanner.InsertOrUpdate("GcReplicas",
			[]string{"ReplicaId", "Version", "GcEnabled", "TouchIntervalSeconds", "CacheTTLSeconds", "LastSeen"},
			[]any{"gone", int64(0), true, int64(0), int64(0), f.now.Add(-25 * time.Hour)}),
	})
	require.NoError(err)

	r := f.run(nil).Report
	require.Equal(int64(2), r.Replicas.Live)
	require.Equal(int64(2), r.Replicas.Stale)
	require.Equal(int64(1), r.Replicas.Removed)
	require.Len(r.Replicas.Warnings, 2)
	for _, w := range r.Replicas.Warnings {
		require.Contains(w, "replica bad: ")
	}

	// The row not seen for a day is gone.
	replicas, err := gc.ReadReplicas(ctx, sp)
	require.NoError(err)
	var ids []string
	for _, rep := range replicas {
		ids = append(ids, rep.ID)
	}
	require.ElementsMatch([]string{"good", "bad", "stopped"}, ids)
}

// The empty file and directory that servers write at start are kept even
// when no tree links them.
func TestDryRunGC_KeepsEmptyObjects(t *testing.T) {
	f := newGCFixture(t)
	f.put(entities.EmptyFileHash, []byte("blob 0\x00"), f.old)
	f.put(entities.EmptyDirHash, []byte("tree 0\x00"), f.old)
	unused := f.blob("unused", f.old)

	res := f.run(nil)
	requireReachable(t, res, true, entities.EmptyFileHash, entities.EmptyDirHash)
	requireReachable(t, res, false, unused)
	require.Equal(t, &gc.RootCounts{Roots: 2}, res.Report.Roots[gc.RootPermanent])
}

// The dry run reads the mount details and pinned manifests of live mounts
// through a server that leaves condemn markers in place, so its own reads do
// not keep condemned objects.
func TestDryRunGC_ReadsLeaveCondemnMarkers(t *testing.T) {
	require := require.New(t)
	f := newGCFixture(t)
	require.NoError(f.s.enableGCHooks(&NewServerOpts{GCReadOnly: true, GCTouchInterval: time.Hour}))
	t.Cleanup(f.s.stopGCHooks)
	commit := f.commit(f.tree(f.old, f.blob("file", f.old)), "pinned", f.old)
	details, pinned := f.mount("refs/abfs/mount/user/u/1", []string{commit.String()}, f.now.Add(-time.Hour))
	condemnObject(t, f.s, details, 3)
	condemnObject(t, f.s, pinned, 3)

	r := f.run(nil).Report
	require.Equal(gc.MountCounts{Live: 1, PinnedManifests: 1, Revisions: 1}, r.LiveMounts)
	require.True(isCondemned(t, f.s, details))
	require.True(isCondemned(t, f.s, pinned))
	require.False(objectTouched(t, f.s, details))
}

// Mount details or pinned manifests that cannot be read, touches of objects
// without a gc row and map rows with malformed keys are counted and do not
// stop the run.
func TestDryRunGC_CountsBrokenInputs(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newGCFixture(t)
	recent := f.now.Add(-time.Hour)
	f.ref(entities.ABFSMetaProject, "refs/abfs/mount/user/u/1", objects.HashBlobBytes([]byte("no such mount details")), recent)
	// Link validation would refuse mount details whose manifest is missing.
	f.s.validateLinks = false
	details := f.object(&binding.MountDetails{
		Version:            binding.CurrentMountDetailsVersion,
		MetaRefPrefix:      "user/u",
		PinnedManifestHash: objects.HashBlobBytes([]byte("no such manifest")),
	}, f.old)
	f.s.validateLinks = true
	f.ref(entities.ABFSMetaProject, "refs/abfs/mount/user/u/2", details, recent)
	f.touch(objects.HashBlobBytes([]byte("touched before its gc row")), f.now)
	mut := bigtable.NewMutation()
	mut.Set("cf", "", bigtable.Now(), []byte("1"))
	require.NoError(f.s.casdb.BTClient().Open("maps").Apply(ctx, "not-a-hash#0000", mut))

	res := f.run(nil)
	r := res.Report
	require.Equal(gc.MountCounts{Live: 2, Errors: 2}, r.LiveMounts)
	require.Equal(int64(1), r.Objects.TouchOnly)
	require.Equal(&gc.MapCounts{Rows: 1}, r.Maps[gc.MapMalformed])
	requireReachable(t, res, true, details)
}

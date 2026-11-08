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
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// collectorSpanner returns a Spanner client of the fixture's database whose
// calls pass through the dial options.
func (f *deleteFixture) collectorSpanner(opts ...grpc.DialOption) *spanner.Client {
	f.t.Helper()
	var copts []option.ClientOption
	for _, o := range opts {
		copts = append(copts, option.WithGRPCDialOption(o))
	}
	sp, err := spanner.NewClient(context.Background(), f.s.casdb.SpannerClient().DatabaseName(), copts...)
	require.NoError(f.t, err)
	f.t.Cleanup(sp.Close)
	return sp
}

// deleteOn runs a delete cycle whose collector uses the Spanner client sp.
func (f *deleteFixture) deleteOn(sp *spanner.Client) *gc.Result {
	f.t.Helper()
	res, err := gc.Delete(context.Background(), sp, f.s.casdb.BTClient(), f.s.storage.Bucket(f.bucket), f.s, f.checks, f.deleteOpts())
	require.NoError(f.t, err)
	return res
}

// refReads runs hook once, before the first read of the Refs table by key,
// which only the removal of the refs past their retention makes.
func refReads(hook func()) grpc.DialOption {
	var once sync.Once
	return grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return s, err
		}
		return &sendHook{ClientStream: s, hook: func(msg any) {
			if req, ok := msg.(*spannerpb.ReadRequest); ok && req.GetTable() == "Refs" {
				once.Do(hook)
			}
		}}, nil
	})
}

type sendHook struct {
	grpc.ClientStream
	hook func(msg any)
}

func (s *sendHook) SendMsg(msg any) error {
	s.hook(msg)
	return s.ClientStream.SendMsg(msg)
}

// failRefDeletes answers every commit that deletes refs with err.
func failRefDeletes(err error) grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if commit, ok := req.(*spannerpb.CommitRequest); ok {
			for _, m := range commit.GetMutations() {
				if m.GetDelete().GetTable() == "Refs" {
					return err
				}
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
}

// metaRefs returns the refs of the project abfs-meta under prefix.
func (f *deleteFixture) metaRefs(prefix string) []string {
	f.t.Helper()
	refs, err := f.s.SearchRefs(context.Background(), gcTestHost, entities.ABFSMetaProject, prefix, 0)
	require.NoError(f.t, err)
	var names []string
	for _, r := range refs {
		names = append(names, r.Ref)
	}
	return names
}

func (f *deleteFixture) refRecords() []string {
	f.t.Helper()
	return slices.DeleteFunc(f.listNames(), func(name string) bool { return !strings.HasPrefix(name, "gc/refs/") })
}

// A delete cycle removes a build ref past its retention, after it recorded
// it, and leaves one written again after the snapshot. The mark of the
// cycle did not count either ref as a root, so the cycle condemns what they
// reach and deletes none of it; the next cycle deletes what the removed ref
// reached, while the ref written again keeps its build.
func TestDeleteGC_RemovesExpiredRefs(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	ninjaLog := f.blob("ninja log", f.old)
	removed := f.object(&binding.BuildInfo{Version: binding.CurrentBuildInfoVersion, BuildStartData: binding.BuildStartData{BuildID: "b1"}, NinjaLog: ninjaLog.String()}, f.old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/build/user/u/b1", removed, f.old)
	kept := f.object(&binding.BuildInfo{Version: binding.CurrentBuildInfoVersion, BuildStartData: binding.BuildStartData{BuildID: "b2"}}, f.old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/build/user/u/b2", kept, f.old)

	var recordsAtRemoval []string
	sp := f.collectorSpanner(refReads(func() {
		recordsAtRemoval = f.refRecords()
		f.ref(entities.ABFSMetaProject, "refs/abfs/build/user/u/b2", kept, time.Now())
	}))
	res := f.deleteOn(sp)
	r := res.Report
	refs := r.Delete.Refs
	require.Equal("gc/refs/default/"+strconv.FormatInt(r.Cycle, 10), refs.Record)
	require.Equal([]string{refs.Record}, recordsAtRemoval)
	require.Equal(map[gc.RefClass]*gc.RefRemovalCounts{gc.RefBuild: {Recorded: 2, Removed: 1, Changed: 1}}, refs.Classes)
	require.Equal([]string{"refs/abfs/build/user/u/b2"}, f.metaRefs("refs/abfs/build/"))

	require.Equal(int64(2), r.Refs[gc.RefBuild].Expired)
	requireReachable(t, res, false, removed, ninjaLog, kept)
	for _, k := range []entities.Hash{removed, ninjaLog, kept} {
		require.Equal([][]byte{marker(byte(r.Cycle))}, f.markerCells(k), k)
	}

	f.age()
	res = f.deletes(nil)
	require.Empty(res.Report.Delete.Refs.Classes)
	requireReachable(t, res, true, kept)
	f.requireGone(removed, ninjaLog)
	f.requirePresent(kept)
}

// A failed removal of the refs past their retention refuses the deletes of
// the cycle, which condemns and completes its list: what an expired ref
// still names is not deleted while the ref exists. The next cycle removes
// the ref and then deletes.
func TestDeleteGC_FailedRefRemovalRefusesDeletes(t *testing.T) {
	require := require.New(t)
	f := newDeleteFixture(t)
	target := f.blob("named by an expired build ref", f.old)
	f.ref(entities.ABFSMetaProject, "refs/abfs/build/user/u/b1", target, f.old)
	other := f.blob("unreachable", f.old)
	_, err := f.condemn(nil)
	require.NoError(err)
	f.age()

	res := f.deleteOn(f.collectorSpanner(failRefDeletes(status.Error(codes.PermissionDenied, "injected"))))
	r := res.Report
	require.Len(r.Delete.Refused, 1)
	require.Contains(r.Delete.Refused[0], "the removal of expired refs failed")
	require.Contains(r.Delete.Refused[0], "injected")
	require.Contains(r.Delete.Refs.Error, "injected")
	require.Nil(r.Delete.Step)
	require.Equal(int64(2), r.Condemn.Condemned)
	location, complete := f.listState()
	require.Equal(r.Condemn.List, location.StringVal)
	require.True(complete.Bool)
	require.Equal([]string{"refs/abfs/build/user/u/b1"}, f.metaRefs("refs/abfs/build/"))
	for _, k := range []entities.Hash{target, other} {
		require.Equal([][]byte{marker(byte(r.Cycle))}, f.markerCells(k), k)
	}

	f.age()
	res = f.deletes(nil)
	require.Equal(int64(1), res.Report.Delete.Refs.Classes[gc.RefBuild].Removed)
	require.Empty(f.metaRefs("refs/abfs/build/"))
	f.requireGone(target, other)
}

// Only a delete cycle that may condemn removes refs: a dry run, a condemn
// cycle and a delete cycle refused by the condemn checks leave them and
// write no record.
func TestDeleteGC_RemovesRefsOnlyInDeleteCycles(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newDeleteFixture(t)
	f.ref(entities.ABFSMetaProject, "refs/abfs/mount/user/u/m1", f.blob("old mount details", f.old), f.old)
	expired := []string{"refs/abfs/mount/user/u/m1"}

	o := gc.DefaultDryRunOpts()
	o.Shards = 3
	res := f.run(&o)
	require.Equal(int64(1), res.Report.Refs[gc.RefMount].Expired)
	require.Equal(expired, f.metaRefs("refs/abfs/mount/"))
	_, err := f.condemn(nil)
	require.NoError(err)
	require.Equal(expired, f.metaRefs("refs/abfs/mount/"))

	_, err = f.s.casdb.SpannerClient().Apply(ctx, []*spanner.Mutation{spanner.Delete("GcServers", spanner.AllKeys())})
	require.NoError(err)
	_, err = f.deleteWith(f.s.casdb.BTClient(), nil)
	var refused *gc.ErrCondemnRefused
	require.ErrorAs(err, &refused)
	require.Equal(expired, f.metaRefs("refs/abfs/mount/"))
	require.Empty(f.refRecords())

	f.server("srv-0-aa", false)
	res = f.delete(nil)
	require.Equal(int64(1), res.Report.Delete.Refs.Classes[gc.RefMount].Removed)
	require.Empty(f.metaRefs("refs/abfs/mount/"))
	require.Len(f.refRecords(), 1)
}

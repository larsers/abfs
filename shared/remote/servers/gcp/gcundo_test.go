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
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// copiedAt holds, per fixture, when copyTables last copied the tables.
var copiedAt sync.Map

// copiedSources tells that each table <table><suffix> was restored from a
// backup of <table> that ended when copyTables copied it.
type copiedSources struct {
	suffix string
	end    time.Time
}

func (s copiedSources) RestoredFrom(_ context.Context, table string) (*gc.RestoredBackup, error) {
	return &gc.RestoredBackup{Backup: "copy", SourceTable: strings.TrimSuffix(table, s.suffix), Start: s.end, End: s.end}, nil
}

// copyTables copies every row of the server tables to <table><suffix>, cell
// times included, as a restore of backups taken now would.
func (f *deleteFixture) copyTables(suffix string) {
	f.t.Helper()
	ctx := context.Background()
	adm, err := bigtable.NewAdminClient(ctx, emulatorProject, emulatorBigtable)
	require.NoError(f.t, err)
	defer adm.Close()
	bt := f.s.casdb.BTClient()
	for _, table := range serverTables(true) {
		require.NoError(f.t, adm.CreateTable(ctx, table.name+suffix))
		for _, family := range table.families {
			require.NoError(f.t, adm.CreateColumnFamily(ctx, table.name+suffix, family))
		}
		require.NoError(f.t, bt.Open(table.name).ReadRows(ctx, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
			mut := bigtable.NewMutation()
			for family, items := range row {
				for _, item := range items {
					mut.Set(family, strings.TrimPrefix(item.Column, family+":"), item.Timestamp, item.Value)
				}
			}
			require.NoError(f.t, bt.Open(table.name+suffix).Apply(ctx, row.Key(), mut))
			return true
		}))
	}
	// Before the records written after it, whose times Cloud Storage sets.
	copiedAt.Store(f, time.Now().Add(-time.Second))
}

// staleLease records in GcState a lease of another holder that expired ten
// minutes ago, as a collector that crashed leaves it, so that the fence of
// the next lease has passed.
func (f *deleteFixture) staleLease() {
	f.t.Helper()
	_, err := f.s.casdb.SpannerClient().Apply(context.Background(), []*spanner.Mutation{spanner.Update("GcState",
		[]string{"Name", "LeaseHolder", "LeaseExpiry"}, []any{gc.DefaultStateName, "crashed-collector", time.Now().Add(-10 * time.Minute)})})
	require.NoError(f.t, err)
}

// undo runs gc undo of the cycles first to last from the tables with the
// suffix -r, with a Bigtable client that records the keys of the objects
// rows it writes, in order.
func (f *deleteFixture) undo(first, last int64) (*gc.UndoReport, []string) {
	f.t.Helper()
	var mu sync.Mutex
	var rows []string
	bt := newInterceptedClient(f.t, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if m, ok := req.(*bigtablepb.MutateRowRequest); ok && strings.HasSuffix(m.GetTableName(), "/objects") {
			mu.Lock()
			rows = append(rows, string(m.GetRowKey()))
			mu.Unlock()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))
	o := gc.DefaultUndoOpts()
	o.Cycle, o.Through, o.FromSuffix = first, last, "-r"
	f.staleLease()
	end, ok := copiedAt.Load(f)
	require.True(f.t, ok)
	rep, err := gc.Undo(context.Background(), f.s.casdb.SpannerClient(), bt, f.s.storage.Bucket(f.bucket), copiedSources{suffix: "-r", end: end.(time.Time)}, &o)
	require.NoError(f.t, err)
	return rep, rows
}

// Objects that delete cycles removed come back from tables restored from
// backups taken before the first of them: children before parents across
// the cycles of the range, and each object only once every chunk of its
// content exists, so that no read finds a row whose chunks are missing. Each
// is readable through the server, carries no condemn marker and is recent
// for the next cycle, which condemns none of them. A map row that expired
// comes back with a new time and survives the next cycle. The undo lease
// does not stop the next cycle from deleting.
func TestUndoGC_RestoresDeletedRows(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newDeleteFixture(t)
	// The depots of the server, so that a large blob has a chunk row and a
	// bucket chunk.
	require.NoError(f.s.setMasterDepot(ctx, f.bucket, false))
	t.Cleanup(func() { _ = f.s.storage.Close() })
	old := f.old
	large := strings.Repeat("large file ", 100000)
	blob := f.blob(large, old)
	small := f.blob("small file", old)
	tree := f.tree(old, blob, small)
	commit := f.commit(tree, "dropped", old)
	env := f.object(&binding.EnvVars{Vars: map[string]string{"PATH": "/bin"}}, old)
	req := f.object(&binding.EvalRequest{Version: binding.CurrentEvalRequestVersion, Command: "expired", EnvVars: env}, old)
	resp := f.object(&binding.EvalResponse{Version: binding.CurrentEvalResponseVersion, EvalRequest: req}, old)
	f.ref("platform/a", "refs/heads/exp", resp, old)
	f.mapRow(req, resp, old)
	mapRow := func() bigtable.Row {
		row, err := f.s.casdb.BTClient().Open("maps").ReadRow(ctx, req.String()+"#"+resp.String())
		require.NoError(err)
		return row
	}
	var chunks []struct{ Depot, ID string }
	for _, item := range f.row("objects", blob)["data"] {
		if item.Column == "data:content" {
			require.NoError(json.Unmarshal(item.Value, &chunks))
		}
	}
	require.Len(chunks, 2)
	require.Equal([]string{"bigtable", "bucket"}, []string{chunks[0].Depot, chunks[1].Depot})
	bucketChunk := f.s.storage.Bucket(f.bucket).Object(chunks[1].ID)
	r, err := bucketChunk.NewReader(ctx)
	require.NoError(err)
	chunkBytes, err := io.ReadAll(r)
	require.NoError(err)
	require.NoError(r.Close())

	// The blob is recent in the first mark, so that cycle 2 deletes its
	// parents and cycle 3 the blob.
	f.touch(blob, time.Now())
	f.delete(nil)
	mut := bigtable.NewMutation()
	mut.DeleteCellsInFamily(gc.TouchFamily)
	require.NoError(f.s.casdb.BTClient().Open(gc.TableName).Apply(ctx, blob.String(), mut))
	f.age()
	f.copyTables("-r")
	res := f.deletes(nil)
	require.Equal(int64(3), res.Report.Delete.Step.Deleted)
	require.Equal(int64(1), res.Report.Delete.Step.MapRowsDeleted)
	f.age()
	res = f.deletes(nil)
	require.Equal(int64(1), res.Report.Delete.Step.Deleted)
	f.requireGone(commit, tree, small, blob)
	require.Empty(mapRow())
	// The chunks of the blob are gone, as after the chunk step.
	mut = bigtable.NewMutation()
	mut.DeleteRow()
	require.NoError(f.s.casdb.BTClient().Open("chunks").Apply(ctx, chunks[0].ID, mut))
	require.NoError(bucketChunk.Delete(ctx))

	// The blob waits for its bucket chunk and keeps its parents from coming
	// back; the small file and the map row come back.
	rep, rows := f.undo(2, 3)
	require.Equal([]string{chunks[1].ID}, rep.MissingBucketObjects)
	require.Equal(int64(1), rep.WaitingForBucketObjects)
	require.Equal(int64(2), rep.BlockedByChild)
	require.Equal(int64(1), rep.Restored)
	require.Equal(int64(1), rep.MapRowsRestored)
	require.NotEmpty(mapRow())
	require.Equal([]string{small.String()}, rows)
	for _, k := range []entities.Hash{blob, tree, commit} {
		require.Empty(f.row("objects", k), k)
	}
	f.requirePresent(small)

	// Restored from soft delete, the bucket chunk lets the rest come back,
	// the blob first.
	w := bucketChunk.NewWriter(ctx)
	_, err = w.Write(chunkBytes)
	require.NoError(err)
	require.NoError(w.Close())
	rep, rows = f.undo(2, 3)
	require.Equal(int64(3), rep.Restored)
	require.Equal(int64(1), rep.Present)
	require.Equal(int64(1), rep.MapRowsPresent)
	require.Zero(rep.NotRestored())
	require.Equal([]string{blob.String(), tree.String(), commit.String()}, rows)
	f.requirePresent(commit, tree, small, blob, req, resp)
	got, err := f.s.GetObject(ctx, blob)
	require.NoError(err)
	content, err := io.ReadAll(got)
	require.NoError(err)
	require.Equal("blob 1100000\x00"+large, string(content))
	for _, k := range []entities.Hash{commit, tree, small, blob} {
		require.Empty(f.markerCells(k), k)
	}

	// The next cycle deletes, finds the restored objects recent and keeps
	// the map row, which it would expire with its old time.
	f.age()
	res = f.deletes(nil)
	require.Zero(res.Report.Delete.Step.Deleted)
	require.Zero(res.Report.Delete.Step.MapRowsDeleted)
	require.Zero(res.Report.Condemn.Condemned)
	require.NotEmpty(mapRow())
	f.requirePresent(commit, tree, small, blob, req, resp)
}

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
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/bttest"
	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
)

// btRecorder records the Bigtable requests that reach an in-process Bigtable,
// one normalized line per row read or written.
type btRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *btRecorder) add(method string, req any) {
	method = method[strings.LastIndex(method, "/")+1:]
	var lines []string
	switch m := req.(type) {
	case *bigtablepb.ReadRowsRequest:
		prefix := fmt.Sprintf("%s %s filter=%s", method, tableID(m.TableName), filterString(m.Filter))
		if m.Rows == nil || len(m.Rows.RowKeys)+len(m.Rows.RowRanges) == 0 {
			lines = append(lines, prefix+" all-rows")
		}
		for _, k := range m.GetRows().GetRowKeys() {
			lines = append(lines, prefix+" row="+rowID(tableID(m.TableName), string(k)))
		}
		for _, rr := range m.GetRows().GetRowRanges() {
			lines = append(lines, fmt.Sprintf("%s range=%v", prefix, rr))
		}
	case *bigtablepb.MutateRowRequest:
		lines = append(lines, fmt.Sprintf("%s %s row=%s %s", method, tableID(m.TableName), rowID(tableID(m.TableName), string(m.RowKey)), mutationsString(m.Mutations)))
	case *bigtablepb.MutateRowsRequest:
		for _, e := range m.Entries {
			lines = append(lines, fmt.Sprintf("%s %s row=%s %s", method, tableID(m.TableName), rowID(tableID(m.TableName), string(e.RowKey)), mutationsString(e.Mutations)))
		}
	case *bigtablepb.CheckAndMutateRowRequest:
		lines = append(lines, fmt.Sprintf("%s %s row=%s", method, tableID(m.TableName), rowID(tableID(m.TableName), string(m.RowKey))))
	case *bigtablepb.ReadModifyWriteRowRequest:
		lines = append(lines, fmt.Sprintf("%s %s row=%s", method, tableID(m.TableName), rowID(tableID(m.TableName), string(m.RowKey))))
	default:
		lines = append(lines, method)
	}
	r.mu.Lock()
	r.calls = append(r.calls, lines...)
	r.mu.Unlock()
}

func (r *btRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func tableID(name string) string {
	return name[strings.LastIndex(name, "/")+1:]
}

// rowID hides the random chunk names.
func rowID(table, key string) string {
	if table == "chunks" {
		return "<chunk>"
	}
	return key
}

func filterString(f *bigtablepb.RowFilter) string {
	switch {
	case f == nil:
		return "none"
	case f.GetChain() != nil:
		var parts []string
		for _, c := range f.GetChain().Filters {
			parts = append(parts, filterString(c))
		}
		return "chain(" + strings.Join(parts, ",") + ")"
	case f.GetFamilyNameRegexFilter() != "":
		return "family(" + f.GetFamilyNameRegexFilter() + ")"
	case f.GetCellsPerColumnLimitFilter() != 0:
		return fmt.Sprintf("latest(%d)", f.GetCellsPerColumnLimitFilter())
	default:
		return fmt.Sprintf("%T", f.Filter)
	}
}

func mutationsString(muts []*bigtablepb.Mutation) string {
	var parts []string
	for _, m := range muts {
		switch {
		case m.GetSetCell() != nil:
			c := m.GetSetCell()
			value := string(c.Value)
			if c.FamilyName == "data" {
				value = "<content>"
			}
			ts := "client-time"
			if c.TimestampMicros == -1 {
				ts = "server-time"
			}
			parts = append(parts, fmt.Sprintf("set(%s:%s=%q,%s)", c.FamilyName, c.ColumnQualifier, value, ts))
		case m.GetDeleteFromColumn() != nil:
			c := m.GetDeleteFromColumn()
			parts = append(parts, fmt.Sprintf("delete(%s:%s)", c.FamilyName, c.ColumnQualifier))
		case m.GetDeleteFromFamily() != nil:
			parts = append(parts, fmt.Sprintf("delete(%s)", m.GetDeleteFromFamily().FamilyName))
		case m.GetDeleteFromRow() != nil:
			parts = append(parts, "delete-row")
		default:
			parts = append(parts, fmt.Sprintf("%T", m.Mutation))
		}
	}
	return strings.Join(parts, " ")
}

type recordingStream struct {
	grpc.ServerStream
	method string
	rec    *btRecorder
}

func (s *recordingStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.rec.add(s.method, m)
	}
	return err
}

// newRecordingBigtable starts an in-process Bigtable with the given tables
// that records every request.
func newRecordingBigtable(t *testing.T, tables []tableFamilies) *btRecorder {
	t.Helper()
	rec := &btRecorder{}
	srv, err := bttest.NewServer("localhost:0",
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			rec.add(info.FullMethod, req)
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, &recordingStream{ServerStream: ss, method: info.FullMethod, rec: rec})
		}),
	)
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	t.Setenv("BIGTABLE_EMULATOR_HOST", srv.Addr)
	ctx := context.Background()
	adm, err := bigtable.NewAdminClient(ctx, emulatorProject, emulatorBigtable)
	require.NoError(t, err)
	defer adm.Close()
	for _, table := range tables {
		require.NoError(t, adm.CreateTable(ctx, table.name))
		for _, f := range table.families {
			require.NoError(t, adm.CreateColumnFamily(ctx, table.name, f))
		}
	}
	rec.calls = nil
	return rec
}

// gcOffOpts returns server options with GC off and every GC tuning option set
// to a value that would cause traffic within milliseconds if it applied.
func gcOffOpts() NewServerOpts {
	return NewServerOpts{
		CacheTTL:            time.Millisecond,
		GCTouchInterval:     time.Millisecond,
		GCHeartbeatInterval: 10 * time.Millisecond,
	}
}

// gcOffScenario runs the reads and writes of a seeding and a build against two
// servers on one deployment, so that one serves from its caches and the other
// from Bigtable. It returns the Bigtable requests of a repeated Headers call
// after the GC intervals and TTL of opts have passed.
func gcOffScenario(t *testing.T, rec *btRecorder, dbID, bucket string, opts NewServerOpts) []string {
	t.Helper()
	require := require.New(t)
	ctx := context.Background()
	opts.ProjectID = emulatorProject
	opts.InstanceID = emulatorInstance
	opts.DatabaseID = dbID
	opts.BtInstance = emulatorBigtable
	opts.BucketName = bucket
	opts.MaxCachedHeaders = 1024
	opts.MaxCachedContents = 1024

	writer, err := New(ctx, &opts)
	require.NoError(err)
	defer writer.Close()
	reader, err := New(ctx, &opts)
	require.NoError(err)
	defer reader.Close()

	put := func(s *GCPServer, data string) entities.Hash {
		key := objects.HashBlobBytes([]byte(data))
		raw := fmt.Appendf(nil, "blob %d\x00%s", len(data), data)
		require.NoError(s.PutObject(ctx, key, remote.NewContentReader(io.NopCloser(strings.NewReader(string(raw))), remote.EncodingRaw)))
		return key
	}
	a := put(writer, "gc off a")
	b := put(writer, "gc off b")
	put(writer, "gc off a")
	tree, treeData := treeOf(a, b)
	require.NoError(writer.PutObject(ctx, tree, remote.NewContentReader(io.NopCloser(strings.NewReader(string(treeData))), remote.EncodingRaw)))
	missing := objects.HashBlobBytes([]byte("gc off missing"))

	for _, s := range []*GCPServer{writer, reader, reader} {
		hdrs, err := s.Headers(ctx, []entities.Hash{a, b, tree, missing})
		require.NoError(err)
		require.NotZero(hdrs[2].Type)
		require.Zero(hdrs[3].Type)
		for _, key := range []entities.Hash{tree, a} {
			r, err := s.GetObject(ctx, key)
			require.NoError(err)
			_, err = io.ReadAll(r)
			require.NoError(err)
			require.NoError(r.Close())
		}
		_, err = s.GetObject(ctx, missing)
		require.True(entities.IsNotExist(err))
	}
	require.NoError(writer.AddMapResult(ctx, tree, a))
	for _, s := range []*GCPServer{writer, reader} {
		results, err := s.MapResults(ctx, tree)
		require.NoError(err)
		require.Equal([]entities.Hash{a}, results)
	}
	require.NoError(writer.PutRefs(ctx, "example.com", "p", []remote.RefHash{{Ref: "refs/heads/main", Hash: tree}}))
	require.NoError(writer.XchgRefs(ctx, "example.com", "p", []remote.RefXchg{{Ref: "refs/heads/main", Current: tree, Desired: a}}))
	refs, err := reader.SearchRefs(ctx, "example.com", "p", "", 0)
	require.NoError(err)
	require.Len(refs, 1)

	// The TTL counts in whole seconds.
	time.Sleep(1100 * time.Millisecond)
	before := len(rec.snapshot())
	_, err = reader.Headers(ctx, []entities.Hash{a, b, tree})
	require.NoError(err)
	_, err = reader.MapResults(ctx, tree)
	require.NoError(err)
	return rec.snapshot()[before:]
}

// writeCallLog writes the sorted Bigtable requests to the file named by
// ABFS_GC_CALL_LOG, so that a run can be compared with one of a server
// without GC code.
func writeCallLog(t *testing.T, calls []string) {
	t.Helper()
	file := os.Getenv("ABFS_GC_CALL_LOG")
	if file == "" {
		return
	}
	sorted := slices.Clone(calls)
	slices.Sort(sorted)
	require.NoError(t, os.WriteFile(file, []byte(strings.Join(sorted, "\n")+"\n"), 0o644))
}

func gcTraffic(calls []string) []string {
	var found []string
	for _, c := range calls {
		if strings.Contains(c, " gc ") || strings.Contains(c, "family(meta|g)") || strings.Contains(c, "set(g:") ||
			strings.Contains(c, "delete(g") || strings.Contains(c, "server-time") ||
			strings.HasPrefix(c, "CheckAndMutateRow") || strings.HasPrefix(c, "ReadModifyWriteRow") {
			found = append(found, c)
		}
	}
	return found
}

func spannerRows(t *testing.T, dbID, table string) int {
	t.Helper()
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", emulatorProject, emulatorInstance, dbID))
	require.NoError(t, err)
	defer client.Close()
	rows := 0
	require.NoError(t, client.Single().Query(ctx, spanner.Statement{SQL: "SELECT 1 FROM " + table}).Do(func(*spanner.Row) error {
		rows++
		return nil
	}))
	return rows
}

// A server with GC off on a deployment that has the GC resources reads and
// writes only what a server without GC code does.
func TestGCOff_NoGCTraffic(t *testing.T) {
	require := require.New(t)
	requireSpannerEmulator(t)
	bucket := newEmulatorBucket(t)
	dbID := newEmulatorDatabase(t)
	rec := newRecordingBigtable(t, serverTables(true))

	late := gcOffScenario(t, rec, dbID, bucket, gcOffOpts())
	calls := rec.snapshot()
	writeCallLog(t, calls)

	require.NotEmpty(calls)
	require.Empty(gcTraffic(calls))
	require.Contains(calls, "ReadRows objects filter=chain(family(meta),latest(1)) row="+objects.HashBlobBytes([]byte("gc off missing")).String())
	// No TTL: the repeated calls are answered from the caches, except the map
	// rows, which are never cached.
	for _, c := range late {
		require.True(strings.HasPrefix(c, "ReadRows maps "), c)
	}
	require.Zero(spannerRows(t, dbID, "GcReplicas"))
	require.Zero(spannerRows(t, dbID, "GcServers"))
	require.Zero(spannerRows(t, dbID, "GcState"))
}

// The same scenario with GC on shows what the check above would catch.
func TestGCOff_GCOnHasGCTraffic(t *testing.T) {
	require := require.New(t)
	requireSpannerEmulator(t)
	bucket := newEmulatorBucket(t)
	dbID := newEmulatorDatabase(t)
	rec := newRecordingBigtable(t, serverTables(true))

	opts := gcOffOpts()
	opts.GC = true
	late := gcOffScenario(t, rec, dbID, bucket, opts)
	calls := rec.snapshot()

	traffic := strings.Join(gcTraffic(calls), "\n")
	require.Contains(traffic, "MutateRow gc row=")
	require.Contains(traffic, "family(meta|g)")
	require.Contains(traffic, "MutateRows gc ")
	require.NotEmpty(gcTraffic(late), "the TTL expired, so the reads went to Bigtable again")
	// Both servers wrote heartbeats and kept their rows when they closed.
	require.Zero(spannerRows(t, dbID, "GcReplicas"))
	require.Equal(2, spannerRows(t, dbID, "GcServers WHERE StoppedAt IS NOT NULL"))
}

// A server with GC off starts on a deployment that plain init created, without
// any garbage collector resource, and one with GC on refuses to.
func TestGCOff_StartsWithoutGCResources(t *testing.T) {
	require := require.New(t)
	requireSpannerEmulator(t)
	bucket := newEmulatorBucket(t)
	dbID := newEmulatorDatabaseWith(t, casdb.SchemaStatements())
	rec := newRecordingBigtable(t, serverTables(false))

	gcOffScenario(t, rec, dbID, bucket, gcOffOpts())
	require.Empty(gcTraffic(rec.snapshot()))

	opts := gcOffOpts()
	opts.GC = true
	opts.ProjectID, opts.InstanceID, opts.DatabaseID, opts.BtInstance, opts.BucketName = emulatorProject, emulatorInstance, dbID, emulatorBigtable, bucket
	_, err := New(context.Background(), &opts)
	require.ErrorContains(err, "abfs server gcp init --gc")
}

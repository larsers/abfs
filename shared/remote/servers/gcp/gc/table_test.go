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
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
)

// newTestBigtable starts an in-process Bigtable with the tables objects and gc
// and returns a client for it.
func newTestBigtable(t *testing.T) *bigtable.Client {
	t.Helper()
	ctx := context.Background()
	srv, err := bttest.NewServer("localhost:0")
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	adm, err := bigtable.NewAdminClient(ctx, "test-proj", "test-bt", option.WithGRPCConn(conn))
	require.NoError(t, err)
	require.NoError(t, adm.CreateTable(ctx, "objects"))
	for _, f := range []string{"meta", "data", ObjectsFamily} {
		require.NoError(t, adm.CreateColumnFamily(ctx, "objects", f))
	}
	require.NoError(t, adm.CreateTable(ctx, TableName))
	require.NoError(t, adm.CreateColumnFamily(ctx, TableName, EdgesFamily))
	require.NoError(t, adm.CreateColumnFamily(ctx, TableName, TouchFamily))

	client, err := bigtable.NewClient(ctx, "test-proj", "test-bt", option.WithGRPCConn(conn))
	require.NoError(t, err)
	return client
}

func readGCRow(t *testing.T, client *bigtable.Client, key Hash) *Row {
	t.Helper()
	row, err := client.Open(TableName).ReadRow(context.Background(), key.String(), bigtable.RowFilter(bigtable.LatestNFilter(1)))
	require.NoError(t, err)
	if len(row) == 0 {
		return nil
	}
	r, err := ParseRow(row)
	require.NoError(t, err)
	return r
}

func TestEdgeWriter_PutWritesRow(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	client := newTestBigtable(t)
	w := NewEdgeWriter(client)

	start := &binding.BuildStart{BuildStartData: binding.BuildStartData{RootBegin: testHash("root").String()}}
	typ, obj := decoded(t, start)
	before := time.Now().Add(-time.Second)
	require.NoError(w.Put(ctx, start.Hash, typ, obj))

	r := readGCRow(t, client, start.Hash)
	require.NotNil(r)
	require.Equal(start.Hash, r.Key)
	require.Equal(string(binding.ObjectTypeBuildStart), r.Type)
	require.Equal(int64(ExtractorVersion), r.Version)
	require.Equal([]Hash{testHash("root")}, r.Edges)
	require.WithinRange(r.Written, before, time.Now().Add(time.Second))

	blob := testHash("blob")
	require.NoError(w.Put(ctx, blob, string(entities.ObjectTypeBlob), nil))
	r = readGCRow(t, client, blob)
	require.NotNil(r)
	require.Empty(r.Edges)
	require.Equal(string(entities.ObjectTypeBlob), r.Type)
}

func TestEdgeWriter_PutFailsForUnknownType(t *testing.T) {
	client := newTestBigtable(t)
	key := testHash("unknown")
	err := NewEdgeWriter(client).Put(context.Background(), key, "unknown", &unknownObject{})
	require.ErrorIs(t, err, errUnsupportedType)
	require.Nil(t, readGCRow(t, client, key))
}

func TestEdgeWriter_RetriesTransientErrors(t *testing.T) {
	tests := []struct {
		name      string
		errs      []error
		wantCalls int
		wantErr   bool
	}{
		{"success", nil, 1, false},
		{"unavailable then success", []error{status.Error(codes.Unavailable, "down")}, 2, false},
		{"aborted twice then success", []error{status.Error(codes.Aborted, "a"), status.Error(codes.Aborted, "a")}, 3, false},
		{"permanent", []error{status.Error(codes.NotFound, "no table")}, 1, true},
		{"not a status", []error{errors.New("other")}, 1, true},
		{"always unavailable", []error{
			status.Error(codes.Unavailable, "1"), status.Error(codes.Unavailable, "2"), status.Error(codes.Unavailable, "3"),
			status.Error(codes.Unavailable, "4"), status.Error(codes.Unavailable, "5"), status.Error(codes.Unavailable, "6"),
		}, writeAttempts, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			w := &EdgeWriter{apply: func(ctx context.Context, row string, m *bigtable.Mutation) error {
				calls++
				if calls <= len(tt.errs) {
					return tt.errs[calls-1]
				}
				return nil
			}}
			err := w.Write(context.Background(), testHash("a"), "tree", nil)
			require.Equal(t, tt.wantCalls, calls)
			if tt.wantErr {
				require.Error(t, err)
				// The chain is cut, so that a missing table is not a missing
				// object for the client.
				require.Equal(t, codes.Unknown, status.Code(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEdgeWriter_StopsRetryingWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	w := &EdgeWriter{apply: func(ctx context.Context, row string, m *bigtable.Mutation) error {
		calls++
		cancel()
		return status.Error(codes.Unavailable, "down")
	}}
	require.Error(t, w.Write(ctx, testHash("a"), "tree", nil))
	require.Equal(t, 1, calls)
}

func TestParseRow_RejectsBadRows(t *testing.T) {
	key := testHash("a").String()
	tests := []struct {
		name string
		row  bigtable.Row
		want string
	}{
		{"bad key", bigtable.Row{}, "invalid gc row key"},
		{"partial edge", bigtable.Row{EdgesFamily: {{Row: key, Column: "e:links", Value: make([]byte, 30)}}}, "30 bytes"},
		{"bad version", bigtable.Row{EdgesFamily: {{Row: key, Column: "e:ver", Value: []byte{1}}}}, "version of 1 bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseRow(tt.row)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

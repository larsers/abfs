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
	"fmt"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// createGCTable creates the table gc in the in-process Bigtable of
// createTestServerWithCASDB.
func createGCTable(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	adm, err := bigtable.NewAdminClient(ctx, "test-proj", "test-bt")
	require.NoError(t, err)
	defer adm.Close()
	require.NoError(t, adm.CreateTable(ctx, gc.TableName))
	require.NoError(t, adm.CreateColumnFamily(ctx, gc.TableName, gc.EdgesFamily))
	require.NoError(t, adm.CreateColumnFamily(ctx, gc.TableName, gc.TouchFamily))
}

func readGCRow(t *testing.T, s *GCPServer, key entities.Hash) *gc.Row {
	t.Helper()
	row, err := s.casdb.BTClient().Open(gc.TableName).ReadRow(context.Background(), key.String(), bigtable.RowFilter(bigtable.LatestNFilter(1)))
	require.NoError(t, err)
	if len(row) == 0 {
		return nil
	}
	r, err := gc.ParseRow(row)
	require.NoError(t, err)
	return r
}

func putRaw(s *GCPServer, key entities.Hash, data []byte) error {
	return s.PutObject(context.Background(), key, remote.NewContentReader(io.NopCloser(bytes.NewReader(data)), remote.EncodingRaw))
}

// treeOf returns a tree object with one file entry for each blob.
func treeOf(blobs ...entities.Hash) (entities.Hash, []byte) {
	var body []byte
	for i, b := range blobs {
		body = append(body, fmt.Appendf(nil, "100644 f%d\x00", i)...)
		body = append(body, b[:]...)
	}
	data := append(fmt.Appendf(nil, "tree %d\x00", len(body)), body...)
	return entities.Hash(sha1.Sum(data)), data
}

func TestPutObject_WritesGCRowBeforeObject(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s, _ := createTestServerWithCASDB(t)
	createGCTable(t)
	s.gcEdges = gc.NewEdgeWriter(s.casdb.BTClient())
	s.addObject = func(ctx context.Context, hdr *ObjectHeader, content *ObjectContent) error {
		require.NotNil(readGCRow(t, s, hdr.Id), "object row written before its gc row")
		return s.casdb.AddObject(ctx, hdr, content)
	}

	blob := objects.HashBlobBytes([]byte("hello"))
	require.NoError(putRaw(s, blob, []byte("blob 5\x00hello")))
	treeKey, treeData := treeOf(blob, blob)
	require.NoError(putRaw(s, treeKey, treeData))
	start := &binding.BuildStart{BuildStartData: binding.BuildStartData{RootBegin: treeKey.String(), PreviousBuild: blob.String()}}
	require.NoError(start.Encode())
	require.NoError(putRaw(s, start.Hash, start.Encoded))

	tests := []struct {
		key       entities.Hash
		wantType  string
		wantEdges []entities.Hash
	}{
		{blob, "blob", []entities.Hash{}},
		{treeKey, "tree", []entities.Hash{blob}},
		{start.Hash, "build-start", []entities.Hash{treeKey}},
	}
	for _, tt := range tests {
		r := readGCRow(t, s, tt.key)
		require.NotNil(r, tt.wantType)
		require.Equal(tt.wantType, r.Type)
		require.Equal(int64(gc.ExtractorVersion), r.Version)
		require.Equal(tt.wantEdges, r.Edges, tt.wantType)
	}

	// A second put of an existing object leaves its gc row as it is.
	before := readGCRow(t, s, treeKey).Written
	time.Sleep(2 * time.Millisecond)
	require.NoError(putRaw(s, treeKey, treeData))
	require.Equal(before, readGCRow(t, s, treeKey).Written)
	hdrs, err := s.Headers(ctx, []entities.Hash{blob, treeKey, start.Hash})
	require.NoError(err)
	for _, h := range hdrs {
		require.NotZero(h.Type)
	}
}

func TestPutObject_FailedGCRowFailsPut(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s, rec := newRecordingServer(t)
	// The table gc does not exist, so its row cannot be written.
	s.gcEdges = gc.NewEdgeWriter(s.casdb.BTClient())
	added := false
	s.addObject = func(ctx context.Context, hdr *ObjectHeader, content *ObjectContent) error {
		added = true
		return s.casdb.AddObject(ctx, hdr, content)
	}

	key := objects.HashBlobBytes([]byte("no edges"))
	err := putRaw(s, key, []byte("blob 8\x00no edges"))
	require.ErrorContains(err, "gc row")
	// The client must not read a missing table as a missing object.
	require.False(entities.IsNotExist(err))
	require.False(added)
	hdrs, err := s.Headers(ctx, []entities.Hash{key})
	require.NoError(err)
	require.Zero(hdrs[0].Type)
	require.Eventually(func() bool { return rec.removedCount() == 1 }, 2*time.Second, 10*time.Millisecond)
}

func TestPutObject_GCOffWritesNoGCRow(t *testing.T) {
	require := require.New(t)
	s, _ := createTestServerWithCASDB(t)
	createGCTable(t)

	key := objects.HashBlobBytes([]byte("old server"))
	require.NoError(putRaw(s, key, []byte("blob 10\x00old server")))
	require.Nil(readGCRow(t, s, key))
}

func TestNew_GCOption(t *testing.T) {
	require := require.New(t)
	s := newEmulatorServer(t, &NewServerOpts{GC: true})
	require.NotNil(s.gcEdges)
	// New adds the empty file and directory through PutObject.
	for _, key := range []entities.Hash{entities.EmptyFileHash, entities.EmptyDirHash} {
		r := readGCRow(t, s, key)
		require.NotNil(r)
		require.Empty(r.Edges)
	}

	s = newEmulatorServer(t, nil)
	require.Nil(s.gcEdges)
	require.False(s.gcHooks.on)
	for _, key := range []entities.Hash{entities.EmptyFileHash, entities.EmptyDirHash} {
		require.Nil(readGCRow(t, s, key))
	}
}

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
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/objects"
)

// newTestBucket creates a bucket on the Cloud Storage emulator named by
// STORAGE_EMULATOR_HOST_GRPC.
func newTestBucket(t *testing.T) *storage.BucketHandle {
	t.Helper()
	if os.Getenv("STORAGE_EMULATOR_HOST_GRPC") == "" {
		t.Skip("STORAGE_EMULATOR_HOST_GRPC is not set")
	}
	ctx := context.Background()
	client, err := storage.NewGRPCClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	b := make([]byte, 6)
	_, err = rand.Read(b)
	require.NoError(t, err)
	bucket := client.Bucket("b" + hex.EncodeToString(b))
	require.NoError(t, bucket.Create(ctx, "test-proj", nil))
	return bucket
}

func listOf(recs []listRecord) func() (listRecord, bool) {
	return func() (listRecord, bool) {
		if len(recs) == 0 {
			return listRecord{}, false
		}
		r := recs[0]
		recs = recs[1:]
		return r, true
	}
}

// A list reads back as written, and a create of an existing list succeeds
// only for the run that wrote it, so that a retried create after a lost
// response is not taken for another collector.
func TestList_WriteReadAndCreateOnce(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	bucket := newTestBucket(t)
	recs := []listRecord{
		{key: objects.HashBlobBytes([]byte("a")), typ: typeIndex["blob"], size: 1},
		{key: objects.HashBlobBytes([]byte("b")), typ: typeIndex["tree"], size: 1 << 40},
		{key: objects.HashBlobBytes([]byte("c")), typ: typeIndex["commit"], size: 300},
	}
	h := &listHeader{cycle: 7, snapshot: time.UnixMicro(time.Now().UnixMicro()), holder: "gc-host-1", epoch: 3, count: int64(len(recs)), types: typeNames}
	name := listObjectName("default", 7)
	require.Equal("gc/condemned/default/7", name)
	require.NoError(writeList(ctx, bucket, name, h, listOf(recs)))

	l, err := readList(ctx, bucket, name)
	require.NoError(err)
	require.Equal(h, l.header)
	for i, r := range recs {
		require.Equal(r.key, l.keys[i])
		require.Equal(typeNames[r.typ], l.typeName(i))
	}

	require.NoError(writeList(ctx, bucket, name, h, listOf(recs)))
	other := *h
	other.epoch = 4
	require.ErrorContains(writeList(ctx, bucket, name, &other, listOf(recs)), `written by the GC run "gc-host-1#3"`)

	// A record count that does not match the header writes nothing.
	short := *h
	short.cycle = 8
	require.Error(writeList(ctx, bucket, listObjectName("default", 8), &short, listOf(recs[:2])))
	_, err = bucket.Object(listObjectName("default", 8)).Attrs(ctx)
	require.ErrorIs(err, storage.ErrObjectNotExist)
}

// Pruning keeps the newest cycles' lists and their companions and leaves
// other objects alone.
func TestPruneLists(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	bucket := newTestBucket(t)
	names := []string{"gc/condemned/default/9", "gc/condemned/default/9.live", "gc/condemned/default/10", "gc/condemned/default/11", "gc/condemned/default/11.live", "gc/condemned/default/notes", "gc/condemned/other/1"}
	for _, n := range names {
		w := bucket.Object(n).NewWriter(ctx)
		require.NoError(w.Close())
	}
	removed, err := pruneLists(ctx, bucket, "default", 2)
	require.NoError(err)
	require.Equal(int64(2), removed)
	for i, n := range names {
		_, err := bucket.Object(n).Attrs(ctx)
		if i < 2 {
			require.ErrorIs(err, storage.ErrObjectNotExist, n)
		} else {
			require.NoError(err, n)
		}
	}
}

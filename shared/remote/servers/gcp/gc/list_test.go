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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"slices"
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
	h := &listHeader{cycle: 7, snapshot: time.UnixMicro(time.Now().UnixMicro()).UTC(), holder: "gc-host-1", epoch: 3, count: int64(len(recs)), types: typeNames}
	name := listObjectName("default", 7)
	require.Equal("abfsgc/condemned/default/7", name)
	require.NoError(writeList(ctx, writeBudget{deadline: DefaultWriteDeadline}, bucket, name, h, listOf(recs)))

	l, err := readList(ctx, bucket, name)
	require.NoError(err)
	require.Equal(h, l.header)
	for i, r := range recs {
		require.Equal(r.key, l.keys[i])
		require.Equal(typeNames[r.typ], l.typeName(i))
	}

	require.NoError(writeList(ctx, writeBudget{deadline: DefaultWriteDeadline}, bucket, name, h, listOf(recs)))
	other := *h
	other.epoch = 4
	require.ErrorContains(writeList(ctx, writeBudget{deadline: DefaultWriteDeadline}, bucket, name, &other, listOf(recs)), `written by the GC run "gc-host-1#3"`)

	// A record count that does not match the header writes nothing.
	short := *h
	short.cycle = 8
	require.Error(writeList(ctx, writeBudget{deadline: DefaultWriteDeadline}, bucket, listObjectName("default", 8), &short, listOf(recs[:2])))
	_, err = bucket.Object(listObjectName("default", 8)).Attrs(ctx)
	require.ErrorIs(err, storage.ErrObjectNotExist)
}

// A condemned list that is truncated, counts more records than its size
// holds, or has bytes after its records fails to decode without a large
// allocation, and a read error at its end is returned as it is. Times decode
// in UTC.
func TestDecodeList_Malformed(t *testing.T) {
	require := require.New(t)
	recs := []listRecord{
		{key: objects.HashBlobBytes([]byte("a")), typ: typeIndex["blob"], size: 1},
		{key: objects.HashBlobBytes([]byte("b")), typ: typeIndex["tree"], size: 2},
	}
	h := &listHeader{cycle: 7, snapshot: time.UnixMicro(1000), holder: "gc", epoch: 3, count: int64(len(recs)), types: []string{"blob", "tree"}}
	var buf bytes.Buffer
	require.NoError(encodeList(&buf, h, listOf(recs)))
	valid := buf.Bytes()
	hdrLen := len(valid) - len(recs)*listRecordSize
	// The count follows the magic, the version, the cycle, the snapshot, the
	// holder and the epoch.
	countAt := len(listMagic) + 4 + 8 + 8 + 2 + len(h.holder) + 8
	patched := func(v uint64) []byte {
		b := slices.Clone(valid)
		binary.BigEndian.PutUint64(b[countAt:], v)
		return b
	}
	l, err := decodeList(bytes.NewReader(valid), int64(len(valid)))
	require.NoError(err)
	require.Equal([]Hash{recs[0].key, recs[1].key}, l.keys)
	require.Equal(time.UTC, l.header.snapshot.Location())

	// A truncated list is read with the size of the whole one, as when a
	// download ends early, and else with its own size.
	tests := []struct {
		name string
		data []byte
		size int
		want string
	}{
		{"truncated header", valid[:countAt+3], len(valid), "failed to read the header"},
		{"negative count", patched(1 << 63), len(valid), "counts"},
		{"count past the size", patched(1 << 40), len(valid), "counts 1099511627776 entries"},
		{"records past the size", valid[:hdrLen+listRecordSize+5], hdrLen + listRecordSize + 5, "counts 2 entries, but 34 bytes are left"},
		{"truncated records", valid[:hdrLen+listRecordSize+5], len(valid), "after 1 records"},
		{"extra byte", append(slices.Clone(valid), 0), len(valid) + 1, "more than the 2 entries its header counts"},
	}
	for _, tt := range tests {
		_, err := decodeList(bytes.NewReader(tt.data), int64(tt.size))
		require.ErrorContains(err, tt.want, tt.name)
	}

	boom := errors.New("boom")
	_, err = decodeList(io.MultiReader(bytes.NewReader(valid), errReader{boom}), int64(len(valid)))
	require.ErrorIs(err, boom)
}

// Pruning keeps the newest cycles' lists and their companions and leaves
// other objects alone. It deletes nothing once the lease could expire before
// two write deadlines and the clock skew margin have passed, as every other
// write of the collector.
func TestPruneLists(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	bucket := newTestBucket(t)
	names := []string{"abfsgc/condemned/default/9", "abfsgc/condemned/default/9.live", "abfsgc/condemned/default/10", "abfsgc/condemned/default/11", "abfsgc/condemned/default/11.live", "abfsgc/condemned/default/notes", "abfsgc/condemned/other/1"}
	for _, n := range names {
		w := bucket.Object(n).NewWriter(ctx)
		require.NoError(w.Close())
	}
	expiring := &Lease{}
	expiring.expiry.Store(time.Now().Add(2*DefaultWriteDeadline + clockSkewMargin - time.Second).UnixNano())
	removed, err := pruneLists(ctx, writeBudget{deadline: DefaultWriteDeadline, lease: expiring}, bucket, "default", 2)
	require.ErrorIs(err, errLeaseExpiring)
	require.Zero(removed)
	for _, n := range names {
		_, err := bucket.Object(n).Attrs(ctx)
		require.NoError(err, n)
	}

	removed, err = pruneLists(ctx, writeBudget{deadline: DefaultWriteDeadline, lease: farLease()}, bucket, "default", 2)
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

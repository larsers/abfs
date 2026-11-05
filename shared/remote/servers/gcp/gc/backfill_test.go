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
	"io"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
)

func TestSplitRange(t *testing.T) {
	tests := []struct {
		name       string
		start, end string
		n          int
		want       []string
	}{
		{"whole table", "", "", 4, []string{"", "40000000", "80000000", "c0000000", ""}},
		{"one shard", "", "", 1, []string{"", ""}},
		{"from a key", "8", "", 2, []string{"8", "c0000000", ""}},
		{"bounded", "10", "20", 2, []string{"10", "18000000", "20"}},
		{"long start key", "abcd1234ff", "abcd1236", 2, []string{"abcd1234ff", "abcd1235", "abcd1236"}},
		{"too small to split", "abcd1234ff", "abcd1234ff01", 4, []string{"abcd1234ff", "abcd1234ff01"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitRange(tt.start, tt.end, tt.n)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSplitRange_RejectsBadKeys(t *testing.T) {
	tests := []struct {
		name       string
		start, end string
		want       string
	}{
		{"upper case", "ABC", "", "lowercase hexadecimal"},
		{"not hex", "xyz", "", "lowercase hexadecimal"},
		{"too long", "00000000000000000000000000000000000000000", "", "lowercase hexadecimal"},
		{"empty range", "20", "10", "empty key range"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := splitRange(tt.start, tt.end, 2)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestLimiter_SpacesOutWork(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	require.NoError(newLimiter(0).wait(ctx, 1000))

	l := newLimiter(100)
	start := time.Now()
	for range 3 {
		require.NoError(l.wait(ctx, 10))
	}
	// The first 10 units go at once, the next two batches 100 ms apart.
	require.GreaterOrEqual(time.Since(start), 190*time.Millisecond)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.NoError(l.wait(ctx, 1000))
	require.ErrorIs(l.wait(cancelled, 1), context.Canceled)
}

// treeServer serves an empty tree for every key and calls onGet first.
type treeServer struct {
	remote.RemoteProvider
	onGet func(key Hash)
}

// GetObject implements remote.RemoteProvider.
func (s *treeServer) GetObject(ctx context.Context, key Hash) (remote.ContentReadCloser, error) {
	if s.onGet != nil {
		s.onGet(key)
	}
	return remote.NewContentReader(io.NopCloser(bytes.NewReader([]byte("tree 0\x00"))), remote.EncodingRaw), nil
}

// A backfill stopped in the middle of its second shard, after the first one
// finished, resumes in the second shard, and a run from the start finds the
// first shard current.
func TestBackfill_ResumesInTheShardItStoppedIn(t *testing.T) {
	require := require.New(t)
	client := newTestBigtable(t)
	objects := client.Open(objectsTable)
	keys := map[string]string{
		"1111111111111111111111111111111111111111": "blob",
		"2222222222222222222222222222222222222222": "blob",
		"3333333333333333333333333333333333333333": "blob",
		"9111111111111111111111111111111111111111": "blob",
		"9222222222222222222222222222222222222222": "blob",
		"9333333333333333333333333333333333333333": "tree",
		"9444444444444444444444444444444444444444": "blob",
	}
	for key, typ := range keys {
		mut := bigtable.NewMutation()
		mut.Set("meta", "type", bigtable.Now(), []byte(typ))
		require.NoError(objects.Apply(context.Background(), key, mut))
	}

	// The read of the tree in the second shard stops the run once the first
	// shard has written its last row.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gcTable := client.Open(TableName)
	stopper := &treeServer{onGet: func(Hash) {
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			row, err := gcTable.ReadRow(context.Background(), "3333333333333333333333333333333333333333")
			if err == nil && len(row) > 0 {
				break
			}
		}
		cancel()
	}}
	opts := BackfillOpts{Shards: 2, BatchSize: 2}
	stats, err := Backfill(ctx, client, stopper, &opts)
	require.Error(err)
	require.False(stats.Done)
	require.Equal("9222222222222222222222222222222222222222", stats.ResumeKey)
	require.Nil(readGCRow(t, client, entities.NewHash("9333333333333333333333333333333333333333")))

	opts.Start = stats.ResumeKey
	stats, err = Backfill(context.Background(), client, &treeServer{}, &opts)
	require.NoError(err)
	require.True(stats.Done)
	require.Equal(int64(3), stats.Scanned)
	require.Equal(int64(1), stats.Current)

	stats, err = Backfill(context.Background(), client, &treeServer{}, &BackfillOpts{Shards: 2, BatchSize: 2})
	require.NoError(err)
	require.Equal(int64(len(keys)), stats.Scanned)
	require.Equal(int64(len(keys)), stats.Current)
}

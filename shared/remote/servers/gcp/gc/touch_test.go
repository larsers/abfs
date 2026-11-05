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
	"crypto/sha1"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/abfs/shared/entities"
)

// newHooksTestBigtable starts an in-process Bigtable with the tables objects,
// maps and gc and returns a client for it.
func newHooksTestBigtable(t *testing.T) *bigtable.Client {
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
	tables := map[string][]string{
		objectsTableName: {"meta", "data", ObjectsFamily},
		mapsTableName:    {mapsFamily},
		TableName:        {EdgesFamily, TouchFamily},
	}
	for table, families := range tables {
		require.NoError(t, adm.CreateTable(ctx, table))
		for _, f := range families {
			require.NoError(t, adm.CreateColumnFamily(ctx, table, f))
		}
	}
	client, err := bigtable.NewClient(ctx, "test-proj", "test-bt", option.WithGRPCConn(conn))
	require.NoError(t, err)
	return client
}

// cellTime returns the timestamp of the newest cell of family:column in row of
// table, and false when there is none.
func cellTime(t *testing.T, client *bigtable.Client, table, row, family, column string) (bigtable.Timestamp, bool) {
	t.Helper()
	r, err := client.Open(table).ReadRow(context.Background(), row, bigtable.RowFilter(bigtable.LatestNFilter(1)))
	require.NoError(t, err)
	for _, item := range r[family] {
		if item.Column == family+":"+column {
			return item.Timestamp, true
		}
	}
	return 0, false
}

func testHashes(n int) []Hash {
	hashes := make([]Hash, n)
	for i := range hashes {
		hashes[i] = Hash(sha1.Sum(fmt.Appendf(nil, "%d", i)))
	}
	return hashes
}

func TestToucher_WritesObjectAndMapRowTouches(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	client := newHooksTestBigtable(t)
	key := entities.NewHash("0123456789012345678901234567890123456789")
	mapRow := key.String() + "#" + key.String()
	added := bigtable.Now()
	mut := bigtable.NewMutation()
	mut.Set(mapsFamily, "", added, []byte("1"))
	require.NoError(client.Open(mapsTableName).Apply(ctx, mapRow, mut))
	time.Sleep(2 * time.Millisecond)

	toucher := NewToucher(client, 10)
	require.True(toucher.TouchObject(key))
	require.True(toucher.TouchMapRow(mapRow))
	toucher.Close()
	toucher.Close()

	require.Equal(TouchStats{Written: 2}, toucher.Stats())
	_, ok := cellTime(t, client, TableName, key.String(), TouchFamily, TouchColumn)
	require.True(ok, "no touch in the gc row")
	ts, ok := cellTime(t, client, mapsTableName, mapRow, mapsFamily, "")
	require.True(ok)
	require.Greater(ts, added, "the map row cell was not rewritten")
}

func TestToucher_DropsWhenTheBufferIsFull(t *testing.T) {
	require := require.New(t)
	client := newHooksTestBigtable(t)
	toucher := newToucher(client, 2)
	toucher.enqueueWait = 50 * time.Millisecond
	keys := []Hash{
		entities.NewHash("1111111111111111111111111111111111111111"),
		entities.NewHash("2222222222222222222222222222222222222222"),
		entities.NewHash("3333333333333333333333333333333333333333"),
		entities.NewHash("5555555555555555555555555555555555555555"),
	}
	require.True(toucher.TouchObject(keys[0]))
	require.True(toucher.TouchObject(keys[1]))
	start := time.Now()
	require.False(toucher.TouchObject(keys[2]))
	require.GreaterOrEqual(time.Since(start), toucher.enqueueWait, "dropped without waiting for room")
	// After a timeout, touches are dropped at once until there is room again.
	start = time.Now()
	require.False(toucher.TouchObject(keys[3]))
	require.Less(time.Since(start), toucher.enqueueWait)
	require.Equal(TouchStats{Dropped: 2}, toucher.Stats())

	go toucher.run()
	toucher.Close()
	require.Equal(TouchStats{Written: 2, Dropped: 2}, toucher.Stats())
	for i, key := range keys {
		_, ok := cellTime(t, client, TableName, key.String(), TouchFamily, TouchColumn)
		require.Equal(i < 2, ok, key)
	}
}

func TestToucher_WaitsForRoom(t *testing.T) {
	require := require.New(t)
	client := newHooksTestBigtable(t)
	toucher := newToucher(client, 1)
	keys := testHashes(3)
	require.True(toucher.TouchObject(keys[0]))
	toucher.stalled.Store(true)
	require.False(toucher.TouchObject(keys[1]))

	// A touch that waits gets the room that a starting worker makes.
	toucher.stalled.Store(false)
	time.AfterFunc(50*time.Millisecond, func() { go toucher.run() })
	require.True(toucher.TouchObject(keys[1]))
	require.Eventually(func() bool { return toucher.TouchObject(keys[2]) }, 5*time.Second, 10*time.Millisecond)
	toucher.Close()
	require.Equal(TouchStats{Written: 3, Dropped: 1}, toucher.Stats())
}

func TestToucher_DrainsABurst(t *testing.T) {
	require := require.New(t)
	client := newHooksTestBigtable(t)
	toucher := NewToucher(client, 1000)
	keys := testHashes(50000)
	for _, key := range keys {
		require.True(toucher.TouchObject(key))
	}
	toucher.Close()
	require.Equal(TouchStats{Written: int64(len(keys))}, toucher.Stats())
}

func TestToucher_FlushesWhileRunning(t *testing.T) {
	client := newHooksTestBigtable(t)
	toucher := NewToucher(client, 10)
	t.Cleanup(toucher.Close)
	key := entities.NewHash("4444444444444444444444444444444444444444")
	require.True(t, toucher.TouchObject(key))
	require.Eventually(t, func() bool { return toucher.Stats().Written == 1 }, 5*time.Second, 50*time.Millisecond)
}

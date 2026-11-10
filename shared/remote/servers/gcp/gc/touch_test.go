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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/abfs/shared/entities"
)

// newHooksTestBigtable starts an in-process Bigtable with the tables objects,
// maps and gc and returns a client for it that dials with opts.
func newHooksTestBigtable(t *testing.T, opts ...grpc.DialOption) *bigtable.Client {
	t.Helper()
	ctx := context.Background()
	srv, err := bttest.NewServer("localhost:0")
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	conn, err := grpc.NewClient(srv.Addr, append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	adm, err := bigtable.NewAdminClient(ctx, "test-proj", "test-bt", option.WithGRPCConn(conn))
	require.NoError(t, err)
	tables := map[string][]string{
		objectsTableName: {"meta", "data", ObjectsFamily},
		mapsTableName:    {mapsFamily},
		TableName:        {EdgesFamily, TouchFamily},
		chunksTableName:  {objectsDataFamily},
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

	require.Equal(TouchStats{Written: 1, MapRowsWritten: 1}, toucher.Stats())
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

func TestToucher_TryTouchObjectDoesNotWait(t *testing.T) {
	require := require.New(t)
	client := newHooksTestBigtable(t)
	toucher := newToucher(client, 1)
	toucher.enqueueWait = time.Hour
	keys := testHashes(3)
	require.True(toucher.TryTouchObject(keys[0]))
	start := time.Now()
	require.False(toucher.TryTouchObject(keys[1]))
	require.Less(time.Since(start), time.Second)
	require.False(toucher.stalled.Load(), "a touch that did not wait stalled the waiting ones")
	require.Equal(TouchStats{Dropped: 1}, toucher.Stats())

	// A waiting touch still gets the room that a starting worker makes.
	time.AfterFunc(50*time.Millisecond, func() { go toucher.run() })
	require.True(toucher.TouchObject(keys[2]))
	toucher.Close()
	require.Equal(TouchStats{Written: 2, Dropped: 1}, toucher.Stats())
}

func TestToucher_DropsTouchesAfterClose(t *testing.T) {
	require := require.New(t)
	client := newHooksTestBigtable(t)
	toucher := NewToucher(client, 10)
	keys := testHashes(3)
	require.True(toucher.TouchObject(keys[0]))
	toucher.Close()
	require.False(toucher.TouchObject(keys[1]))
	require.False(toucher.TryTouchObject(keys[2]))
	require.False(toucher.TouchMapRow(keys[2].String() + "#" + keys[2].String()))
	require.Equal(TouchStats{Written: 1, Dropped: 2, MapRowsDropped: 1}, toucher.Stats())
}

// Map row touches are counted apart from object touches, and a late one is
// counted as failed but never makes the Toucher late.
func TestToucher_CountsMapRowTouchesApart(t *testing.T) {
	require := require.New(t)
	tc := newToucher(newHooksTestBigtable(t), 4)
	tc.enqueueWait = time.Millisecond
	var offset atomic.Int64
	tc.now = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	keys := testHashes(3)
	rows := []string{keys[0].String() + "#" + keys[1].String(), keys[1].String() + "#" + keys[2].String()}
	require.True(tc.TouchMapRow(rows[0]))
	require.True(tc.TouchMapRow(rows[1]))
	require.False(tc.TouchMapRow(rows[0]))
	require.Equal(TouchStats{MapRowsDropped: 1}, tc.Stats())

	offset.Store(int64(10 * TouchDrainBound))
	require.False(tc.Late(), "a late map row touch")
	tc.flush(context.Background(), []touch{<-tc.queue, <-tc.queue})
	require.Equal(TouchStats{MapRowsWritten: 2, MapRowsDropped: 1, MapRowsFailed: 2}, tc.Stats())

	require.True(tc.TouchObject(keys[0]))
	offset.Store(int64(20 * TouchDrainBound))
	require.True(tc.Late())
	tc.flush(context.Background(), []touch{<-tc.queue})
	require.Equal(TouchStats{Written: 1, Failed: 1, MapRowsWritten: 2, MapRowsDropped: 1, MapRowsFailed: 2}, tc.Stats())
}

// Map row touches take at most half of the buffer and never wait, and a
// dropped one does not stall the object touches: an object touch still gets
// the room the map rows left, and still waits for room in a full buffer.
func TestToucher_MapRowTouchesLeaveRoomForObjectTouches(t *testing.T) {
	require := require.New(t)
	tc := newToucher(newHooksTestBigtable(t), 4)
	tc.enqueueWait = time.Hour
	keys := testHashes(4)
	for i := range 2 {
		require.True(tc.TouchMapRow(keys[i].String() + "#" + keys[i].String()))
	}
	start := time.Now()
	require.False(tc.TouchMapRow(keys[2].String() + "#" + keys[2].String()))
	require.Less(time.Since(start), time.Second, "a map row touch waited")
	require.False(tc.stalled.Load(), "a dropped map row touch stalled the object touches")
	require.True(tc.TouchObject(keys[0]))
	require.True(tc.TouchObject(keys[1]))

	// The buffer is full: a map row touch is dropped at once, and an object
	// touch waits for the room a worker makes.
	require.False(tc.TouchMapRow(keys[3].String() + "#" + keys[3].String()))
	require.False(tc.stalled.Load(), "a dropped map row touch stalled the object touches")
	time.AfterFunc(50*time.Millisecond, func() { <-tc.queue })
	require.True(tc.TouchObject(keys[2]))
	require.Equal(TouchStats{MapRowsDropped: 2}, tc.Stats())

	// After object touches stalled, a queued map row touch leaves them
	// stalled until an object touch gets room.
	tc.enqueueWait = time.Millisecond
	require.False(tc.TouchObject(keys[3]))
	require.True(tc.stalled.Load())
	for range 4 {
		<-tc.queue
	}
	require.True(tc.TouchMapRow(keys[3].String() + "#" + keys[3].String()))
	require.True(tc.stalled.Load(), "a map row touch ended the stall of the object touches")
	require.True(tc.TouchObject(keys[3]))
	require.False(tc.stalled.Load())
	require.Equal(TouchStats{Dropped: 1, MapRowsDropped: 2}, tc.Stats())
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

// omitFirstEntry is a client stream that drops the result of entry 0 from
// MutateRows responses, as a server that leaves an entry out does. It counts
// the entries sent in sent.
type omitFirstEntry struct {
	grpc.ClientStream
	sent *atomic.Int32
}

func (s omitFirstEntry) SendMsg(m any) error {
	s.sent.Add(int32(len(m.(*btpb.MutateRowsRequest).Entries)))
	return s.ClientStream.SendMsg(m)
}

func (s omitFirstEntry) RecvMsg(m any) error {
	if err := s.ClientStream.RecvMsg(m); err != nil {
		return err
	}
	res := m.(*btpb.MutateRowsResponse)
	var kept []*btpb.MutateRowsResponse_Entry
	for _, e := range res.Entries {
		if e.Index != 0 {
			kept = append(kept, e)
		}
	}
	res.Entries = kept
	return nil
}

// A touch whose entry is missing from the MutateRows response counts as
// failed, not as written, and is not sent again.
func TestToucher_CountsAnOmittedEntryAsFailed(t *testing.T) {
	require := require.New(t)
	var calls, sent atomic.Int32
	omit := grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || !strings.HasSuffix(method, "/MutateRows") {
			return s, err
		}
		calls.Add(1)
		return omitFirstEntry{ClientStream: s, sent: &sent}, nil
	})
	client := newHooksTestBigtable(t, omit)
	toucher := NewToucher(client, 10)
	keys := testHashes(3)
	for _, key := range keys {
		require.True(toucher.TouchObject(key))
	}
	toucher.Close()
	// The workers may split the touches over several calls; each call loses
	// one entry.
	omitted := int64(calls.Load())
	require.Positive(omitted)
	require.Equal(TouchStats{Written: int64(len(keys)) - omitted, Failed: omitted}, toucher.Stats())
	require.Equal(int32(len(keys)), sent.Load(), "an omitted entry was sent again")
}

// blockMutateRows holds every MutateRows call until release is closed, and
// closes entered at the first one.
func blockMutateRows(entered, release chan struct{}) grpc.DialOption {
	var once atomic.Bool
	return grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if strings.HasSuffix(method, "/MutateRows") {
			if once.CompareAndSwap(false, true) {
				close(entered)
			}
			<-release
		}
		return streamer(ctx, desc, cc, method, opts...)
	})
}

// A late touch is counted as failed before its write, so that a heartbeat
// that reads the stats while the write still runs records the loss.
func TestToucher_CountsALateTouchBeforeItsWrite(t *testing.T) {
	require := require.New(t)
	entered, release := make(chan struct{}), make(chan struct{})
	tc := newToucher(newHooksTestBigtable(t, blockMutateRows(entered, release)), 10)
	require.True(tc.TouchObject(testHashes(1)[0]))
	tc.now = func() time.Time { return time.Now().Add(TouchDrainBound) }
	done := make(chan struct{})
	go func() {
		defer close(done)
		tc.flush(context.Background(), []touch{<-tc.queue})
	}()
	<-entered
	require.Equal(TouchStats{Failed: 1}, tc.Stats())
	close(release)
	<-done
	require.Equal(TouchStats{Written: 1, Failed: 1}, tc.Stats())
}

// Late reports a touch that waited past the late bound while no worker took
// it, as in a frozen process, until a flush counts it.
func TestToucher_Late(t *testing.T) {
	require := require.New(t)
	tc := newToucher(newHooksTestBigtable(t), 10)
	var offset atomic.Int64
	tc.now = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	keys := testHashes(2)
	require.True(tc.TouchObject(keys[0]))
	require.False(tc.Late())
	offset.Store(int64(tc.lateAfter - touchAgeSlot))
	require.False(tc.Late(), "a touch within the late bound")
	offset.Store(int64(tc.lateAfter + 2*touchAgeSlot))
	require.True(tc.Late())
	// A touch queued now is not late; the old one still is.
	require.True(tc.TouchObject(keys[1]))
	first := <-tc.queue
	require.True(tc.Late())
	tc.flush(context.Background(), []touch{first})
	require.False(tc.Late())
	require.Equal(TouchStats{Written: 1, Failed: 1}, tc.Stats())
	// A dropped touch does not stay pending.
	tc.flush(context.Background(), []touch{<-tc.queue})
	close(tc.stop)
	require.False(tc.TouchObject(keys[0]))
	offset.Store(int64(10 * tc.lateAfter))
	require.False(tc.Late())
	require.Equal(TouchStats{Written: 2, Dropped: 1, Failed: 1}, tc.Stats())
}

// The late bound leaves a write its timeout before TouchDrainBound: a touch
// queued for nearly the drain bound is late, as its write could end after it.
func TestToucher_LateBoundLeavesTimeForTheWrite(t *testing.T) {
	require := require.New(t)
	tc := newToucher(newHooksTestBigtable(t), 10)
	var offset atomic.Int64
	tc.now = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	require.True(tc.TouchObject(testHashes(1)[0]))
	offset.Store(int64(TouchDrainBound - touchWriteTimeout/2))
	require.True(tc.Late())
	tc.flush(context.Background(), []touch{<-tc.queue})
	require.Equal(TouchStats{Written: 1, Failed: 1}, tc.Stats())
}

// A touch whose write runs while the process is frozen stays visible to Late
// until the write's outcome is counted, so that a heartbeat right after the
// freeze records the loss the write will count.
func TestToucher_LateWhileTheWriteRuns(t *testing.T) {
	require := require.New(t)
	entered, release := make(chan struct{}), make(chan struct{})
	tc := newToucher(newHooksTestBigtable(t, blockMutateRows(entered, release)), 10)
	var offset atomic.Int64
	tc.now = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	require.True(tc.TouchObject(testHashes(1)[0]))
	done := make(chan struct{})
	go func() {
		defer close(done)
		tc.flush(context.Background(), []touch{<-tc.queue})
	}()
	<-entered
	require.False(tc.Late())
	offset.Store(int64(TouchDrainBound - touchAgeSlot))
	require.False(tc.Late(), "a write within the drain bound")
	offset.Store(int64(TouchDrainBound + time.Minute))
	require.True(tc.Late())
	require.Equal(TouchStats{}, tc.Stats())
	close(release)
	<-done
	require.False(tc.Late())
	require.Equal(TouchStats{Written: 1}, tc.Stats())
}

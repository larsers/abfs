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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// statsToucher returns the stats it is set to, after calling onStats.
type statsToucher struct {
	fakeToucher
	mu      sync.Mutex
	stats   gc.TouchStats
	onStats func()
}

func (f *statsToucher) Stats() gc.TouchStats {
	if f.onStats != nil {
		f.onStats()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

func (f *statsToucher) set(st gc.TouchStats) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stats = st
}

// newHeartbeatServer returns a server on the emulators with the GC hooks on,
// its first heartbeat written, and a statsToucher in place of its toucher.
func newHeartbeatServer(t *testing.T, interval time.Duration) (*GCPServer, *statsToucher) {
	t.Helper()
	s := newEmulatorCASDBServer(t)
	require.NoError(t, s.enableGCHooks(&NewServerOpts{GCHeartbeatInterval: interval, GCTouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour}))
	t.Cleanup(s.stopGCHooks)
	s.gcHooks.toucher.Close()
	st := &statsToucher{}
	s.gcHooks.toucher = st
	return s, st
}

func readServer(t *testing.T, s *GCPServer) gc.Server {
	t.Helper()
	servers, err := gc.ReadServers(context.Background(), s.casdb.SpannerClient())
	require.NoError(t, err)
	require.Len(t, servers, 1)
	return servers[0]
}

// spannerTime returns the read time of a strong Spanner read.
func spannerTime(t *testing.T, s *GCPServer) time.Time {
	t.Helper()
	ro := s.casdb.SpannerClient().Single()
	defer ro.Close()
	require.NoError(t, ro.Query(context.Background(), spanner.Statement{SQL: "SELECT 1"}).Do(func(*spanner.Row) error { return nil }))
	ts, err := ro.Timestamp()
	require.NoError(t, err)
	return ts
}

// The first heartbeat is written before enableGCHooks returns, with the
// server's settings, so a server has its row before its first call.
func TestGCHooks_Heartbeat(t *testing.T) {
	require := require.New(t)
	s, _ := newHeartbeatServer(t, time.Hour)
	r := readServer(t, s)
	require.Equal(s.gcHooks.server.ID, r.ID)
	require.Equal(int64(gc.ExtractorVersion), r.Version)
	require.True(r.GC)
	require.True(r.LinkValidation)
	require.Equal(36*time.Hour, r.TouchInterval)
	require.Equal(6*time.Hour, r.CacheTTL)
	require.Equal(time.Hour, r.HeartbeatInterval)
	require.WithinDuration(time.Now(), r.LastSeen, time.Minute)
	require.False(r.StatsTime.After(r.LastSeen))
	require.Zero(r.TouchLossTime)
	require.Zero(r.StoppedAt)
	require.Zero(s.gcHooks.heartbeatFailures.Load())
}

// StatsTime is a Spanner read time taken before the touch stats are read, so
// that a collector can rely on a row with StatsTime after its snapshot to
// hold every loss counted before the snapshot, whatever the server's clock.
func TestGCHooks_HeartbeatStatsTimeBeforeStatsRead(t *testing.T) {
	require := require.New(t)
	s, st := newHeartbeatServer(t, time.Hour)
	before := spannerTime(t, s)
	var statsRead time.Time
	st.onStats = func() { statsRead = spannerTime(t, s) }
	require.NoError(s.heartbeat(context.Background()))
	r := readServer(t, s)
	require.False(r.StatsTime.Before(before), "StatsTime %s before %s", r.StatsTime, before)
	require.False(r.StatsTime.After(statsRead), "StatsTime %s after the stats read at %s", r.StatsTime, statsRead)
}

// A heartbeat after a lost touch records the loss with its commit time; one
// without a new loss keeps the recorded time.
func TestGCHooks_HeartbeatRecordsLostTouches(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s, st := newHeartbeatServer(t, time.Hour)
	require.NoError(s.heartbeat(ctx))
	require.Zero(readServer(t, s).TouchLossTime)

	st.set(gc.TouchStats{Written: 5, Dropped: 1})
	require.NoError(s.heartbeat(ctx))
	r := readServer(t, s)
	require.Equal(r.LastSeen, r.TouchLossTime)
	first := r.TouchLossTime

	st.set(gc.TouchStats{Written: 9, Dropped: 1})
	require.NoError(s.heartbeat(ctx))
	r = readServer(t, s)
	require.Equal(first, r.TouchLossTime)
	require.True(r.LastSeen.After(first))

	st.set(gc.TouchStats{Written: 9, Dropped: 1, Failed: 1})
	require.NoError(s.heartbeat(ctx))
	r = readServer(t, s)
	require.Equal(r.LastSeen, r.TouchLossTime)
	require.True(r.TouchLossTime.After(first))
}

// A server that stops keeps its row and records the stop, and the touches it
// lost while stopping, so that the collector can tell a clean stop from a
// crash for as long as it needs the row.
func TestGCHooks_StopKeepsHeartbeatRow(t *testing.T) {
	for _, lose := range []bool{false, true} {
		t.Run(fmt.Sprintf("touches lost %v", lose), func(t *testing.T) {
			require := require.New(t)
			s, st := newHeartbeatServer(t, time.Hour)
			if lose {
				st.set(gc.TouchStats{Failed: 1})
			}
			logged := captureLog(t)
			s.stopGCHooks()
			r := readServer(t, s)
			require.NotZero(r.StoppedAt)
			if lose {
				require.Equal(r.StoppedAt, r.TouchLossTime)
				require.Contains(logged.String(), "recorded lost touches in the GC heartbeat row "+r.ID)
			} else {
				require.Zero(r.TouchLossTime)
			}
			require.Contains(logged.String(), "GC touches at stop:")
		})
	}
}

// A server whose first heartbeat cannot be written does not start, so that
// no server rescues or touches without a row the collector can check.
func TestGCHooks_FirstHeartbeatFailureStopsStart(t *testing.T) {
	require := require.New(t)
	s := newEmulatorCASDBServer(t)
	orig, origTimeout := writeServer, firstHeartbeatTimeout
	t.Cleanup(func() { writeServer, firstHeartbeatTimeout = orig, origTimeout })
	writeServer = func(context.Context, *spanner.Client, *gc.Server, time.Time, bool) error {
		return status.Error(codes.PermissionDenied, "injected")
	}
	firstHeartbeatTimeout = 200 * time.Millisecond
	err := s.enableGCHooks(&NewServerOpts{GCHeartbeatInterval: time.Minute, GCTouchInterval: time.Hour, CacheTTL: time.Hour})
	require.ErrorContains(err, "failed to write the first GC heartbeat")
	require.Nil(s.gcHooks.stopHeartbeat)
	servers, err := gc.ReadServers(context.Background(), s.casdb.SpannerClient())
	require.NoError(err)
	require.Empty(servers)
}

// Once the last successful heartbeat is older than two intervals plus the
// write timeout, every call answers Unavailable and the health probe fails,
// until a heartbeat succeeds again. A server that lost its Spanner writes must
// not serve uses whose lost touches no row would show.
func TestGCHooks_StopsServingWithStaleHeartbeat(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	now := uint32(1000)
	setCacheNow(t, &now)
	s, _ := newHeartbeatServer(t, gc.MaxHeartbeatInterval)
	key := putBlob(t, s, "stale heartbeat")
	var failing atomic.Bool
	orig := writeServer
	t.Cleanup(func() { writeServer = orig })
	writeServer = func(ctx context.Context, c *spanner.Client, sv *gc.Server, statsTime time.Time, lost bool) error {
		if failing.Load() {
			return status.Error(codes.PermissionDenied, "injected")
		}
		return orig(ctx, c, sv, statsTime, lost)
	}
	serving := func() error {
		_, herr := s.Headers(ctx, []entities.Hash{key})
		gerr := readObject(s, key)
		perr := s.PutRefs(ctx, "example.com", "p", []remote.RefHash{{Ref: "refs/heads/main", Hash: key}})
		require.Equal(status.Code(herr), status.Code(gerr))
		require.Equal(status.Code(herr), status.Code(perr))
		require.Equal(status.Code(herr), status.Code(s.CheckGCHeartbeat(ctx)))
		return herr
	}
	require.NoError(serving())

	failing.Store(true)
	stale := uint32(2*gc.MaxHeartbeatInterval/time.Second + heartbeatTimeout/time.Second)
	now += uint32(gc.MaxHeartbeatInterval / time.Second)
	require.Error(s.heartbeat(ctx))
	require.NoError(serving(), "one failed heartbeat")
	now = 1000 + stale
	require.NoError(serving())
	now++
	err := serving()
	require.Equal(codes.Unavailable, status.Code(err), err)
	require.ErrorContains(err, "GC heartbeat stale")

	failing.Store(false)
	require.NoError(s.heartbeat(ctx))
	require.NoError(serving())
	require.Equal(int64(1), s.gcHooks.heartbeatFailures.Load())
}

// A server with GC needs a heartbeat interval of at most
// gc.MaxHeartbeatInterval, because the collector waits that long for one.
func TestGCHooks_RejectsBadDurations(t *testing.T) {
	tests := []struct {
		name string
		opts NewServerOpts
		want string
	}{
		{"negative TTL", NewServerOpts{CacheTTL: -time.Second, GCHeartbeatInterval: time.Minute}, "invalid negative cache TTL"},
		{"no heartbeat", NewServerOpts{}, "invalid GC heartbeat interval 0s"},
		{"long heartbeat", NewServerOpts{GCHeartbeatInterval: 11 * time.Minute}, "invalid GC heartbeat interval 11m0s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := createTestServerWithCASDB(t)
			tt.opts.GC = true
			require.ErrorContains(t, s.startGCHooks(context.Background(), &tt.opts), tt.want)
		})
	}
}

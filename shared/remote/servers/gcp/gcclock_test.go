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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
)

// A cache entry expires when the wall clock jumps ahead of the monotonic
// clock, as after a suspend, and the clock never goes back when the wall
// clock steps back.
func TestStopFreeClock(t *testing.T) {
	require := require.New(t)
	c := &stopFreeClock{epoch: time.Now()}
	var mono, wall time.Duration
	orig := cacheNow
	cacheNow = func() uint32 { return c.seconds(mono, wall) }
	t.Cleanup(func() { cacheNow = orig })
	g := &gcHooks{on: true, cacheTTL: 3600}

	mono, wall = 10*time.Second, 10*time.Second
	loaded := cacheNow()
	require.Equal(uint32(11), loaded)
	mono, wall = 20*time.Second, time.Hour+10*time.Second
	require.Equal(uint32(3611), cacheNow())
	require.True(g.expired(loaded), "a suspend of an hour")
	mono, wall = 30*time.Second, 30*time.Second
	require.Equal(uint32(3611), cacheNow(), "the wall clock stepped back")
	mono, wall = 2*time.Hour, 2*time.Hour
	require.Equal(uint32(7201), cacheNow())
}

// readClock is a cacheNow clock that a Bigtable read of the in-process
// server advances by delay seconds while armed.
type readClock struct {
	now   atomic.Uint32
	delay atomic.Uint32
	armed atomic.Bool
}

func (c *readClock) interceptor() grpc.ServerOption {
	return grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.HasSuffix(info.FullMethod, "/ReadRows") && c.armed.Load() {
			c.now.Add(c.delay.Load())
		}
		return handler(srv, ss)
	})
}

// Entries age from the start of the Bigtable read that loaded them, and a
// read that took the cache TTL or longer is not used: what it read may be
// older than the collector lets a server use it.
func TestCacheAge_FromReadStart(t *testing.T) {
	ctx := context.Background()
	clock := &readClock{}
	clock.now.Store(100)
	orig := cacheNow
	cacheNow = clock.now.Load
	t.Cleanup(func() { cacheNow = orig })
	s := newEmulatorCASDBServerWith(t, clock.interceptor())
	require.NoError(t, s.enableGCHooks(&NewServerOpts{CacheTTL: time.Minute, GCTouchInterval: time.Hour}))
	t.Cleanup(s.stopGCHooks)
	key := putBlob(t, s, "read start")
	data := []byte("blob 10\x00read start")

	tests := []struct {
		name    string
		call    func() error
		content bool
	}{
		{"headers", func() error { _, err := s.Headers(ctx, []entities.Hash{key}); return err }, false},
		{"get object", func() error { return readObject(s, key) }, true},
		{"put object", func() error { return putRaw(s, key, data) }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			for _, delay := range []uint32{59, 60} {
				s.forget(key)
				start := clock.now.Load()
				clock.delay.Store(delay)
				clock.armed.Store(true)
				err := tt.call()
				clock.armed.Store(false)
				if delay == 60 {
					require.Equal(codes.Unavailable, status.Code(err), err)
					continue
				}
				require.NoError(err)
				h, ok := s.headers[key[0]].Peek(key)
				require.True(ok)
				require.Equal(start, h.Loaded)
				if tt.content {
					c, ok := s.content[key[0]].Peek(key)
					require.True(ok)
					require.Equal(start, c.loaded)
				}
			}
		})
	}
}

// A heartbeat after the wall clock grew more than 10 s beyond the monotonic
// clock records a touch loss: the process was paused, and touches and cache
// ages are not to be trusted across the pause.
func TestGCHooks_HeartbeatRecordsPause(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	var mu sync.Mutex
	mono, wall := time.Hour, time.Hour
	origElapsed := clockElapsed
	clockElapsed = func() (time.Duration, time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		return mono, wall
	}
	t.Cleanup(func() { clockElapsed = origElapsed })
	now := uint32(1)
	setCacheNow(t, &now)
	advance := func(m, w time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		mono, wall = mono+m, wall+w
	}
	s, _ := newHeartbeatServer(t, time.Hour)

	advance(time.Minute, time.Minute+maxPause)
	require.NoError(s.heartbeat(ctx))
	require.Zero(readServer(t, s).TouchLossTime)

	advance(time.Minute, time.Minute+maxPause+time.Second)
	require.NoError(s.heartbeat(ctx))
	r := readServer(t, s)
	require.Equal(r.LastSeen, r.TouchLossTime)
}

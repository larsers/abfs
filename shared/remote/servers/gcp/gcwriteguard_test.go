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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// When the validation of a call's links started gc.ValidationWriteBound or
// longer ago, the call fails with Unavailable and writes nothing, so that the
// collector can bound the time from a validation that read an object to the
// writes that record a reference to it.
func TestValidationDeadline_FailsLateWrite(t *testing.T) {
	ctx := context.Background()
	clock := &readClock{}
	clock.now.Store(100)
	orig := cacheNow
	cacheNow = clock.now.Load
	t.Cleanup(func() { cacheNow = orig })
	s := newEmulatorCASDBServerWith(t, clock.interceptor())
	require.NoError(t, s.enableGCHooks(&NewServerOpts{CacheTTL: time.Hour, GCTouchInterval: time.Hour}))
	t.Cleanup(s.stopGCHooks)
	blob := putBlob(t, s, "linked")
	other := putBlob(t, s, "other")
	bound := uint32(gc.ValidationWriteBound / time.Second)
	trees := [][]entities.Hash{{blob}, {blob, other}}

	tests := []struct {
		name    string
		call    func(i int) error
		written func(t *testing.T, i int) bool
	}{
		{
			name: "put object",
			call: func(i int) error {
				key, data := treeOf(trees[i]...)
				return putRaw(s, key, data)
			},
			written: func(t *testing.T, i int) bool {
				key, _ := treeOf(trees[i]...)
				_, err := s.casdb.GetHeader(ctx, key)
				if err == nil {
					require.NotNil(t, readGCRow(t, s, key))
					return true
				}
				require.ErrorIs(t, err, casdb.ErrNotFound)
				require.Nil(t, readGCRow(t, s, key), "a gc row without its object")
				return false
			},
		},
		{
			name: "put refs",
			call: func(i int) error {
				return s.PutRefs(ctx, "example.com", "p", []remote.RefHash{{Ref: refName(i), Hash: blob}})
			},
			written: func(t *testing.T, i int) bool {
				hashes, err := s.GetRefs(ctx, "example.com", "p", []string{refName(i)})
				require.NoError(t, err)
				return hashes[0] == blob
			},
		},
		{
			name: "xchg refs",
			call: func(i int) error {
				return s.XchgRefs(ctx, "example.com", "x", []remote.RefXchg{{Ref: refName(i), Desired: blob}})
			},
			written: func(t *testing.T, i int) bool {
				hashes, err := s.GetRefs(ctx, "example.com", "x", []string{refName(i)})
				require.NoError(t, err)
				return hashes[0] == blob
			},
		},
		{
			name: "add map result",
			call: func(i int) error {
				from := blob
				if i == 1 {
					from = other
				}
				return s.AddMapResult(ctx, from, blob)
			},
			written: func(t *testing.T, i int) bool {
				from := blob
				if i == 1 {
					from = other
				}
				results, err := s.casdb.MapResults(ctx, from)
				require.NoError(t, err)
				return len(results) == 1
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			for i, delay := range []uint32{bound / 2, bound} {
				s.forget(blob)
				s.forget(other)
				clock.delay.Store(delay)
				clock.armed.Store(true)
				err := tt.call(i)
				clock.armed.Store(false)
				if delay == bound {
					require.Equal(codes.Unavailable, status.Code(err), err)
					require.False(tt.written(t, i))
					continue
				}
				require.NoError(err)
				require.True(tt.written(t, i))
			}
		})
	}
}

// A gc row write that ends after the bound leaves no object row: the guard
// checks the bound again before the object row is written, so that no new
// parent of an object lands later than the collector allows for.
func TestValidationDeadline_ObjectRowAfterSlowGCRow(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	var now atomic.Uint32
	now.Store(100)
	orig := cacheNow
	cacheNow = now.Load
	t.Cleanup(func() { cacheNow = orig })
	bound := uint32(gc.ValidationWriteBound / time.Second)
	var armed atomic.Bool
	slowGCRow := grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if r, ok := req.(interface{ GetTableName() string }); ok && armed.Load() && strings.HasSuffix(r.GetTableName(), "/"+gc.TableName) {
			now.Add(bound)
		}
		return resp, err
	})
	s := newEmulatorCASDBServerWith(t, slowGCRow)
	require.NoError(s.enableGCHooks(&NewServerOpts{CacheTTL: time.Hour, GCTouchInterval: time.Hour}))
	t.Cleanup(s.stopGCHooks)
	key, data := treeOf(putBlob(t, s, "linked"))

	armed.Store(true)
	err := putRaw(s, key, data)
	armed.Store(false)
	require.Equal(codes.Unavailable, status.Code(err), err)
	require.NotNil(readGCRow(t, s, key), "the gc row was written")
	_, err = s.casdb.GetHeader(ctx, key)
	require.ErrorIs(err, casdb.ErrNotFound)
	require.NoError(putRaw(s, key, data))
}

func refName(i int) string {
	return "refs/heads/b" + string(rune('0'+i))
}

// The write runs under a deadline at the end of the bound, so that a write
// that stalls cannot land later.
func TestWriteGuard_Deadline(t *testing.T) {
	require := require.New(t)
	now := uint32(100)
	setCacheNow(t, &now)
	w := writeGuard{on: true, start: 100}
	now = 130
	var deadline time.Time
	require.NoError(w.do(context.Background(), func(ctx context.Context) error {
		var ok bool
		deadline, ok = ctx.Deadline()
		require.True(ok)
		return nil
	}))
	require.WithinDuration(time.Now().Add(gc.ValidationWriteBound-31*time.Second), deadline, time.Second)
	hasDeadline := true
	require.NoError(writeGuard{}.do(context.Background(), func(ctx context.Context) error {
		_, hasDeadline = ctx.Deadline()
		return nil
	}))
	require.False(hasDeadline, "without GC the write keeps the call's context")
}

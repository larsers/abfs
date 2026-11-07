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
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// stallCalls makes the first n calls of method stall until their context
// ends, as a write that hangs on an overloaded tablet, and counts the calls.
type stallCalls struct {
	method string
	n      atomic.Int32
	calls  atomic.Int32
}

func (s *stallCalls) stall(ctx context.Context, method string) error {
	if !strings.HasSuffix(method, s.method) {
		return nil
	}
	s.calls.Add(1)
	if s.n.Add(-1) < 0 {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *stallCalls) options() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			if err := s.stall(ctx, method); err != nil {
				return nil, err
			}
			return streamer(ctx, desc, cc, method, opts...)
		}),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if err := s.stall(ctx, method); err != nil {
				return err
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
	}
}

func rowKeys(keys []Hash) []string {
	rows := make([]string, len(keys))
	for i, k := range keys {
		rows[i] = k.String()
	}
	return rows
}

// Each attempt of a collector write ends after the write deadline and is
// retried, so a write that stalls does not hold the cycle until the lease is
// lost; after the last attempt the write fails.
func TestApplyBulk_AttemptDeadline(t *testing.T) {
	tests := []struct {
		stalls  int32
		wantErr bool
	}{
		{2, false},
		{writeAttempts, true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d stalls", tt.stalls), func(t *testing.T) {
			require := require.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			stalls := &stallCalls{method: "/MutateRows"}
			client := newHooksTestBigtable(t, stalls.options()...)
			stalls.n.Store(tt.stalls)
			keys := testHashes(3)
			mut := bigtable.NewMutation()
			mut.Set(ObjectsFamily, CondemnedColumn, bigtable.Now(), markerValue(4))
			err := applyBulk(ctx, writeBudget{deadline: 200 * time.Millisecond}, client.Open(objectsTableName), rowKeys(keys), mut)
			require.NoError(ctx.Err(), "the run's context ended first")
			if tt.wantErr {
				require.Error(err)
				require.GreaterOrEqual(stalls.calls.Load(), int32(writeAttempts))
				return
			}
			require.NoError(err)
			for _, key := range keys {
				require.Equal(markerValue(4), condemnedOf(t, client, key))
			}
		})
	}
}

// A rescue write that stalls ends after its attempt timeout and is retried
// within the client's call.
func TestRescuer_AttemptTimeout(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stalls := &stallCalls{method: "/CheckAndMutateRow"}
	client := newHooksTestBigtable(t, stalls.options()...)
	key := testHashes(1)[0]
	putObjectRow(t, client, key, markerValue(3))
	r := NewRescuer(client)
	r.attemptTimeout = 200 * time.Millisecond
	stalls.n.Store(1)
	live, err := r.Rescue(ctx, key, markerValue(3))
	require.NoError(err)
	require.True(live)
	require.Nil(condemnedOf(t, client, key))
	require.Equal(int32(2), stalls.calls.Load())
}

// An attempt that ended at its deadline is transient while the run lives,
// whether the library returns a status or the plain context error.
func TestRetryable_AttemptDeadline(t *testing.T) {
	live := context.Background()
	done, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, retryable(live, context.DeadlineExceeded))
	require.True(t, retryable(live, fmt.Errorf("failed to write: %w", context.DeadlineExceeded)))
	require.False(t, retryable(done, context.DeadlineExceeded))
	require.False(t, retryable(live, context.Canceled))
}

// No write starts when the lease could expire before two attempts and the
// clock skew margin have passed, on the wall clock against the expiry that
// Spanner recorded, so that a holder whose renewals fail or that was paused
// stops before another takes the lease over.
func TestLease_CheckWrite(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	orig := wallNow
	wallNow = func() time.Time { return now }
	t.Cleanup(func() { wallNow = orig })
	l := &Lease{}
	d := 30 * time.Second
	expiry := now.Add(2*d + clockSkewMargin + time.Second)
	l.expiry.Store(expiry.UnixNano())
	require.NoError(l.checkWrite(d))
	now = now.Add(2 * time.Second)
	require.ErrorIs(l.checkWrite(d), errLeaseExpiring)

	stalls := &stallCalls{method: "/MutateRows"}
	client := newHooksTestBigtable(t, stalls.options()...)
	mut := bigtable.NewMutation()
	mut.Set(ObjectsFamily, CondemnedColumn, bigtable.Now(), markerValue(4))
	err := applyBulk(context.Background(), writeBudget{deadline: d, lease: l}, client.Open(objectsTableName), rowKeys(testHashes(2)), mut)
	require.ErrorIs(err, errLeaseExpiring)
	require.Zero(stalls.calls.Load(), "a write was sent")
}

// A failed renewal is tried again after renewRetry rather than after a full
// renewal period, so that one failure does not bring the run close to the
// write check's margin.
func TestLease_RetriesFailedRenewal(t *testing.T) {
	require := require.New(t)
	var mu sync.Mutex
	var calls []time.Time
	origRenew, origRetry := renewLease, renewRetry
	t.Cleanup(func() { renewLease, renewRetry = origRenew, origRetry })
	renewRetry = 100 * time.Millisecond
	renewLease = func(*Lease, context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, time.Now())
		if len(calls) == 1 {
			return fmt.Errorf("injected")
		}
		return nil
	}
	l := &Lease{duration: 6 * time.Second, taken: time.Now()}
	ctx, stop := l.Keep(context.Background())
	defer stop()
	require.Eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) >= 2
	}, 5*time.Second, 10*time.Millisecond)
	mu.Lock()
	gap := calls[1].Sub(calls[0])
	mu.Unlock()
	require.Less(gap, time.Second)
	require.NoError(ctx.Err())
}

// A streamed object write gets the write deadline plus the time to send what
// it wrote at streamRate.
func TestWriteBudget_StreamDeadline(t *testing.T) {
	require := require.New(t)
	b := writeBudget{deadline: 200 * time.Millisecond}
	idle, _, stopIdle, err := b.stream(context.Background())
	require.NoError(err)
	defer stopIdle()
	ctx, wrap, stop, err := b.stream(context.Background())
	require.NoError(err)
	defer stop()
	w := wrap(io.Discard)
	_, err = w.Write(make([]byte, streamRate/2))
	require.NoError(err)
	time.Sleep(400 * time.Millisecond)
	require.Error(idle.Err(), "a write that sent nothing ends at the deadline")
	require.NoError(ctx.Err(), "half a second of data extends the deadline")
	require.Eventually(func() bool { return ctx.Err() != nil }, 2*time.Second, 10*time.Millisecond)
}

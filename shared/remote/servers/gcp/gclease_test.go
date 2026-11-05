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
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

const testGrace = 72 * time.Hour

func TestAcquireLease_RefusesValidLeaseOfAnotherHolder(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	sp := newEmulatorCASDBServer(t).casdb.SpannerClient()

	first, err := gc.AcquireLease(ctx, sp, gc.DefaultStateName, "first", gc.PhaseDryRun, time.Minute, testGrace)
	require.NoError(err)
	require.Equal(int64(1), first.Cycle)
	require.Equal(int64(1), first.Epoch)

	_, err = gc.AcquireLease(ctx, sp, gc.DefaultStateName, "second", gc.PhaseDryRun, time.Minute, testGrace)
	var held *gc.ErrLeaseHeld
	require.ErrorAs(err, &held)
	require.Equal("first", held.Holder)
	require.True(held.Expiry.After(time.Now()))

	// Another garbage collector has its own row.
	_, err = gc.AcquireLease(ctx, sp, "other", "second", gc.PhaseDryRun, time.Minute, testGrace)
	require.NoError(err)

	first.Release()
	second, err := gc.AcquireLease(ctx, sp, gc.DefaultStateName, "second", gc.PhaseDryRun, time.Minute, testGrace)
	require.NoError(err)
	require.Equal(int64(2), second.Cycle)
	require.Equal(int64(2), second.Epoch)
	require.ErrorIs(first.RecordSnapshot(ctx, time.Now()), gc.ErrLeaseLost)

	row, err := sp.Single().ReadRow(ctx, "GcState", spanner.Key{gc.DefaultStateName}, []string{"Phase", "GraceSeconds", "CycleStartTime", "PreviousCycleStartTime"})
	require.NoError(err)
	var phase string
	var grace int64
	var start, prev time.Time
	require.NoError(row.Columns(&phase, &grace, &start, &prev))
	require.Equal(gc.PhaseDryRun, phase)
	require.Equal(int64(testGrace/time.Second), grace)
	require.True(prev.Before(start))
}

func TestAcquireLease_OneOfConcurrentHoldersWins(t *testing.T) {
	ctx := context.Background()
	sp := newEmulatorCASDBServer(t).casdb.SpannerClient()

	const holders = 5
	errs := make(chan error, holders)
	for i := range holders {
		go func() {
			_, err := gc.AcquireLease(ctx, sp, gc.DefaultStateName, fmt.Sprintf("holder-%d", i), gc.PhaseDryRun, time.Minute, testGrace)
			errs <- err
		}()
	}
	var won int
	for range holders {
		err := <-errs
		if err == nil {
			won++
			continue
		}
		var held *gc.ErrLeaseHeld
		require.ErrorAs(t, err, &held)
	}
	require.Equal(t, 1, won)
}

func TestAcquireLease_TakesExpiredLease(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	sp := newEmulatorCASDBServer(t).casdb.SpannerClient()

	_, err := gc.AcquireLease(ctx, sp, gc.DefaultStateName, "stalled", gc.PhaseDryRun, 3*time.Second, testGrace)
	require.NoError(err)
	require.Eventually(func() bool {
		_, err := gc.AcquireLease(ctx, sp, gc.DefaultStateName, "next", gc.PhaseDryRun, time.Minute, testGrace)
		return err == nil
	}, 10*time.Second, 500*time.Millisecond)
}

func TestLease_WatchdogStopsWhenLeaseIsTaken(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	sp := newEmulatorCASDBServer(t).casdb.SpannerClient()

	lease, err := gc.AcquireLease(ctx, sp, gc.DefaultStateName, "first", gc.PhaseDryRun, 3*time.Second, testGrace)
	require.NoError(err)
	runCtx, stop := lease.Keep(ctx)
	defer stop()

	// Another process takes the lease over, for example after this one
	// stalled past its expiry.
	_, err = sp.Apply(ctx, []*spanner.Mutation{spanner.UpdateMap("GcState", map[string]any{
		"Name": gc.DefaultStateName, "LeaseHolder": "other", "LeaseEpoch": lease.Epoch + 1, "UpdateTime": spanner.CommitTimestamp,
	})})
	require.NoError(err)

	select {
	case <-runCtx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the run was not stopped")
	}
	require.ErrorIs(context.Cause(runCtx), gc.ErrLeaseLost)
}

func TestLease_KeepRenews(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	sp := newEmulatorCASDBServer(t).casdb.SpannerClient()

	lease, err := gc.AcquireLease(ctx, sp, gc.DefaultStateName, "first", gc.PhaseDryRun, 3*time.Second, testGrace)
	require.NoError(err)
	runCtx, stop := lease.Keep(ctx)
	// Longer than the lease: the renewals keep it.
	time.Sleep(5 * time.Second)
	require.NoError(runCtx.Err())
	stop()
	require.ErrorIs(runCtx.Err(), context.Canceled)
	require.ErrorIs(context.Cause(runCtx), context.Canceled)
}

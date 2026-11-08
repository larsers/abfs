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
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
)

// newTestSpanner creates a database with the garbage collector tables on the
// Spanner emulator (SPANNER_EMULATOR_HOST) and returns a client for it.
func newTestSpanner(t *testing.T) *spanner.Client {
	t.Helper()
	return newTestSpannerWith(t, casdb.GCSchemaStatements())
}

// newTestSpannerWith creates a database of the statements on the Spanner
// emulator and returns a client for it with the options opts.
func newTestSpannerWith(t *testing.T, statements []string, opts ...option.ClientOption) *spanner.Client {
	t.Helper()
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST is not set")
	}
	ctx := context.Background()
	const project, inst = "test-proj", "test-inst"
	ic, err := instance.NewInstanceAdminClient(ctx)
	require.NoError(t, err)
	defer ic.Close()
	op, err := ic.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + project,
		InstanceId: inst,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", project),
			DisplayName: inst,
			NodeCount:   1,
		},
	})
	if status.Code(err) != codes.AlreadyExists {
		require.NoError(t, err)
		_, err = op.Wait(ctx)
		require.NoError(t, err)
	}

	b := make([]byte, 6)
	_, err = rand.Read(b)
	require.NoError(t, err)
	dbID := "g" + hex.EncodeToString(b)
	dc, err := database.NewDatabaseAdminClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })
	dop, err := dc.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, inst),
		CreateStatement: "CREATE DATABASE `" + dbID + "`",
		ExtraStatements: statements,
	})
	require.NoError(t, err)
	_, err = dop.Wait(ctx)
	require.NoError(t, err)
	name := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, inst, dbID)
	t.Cleanup(func() { _ = dc.DropDatabase(context.Background(), &databasepb.DropDatabaseRequest{Database: name}) })
	client, err := spanner.NewClient(ctx, name, opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

// Each renewal records the expiry Spanner wrote, so that the write check of a
// run that keeps its lease never stops it. With the expiry of the first take
// only, every write after the lease less two write deadlines and the clock
// skew margin would fail.
func TestLease_RenewalMovesWriteCheck(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	sp := newTestSpanner(t)
	const lease, d = 3 * time.Second, DefaultWriteDeadline
	// The check on this clock passes while the expiry is more than half a
	// second after a time one lease later than now.
	orig := wallNow
	wallNow = func() time.Time { return time.Now().Add(lease - time.Second/2 - 2*d - clockSkewMargin) }
	t.Cleanup(func() { wallNow = orig })

	l, err := AcquireLease(ctx, sp, DefaultStateName, "first", PhaseDryRun, lease, 72*time.Hour)
	require.NoError(err)
	defer l.Release()
	time.Sleep(time.Second)
	require.ErrorIs(l.checkWrite(d), errLeaseExpiring, "a second after the take")
	require.NoError(l.renew(ctx))
	require.NoError(l.checkWrite(d), "right after the renewal")
}

// A gc row write of the backfill that stalls ends after its attempt deadline
// and is retried, so that a stalled tablet does not hold the backfill.
func TestBackfill_AttemptDeadline(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stalls := &stallCalls{method: "/MutateRow"}
	client := newHooksTestBigtable(t, stalls.options()...)
	key := "1111111111111111111111111111111111111111"
	mut := bigtable.NewMutation()
	mut.Set("meta", "type", bigtable.Now(), []byte("blob"))
	require.NoError(client.Open(objectsTable).Apply(ctx, key, mut))

	stalls.n.Store(2)
	stats, err := Backfill(ctx, client, &treeServer{}, &BackfillOpts{Shards: 1, WriteDeadline: 200 * time.Millisecond})
	require.NoError(ctx.Err(), "the backfill's context ended first")
	require.NoError(err)
	require.True(stats.Done)
	require.NotNil(readGCRow(t, client, entities.NewHash(key)))
	require.GreaterOrEqual(stalls.calls.Load(), int32(3))
}

// A touch written later than TouchDrainBound after it was queued, as after a
// freeze of the process, counts as failed, so that the next heartbeat records
// a loss: the collector counts on touches landing within the bound.
func TestToucher_LateTouchFails(t *testing.T) {
	require := require.New(t)
	tc := newToucher(newHooksTestBigtable(t), 10)
	tc.lateAfter = 100 * time.Millisecond
	keys := testHashes(2)
	require.True(tc.TouchObject(keys[0]))
	time.Sleep(150 * time.Millisecond)
	require.True(tc.TouchObject(keys[1]))
	tc.flush(context.Background(), []touch{<-tc.queue, <-tc.queue})
	require.Equal(TouchStats{Written: 2, Failed: 1}, tc.Stats())
}

// Two attempts of the write deadline must fit into a quarter of the lease,
// the margin at which the watchdog stops the run.
func TestDryRunOpts_WriteDeadlineFitsLease(t *testing.T) {
	o := DefaultDryRunOpts()
	o.LeaseDuration = 4 * time.Minute
	o.WriteDeadline = 30 * time.Second
	require.NoError(t, o.Validate())
	o.WriteDeadline = 31 * time.Second
	require.ErrorContains(t, o.Validate(), "write deadline 31s is too long for the lease 4m0s")
}

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
	"slices"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// serverRow writes a row of GcServers as a server process with the given
// times left it; a zero stop or loss time leaves the column NULL.
func (f *gcFixture) serverRow(id string, touch time.Duration, lastSeen, stopped, loss time.Time) {
	f.t.Helper()
	null := func(t time.Time) spanner.NullTime { return spanner.NullTime{Time: t, Valid: !t.IsZero()} }
	_, err := f.s.casdb.SpannerClient().Apply(context.Background(), []*spanner.Mutation{spanner.InsertOrUpdate("GcServers",
		[]string{"ServerId", "Version", "GcEnabled", "TouchIntervalSeconds", "CacheTTLSeconds", "HeartbeatIntervalSeconds", "LinkValidation", "LastSeen", "StatsTime", "TouchLossTime", "StoppedAt"},
		[]any{id, gc.ExtractorVersion, true, int64(touch / time.Second), int64(6 * time.Hour / time.Second), int64(300), true, lastSeen, lastSeen, null(loss), null(stopped)})})
	require.NoError(f.t, err)
}

func (f *gcFixture) serverIDs() []string {
	f.t.Helper()
	servers, err := gc.ReadServers(context.Background(), f.s.casdb.SpannerClient())
	require.NoError(f.t, err)
	var ids []string
	for _, s := range servers {
		ids = append(ids, s.ID)
	}
	slices.Sort(ids)
	return ids
}

func (f *gcFixture) legacyGoneColumn() spanner.NullTime {
	f.t.Helper()
	row, err := f.s.casdb.SpannerClient().Single().ReadRow(context.Background(), "GcState", spanner.Key{gc.DefaultStateName}, []string{"LegacyServersGoneTime"})
	require.NoError(f.t, err)
	var gone spanner.NullTime
	require.NoError(f.t, row.Columns(&gone))
	return gone
}

// Every cycle records since when GcReplicas, where servers of an older
// version write their heartbeat and delete it when they stop, has held no
// row: the time stays while it is empty and is cleared by any row.
func TestDryRunGC_LegacyServersGoneTime(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newGCFixture(t)
	sp := f.s.casdb.SpannerClient()

	r := f.run(nil).Report
	require.NotNil(r.Replicas.LegacyServersGoneTime)
	first := *r.Replicas.LegacyServersGoneTime
	require.True(first.After(r.SnapshotTime), "found empty at %s, snapshot %s", first, r.SnapshotTime)
	require.True(first.Equal(f.legacyGoneColumn().Time))
	r = f.run(nil).Report
	require.True(first.Equal(*r.Replicas.LegacyServersGoneTime))

	old := gc.Replica{ID: "old-0-aa", Version: gc.ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour}
	require.NoError(gc.WriteHeartbeat(ctx, sp, &old))
	r = f.run(nil).Report
	require.Nil(r.Replicas.LegacyServersGoneTime)
	require.False(f.legacyGoneColumn().Valid)

	require.NoError(gc.DeleteReplica(ctx, sp, old.ID))
	r = f.run(nil).Report
	require.NotNil(r.Replicas.LegacyServersGoneTime)
	require.True(r.Replicas.LegacyServersGoneTime.After(r.SnapshotTime))
	require.True(r.Replicas.LegacyServersGoneTime.After(first))
}

// A row of GcServers is the only trace of a crash or a lost touch, so every
// mode keeps it for the largest grace of any condemn or delete cycle and its
// own grace, plus an hour and the 9 h before the window in which a lost map
// row touch still counts; crash rows, which have no stop time, are pruned
// too.
func TestDryRunGC_PrunesServerRows(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newGCFixture(t)
	none := time.Time{}
	f.serverRow("crash-old", time.Hour, f.now.Add(-100*time.Hour), none, none)
	f.serverRow("crash-in", time.Hour, f.now.Add(-72*time.Hour-30*time.Minute), none, none)
	f.serverRow("stopped-old", time.Hour, f.now.Add(-100*time.Hour), f.now.Add(-100*time.Hour), f.now.Add(-100*time.Hour))
	f.serverRow("stopped-in", time.Hour, f.now.Add(-100*time.Hour), f.now.Add(-50*time.Hour), none)
	f.serverRow("stopped-maps", time.Hour, f.now.Add(-100*time.Hour), f.now.Add(-81*time.Hour), none)

	r := f.run(nil).Report
	require.Equal(int64(2), r.Replicas.ServersRemoved)
	require.Equal([]string{"crash-in", "stopped-in", "stopped-maps"}, f.serverIDs())

	// A condemn or delete lease with a longer grace keeps rows for it, even
	// in a dry run with the default grace.
	l, err := gc.AcquireLease(ctx, f.s.casdb.SpannerClient(), gc.DefaultStateName, "condemn", gc.PhaseCondemn, time.Minute, 120*time.Hour)
	require.NoError(err)
	l.Release()
	f.serverRow("crash-old", time.Hour, f.now.Add(-100*time.Hour), none, none)
	f.serverRow("crash-older", time.Hour, f.now.Add(-150*time.Hour), none, none)
	r = f.run(nil).Report
	require.Equal(int64(1), r.Replicas.ServersRemoved)
	require.Equal([]string{"crash-in", "crash-old", "stopped-in", "stopped-maps"}, f.serverIDs())

	// So does the run's own grace.
	f.serverRow("crash-older", time.Hour, f.now.Add(-150*time.Hour), none, none)
	o := gc.DefaultDryRunOpts()
	o.Shards, o.Grace = 3, 160*time.Hour
	r = f.run(&o).Report
	require.Zero(r.Replicas.ServersRemoved)
	require.Contains(f.serverIDs(), "crash-older")
	r = f.run(nil).Report
	require.Equal(int64(1), r.Replicas.ServersRemoved)
	require.NotContains(f.serverIDs(), "crash-older")
}

// keepAliveMount writes a live mount ref to mount details whose config has
// the keep-alive with the given touch interval, or none when it is zero.
func (f *gcFixture) keepAliveMount(ref string, touch time.Duration, localWriteCheck bool) entities.Hash {
	f.t.Helper()
	pinned := f.blob(pinnedManifest(), f.old)
	cfg := &instance.Config{}
	if touch > 0 {
		cfg.GCKeepAlive, cfg.GCKeepAliveRefInterval, cfg.GCKeepAliveTouchInterval = true, 15*time.Minute, touch
	}
	details := f.object(&binding.MountDetails{
		Version:            binding.CurrentMountDetailsVersion,
		MetaRefPrefix:      "user/u",
		HostName:           "host-" + ref[len(ref)-1:],
		UserName:           "u",
		PinnedManifestHash: pinned,
		Config:             cfg,
		GCLocalWriteCheck:  localWriteCheck,
	}, f.old)
	f.ref(entities.ABFSMetaProject, ref, details, f.now.Add(-time.Hour))
	return details
}

// The pin reader counts the live mounts whose cache may lose content a
// delete removes: no keep-alive, a cacheman without the local write check,
// or a touch interval too long for the grace less the longest touch interval
// and the longest cache TTL of the servers that ran in the window of the
// cycle.
func TestDryRunGC_CountsKeepAliveMounts(t *testing.T) {
	require := require.New(t)
	f := newGCFixture(t)
	f.serverRow("srv-0-aa", time.Hour, f.now, time.Time{}, time.Time{})
	f.keepAliveMount("refs/abfs/mount/user/u/a", 18*time.Hour, true)
	noCheck := f.keepAliveMount("refs/abfs/mount/user/u/b", 18*time.Hour, false)
	slow := f.keepAliveMount("refs/abfs/mount/user/u/c", 33*time.Hour, true)
	none := f.keepAliveMount("refs/abfs/mount/user/u/d", 0, true)
	ttl := f.keepAliveMount("refs/abfs/mount/user/u/e", 30*time.Hour, true)

	m := f.run(nil).Report.LiveMounts
	require.Equal(int64(5), m.Live)
	require.Equal(int64(1), m.WithoutKeepAlive)
	require.Equal(int64(1), m.WithoutLocalWriteCheck)
	require.Zero(m.TouchIntervalTooLong)
	require.Equal([]gc.MountSample{{Details: none.String(), Host: "host-d", User: "u"}}, m.Samples["without_keepalive"])
	require.Equal([]gc.MountSample{{Details: noCheck.String(), Host: "host-b", User: "u"}}, m.Samples["without_local_write_check"])

	// 1.1 x 33 h is past 72 h less a stopped server's 36 h and the margin,
	// and 1.1 x 30 h past that less the 6 h cache TTL of the servers.
	f.serverRow("srv-1-bb", 36*time.Hour, f.now.Add(-2*time.Hour), f.now.Add(-time.Hour), time.Time{})
	m = f.run(nil).Report.LiveMounts
	require.Equal(int64(2), m.TouchIntervalTooLong)
	require.ElementsMatch([]gc.MountSample{{Details: slow.String(), Host: "host-c", User: "u"}, {Details: ttl.String(), Host: "host-e", User: "u"}}, m.Samples["touch_interval_too_long"])
}

// A server that lost map row touches makes the pin reader count a mount
// whose touch interval no longer fits the grace less also the time clients
// reuse map results and the time a keep-alive pass may take, and the report
// gives the loss time. A row written by a server of an older version has no
// map row loss time.
func TestDryRunGC_MapRowLossShortensKeepAliveLimit(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newGCFixture(t)
	f.serverRow("srv-0-aa", time.Hour, f.now, time.Time{}, time.Time{})
	slow := f.keepAliveMount("refs/abfs/mount/user/u/a", 51*time.Hour, true)

	// 1.1 x 51 h fits 72 h less 1 h, the 6 h cache TTL and the margin, and
	// that less 8 h, but not that less 9 h.
	r := f.run(nil).Report
	require.Zero(r.LiveMounts.TouchIntervalTooLong)
	require.Nil(r.Replicas.MapRowLossTime)

	s := &gc.Server{ID: "srv-1-bb", Version: gc.ExtractorVersion, GC: true, TouchInterval: time.Hour, CacheTTL: 6 * time.Hour, HeartbeatInterval: 5 * time.Minute, LinkValidation: true}
	require.NoError(gc.WriteServer(ctx, f.s.casdb.SpannerClient(), s, time.Now(), false, true))
	servers, err := gc.ReadServers(ctx, f.s.casdb.SpannerClient())
	require.NoError(err)
	var lossTime time.Time
	for _, s := range servers {
		if s.ID == "srv-1-bb" {
			lossTime = s.MapRowLossTime
			require.Zero(s.TouchLossTime)
		} else {
			require.Zero(s.MapRowLossTime, "a row without the column")
		}
	}
	require.NotZero(lossTime)
	r = f.run(nil).Report
	require.Equal(int64(1), r.LiveMounts.TouchIntervalTooLong)
	require.Equal([]gc.MountSample{{Details: slow.String(), Host: "host-a", User: "u"}}, r.LiveMounts.Samples["touch_interval_too_long"])
	require.NotNil(r.Replicas.MapRowLossTime)
	require.True(lossTime.Equal(*r.Replicas.MapRowLossTime))
}

// In a rolling upgrade the live hosts of both heartbeat tables count toward
// the replica range, and the settings of the older servers' rows are checked
// before a condemn.
func TestCondemnGC_MixedFleetHosts(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.blob("unreachable file", f.old)
	sp := f.s.casdb.SpannerClient()
	for _, id := range []string{"new-0-aa", "new-1-bb", "new-2-cc"} {
		s := &gc.Server{ID: id, Version: gc.ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour, HeartbeatInterval: 5 * time.Minute, LinkValidation: true}
		require.NoError(gc.WriteServer(ctx, sp, s, time.Now(), false, false))
	}
	f.heartbeat("old-0-aa")
	f.heartbeat("old-1-bb")

	o := f.opts()
	o.ExpectedReplicas, o.MinReplicas = 0, 6
	res, err := f.condemn(o)
	var refused *gc.ErrCondemnRefused
	require.ErrorAs(err, &refused)
	require.Equal([]string{"5 server hosts write a GC heartbeat, but at least 6 are expected"}, refused.Reasons)
	require.Equal(6, res.Report.Condemn.MinReplicas)

	o.MinReplicas = 5
	res, err = f.condemn(o)
	require.NoError(err)
	require.Equal(int64(1), res.Report.Condemn.Condemned)

	require.NoError(gc.WriteHeartbeat(ctx, sp, &gc.Replica{ID: "old-1-bb", Version: gc.ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour}))
	_, err = f.condemn(o)
	require.ErrorAs(err, &refused)
	require.Len(refused.Reasons, 1)
	require.Contains(refused.Reasons[0], "replica old-1-bb: keeps cache entries without a TTL")
}

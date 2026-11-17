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
		[]string{"ServerId", "Version", "GcEnabled", "TouchIntervalSeconds", "CacheTTLSeconds", "HeartbeatIntervalSeconds", "LinkValidation", "LastSeen", "TouchLossTime", "StoppedAt"},
		[]any{id, gc.ExtractorVersion, true, int64(touch / time.Second), int64(6 * time.Hour / time.Second), int64(300), true, lastSeen, null(loss), null(stopped)})})
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

// A row of GcServers tells which processes ran in the window of a cycle, so
// every mode keeps it for the largest grace of any condemn or delete cycle
// and its own grace, plus an hour and the 9 h before the window in which a
// lost map row touch is still reported; crash rows, which have no stop time,
// are pruned too.
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

// A server that lost map row touches does not shorten the keep-alive limit
// of the mounts; the report gives the loss time.
func TestDryRunGC_MapRowLossReported(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newGCFixture(t)
	f.serverRow("srv-0-aa", time.Hour, f.now, time.Time{}, time.Time{})
	f.keepAliveMount("refs/abfs/mount/user/u/a", 51*time.Hour, true)

	// 1.1 x 51 h fits 72 h less 1 h, the 6 h cache TTL and the margin.
	r := f.run(nil).Report
	require.Zero(r.LiveMounts.TouchIntervalTooLong)
	require.Nil(r.Replicas.MapRowLossTime)

	s := &gc.Server{ID: "srv-1-bb", Version: gc.ExtractorVersion, GC: true, TouchInterval: time.Hour, CacheTTL: 6 * time.Hour, HeartbeatInterval: 5 * time.Minute, LinkValidation: true}
	require.NoError(gc.WriteServer(ctx, f.s.casdb.SpannerClient(), s, true, true))
	servers, err := gc.ReadServers(ctx, f.s.casdb.SpannerClient())
	require.NoError(err)
	var lossTime time.Time
	for _, s := range servers {
		if s.ID == "srv-1-bb" {
			lossTime = s.MapRowLossTime
		}
	}
	require.NotZero(lossTime)
	r = f.run(nil).Report
	require.Zero(r.LiveMounts.TouchIntervalTooLong)
	require.NotNil(r.Replicas.MapRowLossTime)
	require.True(lossTime.Equal(*r.Replicas.MapRowLossTime))
}

// A condemn that finds, after its last marker batch, a server of an older
// version of the GC rules in its window leaves its list without a complete
// record and fails; a live one refuses the condemn before any marker. The
// next cycle with only current servers writes the markers again.
func TestCondemnGC_OlderServerLeavesNoRecord(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	k := f.blob("unreachable file", f.old)
	sp := f.s.casdb.SpannerClient()
	f.heartbeat("srv-0-aa")
	old := &gc.Server{ID: "srv-1-bb", Version: gc.ExtractorVersion - 1, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour, HeartbeatInterval: 5 * time.Minute, LinkValidation: true}
	require.NoError(gc.WriteServer(ctx, sp, old, false, false))

	o := f.opts()
	o.ExpectedReplicas = 2
	_, err := f.condemn(o)
	var refused *gc.ErrCondemnRefused
	require.ErrorAs(err, &refused)
	require.Equal([]string{"replica srv-1-bb: follows the GC rules of version 2, older than version 3 of this collector"}, refused.Reasons)
	require.Empty(f.markerCells(k))

	// Stopped, it no longer counts as a host, but it ran in the window.
	require.NoError(gc.StopServer(ctx, sp, old.ID, false, false))
	o.ExpectedReplicas = 1
	res, err := f.condemn(o)
	require.ErrorContains(err, "does not complete its list")
	require.Equal([]string{"server srv-1-bb follows the GC rules of version 2, older than version 3 of this collector"}, res.Report.Condemn.NotCompleted)
	require.Equal([][]byte{marker(2)}, f.markerCells(k))
	require.False(f.listRecord(2).Complete())

	_, err = sp.Apply(ctx, []*spanner.Mutation{spanner.Delete("GcServers", spanner.Key{old.ID})})
	require.NoError(err)
	res, err = f.condemn(o)
	require.NoError(err)
	require.Equal(int64(1), res.Report.Condemn.Condemned)
	require.Equal([][]byte{marker(3)}, f.markerCells(k))
	require.True(f.listRecord(3).Complete())
}

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
	"errors"
	"testing"
	"time"
	"unsafe"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"
)

// The hold is the grace plus the margin at the defaults, and longer when the
// cache TTL recorded with the list and the reliance of a client need more:
// TTL_M + X + R_D + S with R_D the longest push or the map reuse, the
// keep-alive interval a tenth late and the pass allowance.
func TestHoldOf(t *testing.T) {
	o := DefaultDryRunOpts()
	grace := o.Grace + condemnHoldMargin
	require.Equal(t, 88*time.Hour, grace)
	tests := []struct {
		name           string
		ttl, keepAlive time.Duration
		want           time.Duration
	}{
		{"defaults", 6 * time.Hour, 18 * time.Hour, grace},
		{"no keep-alive mount", 6 * time.Hour, 0, grace},
		// 1 m + 2 min + 8 h + 65 h + 6.5 h + 1 h + 1 min = 80.55 h, but a TTL
		// of 12 h on the list makes it 92.55 h.
		{"long TTL of the list", 12 * time.Hour, 65 * time.Hour, 12*time.Hour + ValidationWriteBound + clientMapReuse + 65*time.Hour + 6*time.Hour + 30*time.Minute + keepAlivePassAllowance + clockSkewMargin},
		{"long keep-alive interval", time.Minute, 72 * time.Hour, time.Minute + ValidationWriteBound + clientMapReuse + 79*time.Hour + 12*time.Minute + keepAlivePassAllowance + clockSkewMargin},
		// The reliance equals the margin exactly: 6 h + 2 min + 1 min +
		// 8 h + 1 h + 1.1 K = 88 h at 1.1 K = 72 h 57 min.
		{"equal", 6 * time.Hour, (72*time.Hour + 57*time.Minute) * 10 / 11, grace},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, holdOf(tt.ttl, tt.keepAlive, &o))
		})
	}
	// A push longer than the reliance of a mount counts instead.
	o.LongestPush = 100 * time.Hour
	require.Equal(t, 6*time.Hour+ValidationWriteBound+100*time.Hour+clockSkewMargin, holdOf(6*time.Hour, 0, &o))
}

// A list is eligible from its completion plus its hold on, and the hold is
// counted per list with the TTL its record holds.
func TestListHolds_Eligible(t *testing.T) {
	require := require.New(t)
	o := DefaultDryRunOpts()
	snapshot := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	holds := &listHolds{records: map[int64]*ListRecord{
		3: {Cycle: 3, CompleteTime: snapshot.Add(-88 * time.Hour), CacheTTL: 6 * time.Hour},
		4: {Cycle: 4, CompleteTime: snapshot.Add(-89 * time.Hour), CacheTTL: 12 * time.Hour},
		5: {Cycle: 5, CompleteTime: snapshot.Add(-time.Hour), CacheTTL: 6 * time.Hour},
	}}
	values := [][]byte{markerValue(3), markerValue(4), markerValue(5), {1, 2}}
	unreachable := []sizedNode{{id: 0, marker: 1}, {id: 1, marker: 1}, {id: 2, marker: 2}, {id: 3, marker: 4}}
	eligible, lists := holds.eligible(values, unreachable, snapshot, 65*time.Hour, &o)
	require.Equal([]bool{false, true, false, false, false}, eligible)
	require.Len(lists, 3)
	require.Equal(int64(2), lists[0].Unreachable)
	require.True(lists[0].Eligible)
	require.InDelta(92.55, lists[1].HoldHours, 0.001)
	require.False(lists[1].Eligible)
	require.Equal(snapshot.Add(-time.Hour).Add(88*time.Hour), lists[2].EligibleTime)

	// A list one cycle old with a marker held stays; an older one whose
	// marker no row holds goes.
	require.Empty(holds.prunable(6, values))
	require.Equal([]int64{3}, holds.prunable(6, values[1:]))
	require.Equal([]int64{3, 4}, holds.prunable(6, nil))
	require.Equal(snapshot.Add(-time.Hour), holds.newest())
	require.Nil((&listHolds{err: errors.New("unavailable")}).prunable(6, nil))
}

// The scan reads the newest marker of every objects row: an unreachable node
// keeps its marker, a live one with a marker goes to the marked list, and a
// node stays 16 bytes.
func TestCheckConsistency_ReadsMarkers(t *testing.T) {
	require := require.New(t)
	require.Equal(uintptr(16), unsafe.Sizeof(sizedNode{}))
	ctx := context.Background()
	f := newStepFixture(t, 2)
	h := testHashes(5)
	f.objectWith(h[0], "blob", nil, markerValue(1), true)
	f.objectWith(h[1], "blob", nil, markerValue(2), true)
	f.objectWith(h[2], "blob", nil, nil, true)
	f.objectWith(h[3], "blob", nil, markerValue(1), true)
	f.objectWith(h[4], "blob", nil, markerValue(2), true)
	// An older cell of another value does not count.
	mut := bigtable.NewMutation()
	mut.Set(ObjectsFamily, CondemnedColumn, bigtable.Time(f.now.Add(-time.Hour)), markerValue(7))
	require.NoError(f.client.Open(objectsTable).Apply(ctx, h[0].String(), mut))

	bounds := f.bounds()
	g, _, err := scanNodes(ctx, f.client.Open(TableName), bounds, f.now.Add(-DefaultGrace))
	require.NoError(err)
	live := newBitmap(g.size())
	live.set(g.find(h[3]))
	live.set(g.find(h[2]))
	scan, err := checkConsistency(ctx, f.client.Open(objectsTable), bounds, g, live, f.now.Add(-time.Minute), true, nil)
	require.NoError(err)
	require.Len(scan.markers, 2)
	value := func(n sizedNode) []byte {
		if n.marker == 0 {
			return nil
		}
		return scan.markers[n.marker-1]
	}
	got := make(map[Hash][]byte)
	for _, n := range scan.unreachable {
		got[g.key(n.id)] = value(n)
	}
	require.Equal(map[Hash][]byte{h[0]: markerValue(1), h[1]: markerValue(2), h[4]: markerValue(2)}, got)
	require.Len(scan.marked, 1)
	require.Equal(g.find(h[3]), scan.marked[0].id)
	require.Equal(markerValue(1), value(scan.marked[0]))
	require.Equal(int64(4), scan.report.Marked)
	require.Equal(int64(1), scan.report.MarkedLive)
}

// The delete step matches each candidate with the marker its own row held at
// the scan, so one step deletes the keys of two lists, and a key whose
// marker changed since keeps its row.
func TestDeleteStep_PerKeyMarker(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newStepFixture(t, 2)
	h := testHashes(3)
	f.objectWith(h[0], "blob", nil, markerValue(stepListCycle), true)
	f.objectWith(h[1], "blob", nil, markerValue(stepListCycle-1), true)
	f.objectWith(h[2], "blob", nil, markerValue(stepListCycle-1), true)
	bounds := f.bounds()
	g, _, err := scanNodes(ctx, f.client.Open(TableName), bounds, f.now.Add(-DefaultGrace))
	require.NoError(err)
	live := newBitmap(g.size())
	scan, err := checkConsistency(ctx, f.client.Open(objectsTable), bounds, g, live, f.now.Add(-time.Minute), true, nil)
	require.NoError(err)
	res := &Result{Report: &Report{Cycle: stepCycle, SnapshotTime: f.now}, graph: g, live: live, seen: scan.seen, unreachable: scan.unreachable, markers: scan.markers, bounds: bounds}
	o := DefaultDryRunOpts()
	holds := &listHolds{records: map[int64]*ListRecord{
		stepListCycle:     {Cycle: stepListCycle, CompleteTime: f.now.Add(-100 * time.Hour)},
		stepListCycle - 1: {Cycle: stepListCycle - 1, CompleteTime: f.now.Add(-120 * time.Hour)},
	}}
	eligible, lists := holds.eligible(res.markers, res.unreachable, f.now, 0, &o)
	res.plan = planDeletes(res, eligible, lists, f.now.Add(-DefaultMapRetention).UnixMicro(), func(string) bool { return true })
	require.Equal(int64(3), res.plan.report.Candidates)

	// h[2] gets the marker of the other list after the scan.
	mut := bigtable.NewMutation()
	mut.Set(ObjectsFamily, CondemnedColumn, bigtable.Now(), markerValue(stepListCycle))
	require.NoError(f.client.Open(objectsTable).Apply(ctx, h[2].String(), mut))

	d := f.deleter(farLease(), nil)
	_, err = d.deleteStep(ctx, farLease(), res)
	require.NoError(err)
	require.Equal(int64(2), d.rep.Step.Deleted)
	require.Equal(int64(1), d.rep.Step.NotMatched)
	require.False(f.rowExists(objectsTable, h[0].String()))
	require.False(f.rowExists(objectsTable, h[1].String()))
	require.True(f.rowExists(objectsTable, h[2].String()))
}

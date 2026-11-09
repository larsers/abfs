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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/entities"
)

func TestDryRunOpts_TimingCheck(t *testing.T) {
	o := DefaultDryRunOpts()
	o.Grace = 56 * time.Hour
	require.ErrorContains(t, o.Validate(), "grace 56h0m0s is shorter than 2 x cycle (24h0m0s) + cache TTL (6h0m0s) + longest push (3h0m0s) = 57h0m0s")
	// The check runs before anything is read or written.
	_, err := DryRun(context.Background(), nil, nil, nil, nil, &o)
	require.ErrorContains(t, err, "is shorter than")

	o.Grace = 57 * time.Hour
	require.NoError(t, o.Validate())
	o.Cycle = 0
	require.ErrorContains(t, o.Validate(), "invalid cycle")
}

func TestClassifyRef(t *testing.T) {
	tests := []struct {
		project, ref string
		want         RefClass
	}{
		{"platform/a", "refs/heads/main", RefSource},
		{"platform/a", "HEAD", RefSource},
		{"platform/a", "abfs/project-status", RefProjectStatus},
		{"platform/a", "abfs/pushed-packs", RefPushedPacks},
		{entities.ABFSMetaProject, "config", RefConfig},
		{entities.ABFSMetaProject, "acl", RefACL},
		{entities.ABFSMetaProject, "refs/abfs/ws/user/u/ws", RefWorkspace},
		{entities.ABFSMetaProject, "refs/abfs/snapshot/user/u/0123456789abcdef0123456789abcdef01234567", RefSnapshot},
		{entities.ABFSMetaProject, "refs/abfs/mount/user/u/1", RefMount},
		{entities.ABFSMetaProject, "refs/abfs/unmount/user/u/1", RefUnmount},
		{entities.ABFSMetaProject, "refs/abfs/build/user/u/b", RefBuild},
		{entities.ABFSMetaProject, "refs/abfs/build-start/user/u/b", RefBuildStart},
		{entities.ABFSMetaProject, "refs/abfs/other/x", RefOtherMeta},
		{entities.ABFSMetaProject, "refs/heads/main", RefSource},
		// Only the project abfs-meta holds the client refs.
		{"platform/a", "refs/abfs/mount/user/u/1", RefSource},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, classifyRef(tt.project, tt.ref), "%s %s", tt.project, tt.ref)
	}
}

func TestRefRetention(t *testing.T) {
	o := DefaultDryRunOpts()
	now := time.Now()
	tests := []struct {
		class   RefClass
		age     time.Duration
		expired bool
	}{
		{RefSource, 10000 * time.Hour, false},
		{RefWorkspace, 10000 * time.Hour, false},
		{RefMount, 335 * time.Hour, false},
		{RefMount, 337 * time.Hour, true},
		{RefBuild, 719 * time.Hour, false},
		{RefBuildStart, 721 * time.Hour, true},
		{RefUnmount, time.Hour, false},
		{RefUnmount, 721 * time.Hour, true},
		{RefSnapshot, 719 * time.Hour, false},
		{RefSnapshot, 721 * time.Hour, true},
	}
	for _, tt := range tests {
		require.Equal(t, tt.expired, o.refRetention(tt.class, now.Add(-tt.age), now), "%s %s", tt.class, tt.age)
	}
}

// testGraph builds a graph from edges between nodes 0..n-1 of the given
// types, in two segments.
func testGraph(types []string, edges map[uint32][]uint32) *graph {
	g := &graph{types: make([]uint8, len(types))}
	for i, typ := range types {
		g.types[i] = typeIndex[typ]
	}
	half := uint32(len(types) / 2)
	for _, r := range [][2]uint32{{0, half}, {half, uint32(len(types))}} {
		seg := segment{lo: r[0]}
		for id := r[0]; id < r[1]; id++ {
			seg.add(edges[id])
		}
		seg.finish()
		g.segs = append(g.segs, seg)
	}
	return g
}

func TestMarker_MapsAndDanglingTargets(t *testing.T) {
	require := require.New(t)
	// 0 ref root tree -> 1 blob; 2 dead tree -> 3 blob; 4, 5 deep tree
	// requests of 0 and 2; 6, 7 their deep trees; 8 eval request -> 9 env;
	// 10 eval response; 11 lfs request; 12 lfs result -> 13 blob.
	g := testGraph([]string{
		"tree", "blob", "tree", "blob",
		"deeptree-request", "deeptree-request", "deeptree", "deeptree",
		"eval-request", "env-vars", "eval-response",
		"lfs-request", "lfs-result", "blob",
	}, map[uint32][]uint32{
		0: {1}, 2: {3}, 4: {0}, 5: {2}, 8: {9}, 10: {8}, 12: {11, 13},
	})
	m := newMarker(g, 14)
	m.add(0)
	m.walk()
	rows := []mapRow{
		{from: 4, to: 6},
		{from: 5, to: 7},
		{from: 8, to: 10, time: 100},
		{from: 11, to: 12},
		{from: noNode, to: noNode},
	}
	counts := m.markMaps(g, [][]mapRow{rows[:2], rows[2:]}, 50)
	require.Equal(&MapCounts{Rows: 2, Live: 1, Expired: 1}, counts[MapPusher])
	require.Equal(&MapCounts{Rows: 1, Live: 1}, counts[MapEval])
	require.Equal(&MapCounts{Rows: 1, Live: 1}, counts[MapLFS])
	require.Equal(&MapCounts{Rows: 1, Live: 1, MissingTo: 1}, counts[MapNoRequest])
	var live []uint32
	for id := range uint32(14) {
		if m.live.has(id) {
			live = append(live, id)
		}
	}
	require.Equal([]uint32{0, 1, 4, 6, 8, 9, 10, 11, 12, 13}, live)

	// The eval row past the map retention keeps nothing.
	m = newMarker(g, 14)
	counts = m.markMaps(g, [][]mapRow{rows[2:3]}, 200)
	require.Equal(&MapCounts{Rows: 1, Expired: 1}, counts[MapEval])
	require.Zero(m.live.count())
}

// Nodes with many, few and no edges round-trip through the blocks of a
// segment, also when a node's edges do not fit the current block.
func TestSegment_Blocks(t *testing.T) {
	require := require.New(t)
	sizes := []int{0, 3, minEdgeBlock - 3, 0, 4, maxEdgeBlock + 5, 0, 1, minEdgeBlock, 2}
	var want [][]uint32
	seg := segment{lo: 7}
	next := uint32(0)
	for _, n := range sizes {
		ids := make([]uint32, n)
		for i := range ids {
			ids[i] = next
			next++
		}
		want = append(want, ids)
		seg.add(ids)
	}
	seg.finish()
	require.Equal(len(sizes), seg.nodes())
	for k := range want {
		got := seg.nodeEdges(uint32(k))
		require.Len(got, len(want[k]), k)
		if len(want[k]) > 0 {
			require.Equal(want[k], got, k)
		}
	}
	for _, b := range seg.blocks {
		require.LessOrEqual(len(b), cap(b))
	}
	g := &graph{segs: []segment{seg}}
	require.Equal(want[1], g.edges(8))
	require.Nil(g.edges(6))
	require.Nil(g.edges(7 + uint32(len(sizes))))
}

func TestPrefixBounds(t *testing.T) {
	bounds, err := splitRange("", "", 3)
	require.NoError(t, err)
	require.Equal(t, []string{"", "55555555", "aaaaaaaa", ""}, bounds)
	require.Equal(t, []string{"", "5555", "aaaa", ""}, prefixBounds(bounds))
	require.Equal(t, []string{"", "0000", "0001", ""}, prefixBounds([]string{"", "00000000", "00000001", "00010000", ""}))
	require.Equal(t, []string{"", ""}, prefixBounds([]string{"", ""}))
}

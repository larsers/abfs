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
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
)

const (
	noNode          = ^uint32(0)
	maxDanglingKept = 10
)

// typeNames lists the object types this binary knows, sorted; a node stores
// its type as an index into it.
var typeNames, typeIndex = knownTypes()

func knownTypes() ([]string, map[string]uint8) {
	names := []string{string(entities.ObjectTypeBlob)}
	for t := range maps.Keys(objects.Factory) {
		names = append(names, string(t))
	}
	slices.Sort(names)
	names = slices.Compact(names)
	if len(names) > 255 {
		panic("too many object types for the GC")
	}
	index := make(map[string]uint8, len(names))
	for i, n := range names {
		index[n] = uint8(i)
	}
	return names, index
}

// graph is the object graph of one scan of the table gc. Nodes are the row
// keys, numbered in key order; the edges of each node are in compressed
// sparse rows, one segment per scan shard.
type graph struct {
	keys []Hash
	// prefix[p] is the first node whose key starts with the 16 bits p.
	prefix []uint32
	types  []uint8
	recent bitmap
	segs   []segment
	// shardSizes holds the number of nodes of each scan shard.
	shardSizes []int
}

type segment struct {
	lo uint32
	// offsets[i] is the start of the edges of node lo+i, offsets[i+1] its end.
	offsets []uint64
	edges   []uint32
}

func compareHash(a, b Hash) int {
	return bytes.Compare(a[:], b[:])
}

// find returns the node of h, or noNode.
func (g *graph) find(h Hash) uint32 {
	p := int(h[0])<<8 | int(h[1])
	lo, hi := int(g.prefix[p]), int(g.prefix[p+1])
	i, ok := slices.BinarySearchFunc(g.keys[lo:hi], h, compareHash)
	if !ok {
		return noNode
	}
	return uint32(lo + i)
}

// edges returns the edges of node id.
func (g *graph) edges(id uint32) []uint32 {
	i, ok := slices.BinarySearchFunc(g.segs, id, func(s segment, id uint32) int {
		switch {
		case s.lo > id:
			return 1
		case s.lo+uint32(len(s.offsets)-1) <= id:
			return -1
		}
		return 0
	})
	if !ok {
		return nil
	}
	s := &g.segs[i]
	k := id - s.lo
	return s.edges[s.offsets[k]:s.offsets[k+1]]
}

// nodeStats counts the rows of the first scan.
type nodeStats struct {
	// TouchOnly counts rows that hold a touch but no edges: objects without a
	// gc row that a server touched.
	TouchOnly int64
	// Outdated and Newer count rows written by an older or a newer edge
	// extractor than this binary's.
	Outdated int64
	Newer    int64
}

type nodeShard struct {
	keys   []Hash
	types  []uint8
	recent []uint32
	stats  nodeStats
}

// scanNodes reads the key, type, first write and touch of every row of the
// table gc, without the edges. A row is recent when it was written or touched
// after recentAfter. An object type this binary does not know stops the scan.
func scanNodes(ctx context.Context, table *bigtable.Table, bounds []string, recentAfter time.Time) (*graph, nodeStats, error) {
	filter := bigtable.ChainFilters(bigtable.LatestNFilter(1), bigtable.InterleaveFilters(
		bigtable.ChainFilters(bigtable.FamilyFilter(EdgesFamily), bigtable.ColumnFilter(LinksColumn), bigtable.StripValueFilter()),
		bigtable.ChainFilters(bigtable.FamilyFilter(EdgesFamily), bigtable.ColumnFilter(TypeColumn+"|"+VersionColumn)),
		bigtable.ChainFilters(bigtable.FamilyFilter(TouchFamily), bigtable.StripValueFilter()),
	))
	shards := make([]nodeShard, len(bounds)-1)
	g, gctx := errgroup.WithContext(ctx)
	for i := range shards {
		g.Go(func() error {
			s := &shards[i]
			var rowErr error
			err := table.ReadRows(gctx, bigtable.NewRange(bounds[i], bounds[i+1]), func(row bigtable.Row) bool {
				r, err := ParseRow(row)
				if err != nil {
					rowErr = err
					return false
				}
				touched := latest(row[TouchFamily])
				if r.Type == "" && r.Written.IsZero() {
					s.stats.TouchOnly++
					return true
				}
				typ, ok := typeIndex[r.Type]
				if !ok {
					rowErr = fmt.Errorf("object %s has the unknown object type %q: this garbage collector is older than the server that wrote it", r.Key, r.Type)
					return false
				}
				switch {
				case r.Version < ExtractorVersion:
					s.stats.Outdated++
				case r.Version > ExtractorVersion:
					s.stats.Newer++
				}
				if r.Written.After(recentAfter) || touched.After(recentAfter) {
					s.recent = append(s.recent, uint32(len(s.keys)))
				}
				s.keys = append(s.keys, r.Key)
				s.types = append(s.types, typ)
				return true
			}, bigtable.RowFilter(filter))
			if err == nil {
				err = rowErr
			}
			if err != nil {
				return fmt.Errorf("failed to scan the table gc from %q: %w", bounds[i], err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nodeStats{}, err
	}

	var n int
	var stats nodeStats
	for i := range shards {
		n += len(shards[i].keys)
		stats.TouchOnly += shards[i].stats.TouchOnly
		stats.Outdated += shards[i].stats.Outdated
		stats.Newer += shards[i].stats.Newer
	}
	if uint64(n) >= uint64(noNode) {
		return nil, stats, fmt.Errorf("the table gc has %d rows, more than one mark can hold", n)
	}
	gr := &graph{keys: make([]Hash, 0, n), types: make([]uint8, 0, n), recent: newBitmap(n), shardSizes: make([]int, len(shards))}
	for i := range shards {
		s := &shards[i]
		gr.shardSizes[i] = len(s.keys)
		base := uint32(len(gr.keys))
		gr.keys = append(gr.keys, s.keys...)
		gr.types = append(gr.types, s.types...)
		for _, r := range s.recent {
			gr.recent.set(base + r)
		}
		shards[i] = nodeShard{}
	}
	if !slices.IsSortedFunc(gr.keys, compareHash) {
		return nil, stats, fmt.Errorf("the scan of the table gc returned keys out of order")
	}
	gr.prefix = make([]uint32, 1<<16+1)
	for _, k := range gr.keys {
		gr.prefix[(int(k[0])<<8|int(k[1]))+1]++
	}
	for p := 1; p < len(gr.prefix); p++ {
		gr.prefix[p] += gr.prefix[p-1]
	}
	return gr, stats, nil
}

func latest(items []bigtable.ReadItem) time.Time {
	var t bigtable.Timestamp
	for _, item := range items {
		t = max(t, item.Timestamp)
	}
	if t == 0 {
		return time.Time{}
	}
	return t.Time()
}

// EdgeCounts reports the edges of the scan.
type EdgeCounts struct {
	Total int64 `json:"total"`
	// Dangling counts edges to objects without a gc row; the mark skips them.
	Dangling int64 `json:"dangling"`
	// DanglingFromRecent counts the dangling edges of recent objects, which
	// include uploads in flight during the scan.
	DanglingFromRecent int64 `json:"dangling_from_recent"`
	// DanglingSample lists a few dangling edges as "from>to".
	DanglingSample []string `json:"dangling_sample,omitempty"`
	// NewRows counts gc rows added between the two scans; they are left out.
	NewRows int64 `json:"new_rows"`
}

type edgeShard struct {
	seg     segment
	counts  EdgeCounts
	perType []int64
}

// scanEdges reads the edges of every node of g, shard by shard over the same
// bounds as scanNodes. It returns the edge counts per type index.
func scanEdges(ctx context.Context, table *bigtable.Table, bounds []string, gr *graph) (EdgeCounts, []int64, error) {
	filter := bigtable.ChainFilters(bigtable.FamilyFilter(EdgesFamily), bigtable.ColumnFilter(LinksColumn), bigtable.LatestNFilter(1))
	shards := make([]edgeShard, len(bounds)-1)
	g, gctx := errgroup.WithContext(ctx)
	var lo uint32
	for i := range shards {
		shardLo := lo
		shardHi := lo + uint32(gr.shardSizes[i])
		lo = shardHi
		g.Go(func() error {
			s := &shards[i]
			s.perType = make([]int64, len(typeNames))
			s.seg = segment{lo: shardLo, offsets: make([]uint64, 1, shardHi-shardLo+1)}
			next := shardLo
			var rowErr error
			err := table.ReadRows(gctx, bigtable.NewRange(bounds[i], bounds[i+1]), func(row bigtable.Row) bool {
				if !entities.IsHash(row.Key()) {
					rowErr = fmt.Errorf("invalid gc row key %q", row.Key())
					return false
				}
				key := entities.NewHash(row.Key())
				for next < shardHi && compareHash(gr.keys[next], key) < 0 {
					s.seg.offsets = append(s.seg.offsets, uint64(len(s.seg.edges)))
					next++
				}
				if next == shardHi || gr.keys[next] != key {
					s.counts.NewRows++
					return true
				}
				for _, item := range row[EdgesFamily] {
					targets, err := UnpackEdges(item.Value)
					if err != nil {
						rowErr = fmt.Errorf("invalid gc row %s: %w", key, err)
						return false
					}
					for _, t := range targets {
						id := gr.find(t)
						if id == noNode {
							s.counts.Dangling++
							if gr.recent.has(next) {
								s.counts.DanglingFromRecent++
							}
							if len(s.counts.DanglingSample) < maxDanglingKept {
								s.counts.DanglingSample = append(s.counts.DanglingSample, key.String()+">"+t.String())
							}
							continue
						}
						s.seg.edges = append(s.seg.edges, id)
					}
					s.counts.Total += int64(len(targets))
					s.perType[gr.types[next]] += int64(len(targets))
				}
				s.seg.offsets = append(s.seg.offsets, uint64(len(s.seg.edges)))
				next++
				return true
			}, bigtable.RowFilter(filter))
			if err == nil {
				err = rowErr
			}
			if err != nil {
				return fmt.Errorf("failed to scan the edges of the table gc from %q: %w", bounds[i], err)
			}
			for ; next < shardHi; next++ {
				s.seg.offsets = append(s.seg.offsets, uint64(len(s.seg.edges)))
			}
			s.seg.edges = slices.Clip(s.seg.edges)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return EdgeCounts{}, nil, err
	}

	var counts EdgeCounts
	perType := make([]int64, len(typeNames))
	gr.segs = make([]segment, 0, len(shards))
	for i := range shards {
		s := &shards[i]
		counts.Total += s.counts.Total
		counts.Dangling += s.counts.Dangling
		counts.DanglingFromRecent += s.counts.DanglingFromRecent
		counts.NewRows += s.counts.NewRows
		counts.DanglingSample = append(counts.DanglingSample, s.counts.DanglingSample...)
		for t, n := range s.perType {
			perType[t] += n
		}
		if len(s.seg.offsets) > 1 {
			gr.segs = append(gr.segs, s.seg)
		}
	}
	slices.Sort(counts.DanglingSample)
	counts.DanglingSample = counts.DanglingSample[:min(len(counts.DanglingSample), maxDanglingKept)]
	return counts, perType, nil
}

// mapRow is a row of the table maps: nodes of from and to (noNode when they
// have no gc row) and the cell time in microseconds.
type mapRow struct {
	from, to uint32
	time     int64
}

// scanMaps reads the keys and cell times of the table maps.
func scanMaps(ctx context.Context, table *bigtable.Table, bounds []string, gr *graph) ([]mapRow, int64, error) {
	filter := bigtable.ChainFilters(bigtable.FamilyFilter(mapsFamily), bigtable.LatestNFilter(1), bigtable.StripValueFilter())
	rows := make([][]mapRow, len(bounds)-1)
	malformed := make([]int64, len(bounds)-1)
	g, gctx := errgroup.WithContext(ctx)
	for i := range rows {
		g.Go(func() error {
			err := table.ReadRows(gctx, bigtable.NewRange(bounds[i], bounds[i+1]), func(row bigtable.Row) bool {
				from, to, ok := strings.Cut(row.Key(), "#")
				if !ok || !entities.IsHash(from) || !entities.IsHash(to) {
					malformed[i]++
					return true
				}
				t := latest(row[mapsFamily])
				rows[i] = append(rows[i], mapRow{
					from: gr.find(entities.NewHash(from)),
					to:   gr.find(entities.NewHash(to)),
					time: t.UnixMicro(),
				})
				return true
			}, bigtable.RowFilter(filter))
			if err != nil {
				return fmt.Errorf("failed to scan the table maps from %q: %w", bounds[i], err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, 0, err
	}
	var all []mapRow
	var bad int64
	for i := range rows {
		all = append(all, rows[i]...)
		rows[i] = nil
		bad += malformed[i]
	}
	return all, bad, nil
}

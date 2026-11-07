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

// TypeNames returns the object types the collector knows, sorted.
func TypeNames() []string {
	return slices.Clone(typeNames)
}

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
// keys, numbered in key order across the scan shards; the edges of each node
// are in compressed sparse rows, one segment per scan shard. The keys stay in
// the arrays their shards filled, so the scan never copies them.
type graph struct {
	shards []keyShard
	n      int
	// prefix[p] is the first node whose key starts with the 16 bits p, and
	// prefixShard[p] the shard that holds those keys.
	prefix      []uint32
	prefixShard []uint16
	types       []uint8
	recent      bitmap
	segs        []segment
}

// keyShard holds the sorted keys of the nodes lo to lo+len(keys)-1.
type keyShard struct {
	lo   uint32
	keys []Hash
}

const (
	minEdgeBlock = 1 << 10
	maxEdgeBlock = 1 << 20
)

// segment holds the edges of the nodes from lo on, in blocks that never
// grow, so that the scan copies no edges and leaves at most one partly used
// block. starts[i] is where the edges of node lo+i start: the block index in
// the high 32 bits, the position in the block in the low 32 bits. They end
// where the edges of the next node start, or at the end of the block when the
// next node starts in a later one.
type segment struct {
	lo     uint32
	starts []uint64
	blocks [][]uint32
}

// add appends the edges of the next node.
func (s *segment) add(ids []uint32) {
	b := len(s.blocks) - 1
	if b < 0 || cap(s.blocks[b])-len(s.blocks[b]) < len(ids) {
		size := minEdgeBlock
		if b >= 0 {
			size = min(2*cap(s.blocks[b]), maxEdgeBlock)
		}
		s.blocks = append(s.blocks, make([]uint32, 0, max(size, len(ids))))
		b++
	}
	s.starts = append(s.starts, uint64(b)<<32|uint64(len(s.blocks[b])))
	s.blocks[b] = append(s.blocks[b], ids...)
}

// finish ends the edges of the last node.
func (s *segment) finish() {
	b := max(len(s.blocks)-1, 0)
	var used int
	if len(s.blocks) > 0 {
		used = len(s.blocks[b])
	}
	s.starts = append(s.starts, uint64(b)<<32|uint64(used))
}

func (s *segment) nodes() int {
	return len(s.starts) - 1
}

// nodeEdges returns the edges of node lo+k.
func (s *segment) nodeEdges(k uint32) []uint32 {
	start, next := s.starts[k], s.starts[k+1]
	block := s.blocks[start>>32]
	end := uint32(len(block))
	if next>>32 == start>>32 {
		end = uint32(next)
	}
	return block[uint32(start):end]
}

// bytes returns the memory the segment holds.
func (s *segment) bytes() int64 {
	n := int64(cap(s.starts)) * 8
	for _, b := range s.blocks {
		n += int64(cap(b)) * 4
	}
	return n
}

func compareHash(a, b Hash) int {
	return bytes.Compare(a[:], b[:])
}

func keyPrefix16(h Hash) int {
	return int(h[0])<<8 | int(h[1])
}

// size returns the number of nodes.
func (g *graph) size() int {
	return g.n
}

// find returns the node of h, or noNode.
func (g *graph) find(h Hash) uint32 {
	p := keyPrefix16(h)
	lo, hi := g.prefix[p], g.prefix[p+1]
	if lo == hi {
		return noNode
	}
	s := &g.shards[g.prefixShard[p]]
	i, ok := slices.BinarySearchFunc(s.keys[lo-s.lo:hi-s.lo], h, compareHash)
	if !ok {
		return noNode
	}
	return lo + uint32(i)
}

// edges returns the edges of node id.
func (g *graph) edges(id uint32) []uint32 {
	i, ok := slices.BinarySearchFunc(g.segs, id, func(s segment, id uint32) int {
		switch {
		case s.lo > id:
			return 1
		case s.lo+uint32(s.nodes()) <= id:
			return -1
		}
		return 0
	})
	if !ok {
		return nil
	}
	s := &g.segs[i]
	return s.nodeEdges(id - s.lo)
}

// keysBytes returns the memory the keys and their indexes hold.
func (g *graph) keysBytes() int64 {
	n := int64(cap(g.types)) + int64(len(g.prefix))*4 + int64(len(g.prefixShard))*2
	for _, s := range g.shards {
		n += int64(cap(s.keys)) * entities.HashSize
	}
	return n
}

// prefixBounds cuts the inner bounds of a key range split to 16-bit key
// prefixes, so that the keys of each prefix fall in one shard, as the graph
// needs.
func prefixBounds(bounds []string) []string {
	out := []string{bounds[0]}
	for _, b := range bounds[1 : len(bounds)-1] {
		b = b[:min(len(b), 4)]
		if b > out[len(out)-1] {
			out = append(out, b)
		}
	}
	return append(out, bounds[len(bounds)-1])
}

// nodeStats counts the rows of the first scan.
type nodeStats struct {
	// TouchOnly counts rows that hold a touch but no edges: objects without a
	// gc row that a server touched.
	TouchOnly int64
	// Outdated counts rows of an older edge extractor than their type
	// requires, per type index in OutdatedTypes. Newer counts rows of a newer
	// edge extractor than this binary's.
	Outdated      int64
	OutdatedTypes []int64
	Newer         int64
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
			s.stats.OutdatedTypes = make([]int64, len(typeNames))
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
				case r.Version < requiredVersion(r.Type):
					s.stats.Outdated++
					s.stats.OutdatedTypes[typ]++
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
	stats := nodeStats{OutdatedTypes: make([]int64, len(typeNames))}
	for i := range shards {
		n += len(shards[i].keys)
		stats.TouchOnly += shards[i].stats.TouchOnly
		stats.Outdated += shards[i].stats.Outdated
		stats.Newer += shards[i].stats.Newer
		for t, c := range shards[i].stats.OutdatedTypes {
			stats.OutdatedTypes[t] += c
		}
	}
	if uint64(n) >= uint64(noNode) {
		return nil, stats, fmt.Errorf("the table gc has %d rows, more than one mark can hold", n)
	}
	if len(shards) > 1<<16 {
		return nil, stats, fmt.Errorf("too many scan shards: %d", len(shards))
	}
	gr := &graph{
		shards:      make([]keyShard, len(shards)),
		n:           n,
		prefix:      make([]uint32, 1<<16+1),
		prefixShard: make([]uint16, 1<<16),
		types:       make([]uint8, 0, n),
		recent:      newBitmap(n),
	}
	var lo uint32
	var last *Hash
	for i := range shards {
		s := &shards[i]
		if !slices.IsSortedFunc(s.keys, compareHash) || (last != nil && len(s.keys) > 0 && compareHash(*last, s.keys[0]) >= 0) {
			return nil, stats, fmt.Errorf("the scan of the table gc returned keys out of order")
		}
		if len(s.keys) > 0 {
			last = &s.keys[len(s.keys)-1]
		}
		gr.shards[i] = keyShard{lo: lo, keys: s.keys}
		gr.types = append(gr.types, s.types...)
		for _, r := range s.recent {
			gr.recent.set(lo + r)
		}
		for _, k := range s.keys {
			p := keyPrefix16(k)
			if gr.prefix[p+1] > 0 && gr.prefixShard[p] != uint16(i) {
				return nil, stats, fmt.Errorf("the keys with the prefix %04x are in two scan shards", p)
			}
			gr.prefix[p+1]++
			gr.prefixShard[p] = uint16(i)
		}
		lo += uint32(len(s.keys))
		shards[i] = nodeShard{}
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
	for i := range shards {
		ks := gr.shards[i]
		g.Go(func() error {
			s := &shards[i]
			s.perType = make([]int64, len(typeNames))
			s.seg = segment{lo: ks.lo, starts: make([]uint64, 0, len(ks.keys)+1)}
			next := 0
			var ids []uint32
			var rowErr error
			err := table.ReadRows(gctx, bigtable.NewRange(bounds[i], bounds[i+1]), func(row bigtable.Row) bool {
				if !entities.IsHash(row.Key()) {
					rowErr = fmt.Errorf("invalid gc row key %q", row.Key())
					return false
				}
				key := entities.NewHash(row.Key())
				for next < len(ks.keys) && compareHash(ks.keys[next], key) < 0 {
					s.seg.add(nil)
					next++
				}
				if next == len(ks.keys) || ks.keys[next] != key {
					s.counts.NewRows++
					return true
				}
				id := ks.lo + uint32(next)
				ids = ids[:0]
				for _, item := range row[EdgesFamily] {
					targets, err := UnpackEdges(item.Value)
					if err != nil {
						rowErr = fmt.Errorf("invalid gc row %s: %w", key, err)
						return false
					}
					for _, t := range targets {
						to := gr.find(t)
						if to == noNode {
							s.counts.Dangling++
							if gr.recent.has(id) {
								s.counts.DanglingFromRecent++
							}
							if len(s.counts.DanglingSample) < maxDanglingKept {
								s.counts.DanglingSample = append(s.counts.DanglingSample, key.String()+">"+t.String())
							}
							continue
						}
						ids = append(ids, to)
					}
					s.counts.Total += int64(len(targets))
					s.perType[gr.types[id]] += int64(len(targets))
				}
				s.seg.add(ids)
				next++
				return true
			}, bigtable.RowFilter(filter))
			if err == nil {
				err = rowErr
			}
			if err != nil {
				return fmt.Errorf("failed to scan the edges of the table gc from %q: %w", bounds[i], err)
			}
			for ; next < len(ks.keys); next++ {
				s.seg.add(nil)
			}
			s.seg.finish()
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
		if s.seg.nodes() > 0 {
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

// scanMaps reads the keys and cell times of the table maps. It returns the
// rows of each scan shard, which are not copied into one array, so that they
// do not take twice their memory while the edges are scanned.
func scanMaps(ctx context.Context, table *bigtable.Table, bounds []string, gr *graph) ([][]mapRow, int64, error) {
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
	var bad int64
	for _, m := range malformed {
		bad += m
	}
	return rows, bad, nil
}

// key returns the key of node id.
func (g *graph) key(id uint32) Hash {
	i, _ := slices.BinarySearchFunc(g.shards, id, func(s keyShard, id uint32) int {
		switch {
		case s.lo > id:
			return 1
		case s.lo+uint32(len(s.keys)) <= id:
			return -1
		}
		return 0
	})
	s := &g.shards[i]
	return s.keys[id-s.lo]
}

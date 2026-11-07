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
	"math/bits"
	"sync/atomic"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/objects"
)

// bitmap is an exact set of nodes.
type bitmap []uint64

func newBitmap(n int) bitmap {
	return make(bitmap, (n+63)/64)
}

// set adds i and reports whether it was new.
func (b bitmap) set(i uint32) bool {
	w, m := i/64, uint64(1)<<(i%64)
	if b[w]&m != 0 {
		return false
	}
	b[w] |= m
	return true
}

// setAtomic adds i; it is safe to call concurrently.
func (b bitmap) setAtomic(i uint32) {
	atomic.OrUint64(&b[i/64], uint64(1)<<(i%64))
}

func (b bitmap) has(i uint32) bool {
	return b[i/64]&(uint64(1)<<(i%64)) != 0
}

func (b bitmap) count() int64 {
	var n int
	for _, w := range b {
		n += bits.OnesCount64(w)
	}
	return int64(n)
}

// adjacency gives the GC edges of a node. The graph of one scan implements it
// in memory. A mark that outgrows one machine would keep the same roots and
// map rules and replace the walk with one level at a time over sorted runs of
// edges.
type adjacency interface {
	edges(id uint32) []uint32
}

// marker walks the graph from the roots; live is the visited set.
type marker struct {
	adj   adjacency
	live  bitmap
	stack []uint32
	// root is the root being walked. first holds the tracked nodes and, once
	// a walk reached them, the root of that walk.
	root  rootRef
	first map[uint32]rootRef
}

func newMarker(adj adjacency, n int) *marker {
	return &marker{adj: adj, live: newBitmap(n)}
}

// add marks id live; the next walk visits what it reaches.
func (m *marker) add(id uint32) {
	if id != noNode && m.live.set(id) {
		m.stack = append(m.stack, id)
		if _, ok := m.first[id]; ok {
			m.first[id] = m.root
		}
	}
}

// track records the first root of the nodes ids. Call it before any add.
func (m *marker) track(ids []uint32) {
	if len(ids) == 0 {
		return
	}
	if m.first == nil {
		m.first = make(map[uint32]rootRef, len(ids))
	}
	for _, id := range ids {
		if id != noNode {
			m.first[id] = rootRef{}
		}
	}
}

// addRoot marks id live as the root r and walks what it reaches, so that the
// nodes the walk reaches first are recorded with r.
func (m *marker) addRoot(id uint32, r rootRef) {
	m.root = r
	m.add(id)
	m.walk()
}

// walk marks everything reachable from the nodes added since the last walk.
func (m *marker) walk() {
	for len(m.stack) > 0 {
		id := m.stack[len(m.stack)-1]
		m.stack = m.stack[:len(m.stack)-1]
		for _, e := range m.adj.edges(id) {
			m.add(e)
		}
	}
}

// MapClass is the retention class of a map row, decided by the type of its
// from object.
type MapClass string

const (
	// MapEval rows, from ninjago, live for the map retention since their last
	// write or use.
	MapEval MapClass = "eval"
	// MapPusher rows, the deep trees and digest maps of the git-pusher, live
	// while the tree their request links is live.
	MapPusher MapClass = "pusher"
	// MapLFS rows are permanent: no project is recorded on them.
	MapLFS MapClass = "lfs"
	// MapOther rows have from objects of other types, such as build requests;
	// they are kept.
	MapOther MapClass = "other"
	// MapNoRequest rows have a from object without a gc row. Their result is
	// kept.
	MapNoRequest MapClass = "no-request"
	// MapMalformed rows have keys that are not two hashes.
	MapMalformed MapClass = "malformed"
)

var (
	evalRequestType  = typeIndex[string(binding.ObjectTypeEvalRequest)]
	deepTreeReqType  = typeIndex[string(objects.ObjectTypeDeepTreeRequest)]
	digestMapReqType = typeIndex[string(binding.ObjectTypeDigestMapRequest)]
	lfsRequestType   = typeIndex[string(binding.ObjectTypeLFSRequest)]
)

func (g *graph) mapClass(r *mapRow) MapClass {
	if r.from == noNode {
		return MapNoRequest
	}
	switch g.types[r.from] {
	case evalRequestType:
		return MapEval
	case deepTreeReqType, digestMapReqType:
		return MapPusher
	case lfsRequestType:
		return MapLFS
	}
	return MapOther
}

// MapCounts counts the map rows of one class.
type MapCounts struct {
	Rows int64 `json:"rows"`
	Live int64 `json:"live"`
	// Expired counts rows the collector would remove: eval rows older than
	// the map retention and pusher rows whose tree is not live.
	Expired int64 `json:"expired"`
	// MissingTo counts rows whose result has no gc row.
	MissingTo int64 `json:"missing_to"`
}

// markMaps adds the roots of the map rows and walks the graph. Pusher rows
// become live when the tree of their request is live, which the walk can only
// tell after it ran, so they are checked again after each walk until none
// changes.
func (m *marker) markMaps(g *graph, shards [][]mapRow, evalAfter int64) map[MapClass]*MapCounts {
	counts := make(map[MapClass]*MapCounts)
	count := func(c MapClass) *MapCounts {
		mc := counts[c]
		if mc == nil {
			mc = &MapCounts{}
			counts[c] = mc
		}
		return mc
	}
	var pusher []*mapRow
	for _, rows := range shards {
		for i := range rows {
			r := &rows[i]
			c := g.mapClass(r)
			mc := count(c)
			mc.Rows++
			if r.to == noNode {
				mc.MissingTo++
			}
			switch c {
			case MapEval:
				if r.time < evalAfter {
					continue
				}
			case MapPusher:
				pusher = append(pusher, r)
				continue
			}
			m.addMapRow(r, c)
			mc.Live++
		}
	}

	for changed := true; changed; {
		changed = false
		kept := pusher[:0]
		for _, r := range pusher {
			if !m.treeLive(r.from) {
				kept = append(kept, r)
				continue
			}
			m.addMapRow(r, MapPusher)
			count(MapPusher).Live++
			changed = true
		}
		pusher = kept
	}
	for _, mc := range counts {
		mc.Expired = mc.Rows - mc.Live
	}
	return counts
}

// addMapRow marks the request and the result of the live map row r live and
// walks what they reach. A row without a request keeps only its result.
func (m *marker) addMapRow(r *mapRow, c MapClass) {
	m.root = rootRef{kind: rootMap, row: r, mapClass: c}
	if c != MapNoRequest {
		m.add(r.from)
	}
	m.add(r.to)
	m.walk()
}

// treeLive reports whether a live node is among the edges of the request
// from: the tree it was made from.
func (m *marker) treeLive(from uint32) bool {
	for _, e := range m.adj.edges(from) {
		if m.live.has(e) {
			return true
		}
	}
	return false
}

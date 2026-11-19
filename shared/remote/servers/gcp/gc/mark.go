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

type adjacency interface {
	edges(id uint32) []uint32
}

type marker struct {
	adj   adjacency
	live  bitmap
	stack []uint32
	// first maps each tracked node to the root whose walk reached it first.
	root  rootRef
	first map[uint32]rootRef
}

func newMarker(adj adjacency, n int) *marker {
	return &marker{adj: adj, live: newBitmap(n)}
}

func (m *marker) add(id uint32) {
	if id != noNode && m.live.set(id) {
		m.stack = append(m.stack, id)
		if _, ok := m.first[id]; ok {
			m.first[id] = m.root
		}
	}
}

// track records the first root of ids. Call it before any add.
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

func (m *marker) addRoot(id uint32, r rootRef) {
	m.root = r
	m.add(id)
	m.walk()
}

func (m *marker) walk() {
	for len(m.stack) > 0 {
		id := m.stack[len(m.stack)-1]
		m.stack = m.stack[:len(m.stack)-1]
		for _, e := range m.adj.edges(id) {
			m.add(e)
		}
	}
}

// MapClass is the retention class of a map row.
type MapClass string

const (
	// MapEval rows (ninjago) live for the map retention after their last use.
	MapEval MapClass = "eval"
	// MapPusher rows live while the tree their request links is live.
	MapPusher MapClass = "pusher"
	// MapLFS rows are permanent.
	MapLFS MapClass = "lfs"
	// MapOther rows, such as those of build requests, are kept.
	MapOther MapClass = "other"
	// MapNoRequest rows have a from object without a gc row; their result is kept.
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
	// Expired counts rows the collector would remove.
	Expired   int64 `json:"expired"`
	MissingTo int64 `json:"missing_to"`
}

// markMaps marks the live map rows. A pusher row turns live once its tree is.
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

func (m *marker) addMapRow(r *mapRow, c MapClass) {
	m.root = rootRef{kind: rootMap, row: r, mapClass: c}
	if c != MapNoRequest {
		m.add(r.from)
	}
	m.add(r.to)
	m.walk()
}

// treeLive reports whether the tree that request from was made from is live.
func (m *marker) treeLive(from uint32) bool {
	for _, e := range m.adj.edges(from) {
		if m.live.has(e) {
			return true
		}
	}
	return false
}

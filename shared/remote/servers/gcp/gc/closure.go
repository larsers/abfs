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
	"slices"
)

// keyOutcome is what the delete step did with a candidate.
type keyOutcome uint8

const (
	outcomePending keyOutcome = iota
	outcomeDeleted
	outcomeNotMatched
	outcomeGone
	outcomeUnknown
	outcomeTypeNotAllowed
	outcomeBlocked
	outcomeUnplaced
	outcomeNotSent
)

// deletePlan is the plan of the delete step of a cycle. A candidate goes only
// after all its parents with an objects row, so no object still read links a
// deleted one; map rows naming a candidate go first, since a map lookup
// returns the result without reading the request.
type deletePlan struct {
	g *graph
	// cands are in key order.
	cands   []uint32
	sizes   []int64
	markers []uint32
	values  [][]byte
	isCand  bitmap
	// levels[0] is the map rows, so it is empty.
	levels  [][]int32
	outcome []keyOutcome
	mapRows []plannedMapRow
	report  *DeletePlanReport
}

// plannedMapRow is a maps row the delete step removes before the objects.
type plannedMapRow struct {
	key      string
	from, to uint32
	expired  bool
}

// DeletePlanReport reports what a delete cycle would remove, if every map
// row delete and every conditional object delete matched.
type DeletePlanReport struct {
	Lists []ListHoldReport `json:"lists,omitempty"`
	// Unrecorded counts unreachable objects whose marker has no record.
	Held                int64                     `json:"held"`
	Unrecorded          int64                     `json:"unrecorded"`
	Candidates          int64                     `json:"candidates"`
	WouldDelete         int64                     `json:"would_delete"`
	WouldDeleteBytes    int64                     `json:"would_delete_bytes"`
	Types               map[string]*PlanTypeCount `json:"types,omitempty"`
	Levels              int                       `json:"levels"`
	MapRows             int64                     `json:"map_rows"`
	ExpiredEvalMapRows  int64                     `json:"expired_eval_map_rows"`
	MapRowsKeptForTypes int64                     `json:"map_rows_kept_for_types,omitempty"`
	TypeNotAllowed      int64                     `json:"type_not_allowed,omitempty"`
	// BlockedSamples lists edges as "parent>child".
	BlockedByNoncandidateParent int64    `json:"blocked_by_noncandidate_parent"`
	BlockedSamples              []string `json:"blocked_samples,omitempty"`
	Blocked                     int64    `json:"blocked"`
	// Unplaced counts candidates on a cycle of edges, which are kept.
	Unplaced int64 `json:"unplaced,omitempty"`
}

// PlanTypeCount counts the candidates of one type.
type PlanTypeCount struct {
	Candidates       int64 `json:"candidates"`
	WouldDelete      int64 `json:"would_delete"`
	WouldDeleteBytes int64 `json:"would_delete_bytes"`
}

// planDeletes plans the delete step for the types allows accepts. evalAfter
// is in microseconds.
func planDeletes(res *Result, eligible []bool, holds []ListHoldReport, evalAfter int64, allows func(string) bool) *deletePlan {
	g := res.graph
	p := &deletePlan{g: g, isCand: newBitmap(g.size()), values: res.markers, report: &DeletePlanReport{Lists: holds, Types: make(map[string]*PlanTypeCount)}}
	rep := p.report
	recorded := make([]bool, len(res.markers)+1)
	for _, h := range holds {
		for i, v := range res.markers {
			if markerCycle(v) == h.Cycle {
				recorded[i+1] = true
			}
		}
	}
	for _, n := range res.unreachable {
		switch {
		case n.marker == 0 || res.live.has(n.id) || g.recent.has(n.id):
		case !recorded[n.marker]:
			rep.Unrecorded++
		case !eligible[n.marker]:
			rep.Held++
		case p.isCand.set(n.id):
			p.cands = append(p.cands, n.id)
			p.sizes = append(p.sizes, n.size)
			p.markers = append(p.markers, n.marker)
		}
	}
	p.outcome = make([]keyOutcome, len(p.cands))
	rep.Candidates = int64(len(p.cands))

	p.planMapRows(res, evalAfter, allows)
	p.layer()
	p.blockNoncandidateParents(res)
	for i, id := range p.cands {
		if p.outcome[i] == outcomePending && !allows(typeNames[g.types[id]]) {
			p.outcome[i] = outcomeTypeNotAllowed
			rep.TypeNotAllowed++
		}
	}
	for _, level := range p.levels {
		for _, i := range level {
			if p.outcome[i] != outcomePending {
				p.fail(int(i))
			}
		}
	}
	for i, id := range p.cands {
		t := rep.Types[typeNames[g.types[id]]]
		if t == nil {
			t = &PlanTypeCount{}
			rep.Types[typeNames[g.types[id]]] = t
		}
		t.Candidates++
		switch p.outcome[i] {
		case outcomePending:
			t.WouldDelete++
			t.WouldDeleteBytes += p.sizes[i]
			rep.WouldDelete++
			rep.WouldDeleteBytes += p.sizes[i]
		case outcomeBlocked:
			rep.Blocked++
		}
	}
	rep.Levels = max(len(p.levels)-1, 0)
	return p
}

func (p *deletePlan) index(id uint32) int {
	if id == noNode || !p.isCand.has(id) {
		return -1
	}
	i, _ := slices.BinarySearch(p.cands, id)
	return i
}

func (p *deletePlan) children(i int, f func(j int)) {
	for _, e := range p.g.edges(p.cands[i]) {
		if j := p.index(e); j >= 0 {
			f(j)
		}
	}
}

// fail blocks every candidate child of i, so no key is tried whose parent was
// not deleted.
func (p *deletePlan) fail(i int) {
	p.children(i, func(j int) {
		if p.outcome[j] == outcomePending {
			p.outcome[j] = outcomeBlocked
		}
	})
}

// layer puts each candidate one level below its deepest candidate parent.
func (p *deletePlan) layer() {
	indeg := make([]int32, len(p.cands))
	for i := range p.cands {
		p.children(i, func(j int) { indeg[j]++ })
	}
	var cur []int32
	for i, d := range indeg {
		if d == 0 {
			cur = append(cur, int32(i))
		}
	}
	p.levels = [][]int32{nil}
	for len(cur) > 0 {
		p.levels = append(p.levels, cur)
		var next []int32
		for _, i := range cur {
			p.children(int(i), func(j int) {
				if indeg[j]--; indeg[j] == 0 {
					next = append(next, int32(j))
				}
			})
		}
		slices.Sort(next)
		cur = next
	}
	for i, d := range indeg {
		if d > 0 {
			p.outcome[i] = outcomeUnplaced
			p.report.Unplaced++
		}
	}
}

// blockNoncandidateParents blocks every candidate with a parent that is not
// live, not a candidate and has an objects row.
func (p *deletePlan) blockNoncandidateParents(res *Result) {
	rep := p.report
	counted := newBitmap(p.g.size())
	for id := range uint32(p.g.size()) {
		if res.live.has(id) || p.isCand.has(id) || !res.seen.has(id) {
			continue
		}
		for _, e := range p.g.edges(id) {
			j := p.index(e)
			if j < 0 {
				continue
			}
			if counted.set(e) {
				rep.BlockedByNoncandidateParent++
			}
			if len(rep.BlockedSamples) < maxSamples {
				rep.BlockedSamples = append(rep.BlockedSamples, p.g.key(id).String()+">"+p.g.key(e).String())
			}
			if p.outcome[j] == outcomePending {
				p.outcome[j] = outcomeBlocked
			}
		}
	}
}

// planMapRows selects the map rows that name a candidate and the expired eval
// rows. A row goes only when the cycle may delete the types of both its ends,
// and a kept row that names a candidate keeps its candidate ends.
func (p *deletePlan) planMapRows(res *Result, evalAfter int64, allows func(string) bool) {
	g, rep := p.g, p.report
	allowed := func(id uint32) bool {
		return id == noNode || allows(typeNames[g.types[id]])
	}
	if res.maps == nil {
		return
	}
	for s, rows := range res.maps.rows {
		for i := range rows {
			r := &rows[i]
			names := (r.from != noNode && p.isCand.has(r.from)) || (r.to != noNode && p.isCand.has(r.to))
			expired := !names && g.mapClass(r) == MapEval && r.time < evalAfter && res.live.has(r.from) && (r.to == noNode || res.live.has(r.to))
			if !names && !expired {
				continue
			}
			switch {
			case !allowed(r.from) || !allowed(r.to):
				if names {
					rep.MapRowsKeptForTypes++
					p.blockAllowed(r.from, allows)
					p.blockAllowed(r.to, allows)
				}
			case names:
				p.mapRows = append(p.mapRows, plannedMapRow{key: res.maps.rowKey(g, s, i), from: r.from, to: r.to})
				rep.MapRows++
			default:
				p.mapRows = append(p.mapRows, plannedMapRow{key: res.maps.rowKey(g, s, i), from: r.from, to: r.to, expired: true})
				rep.ExpiredEvalMapRows++
			}
		}
	}
}

func (p *deletePlan) blockAllowed(id uint32, allows func(string) bool) {
	if j := p.index(id); j >= 0 && p.outcome[j] == outcomePending && allows(typeNames[p.g.types[id]]) {
		p.outcome[j] = outcomeBlocked
	}
}

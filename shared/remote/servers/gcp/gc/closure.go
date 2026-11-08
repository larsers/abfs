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

// keyOutcome is what the delete step did with a candidate, or why it did not
// try it.
type keyOutcome uint8

const (
	// outcomePending is a candidate the step tries.
	outcomePending keyOutcome = iota
	outcomeDeleted
	// outcomeNotMatched is a key whose marker was not the one of the list:
	// a server rescued it.
	outcomeNotMatched
	// outcomeGone is a key whose row was gone before its delete.
	outcomeGone
	// outcomeUnknown is a key whose delete or read failed; the step reads it
	// again at its end.
	outcomeUnknown
	outcomeTypeNotAllowed
	// outcomeBlocked is a key with a parent that was not deleted.
	outcomeBlocked
	// outcomeUnplaced is a key on a cycle of edges, which content hashes
	// cannot form.
	outcomeUnplaced
	// outcomeNotSent is a key of a level the step did not reach because a
	// worker failed.
	outcomeNotSent
)

// deletePlan is the plan of the delete step of a cycle. Its candidates are
// the keys of the previous list that the mark found unreachable, with a node
// and an objects row. A candidate is deleted only after every parent of it
// in the graph that has an objects row was deleted earlier in the step, so
// that no object that is still read links a deleted one. Every such parent
// of a candidate is itself a candidate: it was unreachable at the previous
// mark too, or it would be recent now. The levels order the candidates so
// that each comes after all its candidate parents, and the map rows that
// name a candidate come before all of them, since a map lookup returns the
// result without reading the request.
type deletePlan struct {
	g *graph
	// cands are the candidate nodes in key order, with the sizes of their
	// objects; isCand holds them by node.
	cands  []uint32
	sizes  []int64
	isCand bitmap
	// levels[l] lists the candidates of level l, in key order; level 0 is
	// the map rows, so levels[0] is empty.
	levels [][]int32
	// outcome is the state of each candidate. The plan sets the keys the
	// step must not try; the step records the others.
	outcome []keyOutcome
	mapRows []plannedMapRow
	report  *DeletePlanReport
}

// plannedMapRow is a row of the table maps that the delete step removes
// before the objects: a row that names a candidate, or an eval row past the
// map retention whose two ends stay.
type plannedMapRow struct {
	key      string
	from, to uint32
	expired  bool
}

// DeletePlanReport reports what a delete cycle would remove from the list of
// the cycle before, if every map row delete and every conditional object
// delete matched.
type DeletePlanReport struct {
	List      string `json:"list"`
	ListCycle int64  `json:"list_cycle"`
	// Candidates counts the keys of the list that this mark found unreachable
	// with an objects row; WouldDelete those the step would try, with their
	// size.
	Candidates       int64                     `json:"candidates"`
	WouldDelete      int64                     `json:"would_delete"`
	WouldDeleteBytes int64                     `json:"would_delete_bytes"`
	Types            map[string]*PlanTypeCount `json:"types,omitempty"`
	// Levels is the number of object levels.
	Levels int `json:"levels"`
	// MapRows counts the map rows that name a candidate and go first,
	// ExpiredEvalMapRows the eval rows past the map retention whose ends
	// stay, and MapRowsKeptForTypes the rows naming a candidate with an end
	// of a type the cycle may not delete, which keep their candidate ends.
	MapRows             int64 `json:"map_rows"`
	ExpiredEvalMapRows  int64 `json:"expired_eval_map_rows"`
	MapRowsKeptForTypes int64 `json:"map_rows_kept_for_types,omitempty"`
	// TypeNotAllowed counts candidates of types the cycle may not delete.
	TypeNotAllowed int64 `json:"type_not_allowed,omitempty"`
	// BlockedByNoncandidateParent counts candidates with a parent that is
	// unreachable, has an objects row and is not a candidate, each once. It
	// is zero unless an object was written without the gc row first;
	// BlockedSamples lists such edges as "parent>child".
	BlockedByNoncandidateParent int64    `json:"blocked_by_noncandidate_parent"`
	BlockedSamples              []string `json:"blocked_samples,omitempty"`
	// Blocked counts candidates below a key the step would not try.
	Blocked int64 `json:"blocked"`
	// Unplaced counts candidates on a cycle of edges, which are kept.
	Unplaced int64 `json:"unplaced,omitempty"`
}

// PlanTypeCount counts the candidates of one type.
type PlanTypeCount struct {
	Candidates       int64 `json:"candidates"`
	WouldDelete      int64 `json:"would_delete"`
	WouldDeleteBytes int64 `json:"would_delete_bytes"`
}

// planDeletes plans the delete step from the list l in the mark of res, for
// the types allows accepts. Eval map rows whose newest cell is older than
// evalAfter, in microseconds, are past the map retention.
func planDeletes(res *Result, l *condemnedList, evalAfter int64, allows func(string) bool) *deletePlan {
	g := res.graph
	p := &deletePlan{g: g, isCand: newBitmap(g.size()), report: &DeletePlanReport{ListCycle: l.header.cycle, Types: make(map[string]*PlanTypeCount)}}
	for _, k := range l.keys {
		id := g.find(k)
		if id == noNode || !res.seen.has(id) || res.live.has(id) || g.recent.has(id) {
			continue
		}
		if p.isCand.set(id) {
			p.cands = append(p.cands, id)
		}
	}
	slices.Sort(p.cands)
	p.sizes = make([]int64, len(p.cands))
	for i, id := range p.cands {
		if j, ok := slices.BinarySearchFunc(res.unreachable, id, func(n sizedNode, id uint32) int { return int(int64(n.id) - int64(id)) }); ok {
			p.sizes[i] = res.unreachable[j].size
		}
	}
	p.outcome = make([]keyOutcome, len(p.cands))
	rep := p.report
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

// index returns the candidate index of node id, or -1.
func (p *deletePlan) index(id uint32) int {
	if id == noNode || !p.isCand.has(id) {
		return -1
	}
	i, _ := slices.BinarySearch(p.cands, id)
	return i
}

// children calls f for the candidate index of each edge of candidate i to a
// candidate.
func (p *deletePlan) children(i int, f func(j int)) {
	for _, e := range p.g.edges(p.cands[i]) {
		if j := p.index(e); j >= 0 {
			f(j)
		}
	}
}

// fail marks every candidate child of the candidate i blocked, so that no
// key is tried whose parent was not deleted. A blocked key is itself failed
// when its level ends.
func (p *deletePlan) fail(i int) {
	p.children(i, func(j int) {
		if p.outcome[j] == outcomePending {
			p.outcome[j] = outcomeBlocked
		}
	})
}

// layer puts each candidate one level below its deepest candidate parent, by
// Kahn's layering over the edges between candidates; a candidate without one
// is on level 1. A candidate on a cycle is never placed and is kept.
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
// live, not a candidate and has an objects row: the proof that every such
// parent is a candidate failed for it, so it stays.
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

// planMapRows selects the map rows that name a candidate and the eval rows
// past the map retention whose two ends are live. A row goes only when the
// cycle may delete the types of both its ends; an end without a node has no
// type and allows it. So a cycle limited to some types drops no cached result
// of the others. A row that names a candidate and stays for its types keeps
// its candidate ends, as a kept row does in the step.
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
			expired := !names && g.mapClass(r) == MapEval && r.time < evalAfter && r.to != noNode && res.live.has(r.from) && res.live.has(r.to)
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

// blockAllowed blocks node id if it is a candidate the step would try and of
// a type allows accepts; a candidate of another type is never tried.
func (p *deletePlan) blockAllowed(id uint32, allows func(string) bool) {
	if j := p.index(id); j >= 0 && p.outcome[j] == outcomePending && allows(typeNames[p.g.types[id]]) {
		p.outcome[j] = outcomeBlocked
	}
}

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
	"time"
)

// rootKind tells what a rootRef points to; zero is no root.
type rootKind uint8

const (
	rootOfRef rootKind = iota + 1
	rootPinned
	rootPermanentObject
	rootRecentObject
	rootMap
)

// rootRef names one root: i indexes the ref roots, pinned revisions or nodes.
type rootRef struct {
	kind     rootKind
	i        int
	row      *mapRow
	mapClass MapClass
}

// refClassOrder is the order in which the mark walks the ref roots.
var refClassOrder = []RefClass{
	RefSource, RefProjectStatus, RefPushedPacks, RefConfig, RefACL, RefWorkspace, RefSnapshot, RefMount, RefUnmount,
	RefBuild, RefBuildStart, RefOtherMeta,
}

const pinnedAfter = RefUnmount

func refClassRank(c RefClass) int {
	return slices.Index(refClassOrder, c)
}

func sortRefRoots(roots []refRoot) {
	slices.SortStableFunc(roots, func(a, b refRoot) int {
		return refClassRank(a.class) - refClassRank(b.class)
	})
}

// Root describes the root that first reached an object in the mark.
type Root struct {
	Class string `json:"class"`
	// Key is the root object: ref target, pinned revision, object or map request.
	Key        string     `json:"key,omitempty"`
	Project    string     `json:"project,omitempty"`
	Ref        string     `json:"ref,omitempty"`
	UpdateTime *time.Time `json:"update_time,omitempty"`
	// Pin names the mount details and workspace snapshot of a pinned revision.
	Pin    *MountSample `json:"pin,omitempty"`
	Result string       `json:"result,omitempty"`
}

type rootTable struct {
	g         *graph
	refs      []refRoot
	revisions []pinnedRevision
}

func (t *rootTable) describe(r rootRef) *Root {
	switch r.kind {
	case rootOfRef:
		ref := &t.refs[r.i]
		updated := ref.updated
		return &Root{Class: string(ref.class), Key: ref.target.String(), Project: ref.project, Ref: ref.ref, UpdateTime: &updated}
	case rootPinned:
		rev := &t.revisions[r.i]
		pin := rev.src.sample()
		return &Root{Class: RootPinnedRevisions, Key: rev.hash.String(), Pin: &pin}
	case rootPermanentObject:
		return &Root{Class: RootPermanent, Key: t.g.key(uint32(r.i)).String()}
	case rootRecentObject:
		return &Root{Class: RootRecent, Key: t.g.key(uint32(r.i)).String()}
	case rootMap:
		root := &Root{Class: rootMapPrefix + string(r.mapClass)}
		if r.row.from != noNode {
			root.Key = t.g.key(r.row.from).String()
		}
		if r.row.to != noNode {
			root.Result = t.g.key(r.row.to).String()
		}
		return root
	}
	return nil
}

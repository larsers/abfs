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
	// rootOfRef: i is the index of the ref root.
	rootOfRef rootKind = iota + 1
	// rootPinned: i is the index of the pinned revision.
	rootPinned
	// rootPermanentObject and rootRecentObject: i is the node.
	rootPermanentObject
	rootRecentObject
	// rootMap: row is the map row.
	rootMap
)

// rootRef names one root of a mark.
type rootRef struct {
	kind     rootKind
	i        int
	row      *mapRow
	mapClass MapClass
}

// refClassOrder is the order in which the mark walks the ref roots. Pinned
// revisions come after the unmount refs, before the builds.
var refClassOrder = []RefClass{
	RefSource, RefProjectStatus, RefPushedPacks, RefConfig, RefACL, RefWorkspace, RefMount, RefUnmount,
	RefBuild, RefBuildStart, RefOtherMeta,
}

// pinnedAfter is the last ref class walked before the pinned revisions.
const pinnedAfter = RefUnmount

func refClassRank(c RefClass) int {
	return slices.Index(refClassOrder, c)
}

// sortRefRoots sorts the ref roots into the order of the mark.
func sortRefRoots(roots []refRoot) {
	slices.SortStableFunc(roots, func(a, b refRoot) int {
		return refClassRank(a.class) - refClassRank(b.class)
	})
}

// Root describes the root that first reached an object in the mark.
type Root struct {
	// Class is the root class, as in Report.Roots.
	Class string `json:"class"`
	// Key is the root object: the ref target, the pinned revision, the recent
	// or permanent object, or the request of a map row.
	Key string `json:"key,omitempty"`
	// Project, Ref and UpdateTime name a ref root.
	Project    string     `json:"project,omitempty"`
	Ref        string     `json:"ref,omitempty"`
	UpdateTime *time.Time `json:"update_time,omitempty"`
	// Pin names the mount details, and the workspace snapshot, of a pinned
	// revision.
	Pin *MountSample `json:"pin,omitempty"`
	// Result is the result of a map row.
	Result string `json:"result,omitempty"`
}

// rootTable resolves the rootRefs of one mark.
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

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
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
)

// PreviousListReport reports the keys of the last published condemned list
// in the mark of this cycle.
type PreviousListReport struct {
	Location     string    `json:"location"`
	Cycle        int64     `json:"cycle"`
	SnapshotTime time.Time `json:"snapshot_time"`
	// Complete tells that the cycle that wrote the list wrote all its
	// markers.
	Complete bool `json:"complete"`
	// Error tells why the list could not be read; its markers stay.
	Error string `json:"error,omitempty"`
	Keys  int64  `json:"keys"`
	// Cleared counts the markers of live keys this cycle deleted, and
	// LiveList names the object that lists every live key.
	Cleared  int64                        `json:"cleared"`
	LiveList string                       `json:"live_list,omitempty"`
	Types    map[string]*PreviousListType `json:"types,omitempty"`
}

// PreviousListType counts the keys of one type of the previous list.
type PreviousListType struct {
	Keys int64 `json:"keys"`
	// StillUnreachable counts the keys this cycle condemns again.
	StillUnreachable int64 `json:"still_unreachable"`
	// Gone counts keys without a gc row now.
	Gone int64 `json:"gone,omitempty"`
	// Live counts the keys the mark found live, by the class of the root that
	// reached them first, with up to maxSamples keys per class in Samples.
	Live    map[string]int64    `json:"live,omitempty"`
	Samples map[string][]string `json:"samples,omitempty"`
	// Rescued counts live keys whose marker a server had cleared.
	Rescued int64 `json:"rescued"`
}

// liveKey is a line of the live list: a key of the previous list that this
// cycle found live.
type liveKey struct {
	Key       string     `json:"key"`
	Type      string     `json:"type"`
	Root      *Root      `json:"root"`
	TouchTime *time.Time `json:"touch_time,omitempty"`
	WriteTime *time.Time `json:"write_time,omitempty"`
	// Marker is the cycle number of the marker this cycle cleared.
	Marker *int64 `json:"marker,omitempty"`
	// Rescued tells that the marker was gone before this cycle: a server
	// cleared it on a read. It is also set, with no read, for a key whose
	// marker a cycle cleared before it stopped short of its own list.
	Rescued bool `json:"rescued"`
}

// readPrevious reads the condemned list that GcState names, so that the mark
// tracks its keys.
func (c *condemner) readPrevious(ctx context.Context, lease *Lease, d *dryRun) error {
	state, err := lease.ReadList(ctx)
	if err != nil || state.Location == "" {
		return err
	}
	c.prevState = state
	l, err := readList(ctx, c.bucket, state.Location)
	if errors.Is(err, storage.ErrObjectNotExist) {
		log.Printf("Warning: the condemned list %s of the last condemn cycle is gone; the markers of its keys stay until a read or a cycle replaces them", state.Location)
		c.prevErr = "not found"
		return nil
	}
	if err != nil {
		return err
	}
	c.prev = l
	d.tracked = l.keys
	return nil
}

// clearLive deletes the markers of the keys of the previous list that the
// mark found live and writes every such key, with the root that keeps it, to
// the live list of this cycle. A delete never creates a row.
func (c *condemner) clearLive(ctx context.Context, lease *Lease, res *Result) (*PreviousListReport, error) {
	state := c.prevState
	if state.Location == "" {
		return nil, nil
	}
	rep := &PreviousListReport{Location: state.Location, SnapshotTime: state.SnapshotTime, Complete: state.Complete, Error: c.prevErr}
	l := c.prev
	if l == nil {
		return rep, nil
	}
	rep.Cycle, rep.Keys = l.header.cycle, int64(len(l.keys))
	rep.Types = make(map[string]*PreviousListType)
	typeOf := func(i int) *PreviousListType {
		name := l.typeName(i)
		t := rep.Types[name]
		if t == nil {
			t = &PreviousListType{Live: make(map[string]int64), Samples: make(map[string][]string)}
			rep.Types[name] = t
		}
		return t
	}
	var live []int
	var keys []Hash
	for i, k := range l.keys {
		t := typeOf(i)
		t.Keys++
		switch id := res.graph.find(k); {
		case id == noNode:
			t.Gone++
		case !res.live.has(id):
			t.StillUnreachable++
		default:
			live = append(live, i)
			keys = append(keys, k)
		}
	}
	states, err := readKeyStates(ctx, c.bt, keys)
	if err != nil {
		return nil, err
	}
	var marked []string
	for j, st := range states {
		if st.marker != nil {
			marked = append(marked, keys[j].String())
		}
	}
	if err := c.clearMarkers(ctx, marked); err != nil {
		return nil, err
	}
	rep.Cleared = int64(len(marked))

	lines := make([]liveKey, len(live))
	for j, i := range live {
		st := &states[j]
		root := res.firstRoot(res.graph.find(keys[j]))
		class := "unknown"
		if root != nil {
			class = root.Class
		}
		line := liveKey{Key: keys[j].String(), Type: l.typeName(i), Root: root, TouchTime: optTime(st.touched), WriteTime: optTime(st.written)}
		if st.marker != nil {
			m := markerCycle(st.marker)
			line.Marker = &m
		}
		// Only a rescue clears a marker between two cycles. Markers of an
		// incomplete list may never have been written.
		line.Rescued = st.marker == nil && state.Complete
		lines[j] = line
		t := typeOf(i)
		t.Live[class]++
		if len(t.Samples[class]) < maxSamples {
			t.Samples[class] = append(t.Samples[class], line.Key)
		}
		if line.Rescued {
			t.Rescued++
		}
	}
	name := listObjectName(c.opts.StateName, res.Report.Cycle) + liveSuffix
	err = createObject(ctx, c.budget, c.bucket, name, runID(c.opts.Holder, lease.Epoch), "application/gzip", func(w io.Writer) error {
		zw := gzip.NewWriter(w)
		enc := json.NewEncoder(zw)
		for i := range lines {
			if err := enc.Encode(&lines[i]); err != nil {
				return err
			}
		}
		return zw.Close()
	})
	if err != nil {
		return nil, err
	}
	rep.LiveList = name
	return rep, nil
}

// clearMarkers deletes the condemn markers of rows, in batches.
func (c *condemner) clearMarkers(ctx context.Context, rows []string) error {
	table := c.bt.Open(objectsTable)
	lim := newLimiter(c.opts.Rate)
	mut := bigtable.NewMutation()
	mut.DeleteCellsInColumn(ObjectsFamily, CondemnedColumn)
	for lo := 0; lo < len(rows); lo += c.opts.BatchSize {
		batch := rows[lo:min(lo+c.opts.BatchSize, len(rows))]
		if err := lim.wait(ctx, len(batch)); err != nil {
			return err
		}
		if err := applyBulk(ctx, c.budget, table, batch, mut); err != nil {
			return fmt.Errorf("failed to clear the condemn markers of live objects: %w", err)
		}
	}
	return nil
}

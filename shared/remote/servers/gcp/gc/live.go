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

// PreviousListReport reports how this mark found the keys of the last list.
type PreviousListReport struct {
	Location     string    `json:"location"`
	Cycle        int64     `json:"cycle"`
	SnapshotTime time.Time `json:"snapshot_time"`
	// Complete tells that the cycle that wrote the list wrote all its markers.
	Complete bool                         `json:"complete"`
	Error    string                       `json:"error,omitempty"`
	Keys     int64                        `json:"keys"`
	Types    map[string]*PreviousListType `json:"types,omitempty"`
}

// PreviousListType counts the keys of one type of the previous list.
type PreviousListType struct {
	Keys             int64 `json:"keys"`
	StillUnreachable int64 `json:"still_unreachable"`
	Gone             int64 `json:"gone,omitempty"`
	// Live counts live keys by the class of their first root.
	Live    map[string]int64    `json:"live,omitempty"`
	Samples map[string][]string `json:"samples,omitempty"`
	// Rescued counts live keys whose marker a server cleared on a read.
	Rescued int64 `json:"rescued"`
}

// liveKey is a line of the live list: a live key whose marker this cycle cleared.
type liveKey struct {
	Key    string `json:"key"`
	Type   string `json:"type"`
	Root   *Root  `json:"root,omitempty"`
	Marker int64  `json:"marker"`
}

type previousList struct {
	state ListState
	list  *condemnedList
	err   string
}

// readPreviousList only reports a list it cannot read: no rule reads the list.
func readPreviousList(ctx context.Context, lease *Lease, bucket *storage.BucketHandle) (previousList, error) {
	state, err := lease.ReadList(ctx)
	if err != nil || state.Location == "" {
		return previousList{}, err
	}
	prev := previousList{state: state}
	l, err := readList(ctx, bucket, state.Location)
	switch {
	case errors.Is(err, storage.ErrObjectNotExist):
		log.Printf("Warning: the condemned list %s of the last condemn cycle is gone", state.Location)
		prev.err = "not found"
	case err != nil && ctx.Err() != nil:
		return previousList{}, err
	case err != nil:
		log.Printf("Warning: the report leaves out the condemned list %s of the last condemn cycle: %v", state.Location, err)
		prev.err = err.Error()
	default:
		prev.list = l
	}
	return prev, nil
}

func (c *condemner) readPrevious(ctx context.Context, lease *Lease, d *dryRun) error {
	var err error
	if c.prev, err = readPreviousList(ctx, lease, c.bucket); err != nil {
		return err
	}
	if c.prev.list != nil {
		d.tracked = c.prev.list.keys
	}
	return nil
}

func (c *condemner) reportPrevious(res *Result, cleared bitmap) *PreviousListReport {
	state := c.prev.state
	if state.Location == "" {
		return nil
	}
	rep := &PreviousListReport{Location: state.Location, SnapshotTime: state.SnapshotTime, Complete: state.Complete, Error: c.prev.err}
	l := c.prev.list
	if l == nil {
		return rep
	}
	rep.Cycle, rep.Keys = l.header.cycle, int64(len(l.keys))
	rep.Types = make(map[string]*PreviousListType)
	for i, k := range l.keys {
		name := l.typeName(i)
		t := rep.Types[name]
		if t == nil {
			t = &PreviousListType{Live: make(map[string]int64), Samples: make(map[string][]string)}
			rep.Types[name] = t
		}
		t.Keys++
		id := res.graph.find(k)
		switch {
		case id == noNode:
			t.Gone++
			continue
		case !res.live.has(id):
			t.StillUnreachable++
			continue
		}
		class := "unknown"
		if root := res.firstRoot(id); root != nil {
			class = root.Class
		}
		t.Live[class]++
		if len(t.Samples[class]) < maxSamples {
			t.Samples[class] = append(t.Samples[class], k.String())
		}
		// Only a rescue clears a marker between cycles; an incomplete list may lack markers.
		if !cleared.has(id) && state.Complete {
			t.Rescued++
		}
	}
	return rep
}

// clearMarked deletes the markers of live keys and writes them to the live list.
// It races no delete: a delete never creates a row, a live key is no candidate.
func (c *condemner) clearMarked(ctx context.Context, lease *Lease, res *Result, rep *CondemnReport) (bitmap, error) {
	cleared := newBitmap(res.graph.size())
	rows := make([]string, len(res.marked))
	lines := make([]liveKey, len(res.marked))
	for i, n := range res.marked {
		cleared.set(n.id)
		rows[i] = res.graph.key(n.id).String()
		lines[i] = liveKey{Key: rows[i], Type: typeNames[res.graph.types[n.id]], Root: res.firstRoot(n.id), Marker: markerCycle(res.markers[n.marker-1])}
	}
	if err := c.clearMarkers(ctx, rows); err != nil {
		return nil, err
	}
	rep.Cleared = int64(len(rows))
	if len(lines) == 0 {
		return cleared, nil
	}
	name := listObjectName(c.opts.StateName, res.Report.Cycle) + liveSuffix
	err := createObject(ctx, c.budget, c.bucket, name, runID(c.opts.Holder, lease.Epoch), "application/gzip", func(w io.Writer) error {
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
	return cleared, nil
}

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

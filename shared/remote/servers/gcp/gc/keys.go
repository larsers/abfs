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
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"cloud.google.com/go/bigtable"
)

// keyReadBatch is the number of rows of one read of keys by name.
const keyReadBatch = 1000

// keyState is what the gc row and the objects row of a key hold.
type keyState struct {
	gcRow     bool
	written   time.Time
	touched   time.Time
	objectRow bool
	// marker is the value of g:condemned, nil when the row has none.
	marker []byte
}

// readKeyStates reads the gc rows and the objects rows of keys, in batches.
func readKeyStates(ctx context.Context, bt *bigtable.Client, keys []Hash) ([]keyState, error) {
	states := make([]keyState, len(keys))
	gcFilter := bigtable.ChainFilters(bigtable.LatestNFilter(1), bigtable.InterleaveFilters(
		bigtable.ChainFilters(bigtable.FamilyFilter(EdgesFamily), bigtable.ColumnFilter(LinksColumn), bigtable.StripValueFilter()),
		bigtable.ChainFilters(bigtable.FamilyFilter(TouchFamily), bigtable.StripValueFilter()),
	))
	objectsFilter := bigtable.InterleaveFilters(
		bigtable.ChainFilters(bigtable.FamilyFilter(objectsMetaFamily), bigtable.LatestNFilter(1), bigtable.StripValueFilter()),
		bigtable.ChainFilters(bigtable.FamilyFilter(ObjectsFamily), bigtable.ColumnFilter(CondemnedColumn), bigtable.LatestNFilter(1)),
	)
	gcTable, objects := bt.Open(TableName), bt.Open(objectsTable)
	for lo := 0; lo < len(keys); lo += keyReadBatch {
		batch := keys[lo:min(lo+keyReadBatch, len(keys))]
		index := make(map[string]int, len(batch))
		rows := make(bigtable.RowList, len(batch))
		for i, k := range batch {
			rows[i] = k.String()
			index[rows[i]] = lo + i
		}
		err := gcTable.ReadRows(ctx, rows, func(row bigtable.Row) bool {
			st := &states[index[row.Key()]]
			for _, item := range row[EdgesFamily] {
				st.gcRow = true
				st.written = item.Timestamp.Time()
			}
			st.touched = latest(row[TouchFamily])
			return true
		}, bigtable.RowFilter(gcFilter))
		if err != nil {
			return nil, fmt.Errorf("failed to read rows of the table gc: %w", err)
		}
		err = objects.ReadRows(ctx, rows, func(row bigtable.Row) bool {
			st := &states[index[row.Key()]]
			st.objectRow = len(row[objectsMetaFamily]) > 0
			for _, item := range row[ObjectsFamily] {
				st.marker = append([]byte{}, item.Value...)
			}
			return true
		}, bigtable.RowFilter(objectsFilter))
		if err != nil {
			return nil, fmt.Errorf("failed to read rows of the table objects: %w", err)
		}
	}
	return states, nil
}

// markerCycle returns the cycle number of a condemn marker, or -1 for a value
// that is not one.
func markerCycle(marker []byte) int64 {
	if len(marker) != 8 {
		return -1
	}
	return int64(binary.BigEndian.Uint64(marker))
}

// KeyReport explains the state of one key.
type KeyReport struct {
	Key string `json:"key"`
	// GCRow tells whether the key has a gc row in the scan, Type is its type.
	GCRow     bool   `json:"gc_row"`
	Type      string `json:"type,omitempty"`
	Reachable bool   `json:"reachable"`
	// FirstRoot is the root whose walk reached the key first.
	FirstRoot *Root `json:"first_root,omitempty"`
	// WriteTime and TouchTime are the first write and the last touch in its
	// gc row, read after the mark.
	WriteTime *time.Time `json:"write_time,omitempty"`
	TouchTime *time.Time `json:"touch_time,omitempty"`
	// ObjectRow tells whether its objects row exists; Condemned is the cycle
	// number of its condemn marker.
	ObjectRow bool   `json:"object_row"`
	Condemned *int64 `json:"condemned,omitempty"`
}

func optTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// explainKeys reports the state of keys after the mark.
func (r *Result) explainKeys(ctx context.Context, bt *bigtable.Client, keys []Hash) ([]KeyReport, error) {
	states, err := readKeyStates(ctx, bt, keys)
	if err != nil {
		return nil, err
	}
	reports := make([]KeyReport, len(keys))
	for i, k := range keys {
		st := &states[i]
		kr := KeyReport{Key: k.String(), WriteTime: optTime(st.written), TouchTime: optTime(st.touched), ObjectRow: st.objectRow}
		if id := r.graph.find(k); id != noNode {
			kr.GCRow = true
			kr.Type = typeNames[r.graph.types[id]]
			kr.Reachable = r.live.has(id)
			kr.FirstRoot = r.firstRoot(id)
		}
		if st.marker != nil {
			c := markerCycle(st.marker)
			kr.Condemned = &c
		}
		reports[i] = kr
	}
	return reports, nil
}

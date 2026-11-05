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
	"log"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
)

// The table gc has one row per object, keyed like the objects table. The
// family e holds the edges, the family t the last touch. The family g of the
// objects table holds the condemn marker.
const (
	TableName      = "gc"
	EdgesFamily    = "e"
	TouchFamily    = "t"
	ObjectsFamily  = "g"
	LinksColumn    = "links"
	TypeColumn     = "type"
	VersionColumn  = "ver"
	writeAttempts  = 6
	firstRetryWait = 100 * time.Millisecond
	maxRetryWait   = 2 * time.Second
)

// EdgeWriter writes the gc rows of objects.
type EdgeWriter struct {
	apply     func(ctx context.Context, row string, m *bigtable.Mutation) error
	retryWait time.Duration
}

// NewEdgeWriter returns an EdgeWriter for the table gc of client's instance.
func NewEdgeWriter(client *bigtable.Client) *EdgeWriter {
	table := client.Open(TableName)
	return &EdgeWriter{
		apply: func(ctx context.Context, row string, m *bigtable.Mutation) error {
			return table.Apply(ctx, row, m)
		},
		retryWait: firstRetryWait,
	}
}

// Put writes the gc row of the object key of type typ, decoded as obj (nil for
// a blob).
func (w *EdgeWriter) Put(ctx context.Context, key Hash, typ string, obj objects.Object) error {
	edges, malformed, err := Edges(typ, obj)
	if err != nil {
		return fmt.Errorf("failed to extract the GC edges of %s: %w", key, err)
	}
	if malformed > 0 {
		log.Printf("Warning: %s object %s has %d malformed hashes, which are not GC edges", typ, key, malformed)
	}
	return w.Write(ctx, key, typ, edges)
}

// Write writes the gc row of the object key of type typ with the given edges.
func (w *EdgeWriter) Write(ctx context.Context, key Hash, typ string, edges []Hash) error {
	// The server sets the timestamps, so that the cell timestamp of e:links is
	// the first write time on one clock for all replicas.
	mut := bigtable.NewMutation()
	mut.Set(EdgesFamily, LinksColumn, bigtable.ServerTime, PackEdges(edges))
	mut.Set(EdgesFamily, TypeColumn, bigtable.ServerTime, []byte(typ))
	mut.Set(EdgesFamily, VersionColumn, bigtable.ServerTime, versionBytes(ExtractorVersion))
	if err := w.applyWithRetry(ctx, key.String(), mut); err != nil {
		return fmt.Errorf("failed to write the gc row of %s: %v", key, err) //nolint:errorlint // a missing table must not reach the client as a missing object
	}
	return nil
}

// applyWithRetry retries transient failures. The client library does not
// retry mutations with server timestamps, because a retry after a lost
// response writes the cells again with a later timestamp. For a gc row that is
// harmless: the newest cell wins and the first write time moves later, which
// only keeps the object longer.
func (w *EdgeWriter) applyWithRetry(ctx context.Context, row string, mut *bigtable.Mutation) error {
	wait := w.retryWait
	for attempt := 1; ; attempt++ {
		err := w.apply(ctx, row, mut)
		if err == nil || attempt == writeAttempts || !retryable(ctx, err) {
			return err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
		wait = min(2*wait, maxRetryWait)
	}
}

func retryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.Aborted, codes.DeadlineExceeded:
		return true
	}
	return false
}

func versionBytes(v int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(v))
}

// Row is a row of the table gc.
type Row struct {
	Key     Hash
	Type    string
	Version int64
	Edges   []Hash
	// Written is the time the edges were written, the first write time of the
	// object.
	Written time.Time
}

// ParseRow parses a row of the table gc read with the newest cell of each
// column. A column that is missing keeps its zero value.
func ParseRow(row bigtable.Row) (*Row, error) {
	if !entities.IsHash(row.Key()) {
		return nil, fmt.Errorf("invalid gc row key %q", row.Key())
	}
	r := &Row{Key: entities.NewHash(row.Key())}
	for _, item := range row[EdgesFamily] {
		switch item.Column {
		case EdgesFamily + ":" + LinksColumn:
			edges, err := UnpackEdges(item.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid gc row %s: %w", row.Key(), err)
			}
			r.Edges = edges
			r.Written = item.Timestamp.Time()
		case EdgesFamily + ":" + TypeColumn:
			r.Type = string(item.Value)
		case EdgesFamily + ":" + VersionColumn:
			if len(item.Value) != 8 {
				return nil, fmt.Errorf("invalid gc row %s: version of %d bytes", row.Key(), len(item.Value))
			}
			r.Version = int64(binary.BigEndian.Uint64(item.Value))
		}
	}
	return r, nil
}

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
	"errors"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
)

// Package casdb cannot import this package and keeps its own copy of the
// names of the objects table.
const (
	TableName         = "abfsgc"
	EdgesFamily       = "e"
	TouchFamily       = "t"
	ObjectsFamily     = "abfsgc"
	objectsTable      = "objects"
	objectsMetaFamily = "meta"
	LinksColumn       = "links"
	TypeColumn        = "type"
	VersionColumn     = "ver"
	writeAttempts     = 6
	firstRetryWait    = 100 * time.Millisecond
	maxRetryWait      = 2 * time.Second
)

// EdgeWriter writes the gc rows of objects.
type EdgeWriter struct {
	apply     func(ctx context.Context, row string, m *bigtable.Mutation) error
	retryWait time.Duration
	budget    writeBudget
}

func NewEdgeWriter(client *bigtable.Client) *EdgeWriter {
	table := client.Open(TableName)
	return &EdgeWriter{
		apply: func(ctx context.Context, row string, m *bigtable.Mutation) error {
			return table.Apply(ctx, row, m)
		},
		retryWait: firstRetryWait,
	}
}

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
	// Server timestamps put the first write times of all replicas on one clock.
	return w.write(ctx, key, typ, edges, bigtable.ServerTime, false)
}

// WriteAt writes the gc row of an existing object with the time of its object
// row, replacing older edges, whose cells can be newer than that time.
func (w *EdgeWriter) WriteAt(ctx context.Context, key Hash, typ string, edges []Hash, written bigtable.Timestamp) error {
	return w.write(ctx, key, typ, edges, written, true)
}

func (w *EdgeWriter) write(ctx context.Context, key Hash, typ string, edges []Hash, ts bigtable.Timestamp, replace bool) error {
	mut := bigtable.NewMutation()
	if replace {
		mut.DeleteCellsInFamily(EdgesFamily)
	}
	mut.Set(EdgesFamily, LinksColumn, ts, PackEdges(edges))
	mut.Set(EdgesFamily, TypeColumn, ts, []byte(typ))
	mut.Set(EdgesFamily, VersionColumn, ts, versionBytes(ExtractorVersion))
	// The library does not retry server-timestamp mutations. A retry here only
	// moves the first write time later, which keeps the object longer.
	row := key.String()
	if err := retryWith(ctx, w.retryWait, w.budget, func(actx context.Context) error { return w.apply(actx, row, mut) }); err != nil {
		return fmt.Errorf("failed to write the gc row of %s: %v", key, err) //nolint:errorlint // a missing table must not reach the client as a missing object
	}
	return nil
}

// retryable reports whether err is transient; an attempt deadline is while ctx lives.
func retryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.Aborted, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	}
	return false
}

func versionBytes(v int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(v))
}

// Row is a row of the table abfsgc.
type Row struct {
	Key     Hash
	Type    string
	Version int64
	Edges   []Hash
	// Written is the first write time of the object.
	Written time.Time
}

// ParseRow parses a gc row read with the newest cell of each column.
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

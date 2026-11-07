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

package casdb

import (
	"context"
	_ "embed"
	"fmt"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"

	"github.com/google/abfs/shared/entities"
)

// The garbage collector condemns an object by setting the cell g:condemned of
// its objects row; package gc owns these names. With EnableGC, header reads
// include the family, so that the server can rescue a condemned object that is
// still used.
const (
	headerFamily    = "meta"
	condemnedFamily = "g"
	condemnedColumn = condemnedFamily + ":condemned"
	gcHeaderFamily  = headerFamily + "|" + condemnedFamily
)

//go:embed gcschema.sql
var gcSchemaSQL string

// GCSchemaStatements returns the DDL statements of the garbage collector
// tables in gcschema.sql.
func GCSchemaStatements() []string {
	return splitStatements(gcSchemaSQL)
}

func schemaStatements(gc bool) []string {
	stmts := SchemaStatements()
	if gc {
		stmts = append(stmts, GCSchemaStatements()...)
	}
	return stmts
}

// EnableGC makes header and object reads return the condemn marker of the
// garbage collector. Without it the reads are those of a server without GC.
func (c *CASDB) EnableGC() {
	c.gc = true
}

func (c *CASDB) headerFamilies() string {
	if c.gc {
		return gcHeaderFamily
	}
	return headerFamily
}

// CheckGCSchema checks that the garbage collector tables are readable, with
// the columns added after they were first created.
func (c *CASDB) CheckGCSchema(ctx context.Context) error {
	for _, q := range []struct{ table, columns string }{
		{"GcState", "ListLocation, ListSnapshotTime, ListComplete"},
		{"GcReplicas", "1"},
	} {
		err := c.dbClient.Single().Query(ctx, spanner.Statement{SQL: "SELECT " + q.columns + " FROM " + q.table + " LIMIT 1"}).Do(func(*spanner.Row) error { return nil })
		if err != nil {
			return fmt.Errorf("cannot read Spanner table %s: %w", q.table, err)
		}
	}
	return nil
}

// condemnedValue returns the newest g:condemned value of row, or nil when the
// row has none or GC is off.
func (c *CASDB) condemnedValue(row bigtable.Row) []byte {
	if !c.gc {
		return nil
	}
	for _, item := range row[condemnedFamily] {
		if item.Column == condemnedColumn {
			if item.Value == nil {
				return []byte{}
			}
			return item.Value
		}
	}
	return nil
}

// MapRowKey returns the key of the row of the table maps that records to as a
// result of from.
func MapRowKey(from, to entities.Hash) string {
	return fmt.Sprintf("%s#%s", from, to)
}

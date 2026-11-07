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

package gcp

import (
	"context"
	"fmt"
	"slices"

	"cloud.google.com/go/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// tableFamilies is the element type of bigtableTables.
type tableFamilies = struct {
	name     string
	families []string
}

// gcBigtableTables lists the Bigtable tables and column families that the
// garbage collector adds to bigtableTables: the family g of objects and the
// table gc.
var gcBigtableTables = []tableFamilies{
	{"objects", []string{gc.ObjectsFamily}},
	{gc.TableName, []string{gc.EdgesFamily, gc.TouchFamily}},
}

// serverTables returns the Bigtable tables and families of the server, with
// those of the garbage collector when gc is set.
func serverTables(withGC bool) []tableFamilies {
	tables := slices.Clone(bigtableTables)
	if !withGC {
		return tables
	}
	for _, extra := range gcBigtableTables {
		i := slices.IndexFunc(tables, func(t tableFamilies) bool { return t.name == extra.name })
		if i < 0 {
			tables = append(tables, extra)
			continue
		}
		tables[i].families = append(slices.Clone(tables[i].families), extra.families...)
	}
	return tables
}

// checkGCResources checks that the Spanner tables, the Bigtable table and the
// column family of the garbage collector exist.
func (s *GCPServer) checkGCResources(ctx context.Context) error {
	if err := s.casdb.CheckGCSchema(ctx); err != nil {
		return err
	}
	if _, err := s.casdb.BTClient().Open(gc.TableName).ReadRow(ctx, "abfs-setup-check"); err != nil {
		return fmt.Errorf("cannot read Bigtable table %s: %w", gc.TableName, err)
	}
	// Reads ignore an unknown family, a mutation does not. Deleting a cell of
	// a row that does not exist changes nothing.
	mut := bigtable.NewMutation()
	mut.DeleteCellsInColumn(gc.ObjectsFamily, gc.CondemnedColumn)
	if err := s.casdb.BTClient().Open("objects").Apply(ctx, "abfs-setup-check", mut); err != nil {
		return fmt.Errorf("cannot use column family %s of Bigtable table objects: %w", gc.ObjectsFamily, err)
	}
	return nil
}

// checkSingleRowTransactions checks that the Bigtable app profile allows the
// conditional writes with which reads rescue condemned objects. Without them
// every read of a condemned object would fail. The probe is a conditional
// delete of a cell of a row that does not exist, so it writes nothing. The
// client does not retry conditional writes, so the probe retries transient
// errors as a rescue does; only a refusal names the app profile.
func (s *GCPServer) checkSingleRowTransactions(ctx context.Context, opts *NewServerOpts) error {
	predicate := bigtable.ChainFilters(bigtable.FamilyFilter(gc.ObjectsFamily), bigtable.ColumnFilter(gc.CondemnedColumn))
	del := bigtable.NewMutation()
	del.DeleteCellsInColumn(gc.ObjectsFamily, gc.CondemnedColumn)
	mut := bigtable.NewCondMutation(predicate, del, nil)
	err := gc.Retry(ctx, func() error { return s.casdb.BTClient().Open("objects").Apply(ctx, "abfs-setup-check", mut) })
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.FailedPrecondition, codes.InvalidArgument:
		profile := opts.BtAppProfile
		if profile == "" {
			profile = "default"
		}
		return fmt.Errorf("the Bigtable app profile %s refused a conditional write: with --gc the server needs single-cluster routing with single-row transactions allowed; allow them with 'gcloud bigtable app-profiles update %s --instance=%s --route-to=<cluster> --transactional-writes', or start the server without --gc: %w", profile, profile, opts.BtInstance, err)
	}
	return fmt.Errorf("failed to check that a conditional write to Bigtable table objects works: %w", err)
}

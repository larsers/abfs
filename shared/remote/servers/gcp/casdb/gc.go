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
	"fmt"

	"cloud.google.com/go/bigtable"

	"github.com/google/abfs/shared/entities"
)

// The garbage collector condemns an object by setting the cell g:condemned of
// its objects row; package gc owns these names. Header reads include the
// family, so that the server can rescue a condemned object that is still used.
const (
	condemnedFamily = "g"
	condemnedColumn = condemnedFamily + ":condemned"
	headerFamilies  = "meta|" + condemnedFamily
)

// condemnedValue returns the newest g:condemned value of row, or nil when the
// row has none.
func condemnedValue(row bigtable.Row) []byte {
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

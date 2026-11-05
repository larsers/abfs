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
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/entities"
)

func putObjectRow(t *testing.T, client *bigtable.Client, key Hash, condemned []byte) {
	t.Helper()
	mut := bigtable.NewMutation()
	mut.Set("meta", "type", bigtable.Now(), []byte("blob"))
	if condemned != nil {
		mut.Set(ObjectsFamily, CondemnedColumn, bigtable.Now(), condemned)
	}
	require.NoError(t, client.Open(objectsTableName).Apply(context.Background(), key.String(), mut))
}

func condemnedOf(t *testing.T, client *bigtable.Client, key Hash) []byte {
	t.Helper()
	r, err := client.Open(objectsTableName).ReadRow(context.Background(), key.String(), bigtable.RowFilter(bigtable.LatestNFilter(1)))
	require.NoError(t, err)
	for _, item := range r[ObjectsFamily] {
		if item.Column == ObjectsFamily+":"+CondemnedColumn {
			return item.Value
		}
	}
	return nil
}

func TestRescuer_Rescue(t *testing.T) {
	cycle := func(n int64) []byte { return versionBytes(n) }
	tests := []struct {
		name      string
		stored    []byte // nil: not condemned
		deleted   bool
		read      []byte
		wantLive  bool
		wantTouch bool
	}{
		{name: "condemned", stored: cycle(7), read: cycle(7), wantLive: true, wantTouch: true},
		{name: "condemned again after the read", stored: cycle(8), read: cycle(7), wantLive: true, wantTouch: true},
		{name: "rescued by another server", read: cycle(7), wantLive: true, wantTouch: true},
		{name: "deleted after the read", deleted: true, read: cycle(7)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			client := newHooksTestBigtable(t)
			key := entities.NewHash("5555555555555555555555555555555555555555")
			if !tt.deleted {
				putObjectRow(t, client, key, tt.stored)
			}
			r := NewRescuer(client)

			live, err := r.Rescue(context.Background(), key, tt.read)
			require.NoError(err)
			require.Equal(tt.wantLive, live)
			require.Nil(condemnedOf(t, client, key))
			_, touched := cellTime(t, client, TableName, key.String(), TouchFamily, TouchColumn)
			require.Equal(tt.wantTouch, touched)
			if tt.deleted {
				r, err := client.Open(objectsTableName).ReadRow(context.Background(), key.String())
				require.NoError(err)
				require.Empty(r, "the rescue created the row again")
			}
		})
	}
}

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
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		// The touch comes before the row is found gone, so it stays as a gc
		// row with only the touch.
		{name: "deleted after the read", deleted: true, read: cycle(7), wantTouch: true},
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

// A rescue whose touch fails fails and leaves the marker, so that a marker
// set again by a late condemn write cannot meet a used object without a
// touch. The retry rescues it.
func TestRescuer_FailedTouchKeepsTheMarker(t *testing.T) {
	require := require.New(t)
	client := newHooksTestBigtable(t)
	key := entities.NewHash("7777777777777777777777777777777777777777")
	putObjectRow(t, client, key, versionBytes(7))
	r := NewRescuer(client)
	r.gcTable = client.Open("no-such-table")

	_, err := r.Rescue(context.Background(), key, versionBytes(7))
	require.ErrorContains(err, "failed to touch")
	require.Equal(versionBytes(7), condemnedOf(t, client, key))
	_, touched := cellTime(t, client, TableName, key.String(), TouchFamily, TouchColumn)
	require.False(touched)

	live, err := NewRescuer(client).Rescue(context.Background(), key, versionBytes(7))
	require.NoError(err)
	require.True(live)
	require.Nil(condemnedOf(t, client, key))
	_, touched = cellTime(t, client, TableName, key.String(), TouchFamily, TouchColumn)
	require.True(touched)
}

// Two servers rescue the same object while the collector keeps rewriting
// its marker. Each rescue either clears the marker it read or reports that
// the marker keeps changing; none reports the object deleted.
func TestRescuer_ConcurrentRescuesAndChangingMarker(t *testing.T) {
	require := require.New(t)
	client := newHooksTestBigtable(t)
	key := entities.NewHash("6666666666666666666666666666666666666666")
	putObjectRow(t, client, key, versionBytes(1))
	r := NewRescuer(client)

	stop := make(chan struct{})
	errs := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		objects := client.Open(objectsTableName)
		for cycle := int64(2); ; cycle++ {
			select {
			case <-stop:
				return
			default:
			}
			mut := bigtable.NewMutation()
			mut.Set(ObjectsFamily, CondemnedColumn, bigtable.Now(), versionBytes(cycle))
			if err := objects.Apply(context.Background(), key.String(), mut); err != nil {
				errs <- err
				return
			}
		}
	}()
	var rescues, changing atomic.Int64
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				live, err := r.Rescue(context.Background(), key, versionBytes(1))
				switch {
				case err != nil && strings.Contains(err.Error(), "keeps changing"):
					changing.Add(1)
				case err != nil:
					errs <- err
					return
				case !live:
					errs <- errors.New("rescue reported the object deleted")
					return
				default:
					rescues.Add(1)
				}
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(err)
	}
	require.Positive(rescues.Load() + changing.Load())

	// Once the marker stays, a rescue clears it.
	live, err := r.Rescue(context.Background(), key, condemnedOf(t, client, key))
	require.NoError(err)
	require.True(live)
	require.Nil(condemnedOf(t, client, key))
}

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

package maps

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
)

type testObject struct {
	content []byte
	hash    Hash
}

func (o *testObject) GetHash() Hash      { return o.hash }
func (o *testObject) GetEncoded() []byte { return o.content }
func (o *testObject) Encode() error      { o.hash = objects.HashBlobBytes(o.content); return nil }
func (o *testObject) Decode(h Hash, b []byte) error {
	o.hash, o.content = h, b
	return nil
}

type fakeMapStore struct {
	entities.CASStore
	results   []Hash
	refreshed []Hash
	objects   map[Hash][]byte
	getErr    error
	refreshes int
	puts      []Hash
}

func (f *fakeMapStore) MapResults(ctx context.Context, id Hash) ([]Hash, error) {
	if f.refreshes > 0 {
		return f.refreshed, nil
	}
	return f.results, nil
}

func (f *fakeMapStore) RefreshMapResults(ctx context.Context, id Hash) error {
	f.refreshes++
	return nil
}

func (f *fakeMapStore) GetObject(ctx context.Context, key Hash) ([]byte, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	b, ok := f.objects[key]
	if !ok {
		return nil, entities.NotExistErrf("object %s not found", key)
	}
	return b, nil
}

func (f *fakeMapStore) PutObject(ctx context.Context, key Hash, content []byte) error {
	f.puts = append(f.puts, key)
	return nil
}

func (f *fakeMapStore) AddMapResult(ctx context.Context, from, to Hash) error {
	return nil
}

func TestMapExisting_MissingResult(t *testing.T) {
	gone := objects.HashBlobBytes([]byte("gone"))
	gone2 := objects.HashBlobBytes([]byte("gone2"))
	out := &testObject{content: []byte("out")}
	require.NoError(t, out.Encode())

	tests := []struct {
		name         string
		results      []Hash
		refreshed    []Hash
		wantOut      bool
		wantRefresh  int
		wantMissing  int
		wantPutInput bool
	}{
		{name: "next result", results: []Hash{gone, out.hash}, wantOut: true, wantMissing: 1},
		{name: "after refresh", results: []Hash{gone}, refreshed: []Hash{gone, out.hash}, wantOut: true, wantRefresh: 1, wantMissing: 1},
		{name: "all missing", results: []Hash{gone}, refreshed: []Hash{gone, gone2}, wantRefresh: 1, wantMissing: 2, wantPutInput: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			st := &fakeMapStore{
				results:   tt.results,
				refreshed: tt.refreshed,
				objects:   map[Hash][]byte{out.hash: out.content},
			}
			in := &testObject{content: []byte("in")}
			req := &MapRequest{
				Input: in,
				Store: st,
				MapFunc: func(MapObject) (MapObject, error) {
					return &testObject{content: []byte("new")}, nil
				},
				OutputDecode: func(h Hash, b []byte) (MapObject, error) {
					o := new(testObject)
					return o, o.Decode(h, b)
				},
			}
			got, err := Map(context.Background(), req)
			require.NoError(err)
			require.Equal(tt.wantOut, req.CacheHit)
			if tt.wantOut {
				require.Equal(out.hash, got.GetHash())
			}
			require.Equal(tt.wantRefresh, st.refreshes)
			require.Equal(tt.wantMissing, req.MissingCount)
			require.Equal(tt.wantPutInput, len(st.puts) > 0 && st.puts[0] == in.hash)
		})
	}
}

func TestMapExisting_FetchError(t *testing.T) {
	require := require.New(t)

	fetchErr := errors.New("unavailable")
	st := &fakeMapStore{results: []Hash{objects.HashBlobBytes([]byte("x"))}, getErr: fetchErr}
	req := &MapRequest{Input: &testObject{content: []byte("in")}, Store: st}
	inHash, err := MapInput(context.Background(), req)
	require.NoError(err)
	_, err = MapExisting(context.Background(), inHash, req)
	require.ErrorIs(err, fetchErr)
	require.Zero(req.MissingCount)
}

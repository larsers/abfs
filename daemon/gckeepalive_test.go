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

package daemon

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/filesystem"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/objects"
)

type fakeGCRemote struct {
	refs       [][]entities.RefHash
	refErr     error
	batches    [][]entities.Hash
	known      map[entities.Hash]bool
	headersErr error
}

func (f *fakeGCRemote) PutRefs(server string, project string, refs []entities.RefHash, blocking bool) error {
	if project != entities.ABFSMetaProject || !blocking {
		return errors.New("unexpected ref write")
	}
	f.refs = append(f.refs, refs)
	return f.refErr
}

func (f *fakeGCRemote) RemoteHeaders(keys []entities.Hash) (int, error) {
	if f.headersErr != nil {
		return 0, f.headersErr
	}
	f.batches = append(f.batches, keys)
	found := 0
	for _, k := range keys {
		if f.known[k] {
			found++
		}
	}
	return found, nil
}

func testHashes(n int) []entities.Hash {
	hashes := make([]entities.Hash, n)
	for i := range hashes {
		hashes[i] = objects.HashBlobBytes([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
	}
	return hashes
}

func TestGCKeepAlive_Step(t *testing.T) {
	require := require.New(t)

	target := objects.HashBlobBytes([]byte("details"))
	hashes := testHashes(gcTouchBatchSize + 1)
	remote := &fakeGCRemote{known: map[entities.Hash]bool{hashes[0]: true, hashes[gcTouchBatchSize]: true}}
	scans := 0
	k := &GCKeepAlive{
		Remote:        remote,
		Server:        "host",
		Ref:           "refs/abfs/mount/user/u/1",
		Target:        target,
		RefInterval:   15 * time.Minute,
		TouchInterval: 18 * time.Hour,
		Scan: func(ctx context.Context) ([]entities.Hash, error) {
			scans++
			return hashes, nil
		},
	}
	require.Equal(15*time.Minute, k.tick())

	start := time.Now()
	k.lastRef, k.lastTouch = start, start

	k.step(context.Background(), start.Add(14*time.Minute))
	require.Empty(remote.refs)

	k.step(context.Background(), start.Add(15*time.Minute))
	require.Equal([][]entities.RefHash{{{Ref: k.Ref, Hash: target}}}, remote.refs)
	require.Zero(scans)

	// A failed ref write does not stop the touch.
	remote.refErr = errors.New("unavailable")
	k.step(context.Background(), start.Add(18*time.Hour))
	require.Len(remote.refs, 2)
	require.Equal(1, scans)
	require.Len(remote.batches, 2)
	require.Len(remote.batches[0], gcTouchBatchSize)
	require.Len(remote.batches[1], 1)

	// The next touch waits a full interval, also after a failure.
	remote.headersErr = errors.New("not found")
	k.step(context.Background(), start.Add(18*time.Hour+15*time.Minute))
	require.Equal(1, scans)
	k.step(context.Background(), start.Add(36*time.Hour))
	require.Equal(2, scans)
	require.Len(remote.batches, 2)
}

func jitterKeepAlive(remote *fakeGCRemote, r func() float64, scans *int) *GCKeepAlive {
	return &GCKeepAlive{
		Remote:        remote,
		Server:        "host",
		Ref:           "refs/abfs/mount/user/u/1",
		Target:        objects.HashBlobBytes([]byte("details")),
		RefInterval:   15 * time.Minute,
		TouchInterval: 18 * time.Hour,
		Rand:          r,
		Scan: func(ctx context.Context) ([]entities.Hash, error) {
			if scans != nil {
				*scans++
			}
			return nil, nil
		},
	}
}

func TestGCKeepAlive_FirstWait(t *testing.T) {
	require := require.New(t)

	for _, r := range []float64{0, 0.25, 0.5, 0.999999999999} {
		k := jitterKeepAlive(&fakeGCRemote{}, func() float64 { return r }, nil)
		start := time.Now()
		k.start(start)
		for _, w := range []struct{ wait, d time.Duration }{{k.refWait, k.RefInterval}, {k.touchWait, k.TouchInterval}} {
			require.Positive(w.wait, "r=%v", r)
			require.LessOrEqual(w.wait, w.d, "r=%v", r)
		}
		require.Equal(min(k.refWait, gcKeepAliveMaxTick), k.untilNext(start))
	}

	rng := rand.New(rand.NewPCG(1, 2))
	var lo, hi time.Duration
	for i := range 1000 {
		k := jitterKeepAlive(&fakeGCRemote{}, rng.Float64, nil)
		k.start(time.Now())
		require.Positive(k.refWait)
		require.LessOrEqual(k.refWait, k.RefInterval)
		require.Positive(k.touchWait)
		require.LessOrEqual(k.touchWait, k.TouchInterval)
		if i == 0 || k.touchWait < lo {
			lo = k.touchWait
		}
		hi = max(hi, k.touchWait)
	}
	// The first touches spread over the whole interval.
	require.Less(lo, 2*time.Hour)
	require.Greater(hi, 16*time.Hour)
}

func TestGCKeepAlive_LaterWaits(t *testing.T) {
	require := require.New(t)

	remote := &fakeGCRemote{}
	scans := 0
	k := jitterKeepAlive(remote, rand.New(rand.NewPCG(3, 4)).Float64, &scans)
	now := time.Now()
	k.start(now)
	within := func(wait, d time.Duration) {
		spread := time.Duration(gcKeepAliveSpread * float64(d))
		require.GreaterOrEqual(wait, d-spread)
		require.LessOrEqual(wait, d+spread)
	}

	refs, touches := 0, 0
	for range 2000 {
		wait := k.untilNext(now)
		require.Positive(wait)
		require.LessOrEqual(wait, gcKeepAliveMaxTick)

		// Nothing is due just before the next wait ends.
		k.step(context.Background(), now.Add(wait-time.Nanosecond))
		require.Len(remote.refs, refs)
		require.Equal(touches, scans)

		now = now.Add(wait)
		lastRef, lastTouch := k.lastRef, k.lastTouch
		k.step(context.Background(), now)
		if k.lastRef != lastRef {
			refs++
			within(k.refWait, k.RefInterval)
		}
		if k.lastTouch != lastTouch {
			touches++
			within(k.touchWait, k.TouchInterval)
		}
		require.Len(remote.refs, refs)
		require.Equal(touches, scans)
	}
	require.Greater(refs, 1000)
	require.Greater(touches, 10)
}

func TestGCKeepAlive_NoRand(t *testing.T) {
	require := require.New(t)

	k := jitterKeepAlive(&fakeGCRemote{}, nil, nil)
	start := time.Now()
	k.start(start)
	require.Equal(15*time.Minute, k.refWait)
	require.Equal(18*time.Hour, k.touchWait)
	require.Equal(15*time.Minute, k.nextWait(k.RefInterval))
}

func TestGCKeepAlive_Touch(t *testing.T) {
	require := require.New(t)

	hashes := testHashes(3)
	remote := &fakeGCRemote{known: map[entities.Hash]bool{hashes[1]: true}}
	scanErr := errors.New("permission denied")
	k := &GCKeepAlive{
		Remote: remote,
		Scan: func(ctx context.Context) ([]entities.Hash, error) {
			return hashes, scanErr
		},
	}
	found, total, err := k.touch(context.Background())
	require.ErrorIs(err, scanErr)
	require.Equal(1, found)
	require.Equal(3, total)
	require.Len(remote.batches, 1)
}

func TestNewGCKeepAlive(t *testing.T) {
	config := instance.DefaultConfig(t.TempDir())
	config.GCKeepAlive = true
	details := &binding.MountDetails{
		StartTimestamp: 42,
		MetaRefPrefix:  "user/u",
		Config:         config,
	}
	details.Hash = objects.HashBlobBytes([]byte("details"))

	zeroConfig := config.Clone()
	zeroConfig.GCKeepAliveRefInterval = 0
	zeroConfig.GCKeepAliveTouchInterval = 0
	zeroDetails := *details
	zeroDetails.Config = zeroConfig

	// The default config has the intervals set and the keep-alive off.
	offDetails := *details
	offDetails.Config = instance.DefaultConfig(t.TempDir())

	tests := []struct {
		name      string
		info      filesystem.MountInfo
		details   *binding.MountDetails
		wantNil   bool
		wantRef   string
		wantTick  time.Duration
		wantTouch time.Duration
	}{
		{name: "read-write", details: details, wantRef: "refs/abfs/mount/user/u/42", wantTick: 15 * time.Minute, wantTouch: 18 * time.Hour},
		{name: "remote read-only", info: filesystem.MountInfo{RemoteReadOnly: true}, details: details, wantTick: 15 * time.Minute, wantTouch: 18 * time.Hour},
		{name: "local only", info: filesystem.MountInfo{LocalOnly: true}, details: details, wantNil: true},
		{name: "disabled", details: &zeroDetails, wantNil: true},
		{name: "off by default", details: &offDetails, wantNil: true},
		{name: "no config", details: &binding.MountDetails{}, wantNil: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			k := NewGCKeepAlive(tt.info, tt.details, "host", &fakeGCRemote{}, t.TempDir())
			if tt.wantNil {
				require.Nil(k)
				return
			}
			require.NotNil(k)
			require.Equal(tt.wantRef, k.Ref)
			require.Equal(tt.wantTick, k.tick())
			require.Equal(tt.wantTouch, k.TouchInterval)
			if tt.wantRef != "" {
				require.Equal(details.Hash, k.Target)
			}
		})
	}
}

type fakeMountAttrs struct {
	unloaded map[string]bool
	hashes   map[string]entities.Hash
}

func (f *fakeMountAttrs) Loaded(path string) (bool, error) {
	return !f.unloaded[path], nil
}

func (f *fakeMountAttrs) Hash(path string) (entities.Hash, error) {
	h, ok := f.hashes[path]
	if !ok {
		return entities.Hash{}, os.ErrNotExist
	}
	return h, nil
}

func TestScanMountHashes(t *testing.T) {
	require := require.New(t)

	root := t.TempDir()
	for _, d := range []string{"loaded/inner", "unloaded/inner", ".git/objects", "dup"} {
		require.NoError(os.MkdirAll(filepath.Join(root, d), 0755))
	}
	for _, f := range []string{"file", "loaded/inner/file", "loaded/cold", "unloaded/inner/file", ".git/objects/x", "dup/a", "dup/b", "gone"} {
		require.NoError(os.WriteFile(filepath.Join(root, f), nil, 0644))
	}

	h := testHashes(6)
	p := func(rel string) string { return filepath.Join(root, rel) }
	attrs := &fakeMountAttrs{
		unloaded: map[string]bool{p("unloaded"): true, p("loaded/cold"): true},
		hashes: map[string]entities.Hash{
			root:                     h[0],
			p("loaded"):              h[0],
			p("loaded/inner"):        h[0],
			p("file"):                h[1],
			p("loaded/inner/file"):   h[2],
			p("loaded/cold"):         h[3],
			p("unloaded"):            h[4],
			p("unloaded/inner/file"): h[0],
			p(".git/objects/x"):      h[0],
			p("dup"):                 h[0],
			p("dup/a"):               h[5],
			p("dup/b"):               h[5],
		},
	}

	got, err := scanMountHashes(context.Background(), root, attrs)
	require.NoError(err)
	require.ElementsMatch(h[1:], got)
}

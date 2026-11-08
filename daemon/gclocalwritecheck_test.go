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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/cacheman/protocol"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/objects"
)

type fakeFeatures struct {
	features []string
	err      error
	calls    int
}

func (f *fakeFeatures) Features() ([]string, error) {
	f.calls++
	return f.features, f.err
}

type fakePutter struct {
	objects map[entities.Hash][]byte
}

func (p *fakePutter) PutObject(ctx context.Context, key entities.Hash, content []byte) error {
	p.objects[key] = content
	return nil
}

func TestGCLocalWriteCheck(t *testing.T) {
	require := require.New(t)

	check := []string{protocol.FeatureGCLocalWriteCheck}
	tests := []struct {
		name        string
		gcKeepAlive bool
		cacheman    *fakeFeatures
		want        bool
		wantCalls   int
	}{
		// A mount without gc-keepalive sends cacheman nothing extra.
		{"keep-alive off", false, &fakeFeatures{features: check}, false, 0},
		{"cacheman checks", true, &fakeFeatures{features: check}, true, 1},
		// An older cacheman, or one whose own config has gc-keepalive off.
		{"cacheman without the check", true, &fakeFeatures{}, false, 1},
		{"cacheman unreachable", true, &fakeFeatures{err: errors.New("no cacheman")}, false, 1},
	}
	for _, tt := range tests {
		config := &instance.Config{GCKeepAlive: tt.gcKeepAlive}
		require.Equal(tt.want, GCLocalWriteCheck(config, tt.cacheman), tt.name)
		require.Equal(tt.wantCalls, tt.cacheman.calls, tt.name)
	}
}

// A cacheman that stops checking local writes under a live mount turns the mount's field false
// for good, and the mount ref points at the new details at once.
func TestGCKeepAlive_LocalWriteCheckLost(t *testing.T) {
	require := require.New(t)

	details := &binding.MountDetails{
		Version:           binding.CurrentMountDetailsVersion,
		MetaRefPrefix:     "user/test",
		StartTimestamp:    1,
		Config:            &instance.Config{GCKeepAlive: true},
		GCLocalWriteCheck: true,
	}
	require.NoError(details.Encode())
	remote := &fakeGCRemote{}
	cacheman := &fakeFeatures{features: []string{protocol.FeatureGCLocalWriteCheck}}
	store := &fakePutter{objects: make(map[entities.Hash][]byte)}
	k := &GCKeepAlive{
		Remote:        remote,
		Ref:           MountRef(details),
		Target:        details.Hash,
		RefInterval:   time.Hour,
		TouchInterval: time.Hour,
		Scan:          hashesScan(nil, nil, new(int)),
	}
	k.WatchLocalWriteCheck(details, cacheman.Features, store)

	now := time.Now()
	k.start(now)
	k.step(context.Background(), now.Add(time.Minute))
	require.Empty(store.objects)
	require.Empty(remote.refs)

	cacheman.err = errors.New("cacheman restarting")
	k.step(context.Background(), now.Add(2*time.Minute))
	require.Empty(store.objects)

	cacheman.err = nil
	cacheman.features = nil
	k.step(context.Background(), now.Add(3*time.Minute))
	require.Len(store.objects, 1)
	require.NotEqual(details.Hash, k.Target)
	require.Equal([][]entities.RefHash{{{Ref: MountRef(details), Hash: k.Target}}}, remote.refs)
	var written binding.MountDetails
	require.NoError(written.Decode(k.Target, store.objects[k.Target]))
	require.False(written.GCLocalWriteCheck)
	require.Equal(details.MetaRefPrefix, written.MetaRefPrefix)
	require.True(details.GCLocalWriteCheck)

	// The check comes back, but writes in the gap had none.
	cacheman.features = []string{protocol.FeatureGCLocalWriteCheck}
	calls := cacheman.calls
	k.step(context.Background(), now.Add(4*time.Minute))
	require.Equal(calls, cacheman.calls)
	require.Len(store.objects, 1)
}

func TestGCKeepAlive_LocalWriteCheckNotWatched(t *testing.T) {
	require := require.New(t)

	target := objects.HashBlobBytes([]byte("details"))
	for _, details := range []*binding.MountDetails{
		nil,
		{GCLocalWriteCheck: false},
	} {
		k := &GCKeepAlive{Ref: "refs/abfs/mount/x/1", Target: target, RefInterval: time.Hour}
		k.WatchLocalWriteCheck(details, (&fakeFeatures{}).Features, &fakePutter{})
		require.Nil(k.localWrites)
	}
	// Remote read-only mounts have no mount ref to point elsewhere.
	k := &GCKeepAlive{TouchInterval: time.Hour}
	k.WatchLocalWriteCheck(&binding.MountDetails{GCLocalWriteCheck: true}, (&fakeFeatures{}).Features, &fakePutter{})
	require.Nil(k.localWrites)
}

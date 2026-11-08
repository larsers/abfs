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
	"fmt"
	"log"
	"slices"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/cacheman/protocol"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
)

// CachemanFeatures is the part of the cacheman client that reads cacheman's features.
type CachemanFeatures interface {
	Features() ([]string, error)
}

// GCLocalWriteCheck reports whether the cacheman of a mount with config keeps locally
// written content until the server confirms it, for the mount details. It asks cacheman only
// when config has gc-keepalive on. Without the check the mount still starts: the server GC
// refuses deletes for mount details without it.
func GCLocalWriteCheck(config *instance.Config, cacheman CachemanFeatures) bool {
	if config == nil || !config.GCKeepAlive || config.LocalOnly {
		return false
	}
	features, err := cacheman.Features()
	if err != nil {
		log.Printf("Warning: cannot read the cacheman features, the mount details record no local write check: %v", err)
		return false
	}
	if !slices.Contains(features, protocol.FeatureGCLocalWriteCheck) {
		log.Printf("Warning: gc-keepalive is on, but cacheman does not keep local writes until the server confirms them; restart cacheman with gc-keepalive on")
		return false
	}
	return true
}

// ObjectPutter is the part of a store that writes mount details.
type ObjectPutter interface {
	PutObject(ctx context.Context, key entities.Hash, content []byte) error
}

type localWriteWatch struct {
	details  *binding.MountDetails
	features func() ([]string, error)
	store    ObjectPutter
}

// WatchLocalWriteCheck makes the keep-alive read the cacheman features on every tick. When
// the local write check is gone, for example after a cacheman restart with gc-keepalive off,
// it writes new mount details with GCLocalWriteCheck false and points the mount ref at them.
// The field stays false for the rest of the mount, since writes in the gap had no check.
func (k *GCKeepAlive) WatchLocalWriteCheck(details *binding.MountDetails, features func() ([]string, error), store ObjectPutter) {
	if details == nil || !details.GCLocalWriteCheck || k.Ref == "" || k.Target.IsZero() {
		return
	}
	k.localWrites = &localWriteWatch{details: details, features: features, store: store}
}

func (k *GCKeepAlive) checkLocalWrites(ctx context.Context) {
	w := k.localWrites
	if w == nil {
		return
	}
	features, err := w.features()
	if err != nil {
		log.Printf("Warning: cannot read the cacheman features: %v", err)
		return
	}
	if slices.Contains(features, protocol.FeatureGCLocalWriteCheck) {
		return
	}
	d := *w.details
	d.GCLocalWriteCheck = false
	if err := putMountDetailsObject(ctx, w.store, &d); err != nil {
		log.Printf("Failed to record that cacheman no longer checks local writes: %v", err)
		return
	}
	k.localWrites = nil
	k.Target = d.Hash
	log.Printf("Cacheman no longer keeps local writes until the server confirms them; mount details %s record it", d.Hash)
	if err := k.refreshRef(); err != nil {
		log.Printf("Failed to refresh mount ref %s: %v", k.Ref, err)
	}
}

func putMountDetailsObject(ctx context.Context, store ObjectPutter, d *binding.MountDetails) error {
	if err := d.Encode(); err != nil {
		return fmt.Errorf("failed to encode MountDetails: %w", err)
	}
	if err := store.PutObject(ctx, d.Hash, d.Encoded); err != nil {
		return fmt.Errorf("failed to put MountDetails with hash %s: %w", d.Hash, err)
	}
	return nil
}

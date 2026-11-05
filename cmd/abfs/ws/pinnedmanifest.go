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

package ws

import (
	"context"
	"fmt"
	"os"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/store"
)

type manifestHashesSetter interface {
	SetManifestHashes(manifest, pinned entities.Hash)
}

// setPinnedManifest gives the mount the hash of the pinned manifest that this
// command writes to .repo/pinned.xml, so the mount details link it. A manifest
// file is stored first, because the mount details are pushed before the file
// is written into the mount. A mount from a branch or a workspace snapshot has
// no pinned manifest.
func (c *mountRepoCmd) setPinnedManifest(ctx context.Context, m manifestHashesSetter) error {
	if c.IsSnapshot || c.Store == nil {
		return nil
	}
	h := c.ManifestHash
	if h.IsZero() && c.ManifestFile != "" {
		content, err := os.ReadFile(c.ManifestFile)
		if err != nil {
			return fmt.Errorf("failed to read manifest %s: %w", c.ManifestFile, err)
		}
		if h, _, err = store.StoreBlob(ctx, c.Store, content); err != nil {
			return fmt.Errorf("failed to store manifest %s: %w", c.ManifestFile, err)
		}
	}
	if !h.IsZero() {
		m.SetManifestHashes(h, h)
	}
	return nil
}

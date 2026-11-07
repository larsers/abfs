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
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/casgit"
	"github.com/google/abfs/repo"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/store"
)

type manifestHashesSetter interface {
	SetManifestHashes(manifest, pinned entities.Hash)
}

// setPinnedManifest gives the mount the hash of its pinned manifest, so the
// mount details link it, when gc-keepalive is on. A manifest file is stored
// first, because the mount details are pushed before the file is written into
// the mount. A branch is pinned as the mount resolves it, and a workspace
// snapshot passes on the pin of the mount it was taken from. Neither of these
// two fails the mount: without a pin the mount is kept only by its branch.
func (c *mountRepoCmd) setPinnedManifest(ctx context.Context, m manifestHashesSetter) error {
	if c.Store == nil || c.ConfigProvider == nil || c.Config == nil || !c.Config.GCKeepAlive {
		return nil
	}
	if c.IsSnapshot {
		if err := carrySnapshotPin(ctx, c.Store, c.ManifestHash, m); err != nil {
			log.Printf("Warning: the mount of snapshot %s has no pinned manifest for the server GC: %v", c.ManifestHash, err)
		}
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
		return nil
	}
	// Smart sync and smart tag check out the revisions of the manifest server, not the branch.
	if c.Branch == "" || c.SmartSync || c.SmartTag != "" {
		return nil
	}
	if err := c.pinBranch(ctx, m); err != nil {
		log.Printf("Warning: the mount of branch %s has no pinned manifest for the server GC: %v", c.Branch, err)
	}
	return nil
}

// pinBranch pins the manifest of the branch to the commits it names now and
// stores the pinned manifest, so the mount keeps them after the branch moves.
// Every project has to resolve, whatever the groups.
func (c *mountRepoCmd) pinBranch(ctx context.Context, m manifestHashesSetter) error {
	r := repo.New(c.Store, casgit.New(c.Store), c.Config.ManifestServer, c.Config.ManifestProjectName, c.Config.ManifestScheme, "", false)
	if err := r.ManifestFromBranch(ctx, c.Branch, c.Config.ManifestProjectName, "default.xml"); err != nil {
		return err
	}
	content, err := xml.MarshalIndent(r.PinnedManifest(), "", " ")
	if err != nil {
		return fmt.Errorf("failed to encode the pinned manifest: %w", err)
	}
	h, _, err := store.StoreBlob(ctx, c.Store, content)
	if err != nil {
		return fmt.Errorf("failed to store the pinned manifest: %w", err)
	}
	m.SetManifestHashes(h, h)
	return nil
}

// setSnapshotPin passes the pin of the workspace's last snapshot on to the
// mount, when gc-keepalive is on. It never fails the mount.
func (c *mountCmd) setSnapshotPin(ctx context.Context, m manifestHashesSetter) {
	if c.workspaces == nil || c.ConfigProvider == nil || c.Config == nil || !c.Config.GCKeepAlive {
		return
	}
	h, err := c.workspaces.SnapshotHash(c.WSName)
	if err == nil {
		err = carrySnapshotPin(ctx, c.workspaces.Store, h, m)
	}
	if err != nil {
		log.Printf("Warning: the mount of workspace %s has no pinned manifest for the server GC: %v", c.WSName, err)
	}
}

// carrySnapshotPin gives the mount the manifest hashes of the mount a
// workspace snapshot was taken from.
func carrySnapshotPin(ctx context.Context, s entities.CASStore, snapshot entities.Hash, m manifestHashesSetter) error {
	ws, err := binding.FetchObject[binding.WSSnapshot](ctx, s, snapshot)
	if err != nil {
		return fmt.Errorf("failed to fetch workspace snapshot %s: %w", snapshot, err)
	}
	if ws.MountDetails.IsZero() {
		return errors.New("the snapshot names no mount details")
	}
	md, err := binding.FetchObject[binding.MountDetails](ctx, s, ws.MountDetails)
	if err != nil {
		return fmt.Errorf("failed to fetch mount details %s: %w", ws.MountDetails, err)
	}
	if md.PinnedManifestHash.IsZero() {
		return fmt.Errorf("mount details %s name no pinned manifest", ws.MountDetails)
	}
	m.SetManifestHashes(md.ManifestHash, md.PinnedManifestHash)
	return nil
}

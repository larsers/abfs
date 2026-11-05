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

package mount

import "github.com/google/abfs/shared/entities"

// SetManifestHashes records the manifest and the pinned manifest of the mount
// in its mount details, so the server GC keeps what the mount was made from.
// The blobs must be in the store before the mount starts, because the mount
// details link them.
func (c *casfsMountCmd) SetManifestHashes(manifest, pinned entities.Hash) {
	c.manifestHash = manifest
	c.pinnedManifestHash = pinned
}

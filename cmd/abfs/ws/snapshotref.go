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
	"fmt"
	"log"
	"time"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
)

// snapshotRefTimeout bounds how long ws snapshot waits for cacheman to
// confirm the snapshot ref. Cacheman writes the ref only after the snapshot
// and the content it links are on the server, which can take minutes.
const snapshotRefTimeout = 30 * time.Second

type refPutter interface {
	PutRefs(server string, project string, refs []entities.RefHash, blocking bool) error
}

// snapshotRef returns the ref in the ABFS meta project that keeps the
// workspace snapshot h of user for the server GC.
func snapshotRef(user string, h entities.Hash) string {
	return fmt.Sprintf("refs/abfs/snapshot/user/%s/%s", user, h)
}

// putSnapshotRef writes the snapshot ref of h when gc-keepalive is on. A
// snapshot hash is all the user keeps, and nothing else on the server names
// the snapshot, so without the ref the server GC removes it. Writing the ref
// again moves its update time, which keeps the snapshot for another retention
// period. A failed or unconfirmed write only logs a warning: the snapshot
// itself is stored, and a write still queued in cacheman can land later.
func putSnapshotRef(config *instance.Config, refs refPutter, user string, h entities.Hash) {
	if config == nil || !config.GCKeepAlive || config.LocalOnly {
		return
	}
	ref := snapshotRef(user, h)
	if err := refs.PutRefs(entities.ABFSMetaServer, entities.ABFSMetaProject, []entities.RefHash{{Ref: ref, Hash: h}}, true); err != nil {
		log.Printf("Warning: ref %s was not confirmed: %v; the server GC may remove snapshot %s unless the ref is written", ref, err, h)
	}
}

func (c *snapshotCmd) putRef(h entities.Hash) {
	putSnapshotRef(c.Config, c.refs, c.workspaces.User, h)
}

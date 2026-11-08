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

package gitpusher

import (
	"errors"
	"log"

	"github.com/google/abfs/shared/remote"
)

// forgetMissingDeepTrees makes s forget the deep trees it cached when the
// server rejected a deep tree for a missing link. The server may remove a deep
// tree that nothing references any more, and a cached one would then be linked
// again on every push. The server reports only the direct links of the rejected
// object, so deep trees nested in a missing one may be gone too; the whole
// cache is dropped and the next push stores every subtree again.
func (s *pusher) forgetMissingDeepTrees(err error) {
	var linkErr *remote.ErrObjectLinkValidation
	if !errors.As(err, &linkErr) {
		return
	}
	s.deepTreeMapLock.Lock()
	defer s.deepTreeMapLock.Unlock()
	if len(s.deepTreeMap) > 0 {
		log.Printf("Forgetting %d cached deep trees of %s after the server rejected a deep tree: %v", len(s.deepTreeMap), s, linkErr)
	}
	clear(s.deepTreeMap)
}

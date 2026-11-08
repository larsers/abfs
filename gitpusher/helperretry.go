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
	"maps"

	"github.com/google/abfs/shared/entities"
)

// helperCommits returns the commits whose deep tree and digest map this push
// saves: the new commits, and the commits of an earlier push whose helper
// objects failed while their ref still points at them. The pushed-refs file
// already names those, so without the retry they would get no helper objects
// until their ref moves.
func (s *pusher) helperCommits() map[entities.Hash]string {
	if len(s.helperRetry) == 0 {
		return s.newCommits
	}
	commits := maps.Clone(s.newCommits)
	if commits == nil {
		commits = make(map[entities.Hash]string)
	}
	for h, ref := range s.helperRetry {
		if s.newRefs[ref] == h {
			commits[h] = ref
		}
	}
	return commits
}

// retryHelperObjects makes the next push save the helper objects of every
// commit of this one again. A failed save stops the others, and saving a
// commit whose objects are already stored costs one map lookup each.
func (s *pusher) retryHelperObjects() {
	s.helperRetry = s.helperCommits()
}

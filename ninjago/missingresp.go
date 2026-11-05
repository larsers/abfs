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

package ninjago

import "github.com/google/abfs/shared/entities"

// skipMissingResp reports whether a failed fetch of an eval response is a cache
// miss: the response is gone, for example removed by the server GC while a map
// list fetched earlier still names it. The response counts as tried, so the
// refresh pass does not fetch it again.
func (c *ABFSBackend) skipMissingResp(eval *Eval, hash entities.Hash, err error) bool {
	if !entities.IsNotExist(err) {
		return false
	}
	c.log.Debugf("Skipping response %s for %s because it does not exist", hash, eval.req.Hash)
	eval.triedResps[hash] = struct{}{}
	return true
}

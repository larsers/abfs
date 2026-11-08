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
	"context"
	"errors"
	"log"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
)

// maxHelperRetries bounds the pushes that save the helper objects of a commit
// again, so that a commit whose save keeps failing costs a few rebuilds of its
// deep tree and digest map and no more.
const maxHelperRetries = 3

// helperRetry is a commit whose helper objects the server rejected.
type helperRetry struct {
	ref      string
	attempts int
}

// keepHelperRetries keeps the new commits of this push for a later save of
// their helper objects when the server rejected one for a missing link. The
// pushed-refs file already names those commits, so without the retry they
// would get no helper objects until their ref moves. The rejection also made
// the pusher forget its cached deep trees, so a retry stores them again.
// Other failures are not retried: the next push goes on without the helper
// objects, as before.
func (s *pusher) keepHelperRetries(err error) {
	var linkErr *remote.ErrObjectLinkValidation
	if !errors.As(err, &linkErr) {
		return
	}
	if s.helperRetry == nil {
		s.helperRetry = make(map[entities.Hash]helperRetry)
	}
	for h, ref := range s.newCommits {
		s.helperRetry[h] = helperRetry{ref: ref}
	}
}

// retryHelperObjects saves the helper objects of the kept commits whose ref
// still points at them, and drops the others. A failed save is logged and
// never fails the push; the commits stay for the next push until they reach
// maxHelperRetries.
func (s *pusher) retryHelperObjects(ctx context.Context) {
	commits := make(map[entities.Hash]string)
	for h, r := range s.helperRetry {
		if s.newRefs[r.ref] != h {
			delete(s.helperRetry, h)
			continue
		}
		commits[h] = r.ref
	}
	if len(commits) == 0 {
		return
	}
	err := s.saveTreeHelperObjects(ctx, commits)
	if err == nil {
		for h := range commits {
			delete(s.helperRetry, h)
		}
		return
	}
	var dropped int
	for h := range commits {
		r := s.helperRetry[h]
		if r.attempts++; r.attempts >= maxHelperRetries {
			delete(s.helperRetry, h)
			dropped++
			continue
		}
		s.helperRetry[h] = r
	}
	log.Printf("Warning: failed to save the helper objects of %d earlier commits of %s again, giving up on %d: %v", len(commits), s, dropped, err)
}

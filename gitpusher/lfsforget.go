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

import "log"

// lfsForgetter is a pusher that remembers the LFS objects it pushed.
type lfsForgetter interface {
	forgetPushedLFS()
}

// forgetLFSOnError makes p forget the LFS objects it pushed when its push
// failed, so the next push checks them against the server again instead of
// trusting a set that may no longer match what the server holds.
func forgetLFSOnError(p Pusher, err error) error {
	if err == nil {
		return nil
	}
	if f, ok := p.(lfsForgetter); ok {
		f.forgetPushedLFS()
	}
	return err
}

// forgetPushedLFS runs between pushes, on the goroutine that runs them, so it
// needs no lock.
func (s *pusher) forgetPushedLFS() {
	if len(s.pushedLFSRefs) > 0 {
		log.Printf("Forgetting %d pushed LFS objects of %s after a failed push", len(s.pushedLFSRefs), s)
	}
	clear(s.pushedLFSRefs)
}

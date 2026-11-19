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

package gc

// The outcomes of a cycle that ended without an error, in the order Outcome checks.
const (
	OutcomeOK             = "ok"
	OutcomeCondemnRefused = "condemn_refused"
	OutcomeDeleteRefused  = "delete_refused"
	OutcomeDeleteStopped  = "delete_stopped"
	OutcomeChunksSkipped  = "chunks_skipped"
)

// Outcome returns the first outcome the cycle had, or OutcomeOK, as for a dry run.
func (r *Report) Outcome() string {
	switch {
	case r.Condemn != nil && len(r.Condemn.Refused) > 0:
		return OutcomeCondemnRefused
	case r.Delete == nil:
		return OutcomeOK
	case len(r.Delete.Refused) > 0:
		return OutcomeDeleteRefused
	case r.Delete.Step != nil && r.Delete.Step.Error != "":
		return OutcomeDeleteStopped
	case r.Delete.Chunks != nil && (r.Delete.Chunks.ListFailed || r.Delete.Chunks.RecordsSkipped > 0 || r.Delete.Chunks.Unflagged > 0):
		return OutcomeChunksSkipped
	}
	return OutcomeOK
}

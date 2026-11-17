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

// The outcomes of a cycle that ended without an error, in the order of
// Report.Outcome.
const (
	OutcomeOK             = "ok"
	OutcomeCondemnRefused = "condemn_refused"
	OutcomeDeleteRefused  = "delete_refused"
	OutcomeDeleteStopped  = "delete_stopped"
	OutcomeChunksSkipped  = "chunks_skipped"
)

// Outcome returns the first outcome in this order that the cycle had: the
// cycle refused to condemn, refused to delete, a worker error stopped its
// delete step, or the chunk step deleted no chunks of a record that was due,
// could not list the records, or left a record without the flag a day past
// the undo window. Else it returns OutcomeOK, as for every dry run and for a
// list the cycle did not complete.
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

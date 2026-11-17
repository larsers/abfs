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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The outcome names the first cause an operator must act on, in the order
// of the exit codes, so a cycle with several causes reports the lowest. A
// dry run reports no cause, nor does a chunk step that skips while every
// record still waits for the undo window, as in the first week of delete
// mode. A record that the chunk step keeps failing on is a cause once it is
// a day past the undo window.
func TestReport_Outcome(t *testing.T) {
	skipped := &ChunkStepReport{Skipped: []string{"versioning"}, RecordsSkipped: 1}
	stopped := &DeleteStepReport{Error: "worker failed"}
	tests := []struct {
		name string
		r    Report
		want string
	}{
		{"dry run with a plan", Report{Phase: PhaseDryRun, Delete: &DeleteReport{Plan: &DeletePlanReport{}}}, OutcomeOK},
		{"dry run without list records", Report{Phase: PhaseDryRun, Delete: &DeleteReport{PlanError: "no AbfsGcLists"}}, OutcomeOK},
		{"condemn", Report{Phase: PhaseCondemn, Condemn: &CondemnReport{Condemned: 3}, Delete: &DeleteReport{Plan: &DeletePlanReport{}}}, OutcomeOK},
		{"condemn refused", Report{Phase: PhaseCondemn, Condemn: &CondemnReport{Refused: []string{"hosts"}}}, OutcomeCondemnRefused},
		{"delete", Report{Phase: PhaseDelete, Condemn: &CondemnReport{}, Delete: &DeleteReport{Step: &DeleteStepReport{Deleted: 2}, Chunks: &ChunkStepReport{}}}, OutcomeOK},
		{"delete, condemn refused", Report{Phase: PhaseDelete, Condemn: &CondemnReport{Refused: []string{"hosts"}}, Delete: &DeleteReport{Plan: &DeletePlanReport{}}}, OutcomeCondemnRefused},
		{"delete refused", Report{Phase: PhaseDelete, Condemn: &CondemnReport{}, Delete: &DeleteReport{Refused: []string{"fleet"}, Chunks: &ChunkStepReport{}}}, OutcomeDeleteRefused},
		{"delete step stopped", Report{Phase: PhaseDelete, Condemn: &CondemnReport{}, Delete: &DeleteReport{Step: stopped, Chunks: &ChunkStepReport{}}}, OutcomeDeleteStopped},
		{"chunks skipped", Report{Phase: PhaseDelete, Condemn: &CondemnReport{}, Delete: &DeleteReport{Step: &DeleteStepReport{}, Chunks: skipped}}, OutcomeChunksSkipped},
		{"records not listed", Report{Phase: PhaseDelete, Condemn: &CondemnReport{}, Delete: &DeleteReport{Step: &DeleteStepReport{}, Chunks: &ChunkStepReport{Skipped: []string{"list"}, ListFailed: true}}}, OutcomeChunksSkipped},
		{"records unflagged", Report{Phase: PhaseDelete, Condemn: &CondemnReport{}, Delete: &DeleteReport{Step: &DeleteStepReport{}, Chunks: &ChunkStepReport{Errors: 3, Unflagged: 1}}}, OutcomeChunksSkipped},
		{"chunks skipped, no record due", Report{Phase: PhaseDelete, Condemn: &CondemnReport{}, Delete: &DeleteReport{Step: &DeleteStepReport{}, Chunks: &ChunkStepReport{Skipped: []string{"versioning"}, RecordsWaiting: 4}}}, OutcomeOK},
		{"delete refused and chunks skipped", Report{Phase: PhaseDelete, Condemn: &CondemnReport{}, Delete: &DeleteReport{Refused: []string{"fleet"}, Chunks: skipped}}, OutcomeDeleteRefused},
		{"delete step stopped and chunks skipped", Report{Phase: PhaseDelete, Condemn: &CondemnReport{}, Delete: &DeleteReport{Step: stopped, Chunks: skipped}}, OutcomeDeleteStopped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.r.Outcome())
		})
	}
}

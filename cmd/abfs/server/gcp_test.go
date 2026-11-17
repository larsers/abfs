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

package server

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cmd"
	"github.com/google/abfs/shared/cmd/clierr"
	"github.com/google/abfs/shared/cmd/root"
	"github.com/google/abfs/shared/metrics"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

func TestGCPSubcommandsRunInTheCLI(t *testing.T) {
	var check func(cmds []cmd.ABFSCmd)
	check = func(cmds []cmd.ABFSCmd) {
		for _, sub := range cmds {
			require.Implements(t, (*cmd.InstanceResolver)(nil), sub, sub.Spec().Name)
			check(sub.Spec().Subcommands)
		}
	}
	check(NewCmd(root.NewRootCommand()).subcommands())
}

// gaugeBackend keeps the last value of every gauge, keyed by name and label
// values.
type gaugeBackend struct {
	values map[string]float64
}

func (b *gaugeBackend) NewCounter(name, help string, labels []string) metrics.Counter {
	return &gaugeValue{b: b, name: name}
}

func (b *gaugeBackend) NewGauge(name, help string, labels []string) metrics.Gauge {
	return &gaugeValue{b: b, name: name}
}

func (b *gaugeBackend) NewHistogram(name, help string, buckets []float64, labels []string) metrics.Histogram {
	return &gaugeValue{b: b, name: name}
}

func (b *gaugeBackend) Push(ctx context.Context) error  { return nil }
func (b *gaugeBackend) Close(ctx context.Context) error { return nil }

type gaugeValue struct {
	b    *gaugeBackend
	name string
}

func (g *gaugeValue) key(labelValues []string) string {
	return g.name + "{" + strings.Join(labelValues, ",") + "}"
}

func (g *gaugeValue) Inc(labelValues ...string)                  { g.Add(1, labelValues...) }
func (g *gaugeValue) Add(val float64, labelValues ...string)     { g.b.values[g.key(labelValues)] += val }
func (g *gaugeValue) Set(val float64, labelValues ...string)     { g.b.values[g.key(labelValues)] = val }
func (g *gaugeValue) Observe(val float64, labelValues ...string) { g.Set(val, labelValues...) }

// A cycle exits with the code of its first outcome, 1 when it failed, and
// an error line that names that outcome's cause, and sets its gauges once
// from the report: the end time under its result, and the counts of the
// steps its mode runs. The alerts read only these gauges.
func TestGCPGCRun_EndCycle(t *testing.T) {
	chunks := func(skipped, waiting int64) *gc.ChunkStepReport {
		return &gc.ChunkStepReport{Skipped: []string{"versioning on"}, RecordsSkipped: skipped, RecordsWaiting: waiting, ChunkRowsDeleted: 4, BucketObjectsDeleted: 5}
	}
	refused := &gc.ErrCondemnRefused{Reasons: []string{"2 hosts, expected 3"}}
	tests := []struct {
		name   string
		mode   string
		r      *gc.Report
		err    error
		code   int
		result string
		msg    string
		gauges map[string]float64
	}{
		{name: "dry run", mode: gc.PhaseDryRun, r: &gc.Report{Delete: &gc.DeleteReport{Plan: &gc.DeletePlanReport{}}}, result: "ok"},
		{name: "flags", err: errors.New("pass one of --dry-run, --condemn and --delete"), code: 1, result: "error"},
		{name: "condemn", mode: gc.PhaseCondemn, r: &gc.Report{Condemn: &gc.CondemnReport{Condemned: 7}}, result: "ok",
			gauges: map[string]float64{"abfs_gc_cycle_keys{condemn,condemned}": 7}},
		{name: "list not completed", mode: gc.PhaseCondemn, r: &gc.Report{Condemn: &gc.CondemnReport{Condemned: 7, NotCompleted: []string{"ttl"}}}, err: errors.New("does not complete its list"), code: 1, result: "error",
			gauges: map[string]float64{"abfs_gc_cycle_keys{condemn,condemned}": 7}},
		{name: "condemn refused", mode: gc.PhaseCondemn, r: &gc.Report{Condemn: &gc.CondemnReport{Refused: refused.Reasons}}, err: refused, code: 3, result: "condemn_refused", msg: "2 hosts, expected 3",
			gauges: map[string]float64{"abfs_gc_cycle_keys{condemn,condemned}": 0}},
		{name: "delete refused", mode: gc.PhaseDelete, r: &gc.Report{Condemn: &gc.CondemnReport{Condemned: 7}, Delete: &gc.DeleteReport{Refused: []string{"fleet"}, Chunks: chunks(1, 0)}}, code: 4, result: "delete_refused", msg: "refused to delete and only condemned: fleet",
			gauges: map[string]float64{"abfs_gc_cycle_keys{delete,condemned}": 7, "abfs_gc_cycle_keys{delete,deleted}": 0, "abfs_gc_cycle_chunks_deleted{bigtable}": 4, "abfs_gc_cycle_chunks_deleted{bucket}": 5}},
		{name: "delete step stopped", mode: gc.PhaseDelete, r: &gc.Report{Condemn: &gc.CondemnReport{}, Delete: &gc.DeleteReport{Step: &gc.DeleteStepReport{Deleted: 9, Error: "worker failed"}, Chunks: chunks(1, 0)}}, code: 5, result: "delete_stopped", msg: "stopped its delete step: worker failed",
			gauges: map[string]float64{"abfs_gc_cycle_keys{delete,condemned}": 0, "abfs_gc_cycle_keys{delete,deleted}": 9, "abfs_gc_cycle_chunks_deleted{bigtable}": 4, "abfs_gc_cycle_chunks_deleted{bucket}": 5}},
		{name: "chunks skipped", mode: gc.PhaseDelete, r: &gc.Report{Condemn: &gc.CondemnReport{}, Delete: &gc.DeleteReport{Step: &gc.DeleteStepReport{Deleted: 9}, Chunks: chunks(1, 0)}}, code: 6, result: "chunks_skipped", msg: "deleted no chunks of 1 delete records past the undo window: versioning on",
			gauges: map[string]float64{"abfs_gc_cycle_keys{delete,condemned}": 0, "abfs_gc_cycle_keys{delete,deleted}": 9, "abfs_gc_cycle_chunks_deleted{bigtable}": 4, "abfs_gc_cycle_chunks_deleted{bucket}": 5}},
		{name: "records not listed", mode: gc.PhaseDelete, r: &gc.Report{Condemn: &gc.CondemnReport{}, Delete: &gc.DeleteReport{Step: &gc.DeleteStepReport{}, Chunks: &gc.ChunkStepReport{Skipped: []string{"list failed"}, ListFailed: true}}}, code: 6, result: "chunks_skipped", msg: "could not list the delete records and deleted no chunks: list failed",
			gauges: map[string]float64{"abfs_gc_cycle_keys{delete,condemned}": 0, "abfs_gc_cycle_keys{delete,deleted}": 0, "abfs_gc_cycle_chunks_deleted{bigtable}": 0, "abfs_gc_cycle_chunks_deleted{bucket}": 0}},
		{name: "records unflagged", mode: gc.PhaseDelete, r: &gc.Report{Condemn: &gc.CondemnReport{}, Delete: &gc.DeleteReport{Step: &gc.DeleteStepReport{}, Chunks: &gc.ChunkStepReport{Unflagged: 2, Errors: 3, DeleteRefused: 1}}}, code: 6, result: "chunks_skipped", msg: "left 2 delete records without the chunks-done flag a day past the undo window: 0 unreadable records, 3 errors, 1 bucket deletes refused",
			gauges: map[string]float64{"abfs_gc_cycle_keys{delete,condemned}": 0, "abfs_gc_cycle_keys{delete,deleted}": 0, "abfs_gc_cycle_chunks_deleted{bigtable}": 0, "abfs_gc_cycle_chunks_deleted{bucket}": 0}},
		{name: "chunks skipped, no record due", mode: gc.PhaseDelete, r: &gc.Report{Condemn: &gc.CondemnReport{}, Delete: &gc.DeleteReport{Step: &gc.DeleteStepReport{Deleted: 9}, Chunks: chunks(0, 3)}}, result: "ok",
			gauges: map[string]float64{"abfs_gc_cycle_keys{delete,condemned}": 0, "abfs_gc_cycle_keys{delete,deleted}": 9, "abfs_gc_cycle_chunks_deleted{bigtable}": 4, "abfs_gc_cycle_chunks_deleted{bucket}": 5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			b := &gaugeBackend{values: make(map[string]float64)}
			start := time.Now().Unix()
			err := newCycleMetrics(metrics.WrapBackend(b)).endCycle(tt.mode, tt.r, tt.err)
			switch tt.code {
			case 0:
				require.NoError(err)
			case 1:
				require.Equal(tt.err, err)
			default:
				var cliErr *clierr.CLIError
				require.ErrorAs(err, &cliErr)
				require.Equal(tt.code, cliErr.ExitCode())
				require.Contains(cliErr.Error(), tt.msg)
			}
			end := "abfs_gc_cycle_end_timestamp_seconds{" + tt.mode + "," + tt.result + "}"
			require.GreaterOrEqual(b.values[end], float64(start))
			delete(b.values, end)
			if tt.gauges == nil {
				tt.gauges = map[string]float64{}
			}
			require.Equal(tt.gauges, b.values)
		})
	}
}

// A report that cannot be written fails the cycle whatever its outcome, so
// a refusal without its report exits 1.
func TestGCPGCRun_EndCycleWithoutReport(t *testing.T) {
	b := &gaugeBackend{values: make(map[string]float64)}
	refused := &gc.ErrCondemnRefused{Reasons: []string{"hosts"}}
	werr := writeReport(filepath.Join(t.TempDir(), "missing", "report.json"), &gc.Report{})
	require.ErrorContains(t, werr, "failed to write the GC report")
	err := newCycleMetrics(metrics.WrapBackend(b)).endCycle(gc.PhaseCondemn, nil, errors.Join(refused, werr))
	var cliErr *clierr.CLIError
	require.NotErrorAs(t, err, &cliErr)
	require.Len(t, b.values, 1)
	require.Contains(t, b.values, "abfs_gc_cycle_end_timestamp_seconds{condemn,error}")
}

// The report is one line, so that a container log keeps it as one entry, and
// decodes back to the report.
func TestWriteReport_OneLine(t *testing.T) {
	require := require.New(t)
	r := &gc.Report{
		Cycle: 12, Phase: gc.PhaseDelete,
		Condemn: &gc.CondemnReport{Refused: []string{"a reason\nwith a newline"}, Types: map[string]int64{"blob": 3}},
		Delete:  &gc.DeleteReport{Chunks: &gc.ChunkStepReport{Skipped: []string{"versioning"}, RecordsSkipped: 2}},
	}
	file := filepath.Join(t.TempDir(), "report.json")
	require.NoError(writeReport(file, r))
	data, err := os.ReadFile(file)
	require.NoError(err)
	require.Equal(1, strings.Count(string(data), "\n"))
	require.True(strings.HasSuffix(string(data), "\n"))
	var got gc.Report
	require.NoError(json.Unmarshal(data, &got))
	require.Equal(r, &got)
}

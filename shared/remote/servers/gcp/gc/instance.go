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
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const adminCallTimeout = time.Minute

// backupTables are the Bigtable tables a delete changes, which need a backup
// taken after the list it deletes from was complete.
var backupTables = []string{objectsTable, TableName, mapsTableName, chunksTableName}

// InstanceChecks reads what a delete cycle needs to know about the Bigtable
// instance beyond its tables.
type InstanceChecks interface {
	// Clusters returns the IDs of the clusters of the instance.
	Clusters(ctx context.Context) ([]string, error)
	// Backups returns the backups of the tables of the instance in every
	// cluster.
	Backups(ctx context.Context) ([]Backup, error)
	// ProbeConditionalWrite sends one conditional write that changes nothing
	// with the app profile of the collector; the caller retries it.
	ProbeConditionalWrite(ctx context.Context) error
}

// Backup is a backup of a Bigtable table.
type Backup struct {
	Name  string
	Table string
	// Start is the time the backup started; it holds no data older than
	// that.
	Start time.Time
	// Expire is the time Bigtable deletes the backup.
	Expire time.Time
	Ready  bool
}

// NewInstanceChecks returns the checks of the Bigtable instance of project
// that bt, a client with the app profile of the collector, writes to.
func NewInstanceChecks(project, instance string, bt *bigtable.Client) InstanceChecks {
	return &instanceChecks{project: project, instance: instance, bt: bt}
}

type instanceChecks struct {
	project, instance string
	bt                *bigtable.Client
}

var _ InstanceChecks = (*instanceChecks)(nil)

// Clusters implements InstanceChecks.
func (c *instanceChecks) Clusters(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, adminCallTimeout)
	defer cancel()
	admin, err := bigtable.NewInstanceAdminClient(ctx, c.project)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	clusters, err := admin.Clusters(ctx, c.instance)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(clusters))
	for i, cl := range clusters {
		ids[i] = cl.Name
	}
	return ids, nil
}

// Backups implements InstanceChecks.
func (c *instanceChecks) Backups(ctx context.Context) ([]Backup, error) {
	ctx, cancel := context.WithTimeout(ctx, adminCallTimeout)
	defer cancel()
	admin, err := bigtable.NewAdminClient(ctx, c.project, c.instance)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	var backups []Backup
	// The cluster "-" lists the backups of every cluster.
	it := admin.Backups(ctx, "-")
	for {
		b, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return backups, nil
		}
		if err != nil {
			return nil, err
		}
		backups = append(backups, Backup{Name: b.Name, Table: b.SourceTable, Start: b.StartTime, Expire: b.ExpireTime, Ready: b.State == "READY"})
	}
}

// ProbeConditionalWrite implements InstanceChecks. It deletes a cell of a row
// that does not exist, under a condition, as the server does at start.
func (c *instanceChecks) ProbeConditionalWrite(ctx context.Context) error {
	predicate := bigtable.ChainFilters(bigtable.FamilyFilter(ObjectsFamily), bigtable.ColumnFilter(CondemnedColumn))
	del := bigtable.NewMutation()
	del.DeleteCellsInColumn(ObjectsFamily, CondemnedColumn)
	mut := bigtable.NewCondMutation(predicate, del, nil)
	return c.bt.Open(objectsTable).Apply(ctx, "abfs-setup-check", mut)
}

// instanceEvidence is what the checks of the instance found.
type instanceEvidence struct {
	clusters    []string
	clustersErr error
	probeErr    error
	// backups are the backups found, when backupsChecked.
	backups        []Backup
	backupsErr     error
	backupsChecked bool
}

// readInstanceEvidence runs the checks of the instance that a delete needs:
// the cluster count, the probe and, unless o skips it, the backups. The
// client does not retry conditional writes, so the probe is retried here,
// each attempt within the budget b like every other write of the collector.
func readInstanceEvidence(ctx context.Context, checks InstanceChecks, b writeBudget, o *DeleteOpts) instanceEvidence {
	var e instanceEvidence
	e.clusters, e.clustersErr = checks.Clusters(ctx)
	e.probeErr = retryWith(ctx, firstRetryWait, b, checks.ProbeConditionalWrite)
	if !o.NoBackupCheck {
		e.backupsChecked = true
		e.backups, e.backupsErr = checks.Backups(ctx)
	}
	return e
}

// instanceBlockers returns the reasons in e why a cycle must not delete from
// a list completed at listComplete.
func instanceBlockers(e *instanceEvidence, listComplete time.Time) []string {
	var reasons []string
	add := func(format string, args ...any) {
		reasons = append(reasons, fmt.Sprintf(format, args...))
	}
	switch {
	case e.clustersErr != nil:
		add("cannot list the clusters of the Bigtable instance%s: %v", permissionHint(e.clustersErr, "bigtable.clusters.list"), e.clustersErr)
	case len(e.clusters) != 1:
		add("the Bigtable instance has %d clusters %q: conditional writes sent to different clusters can both succeed, so deletes need an instance with one cluster", len(e.clusters), e.clusters)
	}
	if e.probeErr != nil {
		switch status.Code(e.probeErr) {
		case codes.FailedPrecondition, codes.InvalidArgument:
			add("the Bigtable app profile of the collector refused a conditional write: deletes need single-cluster routing with single-row transactions allowed: %v", e.probeErr)
		default:
			add("the conditional write probe failed%s: %v", permissionHint(e.probeErr, "bigtable.tables.readRows and bigtable.tables.mutateRows"), e.probeErr)
		}
	}
	if !e.backupsChecked {
		return reasons
	}
	if e.backupsErr != nil {
		hint := permissionHint(e.backupsErr, "bigtable.backups.list")
		if status.Code(e.backupsErr) == codes.Unknown {
			// The client library fails the whole listing on a backup it
			// cannot parse, such as one still being created, which has no
			// end time yet.
			hint = " (a backup that is still being created fails the listing until it is ready)"
		}
		add("cannot list the backups of the Bigtable instance%s: %v", hint, e.backupsErr)
		return reasons
	}
	for _, table := range backupTables {
		found := false
		for _, b := range e.backups {
			if b.Table == table && b.Ready && b.Start.After(listComplete) {
				found = true
				break
			}
		}
		if !found {
			add("no ready backup of Bigtable table %s started after the list was completed at %s; create one, or pass --no-backup-check", table, formatTime(listComplete))
		}
	}
	return reasons
}

// permissionHint names the permission a call needs when err tells it lacks
// one.
func permissionHint(err error, permission string) string {
	if status.Code(err) == codes.PermissionDenied {
		return " (the collector needs " + permission + ")"
	}
	return ""
}

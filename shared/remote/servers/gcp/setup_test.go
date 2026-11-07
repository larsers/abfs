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

package gcp

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
)

func setupOpts(t *testing.T, dbID string) *SetupOpts {
	dropDatabaseAtCleanup(t, dbID)
	return &SetupOpts{
		ProjectID:  emulatorProject,
		InstanceID: emulatorInstance,
		DatabaseID: dbID,
		BtInstance: emulatorBigtable,
		BucketName: newEmulatorBucket(t),
	}
}

func TestSetup_CreatesMissingResourcesAndCanRunAgain(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	newEmulatorDatabase(t) // creates the emulator instance
	newEmptyEmulatorBigtable(t)
	opts := setupOpts(t, randomID(t, "t"))

	require.NoError(Setup(ctx, opts))
	require.NoError(Setup(ctx, opts))

	adm, err := bigtable.NewAdminClient(ctx, emulatorProject, emulatorBigtable)
	require.NoError(err)
	defer adm.Close()
	for _, table := range bigtableTables {
		info, err := adm.TableInfo(ctx, table.name)
		require.NoError(err)
		require.ElementsMatch(table.families, info.Families, table.name)
	}

	s, err := New(ctx, &NewServerOpts{
		ProjectID: opts.ProjectID, InstanceID: opts.InstanceID, DatabaseID: opts.DatabaseID,
		BtInstance: opts.BtInstance, BucketName: opts.BucketName, MaxCachedHeaders: 1024, MaxCachedContents: 1024,
	})
	require.NoError(err)
	defer s.Close()
	require.NoError(s.PutRefs(ctx, "example.com", "p", []remote.RefHash{{Ref: "refs/heads/main", Hash: entities.EmptyDirHash}}))
}

// The schema shipped before had a foreign key from Refs.Target to an Objects
// table the server never writes.
var schemaWithRefsTargetForeignKey = []string{
	"CREATE TABLE Objects (ObjectId STRING(128), AddTime TIMESTAMP NOT NULL, Type STRING(64) NOT NULL, Size INT64 NOT NULL, ContentEncoding STRING(64) NOT NULL, Content STRING(MAX) NOT NULL) PRIMARY KEY (ObjectId)",
	`CREATE SEQUENCE ProjectIdSequence OPTIONS (sequence_kind="bit_reversed_positive")`,
	"CREATE TABLE Projects (ProjectId INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE ProjectIdSequence)), Host STRING(MAX) NOT NULL, Project STRING(MAX) NOT NULL) PRIMARY KEY (ProjectId)",
	"CREATE UNIQUE INDEX ProjectsIndex ON Projects(Host, Project)",
	"CREATE TABLE Refs (ProjectId INT64 NOT NULL, Ref STRING(MAX) NOT NULL, UpdateTime TIMESTAMP NOT NULL, Target STRING(128), CONSTRAINT FK_Refs_ProjectId FOREIGN KEY (ProjectId) REFERENCES Projects (ProjectId), CONSTRAINT FK_Refs_TargetId FOREIGN KEY (Target) REFERENCES Objects (ObjectId)) PRIMARY KEY (ProjectId, Ref)",
	"CREATE INDEX RefsRefIndex ON Refs(Ref)",
}

func TestSetup_DropsRefsTargetForeignKeyAndKeepsData(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	newEmulatorDatabase(t)
	newEmulatorBigtable(t)
	opts := setupOpts(t, randomID(t, "t"))
	spannerDB := fmt.Sprintf("projects/%s/instances/%s/databases/%s", opts.ProjectID, opts.InstanceID, opts.DatabaseID)

	dc, err := database.NewDatabaseAdminClient(ctx)
	require.NoError(err)
	defer dc.Close()
	op, err := dc.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", opts.ProjectID, opts.InstanceID),
		CreateStatement: "CREATE DATABASE `" + opts.DatabaseID + "`",
		ExtraStatements: schemaWithRefsTargetForeignKey,
	})
	require.NoError(err)
	_, err = op.Wait(ctx)
	require.NoError(err)
	client, err := spanner.NewClient(ctx, spannerDB)
	require.NoError(err)
	defer client.Close()
	_, err = client.Apply(ctx, []*spanner.Mutation{spanner.Insert("Projects", []string{"Host", "Project"}, []any{"example.com", "kept"})})
	require.NoError(err)

	require.NoError(Setup(ctx, opts))

	s, err := New(ctx, &NewServerOpts{
		ProjectID: opts.ProjectID, InstanceID: opts.InstanceID, DatabaseID: opts.DatabaseID,
		BtInstance: opts.BtInstance, BucketName: opts.BucketName, MaxCachedHeaders: 1024, MaxCachedContents: 1024,
	})
	require.NoError(err)
	defer s.Close()
	require.NoError(s.PutRefs(ctx, "example.com", "kept", []remote.RefHash{{Ref: "refs/heads/main", Hash: entities.EmptyDirHash}}))
	refs, err := s.SearchRefs(ctx, "example.com", "", "", 0)
	require.NoError(err)
	require.Equal([]remote.ProjectRefHash{{Project: "kept", Ref: "refs/heads/main", Hash: entities.EmptyDirHash}}, refs)
}

func TestSetup_AddsGCResourcesToExistingDeployment(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	newEmulatorDatabase(t)
	newEmptyEmulatorBigtable(t)
	opts := setupOpts(t, randomID(t, "t"))

	// The resources as plain init creates them.
	require.NoError(Setup(ctx, opts))
	btClient, err := bigtable.NewClient(ctx, emulatorProject, emulatorBigtable)
	require.NoError(err)
	defer btClient.Close()
	mut := bigtable.NewMutation()
	mut.Set("meta", "type", bigtable.Now(), []byte("blob"))
	require.NoError(btClient.Open("objects").Apply(ctx, "kept", mut))
	adm, err := bigtable.NewAdminClient(ctx, emulatorProject, emulatorBigtable)
	require.NoError(err)
	defer adm.Close()
	tables, err := adm.Tables(ctx)
	require.NoError(err)
	require.NotContains(tables, "gc")
	info, err := adm.TableInfo(ctx, "objects")
	require.NoError(err)
	require.ElementsMatch([]string{"meta", "data"}, info.Families)
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", opts.ProjectID, opts.InstanceID, opts.DatabaseID))
	require.NoError(err)
	defer client.Close()
	for _, table := range []string{"GcState", "GcReplicas"} {
		err := client.Single().Query(ctx, spanner.Statement{SQL: "SELECT 1 FROM " + table}).Do(func(*spanner.Row) error { return nil })
		require.Error(err, table)
	}

	opts.GC = true
	require.NoError(Setup(ctx, opts))
	require.NoError(Setup(ctx, opts))

	for _, table := range serverTables(true) {
		info, err := adm.TableInfo(ctx, table.name)
		require.NoError(err)
		require.ElementsMatch(table.families, info.Families, table.name)
	}
	info, err = adm.TableInfo(ctx, "gc")
	require.NoError(err)
	for _, f := range info.FamilyInfos {
		require.Equal("versions() > 1", f.GCPolicy, f.Name)
	}
	row, err := btClient.Open("objects").ReadRow(ctx, "kept")
	require.NoError(err)
	require.NotEmpty(row)
	for _, table := range []string{"GcState", "GcReplicas"} {
		err := client.Single().Query(ctx, spanner.Statement{SQL: "SELECT 1 FROM " + table}).Do(func(*spanner.Row) error { return nil })
		require.NoError(err, table)
	}
	s, err := New(ctx, &NewServerOpts{
		ProjectID: opts.ProjectID, InstanceID: opts.InstanceID, DatabaseID: opts.DatabaseID,
		BtInstance: opts.BtInstance, BucketName: opts.BucketName, MaxCachedHeaders: 1024, MaxCachedContents: 1024, GC: true,
	})
	require.NoError(err)
	require.NoError(s.Close())
}

// A database whose GcState was created before the condemned list columns
// existed stops a server with --gc with a message that names init --gc, and
// init --gc adds the columns and keeps the state.
func TestSetup_AddsGCStateListColumns(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	var phase1 []string
	for _, stmt := range casdb.GCSchemaStatements() {
		if !strings.HasPrefix(stmt, "ALTER TABLE GcState") {
			phase1 = append(phase1, stmt)
		}
	}
	dbID := newEmulatorDatabaseWith(t, append(casdb.SchemaStatements(), phase1...))
	newEmulatorBigtableWith(t, serverTables(true))
	opts := setupOpts(t, dbID)
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", opts.ProjectID, opts.InstanceID, opts.DatabaseID))
	require.NoError(err)
	defer client.Close()
	_, err = client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("GcState",
		[]string{"Name", "Cycle", "LeaseEpoch", "UpdateTime"}, []any{"default", int64(41), int64(5), spanner.CommitTimestamp})})
	require.NoError(err)
	serverOpts := &NewServerOpts{
		ProjectID: opts.ProjectID, InstanceID: opts.InstanceID, DatabaseID: opts.DatabaseID,
		BtInstance: opts.BtInstance, BucketName: opts.BucketName, MaxCachedHeaders: 1024, MaxCachedContents: 1024, GC: true,
	}
	_, err = New(ctx, serverOpts)
	require.ErrorContains(err, "init --gc")

	opts.GC = true
	require.NoError(Setup(ctx, opts))
	s, err := New(ctx, serverOpts)
	require.NoError(err)
	require.NoError(s.Close())
	row, err := client.Single().ReadRow(ctx, "GcState", spanner.Key{"default"}, []string{"Cycle", "ListComplete"})
	require.NoError(err)
	var cycle int64
	var complete spanner.NullBool
	require.NoError(row.Columns(&cycle, &complete))
	require.Equal(int64(41), cycle)
	require.False(complete.Valid)
}

func TestSetup_MissingBucket(t *testing.T) {
	newEmulatorDatabase(t)
	newEmulatorBigtable(t)
	opts := setupOpts(t, randomID(t, "t"))
	opts.BucketName = "no-such-bucket"
	require.ErrorContains(t, Setup(context.Background(), opts), "no-such-bucket")
}

func TestNew_ChecksResources(t *testing.T) {
	ctx := context.Background()
	newOpts := func(o *SetupOpts) *NewServerOpts {
		return &NewServerOpts{
			ProjectID: o.ProjectID, InstanceID: o.InstanceID, DatabaseID: o.DatabaseID,
			BtInstance: o.BtInstance, BucketName: o.BucketName, MaxCachedHeaders: 1024, MaxCachedContents: 1024, GC: o.GC,
		}
	}
	tests := []struct {
		name    string
		prepare func(t *testing.T) *SetupOpts
		wantErr string
	}{
		{
			name: "GC without GC Spanner tables",
			prepare: func(t *testing.T) *SetupOpts {
				newEmulatorBigtable(t)
				o := setupOpts(t, newEmulatorDatabaseWith(t, casdb.SchemaStatements()))
				o.GC = true
				return o
			},
			wantErr: "init --gc",
		},
		{
			name: "GC without table gc",
			prepare: func(t *testing.T) *SetupOpts {
				newEmulatorBigtableWith(t, serverTables(false))
				o := setupOpts(t, newEmulatorDatabase(t))
				o.GC = true
				return o
			},
			wantErr: "Bigtable table gc",
		},
		{
			name: "GC without family g",
			prepare: func(t *testing.T) *SetupOpts {
				tables := serverTables(true)
				tables[0].families = []string{"meta", "data"}
				newEmulatorBigtableWith(t, tables)
				o := setupOpts(t, newEmulatorDatabase(t))
				o.GC = true
				return o
			},
			wantErr: "column family g",
		},
		{
			name: "missing bucket",
			prepare: func(t *testing.T) *SetupOpts {
				newEmulatorBigtable(t)
				o := setupOpts(t, newEmulatorDatabase(t))
				o.BucketName = "no-such-bucket"
				return o
			},
			wantErr: "no-such-bucket",
		},
		{
			name: "missing Bigtable table",
			prepare: func(t *testing.T) *SetupOpts {
				newEmptyEmulatorBigtable(t)
				return setupOpts(t, newEmulatorDatabase(t))
			},
			wantErr: "Bigtable table objects",
		},
		{
			name: "missing Spanner table",
			prepare: func(t *testing.T) *SetupOpts {
				newEmulatorBigtable(t)
				newEmulatorDatabase(t)
				o := setupOpts(t, randomID(t, "t"))
				createDatabase(t, o, []string{"CREATE TABLE Other (Id INT64) PRIMARY KEY (Id)"})
				return o
			},
			wantErr: "Projects",
		},
		{
			name: "refs target foreign key",
			prepare: func(t *testing.T) *SetupOpts {
				newEmulatorBigtable(t)
				newEmulatorDatabase(t)
				o := setupOpts(t, randomID(t, "t"))
				createDatabase(t, o, schemaWithRefsTargetForeignKey)
				return o
			},
			wantErr: "FK_Refs_TargetId",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := tt.prepare(t)
			s, err := New(ctx, newOpts(o))
			if err == nil {
				_ = s.Close()
			}
			require.ErrorContains(t, err, tt.wantErr)
			require.ErrorContains(t, err, "abfs server gcp init")
		})
	}
}

// A server with --gc stops at start on an app profile that refuses the
// conditional writes of a rescue, and the error names the fix. A transient
// error is retried, and another error stops the server without blaming the
// profile. A read-only server, as the collector's commands use, rescues
// nothing and starts.
func TestNew_ChecksSingleRowTransactions(t *testing.T) {
	const refusal = "single-row transactions are not allowed by this app profile"
	const profileFix = "gcloud bigtable app-profiles update default --instance=test-bt --route-to=<cluster> --transactional-writes"
	tests := []struct {
		name string
		// fail is the code of the first failed probes, failures their number,
		// 0 for all.
		fail     codes.Code
		failures int32
		readOnly bool
		wantErr  []string
		notErr   string
	}{
		{name: "profile refuses conditional writes", fail: codes.FailedPrecondition, wantErr: []string{profileFix, refusal}},
		{name: "read-only on a profile that refuses them", fail: codes.FailedPrecondition, readOnly: true},
		{name: "one transient error", fail: codes.Unavailable, failures: 1},
		{name: "other error", fail: codes.PermissionDenied, wantErr: []string{"failed to check that a conditional write to Bigtable table objects works", refusal}, notErr: "app-profiles"},
		{name: "emulator default profile"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			fail := grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if strings.HasSuffix(info.FullMethod, "/CheckAndMutateRow") {
					if n := calls.Add(1); tt.fail != codes.OK && (tt.failures == 0 || n <= tt.failures) {
						return nil, status.Error(tt.fail, refusal)
					}
				}
				return handler(ctx, req)
			})
			newEmulatorBigtableWith(t, serverTables(true), fail)
			o := setupOpts(t, newEmulatorDatabase(t))
			s, err := New(context.Background(), &NewServerOpts{
				ProjectID: o.ProjectID, InstanceID: o.InstanceID, DatabaseID: o.DatabaseID, BtInstance: o.BtInstance, BucketName: o.BucketName,
				MaxCachedHeaders: 1024, MaxCachedContents: 1024, GC: true, GCReadOnly: tt.readOnly,
			})
			if err == nil {
				_ = s.Close()
			}
			if tt.wantErr == nil {
				require.NoError(t, err)
				if tt.failures > 0 {
					require.Equal(t, tt.failures+1, calls.Load())
				}
				return
			}
			for _, w := range tt.wantErr {
				require.ErrorContains(t, err, w)
			}
			if tt.notErr != "" {
				require.NotContains(t, err.Error(), tt.notErr)
			}
		})
	}
}

func createDatabase(t *testing.T, o *SetupOpts, ddl []string) {
	t.Helper()
	ctx := context.Background()
	dc, err := database.NewDatabaseAdminClient(ctx)
	require.NoError(t, err)
	defer dc.Close()
	op, err := dc.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", o.ProjectID, o.InstanceID),
		CreateStatement: "CREATE DATABASE `" + o.DatabaseID + "`",
		ExtraStatements: ddl,
	})
	require.NoError(t, err)
	_, err = op.Wait(ctx)
	require.NoError(t, err)
}

func TestClose_ClosesStorageClient(t *testing.T) {
	ctx := context.Background()
	newEmulatorBigtable(t)
	o := setupOpts(t, newEmulatorDatabase(t))
	s, err := New(ctx, &NewServerOpts{
		ProjectID: o.ProjectID, InstanceID: o.InstanceID, DatabaseID: o.DatabaseID,
		BtInstance: o.BtInstance, BucketName: o.BucketName, MaxCachedHeaders: 1024, MaxCachedContents: 1024,
	})
	require.NoError(t, err)
	require.NoError(t, s.Close())
	// An open client lists the empty bucket and returns iterator.Done.
	_, err = s.storage.Bucket(o.BucketName).Objects(ctx, nil).Next()
	require.Error(t, err)
	require.NotErrorIs(t, err, iterator.Done)
}

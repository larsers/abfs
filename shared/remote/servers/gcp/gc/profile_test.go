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
	"testing"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"github.com/stretchr/testify/require"
)

// The GC commands need an app profile with single-cluster routing,
// single-row transactions and priority low; every other profile is reported
// with what it lacks and how to create one.
func TestCheckAppProfile(t *testing.T) {
	single := func(tx bool) *adminpb.AppProfile_SingleClusterRouting_ {
		return &adminpb.AppProfile_SingleClusterRouting_{SingleClusterRouting: &adminpb.AppProfile_SingleClusterRouting{ClusterId: "c1", AllowTransactionalWrites: tx}}
	}
	standard := func(p adminpb.AppProfile_Priority) *adminpb.AppProfile_StandardIsolation_ {
		return &adminpb.AppProfile_StandardIsolation_{StandardIsolation: &adminpb.AppProfile_StandardIsolation{Priority: p}}
	}
	tests := []struct {
		name     string
		profile  *adminpb.AppProfile
		priority string
		problems []string
	}{
		{"single cluster, transactions, low", &adminpb.AppProfile{RoutingPolicy: single(true), Isolation: standard(adminpb.AppProfile_PRIORITY_LOW)}, "low", nil},
		{"multi-cluster", &adminpb.AppProfile{RoutingPolicy: &adminpb.AppProfile_MultiClusterRoutingUseAny_{MultiClusterRoutingUseAny: &adminpb.AppProfile_MultiClusterRoutingUseAny{}}, Isolation: standard(adminpb.AppProfile_PRIORITY_LOW)}, "low", []string{"single-cluster routing"}},
		{"no transactions", &adminpb.AppProfile{RoutingPolicy: single(false), Isolation: standard(adminpb.AppProfile_PRIORITY_LOW)}, "low", []string{"single-row transactions"}},
		{"data boost", &adminpb.AppProfile{RoutingPolicy: single(true), Isolation: &adminpb.AppProfile_DataBoostIsolationReadOnly_{DataBoostIsolationReadOnly: &adminpb.AppProfile_DataBoostIsolationReadOnly{}}}, "data-boost", []string{"Data Boost"}},
		{"high", &adminpb.AppProfile{RoutingPolicy: single(true), Isolation: standard(adminpb.AppProfile_PRIORITY_HIGH)}, "high", []string{"priority is high"}},
		{"medium", &adminpb.AppProfile{RoutingPolicy: single(true), Isolation: standard(adminpb.AppProfile_PRIORITY_MEDIUM)}, "medium", []string{"priority is medium"}},
		{"unspecified", &adminpb.AppProfile{RoutingPolicy: single(true), Isolation: standard(adminpb.AppProfile_PRIORITY_UNSPECIFIED)}, "high", []string{"priority is high"}},
		{"no isolation", &adminpb.AppProfile{RoutingPolicy: single(true)}, "high", []string{"priority is high"}},
		{"deprecated priority field", &adminpb.AppProfile{RoutingPolicy: single(true), Isolation: &adminpb.AppProfile_Priority_{Priority: adminpb.AppProfile_PRIORITY_LOW}}, "low", nil}, //nolint:staticcheck // profiles created before standard isolation hold this field
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			rep := checkAppProfile("gc", "inst", tt.profile)
			require.Equal(tt.priority, rep.Priority)
			require.Len(rep.Problems, len(tt.problems), rep.Problems)
			for i, p := range tt.problems {
				require.Contains(rep.Problems[i], p)
			}
			if tt.problems == nil {
				require.Empty(rep.Fix)
				require.Equal("c1", rep.Cluster)
				return
			}
			require.Contains(rep.Fix, "gcloud bigtable app-profiles create abfs-gc --instance=inst")
		})
	}
}

// An empty profile ID reads the default profile, and a profile that cannot
// be read is a problem.
func TestReadAppProfile(t *testing.T) {
	require := require.New(t)
	var read string
	rep := readAppProfile(context.Background(), func(_ context.Context, id string) (*adminpb.AppProfile, error) {
		read = id
		return nil, errors.New("permission denied")
	}, "inst", "")
	require.Equal("default", read)
	require.Equal("default", rep.ID)
	require.Len(rep.Problems, 1)
	require.Contains(rep.Problems[0], "cannot read it: permission denied")
	require.NotEmpty(rep.Fix)
}

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
	"fmt"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
)

const profileReadTimeout = 30 * time.Second

// ProfileReport describes the Bigtable app profile of a GC command. The scans
// and writes of the collector are batch work: they need single-cluster
// routing with single-row transactions for the conditional writes, and low
// priority so that they do not slow down the servers.
type ProfileReport struct {
	ID string `json:"id"`
	// Routing is single-cluster or multi-cluster, Cluster the cluster of
	// single-cluster routing.
	Routing      string `json:"routing,omitempty"`
	Cluster      string `json:"cluster,omitempty"`
	Transactions bool   `json:"transactional_writes"`
	// Priority is low, medium or high, or data-boost for a Data Boost
	// profile.
	Priority string `json:"priority,omitempty"`
	// Problems lists what breaks the settings the collector needs, or why the
	// profile could not be read, and Fix how to create a profile that has
	// them.
	Problems []string `json:"problems,omitempty"`
	Fix      string   `json:"fix,omitempty"`
}

// ReadAppProfile reads the app profile id of the Bigtable instance and checks
// its settings; an empty id is the instance's default profile. A profile that
// cannot be read is reported as a problem.
func ReadAppProfile(ctx context.Context, project, instance, id string) *ProfileReport {
	get := func(ctx context.Context, id string) (*adminpb.AppProfile, error) {
		// The admin client retries transient errors until its context ends.
		ctx, cancel := context.WithTimeout(ctx, profileReadTimeout)
		defer cancel()
		admin, err := bigtable.NewInstanceAdminClient(ctx, project)
		if err != nil {
			return nil, err
		}
		defer admin.Close()
		return admin.GetAppProfile(ctx, instance, id)
	}
	return readAppProfile(ctx, get, instance, id)
}

func readAppProfile(ctx context.Context, get func(ctx context.Context, id string) (*adminpb.AppProfile, error), instance, id string) *ProfileReport {
	if id == "" {
		id = "default"
	}
	ap, err := get(ctx, id)
	if err != nil {
		rep := &ProfileReport{ID: id, Problems: []string{fmt.Sprintf("cannot read it: %v", err)}}
		rep.Fix = profileFix(instance, "<cluster>")
		return rep
	}
	return checkAppProfile(id, instance, ap)
}

func checkAppProfile(id, instance string, ap *adminpb.AppProfile) *ProfileReport {
	rep := &ProfileReport{ID: id}
	if single := ap.GetSingleClusterRouting(); single != nil {
		rep.Routing, rep.Cluster, rep.Transactions = "single-cluster", single.GetClusterId(), single.GetAllowTransactionalWrites()
		if !rep.Transactions {
			rep.Problems = append(rep.Problems, "it does not allow single-row transactions, which the conditional writes of the collector need")
		}
	} else {
		rep.Routing = "multi-cluster"
		rep.Problems = append(rep.Problems, "it routes to any cluster, but conditional writes need single-cluster routing")
	}
	if ap.GetDataBoostIsolationReadOnly() != nil {
		rep.Priority = "data-boost"
		rep.Problems = append(rep.Problems, "it uses Data Boost, which allows no writes")
	} else {
		// Older profiles hold the priority in the deprecated field.
		p := ap.GetPriority() //nolint:staticcheck // read when the isolation holds the old field
		if si := ap.GetStandardIsolation(); si != nil {
			p = si.GetPriority()
		}
		switch p {
		case adminpb.AppProfile_PRIORITY_LOW:
			rep.Priority = "low"
		case adminpb.AppProfile_PRIORITY_MEDIUM:
			rep.Priority = "medium"
		default:
			// An unspecified priority is high.
			rep.Priority = "high"
		}
		if rep.Priority != "low" {
			rep.Problems = append(rep.Problems, fmt.Sprintf("its priority is %s, so the scans and writes of the collector compete with the servers; use priority low", rep.Priority))
		}
	}
	if len(rep.Problems) > 0 {
		cluster := rep.Cluster
		if cluster == "" {
			cluster = "<cluster>"
		}
		rep.Fix = profileFix(instance, cluster)
	}
	return rep
}

func profileFix(instance, cluster string) string {
	return fmt.Sprintf(`create a profile with 'gcloud bigtable app-profiles create abfs-gc --instance=%s --route-to=%s --transactional-writes --priority=PRIORITY_LOW --description="ABFS GC collector"' and pass --bt-app-profile abfs-gc`, instance, cluster)
}

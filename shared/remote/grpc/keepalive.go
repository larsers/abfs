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

package grpc

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// grpc-go clients cannot ping more often than every 10 s, so this lets any of them ping
	// on an open call. grpc-go's default, 5 minutes, closes the connection with
	// too_many_pings after a few pings on a call the server writes nothing to.
	serverKeepaliveMinTime = 10 * time.Second
)

// ServerKeepalivePolicy returns the keepalive enforcement policy of the ABFS gRPC servers.
func ServerKeepalivePolicy() grpc.ServerOption {
	return grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             serverKeepaliveMinTime,
		PermitWithoutStream: false,
	})
}

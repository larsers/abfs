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
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cmd/root"
	"github.com/google/abfs/shared/entities"
)

func TestGCPGCWalkFlags(t *testing.T) {
	const h1, h2 = "1111111111111111111111111111111111111111", "2222222222222222222222222222222222222222"
	tests := []struct {
		args    []string
		wantErr string
	}{
		{nil, "pass --mount-details and --key"},
		{[]string{"--mount-details=" + h1}, "pass --mount-details and --key"},
		{[]string{"--key=" + h1}, "pass --mount-details and --key"},
		{[]string{"--mount-details=main", "--key=" + h2}, `invalid key "main" for --mount-details`},
		{[]string{"--mount-details=" + h1, "--key=" + h2 + ",abc"}, `invalid key "abc" for --key`},
		{[]string{"--mount-details=" + h1 + "," + h2, "--key=" + h2}, ""},
	}
	for _, tt := range tests {
		c := &gcpGCWalkCmd{RootCmd: root.NewRootCommand()}
		fl := pflag.NewFlagSet("walk", pflag.ContinueOnError)
		c.RegisterFlags(fl)
		require.NoError(t, fl.Parse(tt.args))
		err := c.checkOpts()
		if tt.wantErr != "" {
			require.ErrorContains(t, err, tt.wantErr, tt.args)
			continue
		}
		require.NoError(t, err, tt.args)
		require.Equal(t, []entities.Hash{entities.NewHash(h1), entities.NewHash(h2)}, c.opts.MountDetails)
		require.Equal(t, []entities.Hash{entities.NewHash(h2)}, c.opts.Keys)
	}
}

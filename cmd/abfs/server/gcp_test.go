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

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cmd"
	"github.com/google/abfs/shared/cmd/root"
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

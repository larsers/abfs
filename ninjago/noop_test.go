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

package ninjago

import (
	"testing"

	"github.com/google/ninja-go/ninja"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoOpCommand(t *testing.T) {
	tests := []struct {
		cmd  string
		want bool
	}{
		{cmd: "", want: true},
		{cmd: "true", want: true},
		{cmd: ":", want: true},
		{cmd: `/bin/bash -c ""`, want: true},
		{cmd: `/bin/bash -c ": created by out/release-config"`, want: true},
		{cmd: `/bin/sh -c ': created by out/release-config'`, want: true},
		{cmd: `/bin/bash -ec "(: a) && (true) ; true"`, want: true},
		{cmd: `/bin/bash -c ": a;"`, want: true},
		{cmd: "touch out/a", want: false},
		{cmd: `/bin/bash -c "touch out/a"`, want: false},
		{cmd: `/bin/bash -c ": a && touch out/a"`, want: false},
		{cmd: `/bin/bash -c ": a & touch out/a"`, want: false},
		{cmd: `/bin/bash -c ": a || touch out/a"`, want: false},
		{cmd: `/bin/bash -c ": a > out/a"`, want: false},
		{cmd: `/bin/bash -c ": $(touch out/a)"`, want: false},
		{cmd: "/bin/bash -c \": `touch out/a`\"", want: false},
		{cmd: `/bin/bash -c ": a" ; touch out/a`, want: false},
		{cmd: `/bin/bash -c ": a"" && touch out/a"`, want: false},
		{cmd: `/bin/bash -c ": a ; ; :"`, want: false},
		{cmd: `/bin/bash out/a.rsp`, want: false},
		{cmd: `/usr/bin/python -c ":"`, want: false},
		{cmd: `/bin/bash -x ":"`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.cmd, func(t *testing.T) {
			assert.Equal(t, tt.want, noOpCommand(tt.cmd))
		})
	}
}

func TestIsClean_NoOpStep(t *testing.T) {
	g := loadTestGraph(t, &ninja.NinjaFile{
		Rules: []*ninja.Rule{
			{Name: "created_by", Command: `/bin/bash -c ": created by $out"`},
			{Name: "created_by_depfile", Command: `/bin/bash -c ": created by $out"`, DepFile: "$out.d"},
			{Name: "touch", Command: "touch $out"},
		},
		Builds: []*ninja.Build{
			{Outputs: []string{"out/placeholder"}, RuleName: "created_by"},
			{Outputs: []string{"out/depfile"}, RuleName: "created_by_depfile"},
			{Outputs: []string{"out/touched"}, RuleName: "touch"},
			{Outputs: []string{"out/all"}, RuleName: "touch", Inputs: []string{"out/placeholder", "out/depfile", "out/touched"}},
		},
	}, "out/all")
	c := newTestBackend(t, g, &Configuration{})

	tests := []struct {
		target string
		want   bool
	}{
		{target: "out/placeholder", want: true},
		{target: "out/depfile", want: false},
		{target: "out/touched", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			clean, err := c.IsClean(loadedStep(g, tt.target))
			require.NoError(t, err)
			assert.Equal(t, tt.want, clean)
		})
	}
}

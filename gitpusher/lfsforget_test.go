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

package gitpusher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForgetLFSOnError(t *testing.T) {
	require := require.New(t)

	p := &pusher{
		config:        &ProjectConfig{},
		pushedLFSRefs: map[string]struct{}{"oid1": {}, "oid2": {}},
	}

	require.NoError(forgetLFSOnError(p, nil))
	require.Len(p.pushedLFSRefs, 2)

	pushErr := errors.New("push failed")
	require.ErrorIs(forgetLFSOnError(p, pushErr), pushErr)
	require.Empty(p.pushedLFSRefs)

	// Pushers that keep no LFS set pass the error through.
	require.ErrorIs(forgetLFSOnError(&testPusher{}, pushErr), pushErr)
}

// failingPusher is a pusher whose push fails.
type failingPusher struct {
	*pusher
}

func (f failingPusher) Push(ctx context.Context) error {
	return errors.New("push failed")
}

func TestProjPushWrap_ForgetsLFSOnFailure(t *testing.T) {
	require := require.New(t)

	p := &pusher{
		config:        &ProjectConfig{},
		pushedLFSRefs: map[string]struct{}{"oid": {}},
	}
	w := &projPushWrap{config: p.config, pusher: failingPusher{p}}
	require.Error(w.push(context.Background()))
	require.Empty(p.pushedLFSRefs)
}

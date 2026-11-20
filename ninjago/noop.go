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
	"path"
	"strings"

	"github.com/google/ninja-go/build"
)

// noOpStep reports whether running the step changes nothing, as with the placeholder rules that
// kati writes for files soong_ui creates before ninja runs, like
//
//	command = /bin/bash -c ": created by out/release-config"
//
// Such a step writes none of its outputs, so a cached response for it never has them and the step
// misses the cache on every build.
func noOpStep(step *build.Step) bool {
	if step.RSPFile() != "" || step.Node.DepFile() != "" {
		return false
	}
	return noOpCommand(step.Command())
}

// noOpCommand reports whether cmd, run by ninja with "/bin/sh -c", only runs ":" or "true", either
// directly or as the script of "sh -c" or "bash -c".
func noOpCommand(cmd string) bool {
	if noOpScript(cmd) {
		return true
	}
	script, ok := shellScript(cmd)
	return ok && noOpScript(script)
}

// shellScript returns the script of a command like `/bin/bash -c "script"`.
func shellScript(cmd string) (string, bool) {
	shell, rest, _ := strings.Cut(strings.TrimSpace(cmd), " ")
	if base := path.Base(shell); base != "sh" && base != "bash" {
		return "", false
	}
	flags, script, _ := strings.Cut(strings.TrimLeft(rest, " "), " ")
	if len(flags) < 2 || flags[0] != '-' || !strings.HasSuffix(flags, "c") || strings.Trim(flags[1:], "abcefhkmnptuvxBCEHP") != "" {
		return "", false
	}
	script = strings.TrimSpace(script)
	if len(script) < 2 || (script[0] != '"' && script[0] != '\'') || script[len(script)-1] != script[0] {
		return "", false
	}
	return script[1 : len(script)-1], true
}

// noOpScript reports whether script is a list of ":" and "true" commands, joined with "&&" or ";"
// and possibly in parentheses. Any character that could run, expand or redirect something makes it
// not a no-op.
func noOpScript(script string) bool {
	if strings.ContainsAny(script, "$`\\<>|'\"#{}\n\r") {
		return false
	}
	parts := strings.Split(strings.ReplaceAll(script, "&&", ";"), ";")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		for len(part) >= 2 && part[0] == '(' && part[len(part)-1] == ')' {
			part = strings.TrimSpace(part[1 : len(part)-1])
		}
		if strings.ContainsAny(part, "&()") {
			return false
		}
		fields := strings.Fields(part)
		if len(fields) == 0 {
			// Only a whole empty script or a trailing ";" runs nothing.
			if len(parts) == 1 || (i == len(parts)-1 && i > 0) {
				continue
			}
			return false
		}
		if fields[0] != ":" && fields[0] != "true" {
			return false
		}
	}
	return true
}

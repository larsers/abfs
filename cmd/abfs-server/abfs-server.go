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

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"github.com/google/abfs/cmd/abfs/server"
	"github.com/google/abfs/shared/cmd/root"
	"github.com/google/abfs/shared/metrics"
	"github.com/google/abfs/shared/metrics/otelmetrics"
	"github.com/google/abfs/shared/telemetry"
)

func main() {
	fl := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	fl.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Description: Start ABFS server for blobs, objects and map results.\n")
		fl.PrintDefaults()
		os.Exit(1)
	}

	cmd := server.NewStandaloneGCPServerCmd(root.NewRootCommand())
	cmd.RegisterFlags(fl)

	if err := fl.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	flush := startTelemetry(ctx)
	err := cmd.Run(ctx)
	flush()
	if err != nil {
		fmt.Fprintf(os.Stderr, "server failed: %v\n", err)
		os.Exit(1)
	}
}

// startTelemetry turns on what the OTEL_* variables ask for: metrics when
// OTEL_METRICS_EXPORTER names an exporter other than none, traces when
// OTEL_TRACES_EXPORTER does. This binary reads no instance config, so the
// variables take the place of metrics-provider. It returns the function that
// flushes both at exit.
func startTelemetry(ctx context.Context) func() {
	var provider metrics.Provider
	if v := strings.TrimSpace(os.Getenv("OTEL_METRICS_EXPORTER")); v != "" && v != "none" {
		provider = metrics.NewProvider(otelmetrics.ProviderName, true)
		metrics.SetProvider(provider)
	}
	stopTracing, err := telemetry.StartTracing(ctx)
	if err != nil {
		log.Printf("Warning: traces: %v, continuing without traces", err)
		stopTracing = nil
	}
	return func() {
		// Up to 5 s of spacing from the last export, plus the export itself.
		flushCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		if stopTracing != nil {
			wg.Go(func() {
				if err := stopTracing(flushCtx); err != nil {
					log.Printf("Failed to flush traces on exit: %v", err)
				}
			})
		}
		if provider != nil {
			if err := provider.Close(flushCtx); err != nil {
				log.Printf("Failed to flush metrics on exit: %v", err)
			}
		}
		wg.Wait()
	}
}

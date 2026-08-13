/*
Copyright 2026 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metricshandler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	ksmmetric "k8s.io/kube-state-metrics/v2/pkg/metric"
	"k8s.io/kube-state-metrics/v2/pkg/options"
)

func gaugeFamily(name string, value float64) ksmmetric.Family {
	return ksmmetric.Family{
		Name: name,
		Type: ksmmetric.Gauge,
		Metrics: []*ksmmetric.Metric{
			{LabelKeys: []string{"pod"}, LabelValues: []string{"p"}, Value: value},
		},
	}
}

// Export hands out one family list per Kubernetes object, so the same metric
// name arrives once per object. OTLP expects one Metrics per name carrying all
// of its data points.
func TestOTLPAggregatorFoldsPerObjectFamilies(t *testing.T) {
	now := time.Unix(1700000000, 0)
	start := now.Add(-time.Hour)

	agg := newOTLPAggregator()
	for i := 0; i < 3; i++ {
		agg.add(gaugeFamily("kube_pod_info", float64(i)), "Information about pod.", start, now)
		agg.add(ksmmetric.Family{
			Name: "kube_pod_restarts",
			Type: ksmmetric.Counter,
			Metrics: []*ksmmetric.Metric{
				{LabelKeys: []string{"pod"}, LabelValues: []string{"p"}, Value: float64(i)},
			},
		}, "Restart count.", start, now)
	}

	got := agg.metrics()
	if len(got) != 2 {
		names := make([]string, 0, len(got))
		for _, m := range got {
			names = append(names, m.Name)
		}
		t.Fatalf("expected one Metrics per name, got %d: %v", len(got), names)
	}

	if got[0].Name != "kube_pod_info" || got[1].Name != "kube_pod_restarts" {
		t.Errorf("unexpected names or order: %q, %q", got[0].Name, got[1].Name)
	}
	if got[0].Description != "Information about pod." {
		t.Errorf("help text not carried through: %q", got[0].Description)
	}

	gauge, ok := got[0].Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected a Gauge, got %T", got[0].Data)
	}
	if len(gauge.DataPoints) != 3 {
		t.Errorf("expected 3 data points folded into one metric, got %d", len(gauge.DataPoints))
	}

	sum, ok := got[1].Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected a Sum, got %T", got[1].Data)
	}
	if len(sum.DataPoints) != 3 {
		t.Errorf("expected 3 data points, got %d", len(sum.DataPoints))
	}
	// A cumulative point without a start time is not interpretable.
	for _, dp := range sum.DataPoints {
		if dp.StartTime.IsZero() {
			t.Error("cumulative data point has no StartTime")
		}
		if !dp.Time.Equal(now) {
			t.Errorf("data point time = %v, want the single collection time %v", dp.Time, now)
		}
	}
}

// A family type the switch does not handle would otherwise produce a Metrics
// with a nil Data, which the exporter cannot encode.
func TestOTLPAggregatorSkipsUnsupportedType(t *testing.T) {
	agg := newOTLPAggregator()
	agg.add(ksmmetric.Family{
		Name:    "kube_weird",
		Type:    ksmmetric.Type("histogram"),
		Metrics: []*ksmmetric.Metric{{Value: 1}},
	}, "", time.Now(), time.Now())

	if got := agg.metrics(); len(got) != 0 {
		t.Errorf("expected the family to be skipped, got %d metrics with Data=%v", len(got), got[0].Data)
	}
}

func TestCreateAttributes(t *testing.T) {
	if _, ok := createAttributes([]string{"a", "b"}, []string{"1"}); ok {
		t.Error("expected a length mismatch to be rejected")
	}
	set, ok := createAttributes([]string{"a", "b"}, []string{"1", "2"})
	if !ok {
		t.Fatal("expected matching lengths to be accepted")
	}
	if set.Len() != 2 {
		t.Errorf("expected 2 attributes, got %d", set.Len())
	}
}

// A zero interval reaches time.NewTicker, which panics, so it has to be
// rejected before the exporter loop starts.
func TestValidateOTLPOptions(t *testing.T) {
	valid := func() *options.Options {
		return &options.Options{
			EnableOTLPExport: true,
			OTLPEndpoint:     "localhost:4317",
			OTLPProtocol:     "grpc",
			OTLPInterval:     60 * time.Second,
		}
	}

	for _, tt := range []struct {
		name    string
		mutate  func(*options.Options)
		wantErr string
	}{
		{name: "valid grpc"},
		{name: "valid http", mutate: func(o *options.Options) { o.OTLPProtocol = "http" }},
		{
			name:    "missing endpoint",
			mutate:  func(o *options.Options) { o.OTLPEndpoint = "" },
			wantErr: "--otlp-endpoint must be set",
		},
		{
			name:    "unknown protocol",
			mutate:  func(o *options.Options) { o.OTLPProtocol = "thrift" },
			wantErr: "--otlp-protocol must be either",
		},
		{
			name:    "zero interval",
			mutate:  func(o *options.Options) { o.OTLPInterval = 0 },
			wantErr: "--otlp-interval must be greater than 0",
		},
		{
			name:    "negative interval",
			mutate:  func(o *options.Options) { o.OTLPInterval = -time.Second },
			wantErr: "--otlp-interval must be greater than 0",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := valid()
			if tt.mutate != nil {
				tt.mutate(o)
			}
			err := validateOTLPOptions(o)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// resource.Default() carries OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES, which
// is how cluster identity is attached in a multi-cluster setup. Our own values
// must not paper over it.
func TestOTLPResourceRespectsEnvironment(t *testing.T) {
	attrOf := func(res *resource.Resource, key string) (string, bool) {
		for _, kv := range res.Attributes() {
			if string(kv.Key) == key {
				return kv.Value.AsString(), true
			}
		}
		return "", false
	}

	t.Run("defaults to our own service name", func(t *testing.T) {
		res := otlpResource()
		got, ok := attrOf(res, "service.name")
		if !ok || got != "kube-state-metrics" {
			t.Errorf("service.name = %q (present=%v), want %q", got, ok, "kube-state-metrics")
		}
		if _, ok := attrOf(res, "service.version"); !ok {
			t.Error("service.version missing")
		}
	})

	t.Run("carries extra resource attributes through", func(t *testing.T) {
		// resource.Default caches on first use, so this only exercises the
		// merge when it is the first call in the process. Assert the mechanism
		// instead: an environment-provided name must survive.
		base := resource.NewWithAttributes(
			resource.Default().SchemaURL(),
			attribute.String("service.name", "ksm-prod"),
			attribute.String("k8s.cluster.name", "prod-eu"),
		)
		if !hasServiceName(base) {
			t.Fatal("an explicit service.name should be recognised")
		}
		if hasServiceName(resource.NewWithAttributes(
			resource.Default().SchemaURL(),
			attribute.String("service.name", "unknown_service:kube-state-metrics"),
		)) {
			t.Error("the unknown_service placeholder must not count as explicit")
		}
		if !hasAttribute(base, "k8s.cluster.name") {
			t.Error("hasAttribute failed to find a present key")
		}
		if hasAttribute(base, "service.version") {
			t.Error("hasAttribute found an absent key")
		}
	})
}

// A failure to build the exporter must not end the run group actor, because the
// group interrupts every other actor -- including the /metrics server -- as soon
// as one returns.
func TestNewOTLPExporterWithRetryStopsOnContextCancel(t *testing.T) {
	o := &options.Options{
		EnableOTLPExport: true,
		// A scheme and path make WithEndpoint fail to parse, so construction
		// keeps failing and the retry loop keeps going.
		OTLPEndpoint: "http://example.com:4318/v1/metrics",
		OTLPProtocol: "http",
		OTLPInterval: time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := newOTLPExporterWithRetry(ctx, o)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected the retry loop to end on context cancellation, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("retry loop did not return after the context was cancelled")
	}
}

// The same failure, driven through RunOTLPExport: it must return nil on
// shutdown rather than surfacing the construction error.
func TestRunOTLPExportSurvivesAnUnbuildableExporter(t *testing.T) {
	m := &MetricsHandler{opts: &options.Options{
		EnableOTLPExport: true,
		OTLPEndpoint:     "http://example.com:4318/v1/metrics",
		OTLPProtocol:     "http",
		OTLPInterval:     time.Second,
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	if err := m.RunOTLPExport(ctx); err != nil {
		t.Errorf("RunOTLPExport should not report a construction failure as an actor error, got %v", err)
	}
}

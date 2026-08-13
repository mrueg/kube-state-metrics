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
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

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

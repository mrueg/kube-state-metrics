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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func findMetric(t *testing.T, ms []metricdata.Metrics, name string) metricdata.Metrics {
	t.Helper()
	for _, m := range ms {
		if m.Name == name {
			return m
		}
	}
	t.Fatalf("metric %q not found in %d exported metrics", name, len(ms))
	return metricdata.Metrics{}
}

func TestSelfMetricsConversion(t *testing.T) {
	reg := prometheus.NewRegistry()

	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "kube_state_metrics_list_total", Help: "Number of list calls."},
		[]string{"result"})
	counter.WithLabelValues("success").Add(3)
	gauge := prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "kube_state_metrics_list_objects", Help: "Objects listed."})
	gauge.Set(42)
	hist := prometheus.NewHistogram(
		prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "Durations.",
			Buckets: []float64{0.1, 1}})
	hist.Observe(0.05)
	hist.Observe(0.5)
	hist.Observe(5)
	// Summaries have no faithful OTLP equivalent and must be skipped.
	summary := prometheus.NewSummary(
		prometheus.SummaryOpts{Name: "legacy_summary_seconds", Help: "A summary."})
	summary.Observe(1)

	reg.MustRegister(counter, gauge, hist, summary)

	now := time.Unix(1700000000, 0)
	start := now.Add(-time.Hour)
	got := selfMetrics(reg, start, now)

	if len(got) != 3 {
		names := []string{}
		for _, m := range got {
			names = append(names, m.Name)
		}
		t.Fatalf("expected the summary to be skipped and 3 metrics kept, got %v", names)
	}

	c := findMetric(t, got, "kube_state_metrics_list_total")
	sum, ok := c.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("counter became %T, want a Sum", c.Data)
	}
	if !sum.IsMonotonic || sum.Temporality != metricdata.CumulativeTemporality {
		t.Errorf("counter should be a monotonic cumulative Sum, got monotonic=%v temporality=%v",
			sum.IsMonotonic, sum.Temporality)
	}
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 3 {
		t.Errorf("unexpected counter data points: %+v", sum.DataPoints)
	}
	if sum.DataPoints[0].StartTime.IsZero() {
		t.Error("counter data point has no StartTime")
	}
	if c.Description != "Number of list calls." {
		t.Errorf("help text not carried through: %q", c.Description)
	}

	g := findMetric(t, got, "kube_state_metrics_list_objects")
	if _, ok := g.Data.(metricdata.Gauge[float64]); !ok {
		t.Errorf("gauge became %T, want a Gauge", g.Data)
	}

	h := findMetric(t, got, "http_request_duration_seconds")
	hd, ok := h.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("histogram became %T, want a Histogram", h.Data)
	}
	dp := hd.DataPoints[0]
	// Prometheus reports cumulative buckets; OTLP wants per-bucket counts, and
	// one more count than bounds for the overflow.
	if len(dp.BucketCounts) != len(dp.Bounds)+1 {
		t.Errorf("bounds=%v bucketCounts=%v: expected one more count than bounds", dp.Bounds, dp.BucketCounts)
	}
	if want := []uint64{1, 1, 1}; len(dp.BucketCounts) != 3 ||
		dp.BucketCounts[0] != want[0] || dp.BucketCounts[1] != want[1] || dp.BucketCounts[2] != want[2] {
		t.Errorf("bucketCounts = %v, want %v (de-cumulated)", dp.BucketCounts, want)
	}
	if dp.Count != 3 || dp.Sum != 5.55 {
		t.Errorf("count=%d sum=%v, want 3 and 5.55", dp.Count, dp.Sum)
	}
}

func TestSelfMetricsNilGatherer(t *testing.T) {
	if got := selfMetrics(nil, time.Now(), time.Now()); got != nil {
		t.Errorf("expected no metrics without a gatherer, got %d", len(got))
	}
}

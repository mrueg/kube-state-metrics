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
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/klog/v2"
)

// SetSelfMetricsGatherer makes kube-state-metrics' own telemetry -- the
// kube_state_metrics_* families served on the telemetry port -- part of the OTLP
// export.
//
// Without it a push-only deployment has no view of the exporter's own health:
// kube_state_metrics_watch_total{result="error"} and the config-reload gauges
// are only reachable by scraping, which is the thing such a deployment does not
// do.
func (m *MetricsHandler) SetSelfMetricsGatherer(g prometheus.Gatherer) {
	m.selfGatherer = g
}

// selfMetrics converts the self-telemetry registry into OTLP metrics.
//
// Unlike the object stores, a prometheus.Gatherer already returns one family per
// metric name, so these do not go through otlpAggregator.
func selfMetrics(gatherer prometheus.Gatherer, startTime, now time.Time) []metricdata.Metrics {
	if gatherer == nil {
		return nil
	}

	families, err := gatherer.Gather()
	if err != nil {
		// Gather reports partial results alongside the error, so keep whatever
		// was collected rather than dropping the whole batch.
		klog.ErrorS(err, "Failed to gather kube-state-metrics' own metrics for OTLP export")
	}

	out := make([]metricdata.Metrics, 0, len(families))
	for _, mf := range families {
		md, ok := convertPrometheusFamily(mf, startTime, now)
		if !ok {
			continue
		}
		out = append(out, md)
	}
	return out
}

func convertPrometheusFamily(mf *dto.MetricFamily, startTime, now time.Time) (metricdata.Metrics, bool) {
	md := metricdata.Metrics{
		Name:        mf.GetName(),
		Description: mf.GetHelp(),
	}

	switch mf.GetType() {
	case dto.MetricType_GAUGE:
		points := make([]metricdata.DataPoint[float64], 0, len(mf.GetMetric()))
		for _, m := range mf.GetMetric() {
			points = append(points, numberPoint(m, m.GetGauge().GetValue(), startTime, now))
		}
		md.Data = metricdata.Gauge[float64]{DataPoints: points}

	case dto.MetricType_UNTYPED:
		points := make([]metricdata.DataPoint[float64], 0, len(mf.GetMetric()))
		for _, m := range mf.GetMetric() {
			points = append(points, numberPoint(m, m.GetUntyped().GetValue(), startTime, now))
		}
		md.Data = metricdata.Gauge[float64]{DataPoints: points}

	case dto.MetricType_COUNTER:
		points := make([]metricdata.DataPoint[float64], 0, len(mf.GetMetric()))
		for _, m := range mf.GetMetric() {
			points = append(points, numberPoint(m, m.GetCounter().GetValue(), startTime, now))
		}
		md.Data = metricdata.Sum[float64]{
			DataPoints:  points,
			IsMonotonic: true,
			Temporality: metricdata.CumulativeTemporality,
		}

	case dto.MetricType_HISTOGRAM:
		points := make([]metricdata.HistogramDataPoint[float64], 0, len(mf.GetMetric()))
		for _, m := range mf.GetMetric() {
			points = append(points, histogramPoint(m, startTime, now))
		}
		md.Data = metricdata.Histogram[float64]{
			DataPoints:  points,
			Temporality: metricdata.CumulativeTemporality,
		}

	default:
		// Summaries carry quantiles rather than buckets and have no faithful
		// OTLP equivalent -- the protocol's Summary type exists only for
		// backwards compatibility and is not meant for new data.
		klog.V(4).InfoS("Skipping a self metric with a type that does not map to OTLP",
			"metric", mf.GetName(), "type", mf.GetType().String())
		return metricdata.Metrics{}, false
	}

	return md, true
}

func numberPoint(m *dto.Metric, value float64, startTime, now time.Time) metricdata.DataPoint[float64] {
	return metricdata.DataPoint[float64]{
		Attributes: labelsToAttributes(m.GetLabel()),
		Value:      value,
		StartTime:  startTime,
		Time:       now,
	}
}

// histogramPoint converts Prometheus' cumulative buckets into OTLP's explicit
// bounds plus per-bucket counts.
func histogramPoint(m *dto.Metric, startTime, now time.Time) metricdata.HistogramDataPoint[float64] {
	h := m.GetHistogram()

	var (
		bounds       []float64
		bucketCounts []uint64
		previous     uint64
	)
	for _, b := range h.GetBucket() {
		count := b.GetCumulativeCount()
		// The +Inf bucket is the overflow count and carries no bound.
		if !math.IsInf(b.GetUpperBound(), 1) {
			bounds = append(bounds, b.GetUpperBound())
		}
		bucketCounts = append(bucketCounts, count-previous)
		previous = count
	}
	// Prometheus may omit the explicit +Inf bucket; OTLP always expects one more
	// count than bounds.
	if len(bucketCounts) == len(bounds) {
		bucketCounts = append(bucketCounts, h.GetSampleCount()-previous)
	}

	return metricdata.HistogramDataPoint[float64]{
		Attributes:   labelsToAttributes(m.GetLabel()),
		StartTime:    startTime,
		Time:         now,
		Count:        h.GetSampleCount(),
		Sum:          h.GetSampleSum(),
		Bounds:       bounds,
		BucketCounts: bucketCounts,
	}
}

func labelsToAttributes(pairs []*dto.LabelPair) attribute.Set {
	kv := make([]attribute.KeyValue, 0, len(pairs))
	for _, p := range pairs {
		kv = append(kv, attribute.String(p.GetName(), p.GetValue()))
	}
	return attribute.NewSet(kv...)
}

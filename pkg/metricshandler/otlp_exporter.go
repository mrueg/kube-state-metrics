/*
Copyright 2021 The Kubernetes Authors All rights reserved.

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
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/klog/v2"

	ksmmetric "k8s.io/kube-state-metrics/v2/pkg/metric"
)

// RunOTLPExport runs the OTLP exporter loop.
func (m *MetricsHandler) RunOTLPExport(ctx context.Context) error {
	if !m.opts.EnableOTLPExport {
		return nil
	}

	klog.InfoS("Starting OTLP exporter", "endpoint", m.opts.OTLPEndpoint, "protocol", m.opts.OTLPProtocol, "interval", m.opts.OTLPInterval)

	var exporter metric.Exporter
	var err error

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if m.opts.OTLPProtocol == "http" {
		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(m.opts.OTLPEndpoint),
		}
		if m.opts.OTLPURLPath != "" {
			opts = append(opts, otlpmetrichttp.WithURLPath(m.opts.OTLPURLPath))
		}
		if m.opts.OTLPInsecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		exporter, err = otlpmetrichttp.New(ctxTimeout, opts...)
	} else {
		opts := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(m.opts.OTLPEndpoint),
		}
		if m.opts.OTLPInsecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		exporter, err = otlpmetricgrpc.New(ctxTimeout, opts...)
	}

	if err != nil {
		return err
	}

	ticker := time.NewTicker(m.opts.OTLPInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return exporter.Shutdown(context.Background())
		case <-ticker.C:
			rm := m.gatherOTLPMetrics()
			if rm != nil {
				if err := exporter.Export(ctx, rm); err != nil {
					klog.ErrorS(err, "Failed to export metrics to OTLP")
				}
			}
		}
	}
}

func (m *MetricsHandler) gatherOTLPMetrics() *metricdata.ResourceMetrics {
	m.mtx.RLock()
	writers := m.metricsWriters
	m.mtx.RUnlock()

	if len(writers) == 0 {
		return nil
	}

	var allMetrics []metricdata.Metrics

	for _, w := range writers {
		for _, s := range w.Stores() {
			families := s.Export()
			for _, familyList := range families {
				for _, f := range familyList {
					f.Inspect(func(fam ksmmetric.Family) {
						md := convertToOTLP(fam)
						allMetrics = append(allMetrics, md)
					})
				}
			}
		}
	}

	return &metricdata.ResourceMetrics{
		ScopeMetrics: []metricdata.ScopeMetrics{
			{
				Scope:   pluginScope,
				Metrics: allMetrics,
			},
		},
	}
}

var pluginScope = instrumentation.Scope{
	Name:    "k8s.io/kube-state-metrics",
	Version: "v2",
}

func convertToOTLP(f ksmmetric.Family) metricdata.Metrics {
	md := metricdata.Metrics{
		Name:        f.Name,
		Description: "", // Help text not easily available here, assuming consistent
		Unit:        "",
	}

	switch f.Type {
	case ksmmetric.Gauge, ksmmetric.Info, ksmmetric.StateSet:
		dataPoints := make([]metricdata.DataPoint[float64], 0, len(f.Metrics))
		for _, m := range f.Metrics {
			dataPoints = append(dataPoints, metricdata.DataPoint[float64]{
				Attributes: createAttributes(m.LabelKeys, m.LabelValues),
				Value:      m.Value,
				Time:       time.Now(),
			})
		}
		md.Data = metricdata.Gauge[float64]{
			DataPoints: dataPoints,
		}
	case ksmmetric.Counter:
		dataPoints := make([]metricdata.DataPoint[float64], 0, len(f.Metrics))
		for _, m := range f.Metrics {
			dataPoints = append(dataPoints, metricdata.DataPoint[float64]{
				Attributes: createAttributes(m.LabelKeys, m.LabelValues),
				Value:      m.Value,
				Time:       time.Now(),
			})
		}
		md.Data = metricdata.Sum[float64]{
			DataPoints:  dataPoints,
			IsMonotonic: true,
			Temporality: metricdata.CumulativeTemporality,
		}
	}
	return md
}

func createAttributes(keys, values []string) attribute.Set {
	kv := make([]attribute.KeyValue, 0, len(keys))
	for i, k := range keys {
		if i < len(values) {
			kv = append(kv, attribute.String(k, values[i]))
		}
	}
	return attribute.NewSet(kv...)
}

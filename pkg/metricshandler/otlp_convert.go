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
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"k8s.io/klog/v2"

	ksmmetric "k8s.io/kube-state-metrics/v2/pkg/metric"
	metricsstore "k8s.io/kube-state-metrics/v2/pkg/metrics_store"
)

// Estimated encoded sizes, in bytes, of the parts of an export request that
// are not label keys and values. They err on the large side, so that a request
// stays below the size it is estimated at.
const (
	// otlpPointOverhead covers a data point's timestamps, value and framing.
	otlpPointOverhead = 40
	// otlpAttributeOverhead covers the framing of one string attribute.
	otlpAttributeOverhead = 12
	// otlpMetricOverhead covers a metric's framing and type fields.
	otlpMetricOverhead = 24
)

// exportOTLP sends the metrics of every store, followed by kube-state-metrics'
// own telemetry, to exporter.
//
// The metrics are read from the families the stores render for /metrics, so
// the export carries exactly the series a scrape would. They are sent in
// requests of an estimated maxBytes or less, built and sent one at a time, so
// the memory an export takes is bounded by one request rather than the size of
// the cluster. The first request that fails ends the export.
func (m *MetricsHandler) exportOTLP(ctx context.Context, exporter metric.Exporter, res *resource.Resource, startTime, now time.Time, maxBytes int) error {
	m.mtx.RLock()
	writers := m.metricsWriters
	m.mtx.RUnlock()

	b := newOTLPBatcher(ctx, exporter, res, startTime, now, maxBytes)
	var objects [][][]byte
	for _, w := range writers {
		objects = objects[:0]
		w.RangeObjects(func(families [][]byte) bool {
			objects = append(objects, families)
			return true
		})
		if len(objects) == 0 {
			continue
		}
		// Walk the families one at a time, as WriteAll does, so the data
		// points of a metric are grouped into as few OTLP metrics as possible.
		for i, family := range w.Families() {
			cumulative, ok := otlpCumulative(family.Type)
			if !ok {
				klog.V(2).InfoS("Skipping metric family with a type OTLP export does not support", "family", family.Name, "type", family.Type)
				continue
			}
			for _, families := range objects {
				if i >= len(families) {
					continue
				}
				if err := b.addFamily(family, cumulative, families[i]); err != nil {
					return err
				}
			}
		}
	}
	b.extra = selfMetrics(m.selfGatherer, startTime, now)
	return b.flush()
}

// otlpCumulative reports whether a family of the given type is exported as a
// cumulative sum rather than a gauge, and whether it can be exported at all.
func otlpCumulative(familyType string) (cumulative, ok bool) {
	switch ksmmetric.Type(familyType) {
	case ksmmetric.Counter:
		return true, true
	case ksmmetric.Gauge, ksmmetric.Info, ksmmetric.StateSet:
		return false, true
	default:
		return false, false
	}
}

// otlpBatcher collects data points into export requests and sends each request
// once it reaches its size limit.
type otlpBatcher struct {
	ctx       context.Context
	exporter  metric.Exporter
	res       *resource.Resource
	startTime time.Time
	now       time.Time
	maxBytes  int

	// size is the estimated encoded size of the pending request.
	size int
	// metrics are the metrics of the pending request, in the order they were
	// first seen, and byName indexes them.
	metrics []*otlpMetric
	byName  map[string]*otlpMetric
	// extra are complete metrics sent with the last request.
	extra []metricdata.Metrics

	// free holds data point slices of sent requests for reuse.
	free [][]metricdata.DataPoint[float64]
	// keys interns label keys, which repeat on every line of a family.
	keys map[string]string
	// kvs is reused to collect the attributes of a line.
	kvs []attribute.KeyValue
}

type otlpMetric struct {
	name        string
	description string
	cumulative  bool
	points      []metricdata.DataPoint[float64]
}

func newOTLPBatcher(ctx context.Context, exporter metric.Exporter, res *resource.Resource, startTime, now time.Time, maxBytes int) *otlpBatcher {
	return &otlpBatcher{
		ctx:       ctx,
		exporter:  exporter,
		res:       res,
		startTime: startTime,
		now:       now,
		maxBytes:  maxBytes,
		byName:    map[string]*otlpMetric{},
		keys:      map[string]string{},
	}
}

// addFamily adds a data point for every line of a rendered family, sending the
// pending request first whenever a point would take it past the size limit.
func (b *otlpBatcher) addFamily(family metricsstore.FamilyMetadata, cumulative bool, rendered []byte) error {
	for len(rendered) > 0 {
		line := rendered
		if nl := bytes.IndexByte(rendered, '\n'); nl >= 0 {
			line, rendered = rendered[:nl], rendered[nl+1:]
		} else {
			rendered = nil
		}
		if len(line) == 0 {
			continue
		}

		name, attrs, value, size, ok := b.parseLine(line)
		if !ok {
			klog.V(4).InfoS("Skipping a metric line OTLP export cannot parse", "family", family.Name, "line", string(line))
			continue
		}
		// The name of a line is the name of its family, so it costs no
		// allocation in the common case.
		metricName := family.Name
		if string(name) != family.Name {
			metricName = string(name)
		}

		m, exists := b.byName[metricName]
		if !exists {
			size += otlpMetricOverhead + len(metricName) + len(family.Help)
		}
		if b.size > 0 && b.size+size > b.maxBytes {
			if err := b.flush(); err != nil {
				return err
			}
			m, exists = nil, false
			size += otlpMetricOverhead + len(metricName) + len(family.Help)
		}
		if !exists {
			m = &otlpMetric{name: metricName, description: family.Help, cumulative: cumulative}
			if n := len(b.free); n > 0 {
				m.points, b.free = b.free[n-1], b.free[:n-1]
			}
			b.byName[metricName] = m
			b.metrics = append(b.metrics, m)
		}

		dp := metricdata.DataPoint[float64]{Attributes: attrs, Value: value, Time: b.now}
		// A start time only means something for a cumulative sum: it is the
		// point the sum counts from.
		if cumulative {
			dp.StartTime = b.startTime
		}
		m.points = append(m.points, dp)
		b.size += size
	}
	return nil
}

// flush sends the pending request, if any, and resets the batcher for the next.
func (b *otlpBatcher) flush() error {
	if len(b.metrics) == 0 && len(b.extra) == 0 {
		return nil
	}

	ms := make([]metricdata.Metrics, 0, len(b.metrics)+len(b.extra))
	for _, m := range b.metrics {
		md := metricdata.Metrics{Name: m.name, Description: m.description}
		if m.cumulative {
			md.Data = metricdata.Sum[float64]{
				DataPoints:  m.points,
				IsMonotonic: true,
				Temporality: metricdata.CumulativeTemporality,
			}
		} else {
			md.Data = metricdata.Gauge[float64]{DataPoints: m.points}
		}
		ms = append(ms, md)
	}
	ms = append(ms, b.extra...)

	err := b.exporter.Export(b.ctx, &metricdata.ResourceMetrics{
		Resource: b.res,
		ScopeMetrics: []metricdata.ScopeMetrics{
			{Scope: pluginScope, Metrics: ms},
		},
	})

	// Export does not retain the request, so its data point slices can hold
	// the next one.
	for _, m := range b.metrics {
		clear(m.points)
		b.free = append(b.free, m.points[:0])
	}
	clear(b.metrics)
	b.metrics = b.metrics[:0]
	clear(b.byName)
	b.extra = nil
	b.size = 0
	return err
}

// parseLine parses one line of the text exposition format as kube-state-metrics
// renders it: a name, optional labels and a value. It returns the attributes of
// the labels, and the estimated encoded size of the resulting data point.
func (b *otlpBatcher) parseLine(line []byte) (name []byte, attrs attribute.Set, value float64, size int, ok bool) {
	end := bytes.IndexAny(line, "{ ")
	if end <= 0 {
		return nil, attrs, 0, 0, false
	}
	name, rest := line[:end], line[end:]
	size = otlpPointOverhead

	kvs := b.kvs[:0]
	if rest[0] == '{' {
		rest = rest[1:]
		for len(rest) > 0 && rest[0] != '}' {
			eq := bytes.IndexByte(rest, '=')
			if eq <= 0 || eq+1 >= len(rest) || rest[eq+1] != '"' {
				return nil, attrs, 0, 0, false
			}
			key, ok := b.keys[string(rest[:eq])]
			if !ok {
				key = string(rest[:eq])
				b.keys[key] = key
			}
			val, n, ok := unquoteLabelValue(rest[eq+2:])
			if !ok {
				return nil, attrs, 0, 0, false
			}
			kvs = append(kvs, attribute.String(key, val))
			size += otlpAttributeOverhead + len(key) + len(val)
			rest = rest[eq+2+n:]
			if len(rest) > 0 && rest[0] == ',' {
				rest = rest[1:]
			}
		}
		if len(rest) == 0 {
			return nil, attrs, 0, 0, false
		}
		rest = rest[1:]
	}
	b.kvs = kvs

	rest = bytes.TrimLeft(rest, " ")
	// A timestamp may follow the value; the data point carries the time of
	// the export instead.
	if sp := bytes.IndexByte(rest, ' '); sp >= 0 {
		rest = rest[:sp]
	}
	value, err := strconv.ParseFloat(string(rest), 64)
	if err != nil {
		return nil, attrs, 0, 0, false
	}
	return name, attribute.NewSet(kvs...), value, size, true
}

// unquoteLabelValue returns the label value that starts after an opening quote,
// with the escaping of backslashes, quotes and newlines reversed, and the number
// of bytes up to and including its closing quote.
func unquoteLabelValue(b []byte) (value string, n int, ok bool) {
	escaped := false
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '\\':
			escaped = true
			i++
		case '"':
			if !escaped {
				return string(b[:i]), i + 1, true
			}
			return labelValueUnescaper.Replace(string(b[:i])), i + 1, true
		}
	}
	return "", 0, false
}

// labelValueUnescaper reverses the escaping pkg/metric applies to label values.
var labelValueUnescaper = strings.NewReplacer(`\\`, `\`, `\"`, `"`, `\n`, "\n")

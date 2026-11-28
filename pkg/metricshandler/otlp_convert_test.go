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
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	ksmmetric "k8s.io/kube-state-metrics/v2/pkg/metric"
	metricsstore "k8s.io/kube-state-metrics/v2/pkg/metrics_store"
	"k8s.io/kube-state-metrics/v2/pkg/options"
)

// recordingExporter records the data points of every request it is handed. The
// batcher reuses a request's slices once Export returns, so they are copied.
type recordingExporter struct {
	metric.Exporter
	mu       sync.Mutex
	requests [][]recordedMetric
	err      error
}

type recordedMetric struct {
	name, description string
	cumulative        bool
	points            []metricdata.DataPoint[float64]
}

func (e *recordingExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var request []recordedMetric
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			r := recordedMetric{name: m.Name, description: m.Description}
			switch data := m.Data.(type) {
			case metricdata.Gauge[float64]:
				r.points = append(r.points, data.DataPoints...)
			case metricdata.Sum[float64]:
				r.cumulative = true
				r.points = append(r.points, data.DataPoints...)
			}
			request = append(request, r)
		}
	}
	e.requests = append(e.requests, request)
	return e.err
}

func newOTLPTestHandler(t testing.TB, stores ...*metricsstore.MetricsStore) *MetricsHandler {
	t.Helper()
	stub := &stubStoreBuilder{buildWriters: metricsstore.MetricsWriterList{metricsstore.NewMetricsWriter("test", stores...)}}
	h := New(&options.Options{}, fake.NewSimpleClientset(), stub, false)
	h.ConfigureSharding(context.Background(), 0, 1)
	return h
}

func testPod(uid string) *v1.Pod {
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: uid, Namespace: "ns"}}
}

func TestExportOTLPConvertsRenderedFamilies(t *testing.T) {
	headers := []string{
		"# HELP kube_test_restarts_total Restarts.\n# TYPE kube_test_restarts_total counter",
		"# HELP kube_test_value A value,\\nover two lines.\n# TYPE kube_test_value gauge",
		"# HELP kube_test_info Information.\n# TYPE kube_test_info info",
		"# HELP kube_test_phase Phase.\n# TYPE kube_test_phase stateset",
		"# HELP kube_test_histogram Not supported.\n# TYPE kube_test_histogram histogram",
	}
	gen := func(obj any) []ksmmetric.FamilyInterface {
		name := string(obj.(*v1.Pod).UID)
		family := func(name string, keys, values []string, value float64) *ksmmetric.Family {
			return &ksmmetric.Family{Name: name, Metrics: []*ksmmetric.Metric{{LabelKeys: keys, LabelValues: values, Value: value}}}
		}
		return []ksmmetric.FamilyInterface{
			family("kube_test_restarts_total", []string{"pod"}, []string{name}, 3),
			family("kube_test_value", []string{"pod", "note"}, []string{name, "a \"quoted\" {value},\nwith a \\ backslash"}, 0.5),
			family("kube_test_info", []string{"pod"}, []string{name}, 1),
			family("kube_test_phase", nil, nil, 0),
			family("kube_test_histogram", []string{"pod"}, []string{name}, 1),
		}
	}
	store := metricsstore.NewMetricsStore(headers, gen)
	for _, uid := range []string{"a", "b"} {
		if err := store.Add(testPod(uid)); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Unix(1700000000, 0)
	start := now.Add(-time.Hour)
	exp := &recordingExporter{}
	if err := newOTLPTestHandler(t, store).exportOTLP(context.Background(), exp, resource.Empty(), start, now, otlpMaxRequestBytes); err != nil {
		t.Fatal(err)
	}

	if len(exp.requests) != 1 {
		t.Fatalf("expected one request, got %d", len(exp.requests))
	}
	got := map[string]recordedMetric{}
	var order []string
	for _, m := range exp.requests[0] {
		got[m.name] = m
		order = append(order, m.name)
	}
	if want := "kube_test_restarts_total kube_test_value kube_test_info kube_test_phase"; strings.Join(order, " ") != want {
		t.Fatalf("metrics = %q, want one metric per supported family in header order, %q", strings.Join(order, " "), want)
	}

	for _, name := range order {
		m := got[name]
		if len(m.points) != 2 {
			t.Errorf("%s: %d data points, want one per object", name, len(m.points))
		}
		if wantCumulative := name == "kube_test_restarts_total"; m.cumulative != wantCumulative {
			t.Errorf("%s: cumulative = %v, want %v", name, m.cumulative, wantCumulative)
		}
		for _, dp := range m.points {
			if !dp.Time.Equal(now) {
				t.Errorf("%s: time = %v, want %v", name, dp.Time, now)
			}
			// A start time only means something for a cumulative sum.
			if wantStart := m.cumulative; dp.StartTime.Equal(start) != wantStart || (!wantStart && !dp.StartTime.IsZero()) {
				t.Errorf("%s: start time = %v, cumulative %v", name, dp.StartTime, m.cumulative)
			}
		}
	}

	if d := got["kube_test_value"].description; d != "A value,\nover two lines." {
		t.Errorf("description = %q, want the unescaped help text", d)
	}
	dp := got["kube_test_value"].points[0]
	if v, _ := dp.Attributes.Value("note"); v.AsString() != "a \"quoted\" {value},\nwith a \\ backslash" {
		t.Errorf("note = %q, want the unescaped label value", v.AsString())
	}
	if dp.Value != 0.5 {
		t.Errorf("value = %v, want 0.5", dp.Value)
	}
	if n := got["kube_test_phase"].points[0].Attributes.Len(); n != 0 {
		t.Errorf("a metric without labels has %d attributes", n)
	}
}

// countPoints returns the number of data points in an OTLP export request.
func countPoints(t *testing.T, body []byte) int {
	t.Helper()
	var req collectormetrics.ExportMetricsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				n += len(m.GetGauge().GetDataPoints()) + len(m.GetSum().GetDataPoints())
			}
		}
	}
	return n
}

// A cluster's metrics do not fit in one request a collector accepts, so they
// are split into requests of a bounded size. Checked against the real OTLP/HTTP
// exporter, so that the size estimate is checked against the encoding.
func TestExportOTLPSplitsIntoBoundedRequests(t *testing.T) {
	store := newBenchStore(t, 200, 40)

	var scrape bytes.Buffer
	if err := metricsstore.NewMetricsWriter("test", store).WriteAll(&scrape); err != nil {
		t.Fatal(err)
	}
	wantPoints := 0
	for line := range strings.Lines(scrape.String()) {
		if !strings.HasPrefix(line, "#") {
			wantPoints++
		}
	}

	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
	}))
	defer srv.Close()
	exp, err := otlpmetrichttp.New(context.Background(), otlpmetrichttp.WithEndpointURL(srv.URL+"/v1/metrics"))
	if err != nil {
		t.Fatal(err)
	}

	const maxBytes = 64 << 10
	now := time.Now()
	if err := newOTLPTestHandler(t, store).exportOTLP(context.Background(), exp, resource.Default(), now, now, maxBytes); err != nil {
		t.Fatal(err)
	}

	if len(bodies) < 2 {
		t.Fatalf("expected the export to be split, got %d requests", len(bodies))
	}
	gotPoints := 0
	for i, body := range bodies {
		if len(body) > maxBytes {
			t.Errorf("request %d is %d bytes, above the %d byte limit", i, len(body), maxBytes)
		}
		gotPoints += countPoints(t, body)
	}
	if gotPoints != wantPoints {
		t.Errorf("exported %d data points, want the %d series a scrape returns", gotPoints, wantPoints)
	}
}

// Once a request fails, the rest of the export would most likely fail the same
// way, each after a timeout, so it is abandoned.
func TestExportOTLPStopsAtFirstFailedRequest(t *testing.T) {
	exp := &recordingExporter{err: errors.New("collector unavailable")}
	now := time.Now()
	err := newOTLPTestHandler(t, newBenchStore(t, 100, 40)).exportOTLP(context.Background(), exp, resource.Empty(), now, now, 16<<10)
	if err == nil {
		t.Fatal("expected the export to fail")
	}
	if len(exp.requests) != 1 {
		t.Errorf("sent %d requests, want the export to stop after the first failed", len(exp.requests))
	}
}

func TestExportOTLPWithoutObjectsSendsNothing(t *testing.T) {
	exp := &recordingExporter{}
	now := time.Now()
	store := metricsstore.NewMetricsStore([]string{"# HELP kube_x X.\n# TYPE kube_x gauge"}, nil)
	if err := newOTLPTestHandler(t, store).exportOTLP(context.Background(), exp, resource.Empty(), now, now, otlpMaxRequestBytes); err != nil {
		t.Fatal(err)
	}
	if len(exp.requests) != 0 {
		t.Errorf("sent %d requests for an empty store", len(exp.requests))
	}
}

func TestParseLineRejectsMalformedLines(t *testing.T) {
	b := newOTLPBatcher(context.Background(), nil, nil, time.Time{}, time.Time{}, 0)
	for _, line := range []string{
		`kube_x{a="1"`,
		`kube_x{a="1} 1`,
		`kube_x{a=1} 1`,
		`kube_x{a="1"} one`,
		`{a="1"} 1`,
	} {
		if _, _, _, _, ok := b.parseLine([]byte(line)); ok {
			t.Errorf("parseLine(%q) succeeded, want it rejected", line)
		}
	}
	for line, want := range map[string]float64{
		`kube_x 2`:                 2,
		`kube_x{} 3`:               3,
		`kube_x{a="1",b="2"} 4`:    4,
		`kube_x{a="1"} 5 12345678`: 5,
		`kube_x +Inf`:              math.Inf(1),
	} {
		_, _, got, _, ok := b.parseLine([]byte(line))
		if !ok || got != want {
			t.Errorf("parseLine(%q) = %v, %v, want %v", line, got, ok, want)
		}
	}
}

func BenchmarkExportOTLP(b *testing.B) {
	h := newOTLPTestHandler(b, newBenchStore(b, 1000, 40))

	var requests, maxBody, total int64
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		requests++
		total += n
		maxBody = max(maxBody, n)
	}))
	defer srv.Close()
	exp, err := otlpmetrichttp.New(context.Background(), otlpmetrichttp.WithEndpointURL(srv.URL+"/v1/metrics"))
	if err != nil {
		b.Fatal(err)
	}

	res := resource.Default()
	b.ReportAllocs()
	for b.Loop() {
		now := time.Now()
		if err := h.exportOTLP(context.Background(), exp, res, now, now, otlpMaxRequestBytes); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(requests)/float64(b.N), "req/op")
	b.ReportMetric(float64(maxBody), "max-req-bytes")
	b.ReportMetric(float64(total)/float64(b.N), "wire-bytes/op")
}

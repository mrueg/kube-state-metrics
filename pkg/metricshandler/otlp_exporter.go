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
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/prometheus/common/version"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"k8s.io/klog/v2"

	"k8s.io/kube-state-metrics/v2/pkg/options"
)

const (
	// otlpExporterInitTimeout bounds construction of the exporter.
	otlpExporterInitTimeout = 5 * time.Second
	// otlpInitialRetryBackoff and otlpMaxRetryBackoff bound the wait between
	// attempts to construct the exporter.
	otlpInitialRetryBackoff = time.Second
	otlpMaxRetryBackoff     = time.Minute
	// otlpShutdownTimeout bounds the final flush. The context handed to
	// RunOTLPExport is already cancelled by then, so without a bound of its own
	// an unreachable collector would stall process shutdown indefinitely.
	otlpShutdownTimeout = 5 * time.Second
	// otlpMaxRequestBytes bounds the estimated size of one export request. The
	// metrics of a cluster are far larger than the 4 MiB the OpenTelemetry
	// Collector accepts in one gRPC message by default, so they are sent in
	// requests of about this size.
	otlpMaxRequestBytes = 2 << 20
)

var pluginScope = instrumentation.Scope{
	Name:    "k8s.io/kube-state-metrics",
	Version: version.Version,
}

// validateOTLPOptions checks the OTLP flags. Options.Validate covers the same
// ground, but it is unreachable from main today (it is called after Parse,
// which never returns), and an interval of zero reaches time.NewTicker, which
// panics. Checking here keeps a misconfiguration a startup error.
func validateOTLPOptions(opts *options.Options) error {
	if opts.OTLPEndpoint == "" {
		return errors.New("--otlp-endpoint must be set when --enable-otlp-export is true")
	}
	if opts.OTLPProtocol != "grpc" && opts.OTLPProtocol != "http" {
		return fmt.Errorf("--otlp-protocol must be either %q or %q, got %q", "grpc", "http", opts.OTLPProtocol)
	}
	if opts.OTLPInterval <= 0 {
		return fmt.Errorf("--otlp-interval must be greater than 0, got %s", opts.OTLPInterval)
	}
	if opts.OTLPCompression != "" && opts.OTLPCompression != "gzip" && opts.OTLPCompression != "none" {
		return fmt.Errorf("--otlp-compression must be either %q or %q, got %q", "gzip", "none", opts.OTLPCompression)
	}
	return nil
}

// RunOTLPExport runs the OTLP exporter loop.
func (m *MetricsHandler) RunOTLPExport(ctx context.Context) error {
	if !m.opts.EnableOTLPExport {
		return nil
	}

	if err := validateOTLPOptions(m.opts); err != nil {
		return err
	}

	klog.InfoS("Starting OTLP exporter", "endpoint", m.opts.OTLPEndpoint, "protocol", m.opts.OTLPProtocol, "interval", m.opts.OTLPInterval)

	exporter, err := newOTLPExporterWithRetry(ctx, m.opts)
	if err != nil {
		// The context was cancelled while retrying, i.e. shutdown.
		return nil
	}

	res := otlpResource(m.opts)

	// Cumulative sums are only interpretable against the point in time the
	// counters started from. Every export reports the same start, which is when
	// this exporter came up.
	startTime := time.Now()

	ticker := time.NewTicker(m.opts.OTLPInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return shutdownOTLPExporter(exporter)
		case <-ticker.C:
			if err := m.exportOTLP(ctx, exporter, res, startTime, time.Now(), otlpMaxRequestBytes); err != nil {
				klog.ErrorS(err, "Failed to export metrics to OTLP")
			}
		}
	}
}

// otlpResource describes the process the metrics come from.
//
// resource.Default() carries the OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES
// values, which is how an operator attaches cluster identity in a multi-cluster
// setup, so it is the base rather than something to replace. Our own values are
// layered on top only where the environment has not already supplied them --
// resource.Default() falls back to "unknown_service:<binary>" when nothing names
// the service, and that fallback should lose to our name but an explicit
// setting should win.
func otlpResource(opts *options.Options) *resource.Resource {
	base := resource.Default()

	var attrs []attribute.KeyValue
	if !hasServiceName(base) {
		attrs = append(attrs, attribute.String("service.name", "kube-state-metrics"))
	}
	if !hasAttribute(base, "service.version") {
		attrs = append(attrs, attribute.String("service.version", version.Version))
	}
	// Without an instance id every replica reports the same resource identity.
	// Prometheus derives the instance label from it, so a sharded deployment
	// would have all shards write the same target_info series, interleaving
	// samples from N writers into one timeline.
	if !hasAttribute(base, "service.instance.id") {
		if id := instanceID(opts); id != "" {
			attrs = append(attrs, attribute.String("service.instance.id", id))
		}
	}
	if len(attrs) == 0 {
		return base
	}

	merged, err := resource.Merge(base, resource.NewWithAttributes(base.SchemaURL(), attrs...))
	if err != nil {
		klog.ErrorS(err, "Failed to build the OTLP resource, falling back to the default")
		return base
	}
	return merged
}

// instanceID identifies this replica. --pod is set from the downward API for
// autosharding; outside that, the hostname is the pod name in a cluster.
func instanceID(opts *options.Options) string {
	if opts != nil && opts.Pod != "" {
		return opts.Pod
	}
	host, err := os.Hostname()
	if err != nil {
		klog.ErrorS(err, "Failed to determine the hostname for service.instance.id")
		return ""
	}
	return host
}

func hasAttribute(res *resource.Resource, key string) bool {
	for _, kv := range res.Attributes() {
		if string(kv.Key) == key {
			return true
		}
	}
	return false
}

// hasServiceName reports whether the resource names the service as something
// other than resource.Default()'s "unknown_service:<binary>" placeholder.
func hasServiceName(res *resource.Resource) bool {
	for _, kv := range res.Attributes() {
		if string(kv.Key) == "service.name" {
			return !strings.HasPrefix(kv.Value.AsString(), "unknown_service")
		}
	}
	return false
}

// newOTLPExporterWithRetry keeps trying until the exporter is built or the
// context is cancelled.
//
// Returning the error instead would end this run group actor, and the group
// interrupts every other actor as soon as one returns -- so a push-path problem
// would take the /metrics endpoint down with it. Flag validation still fails
// fast, because that is a deterministic operator error caught before anything
// starts; a construction failure is not necessarily permanent.
func newOTLPExporterWithRetry(ctx context.Context, opts *options.Options) (metric.Exporter, error) {
	backoff := otlpInitialRetryBackoff
	for {
		exporter, err := newOTLPExporter(ctx, opts)
		if err == nil {
			return exporter, nil
		}

		klog.ErrorS(err, "Failed to create the OTLP exporter, retrying",
			"endpoint", opts.OTLPEndpoint, "protocol", opts.OTLPProtocol, "retryIn", backoff)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}

		if backoff *= 2; backoff > otlpMaxRetryBackoff {
			backoff = otlpMaxRetryBackoff
		}
	}
}

// shutdownOTLPExporter flushes and closes the exporter. The context that drove
// the export loop is already cancelled by the time this runs, so the flush gets
// a fresh, bounded one of its own -- otherwise an unreachable collector would
// hold up process shutdown.
func shutdownOTLPExporter(exporter metric.Exporter) error {
	ctx, cancel := context.WithTimeout(context.Background(), otlpShutdownTimeout)
	defer cancel()
	return exporter.Shutdown(ctx)
}

func newOTLPExporter(ctx context.Context, opts *options.Options) (metric.Exporter, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, otlpExporterInitTimeout)
	defer cancel()

	if opts.OTLPProtocol == "http" {
		httpOpts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(opts.OTLPEndpoint),
		}
		if opts.OTLPURLPath != "" {
			httpOpts = append(httpOpts, otlpmetrichttp.WithURLPath(opts.OTLPURLPath))
		}
		if opts.OTLPInsecure {
			httpOpts = append(httpOpts, otlpmetrichttp.WithInsecure())
		}
		if opts.OTLPCompression == "gzip" {
			httpOpts = append(httpOpts, otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression))
		}
		return otlpmetrichttp.New(ctxTimeout, httpOpts...)
	}

	grpcOpts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(opts.OTLPEndpoint),
	}
	if opts.OTLPInsecure {
		grpcOpts = append(grpcOpts, otlpmetricgrpc.WithInsecure())
	}
	if opts.OTLPCompression == "gzip" {
		grpcOpts = append(grpcOpts, otlpmetricgrpc.WithCompressor("gzip"))
	}
	return otlpmetricgrpc.New(ctxTimeout, grpcOpts...)
}

# OTLP Metrics Exporter (Experimental)

> [!WARNING]
> This feature is experimental and may change or be removed in future releases.

`kube-state-metrics` supports exporting metrics via the [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otel/protocol/). This allows you to push metrics directly to an OTLP-compatible backend (e.g., OpenTelemetry Collector, Prometheus with OTLP receiver, Jaeger, etc.) instead of or in addition to scraping the `/metrics` endpoint.

## Configuration

The OTLP exporter is disabled by default. You can enable and configure it using the following CLI flags:

| Flag | Description | Default |
|---|---|---|
| `--enable-otlp-export` | Enable the OTLP exporter. | `false` |
| `--otlp-endpoint` | The endpoint of the OTLP receiver. Required if export is enabled. | `""` |
| `--otlp-protocol` | The protocol to use for export. Supported values: `grpc`, `http`. | `grpc` |
| `--otlp-insecure` | Allow insecure connections (no TLS) to the OTLP receiver. | `false` |
| `--otlp-interval` | The interval at which metrics are exported. Must be greater than 0. | `1m0s` |
| `--otlp-url-path` | Override the URL path of the OTLP HTTP receiver. Ignored for `grpc`. | `""` |

### Examples

**Export to a local OpenTelemetry Collector via gRPC:**

```bash
kube-state-metrics \
  --enable-otlp-export \
  --otlp-endpoint=localhost:4317 \
  --otlp-protocol=grpc \
  --otlp-insecure
```

**Export to a remote HTTP endpoint:**

`--otlp-endpoint` takes a host and optional port only — no scheme and no path.
TLS is used unless `--otlp-insecure` is set, and the path defaults to
`/v1/metrics`; override it with `--otlp-url-path`.

```bash
kube-state-metrics \
  --enable-otlp-export \
  --otlp-endpoint=otel-collector.example.com:4318 \
  --otlp-protocol=http
```

Passing a full URL such as `https://otel-collector.example.com:4318/v1/metrics`
fails at startup with an "invalid port" parse error.

## Metric conversion

`kube-state-metrics` converts its internal metrics (which follow the OpenMetrics/Prometheus model) to OTLP metrics as follows:

*   **Gauge** -> OTLP Gauge
*   **Counter** -> OTLP Sum (Monotonic, Cumulative)
*   **Info** -> OTLP Gauge (value 1, labels as attributes)
*   **StateSet** -> OTLP Gauge (value 0 or 1 per series)

Labels are converted to OTLP Attributes, and each metric's `# HELP` text is
carried across as the OTLP metric description.

All data points of a metric are folded into a single OTLP metric, regardless of
how many Kubernetes objects contributed to it. Cumulative sums report the time
the exporter started as their start timestamp.

Exported batches carry the resource attributes `service.name=kube-state-metrics`
and `service.version`.

> [!NOTE]
> Enabling the exporter makes each store retain its generated metric families in
> addition to their rendered exposition bytes, which increases memory use per
> watched object. Nothing is retained while `--enable-otlp-export` is `false`.

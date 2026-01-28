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
| `--otlp-interval` | The interval at which metrics are exported. | `1m0s` |

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

```bash
kube-state-metrics \
  --enable-otlp-export \
  --otlp-endpoint=https://otel-collector.example.com:4318/v1/metrics \
  --otlp-protocol=http
```

## Metric conversion

`kube-state-metrics` converts its internal metrics (which follow the OpenMetrics/Prometheus model) to OTLP metrics as follows:

*   **Gauge** -> OTLP Gauge
*   **Counter** -> OTLP Sum (Monotonic, Cumulative)
*   **Info** -> OTLP Gauge (value 1, labels as attributes)
*   **StateSet** -> OTLP Gauge (value 0 or 1 per series)

Labels are converted to OTLP Attributes.

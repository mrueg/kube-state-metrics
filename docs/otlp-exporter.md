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
| `--otlp-compression` | Compression for the export payload: `gzip` or `none`. | `gzip` |

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

Exported batches carry the resource attributes `service.name=kube-state-metrics`,
`service.version` and `service.instance.id`. The instance id is taken from
`--pod` when set (the downward API value used for autosharding) and otherwise
from the hostname, so each replica of a sharded deployment is a distinct target.
Without it every shard would report the same identity and their `target_info`
samples would interleave into a single series.

`OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` are honoured and take
precedence over all three -- this is how you attach cluster identity in a
multi-cluster setup. They are read once, when the process starts.

kube-state-metrics' own telemetry (the `kube_state_metrics_*` families otherwise
served on the telemetry port) is exported too, so a push-only deployment can
still alert on `kube_state_metrics_watch_total{result="error"}` and the
config-reload metrics. Prometheus summaries among them are skipped: the OTLP
Summary type exists only for backwards compatibility.

Metric units are not populated. kube-state-metrics has no family-level unit
metadata -- units are encoded in metric names (`_seconds`, `_bytes`) and, for
resource metrics, in a `unit` label -- and receivers that append a unit suffix
during translation could otherwise produce names like
`kube_pod_start_time_seconds_seconds`.

> [!NOTE]
> Enabling the exporter makes each store retain its generated metric families in
> addition to their rendered exposition bytes, which increases memory use per
> watched object. Nothing is retained while `--enable-otlp-export` is `false`.

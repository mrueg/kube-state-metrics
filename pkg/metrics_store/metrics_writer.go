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

package metricsstore

import (
	"fmt"
	"io"
	"strings"

	"github.com/prometheus/common/expfmt"

	"k8s.io/kube-state-metrics/v2/pkg/metric"
)

const (
	helpPrefix = "# HELP "
	typePrefix = "# TYPE "
)

var (
	infoTypeString     = string(metric.Info)
	stateSetTypeString = string(metric.StateSet)
	gaugeTypeString    = string(metric.Gauge)
)

// MetricsWriterList represent a list of MetricsWriter
type MetricsWriterList []*MetricsWriter

// MetricsWriter is a struct that holds multiple MetricsStore(s) and
// implements the MetricsWriter interface.
// It should be used with stores which have the same metric headers.
//
// MetricsWriter writes out metrics from the underlying stores so that
// metrics with the same name coming from different stores end up grouped together.
// It also ensures that the metric headers are only written out once.
type MetricsWriter struct {
	stores       []*MetricsStore
	ResourceName string
}

func metricNameFromHeaderLine(line, prefix string) (string, bool) {
	if !strings.HasPrefix(line, prefix) {
		return "", false
	}

	rest := line[len(prefix):]
	if rest == "" {
		return "", false
	}

	spaceIdx := strings.IndexByte(rest, ' ')
	if spaceIdx == -1 {
		return rest, true
	}
	if spaceIdx == 0 {
		return "", false
	}
	return rest[:spaceIdx], true
}

// NewMetricsWriter creates a new MetricsWriter.
func NewMetricsWriter(resourceName string, stores ...*MetricsStore) *MetricsWriter {
	return &MetricsWriter{
		stores:       stores,
		ResourceName: resourceName,
	}
}

// WriteAll writes out metrics from the underlying stores to the given writer.
//
// WriteAll writes metrics so that the ones with the same name
// are grouped together when written out.
func (m MetricsWriter) WriteAll(w io.Writer) error {
	if len(m.stores) == 0 {
		return nil
	}

	for i, help := range m.stores[0].headers {
		// Skip empty headers (set by SanitizeHeaders for duplicates)
		if help == "" {
			continue
		}

		var err error
		m.stores[0].metrics.Range(func(_ interface{}, _ interface{}) bool {
			_, err = io.WriteString(w, help)
			if err != nil {
				err = fmt.Errorf("failed to write help text: %v", err)
			}
			return false
		})
		if err != nil {
			return err
		}

		for _, s := range m.stores {
			s.metrics.Range(func(_ interface{}, value interface{}) bool {
				metricFamilies := value.([][]byte)
				_, err = w.Write(metricFamilies[i])
				if err != nil {
					err = fmt.Errorf("failed to write metrics family: %v", err)
					return false
				}
				return true
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// SanitizeHeaders sanitizes the headers of the given MetricsWriterList.
func SanitizeHeaders(contentType expfmt.Format, writers MetricsWriterList) MetricsWriterList {
	clonedWriters := make(MetricsWriterList, 0, len(writers))
	for _, writer := range writers {
		clonedStores := make([]*MetricsStore, 0, len(writer.stores))
		for _, store := range writer.stores {
			clonedHeaders := make([]string, len(store.headers))
			copy(clonedHeaders, store.headers)
			clonedStore := &MetricsStore{
				headers: clonedHeaders,
			}
			// Share the metrics backing storage by sharing the pointer.
			clonedStore.metrics = store.metrics
			clonedStores = append(clonedStores, clonedStore)
		}
		clonedWriters = append(clonedWriters, &MetricsWriter{stores: clonedStores, ResourceName: writer.ResourceName})
	}

	isTextPlain := contentType.FormatType() == expfmt.TypeTextPlain

	// Deduplicate by metric name across all writers to handle non-consecutive duplicates during CRS reload.
	capHint := 0
	for _, w := range clonedWriters {
		if len(w.stores) > 0 {
			capHint += len(w.stores[0].headers)
		}
	}
	seenHELP := make(map[string]struct{}, capHint)
	seenTYPE := make(map[string]struct{}, capHint)
	for _, writer := range clonedWriters {
		if len(writer.stores) > 0 {
			for i := 0; i < len(writer.stores[0].headers); i++ {
				header := writer.stores[0].headers[i]
				if header == "" {
					continue
				}

				// First pass: check if we need to modify this header
				shouldRemove := false
				needsModification := false
				var helpMetricName, typeMetricName string

				rest := header
				for rest != "" {
					var line string
					idx := strings.IndexByte(rest, '\n')
					if idx != -1 {
						line = rest[:idx]
						rest = rest[idx+1:]
					} else {
						line = rest
						rest = ""
					}

					if line == "" && rest == "" {
						break
					}

					switch {
					case strings.HasPrefix(line, helpPrefix):
						metricName, ok := metricNameFromHeaderLine(line, helpPrefix)
						if ok {
							helpMetricName = metricName
							if _, seen := seenHELP[metricName]; seen {
								shouldRemove = true
								break
							}
						}

					case strings.HasPrefix(line, typePrefix):
						if shouldRemove {
							break
						}
						metricName, ok := metricNameFromHeaderLine(line, typePrefix)
						if ok {
							typeMetricName = metricName
							// Check if this type line needs modification
							if isTextPlain {
								if strings.HasSuffix(line, infoTypeString) || strings.HasSuffix(line, stateSetTypeString) {
									needsModification = true
								}
							}
							if _, seen := seenTYPE[metricName]; seen {
								shouldRemove = true
								break
							}
						}
					}
					if shouldRemove {
						break
					}
				}

				// Handle removal case
				if shouldRemove {
					writer.stores[0].headers[i] = ""
					continue
				}

				// Mark as seen
				if helpMetricName != "" {
					seenHELP[helpMetricName] = struct{}{}
				}
				if typeMetricName != "" {
					seenTYPE[typeMetricName] = struct{}{}
				}

				// Fast path: no modification needed
				if !needsModification {
					// Ensure header ends with newline (original behavior)
					if !strings.HasSuffix(header, "\n") {
						writer.stores[0].headers[i] = header + "\n"
					}
					continue
				}

				// Slow path: rebuild header with modifications
				var sb strings.Builder
				sb.Grow(len(header) + 1)

				rest = header
				for rest != "" {
					var line string
					idx := strings.IndexByte(rest, '\n')
					if idx != -1 {
						line = rest[:idx]
						rest = rest[idx+1:]
					} else {
						line = rest
						rest = ""
					}

					if line == "" && rest == "" {
						break
					}

					switch {
					case strings.HasPrefix(line, typePrefix):
						// Apply type modifications
						modifiedLine := line
						if strings.HasSuffix(line, infoTypeString) {
							modifiedLine = line[:len(line)-len(infoTypeString)] + gaugeTypeString
						} else if strings.HasSuffix(line, stateSetTypeString) {
							modifiedLine = line[:len(line)-len(stateSetTypeString)] + gaugeTypeString
						}
						sb.WriteString(modifiedLine)
						sb.WriteByte('\n')
					default:
						sb.WriteString(line)
						sb.WriteByte('\n')
					}
				}

				writer.stores[0].headers[i] = sb.String()
			}
		}
	}

	return clonedWriters
}

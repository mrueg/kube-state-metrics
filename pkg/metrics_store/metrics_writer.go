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
	"sync"

	"github.com/prometheus/common/expfmt"

	"k8s.io/kube-state-metrics/v2/pkg/metric"
)

const (
	helpPrefix = "# HELP "
	typePrefix = "# TYPE "

	// Bitmask flags for tracking seen headers (optimization: single map instead of two)
	seenHelpFlag uint8 = 1 << 0
	seenTypeFlag uint8 = 1 << 1
)

var (
	infoTypeString     = string(metric.Info)
	stateSetTypeString = string(metric.StateSet)
	gaugeTypeString    = string(metric.Gauge)

	// stringBuilderPool pools strings.Builder instances to reduce allocations
	stringBuilderPool = sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}
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
	rest, found := strings.CutPrefix(line, prefix)
	if !found || rest == "" {
		return "", false
	}

	// Extract metric name (first token before space)
	name, _, _ := strings.Cut(rest, " ")
	if name == "" {
		return "", false
	}
	return name, true
}

// extractMetricNameFromHeader extracts the metric name from a header string.
// Headers are expected to start with "# HELP <metric_name> ..." or "# TYPE <metric_name> ...".
// This function extracts the metric name once instead of parsing both HELP and TYPE lines.
func extractMetricNameFromHeader(header string) string {
	// Headers should start with HELP line
	if !strings.HasPrefix(header, helpPrefix) {
		return ""
	}

	rest := header[len(helpPrefix):]
	if rest == "" {
		return ""
	}

	// Find first space or newline - manual scan is faster than strings.Cut
	for i := 0; i < len(rest); i++ {
		if rest[i] == ' ' || rest[i] == '\n' {
			return rest[:i]
		}
	}
	
	return rest
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
	// Use single map with bitmask flags instead of two separate maps for better cache locality
	capHint := 0
	for _, w := range clonedWriters {
		if len(w.stores) > 0 {
			capHint += len(w.stores[0].headers)
		}
	}
	seen := make(map[string]uint8, capHint)
	for _, writer := range clonedWriters {
		if len(writer.stores) > 0 {
			for i := 0; i < len(writer.stores[0].headers); i++ {
				header := writer.stores[0].headers[i]
				if header == "" {
					continue
				}

				// First pass: check if we need to modify this header
				needsModification := false

				// Extract metric name once from the header
				metricName := extractMetricNameFromHeader(header)
				if metricName == "" {
					continue
				}

				// Check for duplicates using bitmask flags
				flags := seen[metricName]

				// If both HELP and TYPE already seen, this is a duplicate
				if flags&seenHelpFlag != 0 && flags&seenTypeFlag != 0 {
					writer.stores[0].headers[i] = ""
					continue
				}

				// Mark as seen (set both flags)
				seen[metricName] = flags | seenHelpFlag | seenTypeFlag

				// Check if TYPE line needs modification (only for text plain format)
				if isTextPlain {
					// Find TYPE line to check if it needs modification
					typeIdx := strings.Index(header, typePrefix)
					if typeIdx != -1 {
						lineEnd := strings.IndexByte(header[typeIdx:], '\n')
						if lineEnd == -1 {
							lineEnd = len(header) - typeIdx
						}
						typeLine := header[typeIdx : typeIdx+lineEnd]
						if strings.HasSuffix(typeLine, infoTypeString) || strings.HasSuffix(typeLine, stateSetTypeString) {
							needsModification = true
						}
					}
				}

				// Fast path: no modification needed
				if !needsModification {
					// Ensure header ends with newline (original behavior)
					if !strings.HasSuffix(header, "\n") {
						writer.stores[0].headers[i] = header + "\n"
					}
					continue
				}

				// Surgical replacement: only modify the TYPE line
				typeIdx := strings.Index(header, typePrefix)
				if typeIdx == -1 {
					// Should not happen if needsModification is true, but be defensive
					continue
				}

				// Find the end of the TYPE line
				lineEnd := strings.IndexByte(header[typeIdx:], '\n')
				afterTypeIdx := typeIdx + lineEnd
				if lineEnd == -1 {
					lineEnd = len(header) - typeIdx
					afterTypeIdx = len(header)
				}
				typeLine := header[typeIdx : typeIdx+lineEnd]

				// Determine the replacement type
				var oldTypeLen int
				if strings.HasSuffix(typeLine, infoTypeString) {
					oldTypeLen = len(infoTypeString)
				} else if strings.HasSuffix(typeLine, stateSetTypeString) {
					oldTypeLen = len(stateSetTypeString)
				} else {
					// No modification needed after all
					if !strings.HasSuffix(header, "\n") {
						writer.stores[0].headers[i] = header + "\n"
					}
					continue
				}

				// Build the new header by concatenating three parts:
				// 1. Everything before TYPE line
				// 2. Modified TYPE line (built with StringBuilder to avoid intermediate allocations)
				// 3. Everything after TYPE line
				sb := stringBuilderPool.Get().(*strings.Builder)
				sb.Reset()

				// Calculate exact size needed for optimal allocation
				needsTrailingNewline := len(header) == 0 || header[len(header)-1] != '\n'
				beforeLen := typeIdx
				typeLineLen := len(typeLine) - oldTypeLen + len(gaugeTypeString)
				afterLen := 0
				if afterTypeIdx < len(header) {
					afterLen = len(header) - afterTypeIdx
				}
				
				exactSize := beforeLen + typeLineLen + afterLen
				if needsTrailingNewline {
					exactSize++
				}

				sb.Grow(exactSize)
				
				// Write: everything before TYPE line
				sb.WriteString(header[:typeIdx])
				
				// Write: modified TYPE line (use StringBuilder to avoid allocations)
				sb.WriteString(typeLine[:len(typeLine)-oldTypeLen])
				sb.WriteString(gaugeTypeString)

				// Write: everything after the TYPE line
				if afterTypeIdx < len(header) {
					sb.WriteString(header[afterTypeIdx:])
				}
				
				// Ensure trailing newline
				if needsTrailingNewline {
					sb.WriteByte('\n')
				}

				writer.stores[0].headers[i] = sb.String()
				stringBuilderPool.Put(sb)
			}
		}
	}

	return clonedWriters
}

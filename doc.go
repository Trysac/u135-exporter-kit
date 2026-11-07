// Package exporterkit implements a zero-dependency Prometheus text exposition
// format 0.0.4 collector registry and HTTP handler for small, custom exporters.
//
// # Collect-on-Scrape vs Push
//
// Traditional Prometheus client libraries (such as client_golang) are designed
// primarily around an in-memory push model: application routines increment counters
// or record gauge values continuously, and an HTTP scrape simply snapshots the
// current in-memory values. However, custom service exporters (monitoring
// relational databases, queue backends, hardware appliances, or vendor APIs)
// operate on a collect-on-scrape model: each incoming scrape triggers live queries
// against the monitored system.
//
// If a backend stalls, a collect-on-scrape exporter can easily hang indefinitely
// or block Prometheus scrape pools. exporterkit provides an isolated collector model
// where each [Collector] executes in its own goroutine with a dedicated
// context deadline ([Options.Timeout]).
//
// Key guarantees:
//   - Byte-exact Prometheus text exposition format 0.0.4 output.
//   - Automatic built-in `exporter_collector_up` gauge tracking individual collector success (1) or failure (0).
//   - Isolated failure domains: if one collector panics, times out, or fails validation, its samples are discarded while other collectors continue successfully.
//   - Deterministic lexical sorting: metric families and sample label sets are ordered predictably for stable diffs and byte-identical scrapes.
//   - Standard library only: no heavy external dependencies.
package exporterkit

// Package exporterkit is a small, dependency-free Go library: Write Prometheus exporters without client_golang: collect-on-scrape collectors, byte-exact text format 0.0.4, per-collector timeouts and up gauges
//
// The public API is every exported identifier of this root package. Functions are pure: they do
// no I/O, keep no global mutable state and report invalid input through returned errors.
package exporterkit

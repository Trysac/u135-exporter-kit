# exporter-kit

[![Go Reference](https://pkg.go.dev/badge/github.com/Trysac/u135-exporter-kit.svg)](https://pkg.go.dev/github.com/Trysac/u135-exporter-kit)

`exporter-kit` is a zero-dependency, enterprise-grade Go library for building Prometheus exporters with a collect-on-scrape architecture. It implements Prometheus text exposition format 0.0.4 with strict validation, per-collector timeouts, and automatic health metric tracking (`exporter_collector_up`).

## The Problem

Standard Prometheus Go clients are designed around continuous in-memory metric updates. Exporters monitoring external services like databases or vendor APIs need to gather metrics dynamically at scrape time. Pulling in heavy client dependencies or relying on unconstrained collection routines often leads to scrape timeouts, thread exhaustion, or corrupted exposition formats.

## Why not client_golang?

`exporter-kit` provides a dedicated collect-on-scrape model. Unlike `client_golang`, it has zero external dependencies, enforces strict per-collector deadlines via `context.Context`, and ensures byte-exact 0.0.4 text output without accidental escaping errors.

## Usage

```go
import "github.com/Trysac/u135-exporter-kit/exporterkit"

reg := exporterkit.NewRegistry(exporterkit.Options{})
err := reg.Register(myCollector)
http.Handle("/metrics", reg.Handler())
_ = http.ListenAndServe(":9100", nil)
```

## Public API Summary

- **Registry**: `NewRegistry(Options) *Registry`, `Register(Collector) error`, `Gather(context.Context) ([]Family, error)`, `Handler() http.Handler`.
- **Writer**: `WriteText(io.Writer, []Family) error`.
- **Types**: `MetricType` (Counter, Gauge, Untyped), `Desc`, `Sample`, `Collector` (interface), `Options`.
- **Errors**: `ErrInvalidName`, `ErrDuplicateCollector`, `ErrInvalidSample`, `ErrTooManySamples`.

For threat model and security limitations, see [docs/SECURITY.md](docs/SECURITY.md).
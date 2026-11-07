# Security notes

## Threat Model

`github.com/Trysac/u135-exporter-kit` is a pure Go library without network or file I/O dependencies. It operates within the memory space of the host process.

### Defended

- **Malformed Exposition**: Input validation ensures that `WriteText` produces byte-exact 0.0.4 output. Metric and label names are validated against strict regex patterns (`^[a-zA-Z_:][a-zA-Z0-9_:]*$`).
- **Scrape Exhaustion**: `Gather` enforces `context.WithTimeout` for every collector. A collector that hangs or ignores context cancellation will not block the global scrape beyond the registry's timeout limit.
- **Panic Propagation**: Panics within `Collect` are recovered; the collector is marked as unhealthy (`exporter_collector_up` = 0) and the scrape continues.

### Not Defended

- **Logic Errors in Collectors**: While the library manages concurrent execution, the implementation of `Collect` logic remains the responsibility of the user. Malicious or poorly written collectors can still perform arbitrary operations if the `Collect` method performs I/O or other side effects.
- **Unbounded Resource Usage**: If a collector allocates excessive memory within its valid runtime, the library cannot prevent OOM events.

## Vulnerability Reporting

To report a vulnerability, please contact the repository maintainers directly or use the GitHub security advisory feature.
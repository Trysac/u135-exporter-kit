# Architecture & Design Decisions

`exporterkit` provides a lightweight, robust registry and exposition pipeline designed specifically for collect-on-scrape Prometheus exporters.

## Registry and Scrape Lifecycle

1. **Collector Registration (`Register`)**:
   - Collectors define their unique `Name()` and declare `Describe() []Desc`.
   - Family names, metric names, and label names are validated against Prometheus 0.0.4 lexical grammar.
   - No two collectors can declare the same metric family name.
   - Collector registration is thread-safe.

2. **Concurrent Gather (`Gather`)**:
   - On each scrape, a snapshot of registered collectors is sorted lexically by collector name.
   - Each collector runs in its own goroutine with a dedicated `context.WithTimeout` derived from the scrape context.
   - Collectors emit samples into a thread-safe private collector buffer.
   - Results are passed via buffered channels so a blocked collector that ignores context cancellation does not hang the scrape or leak channel resources.
   - Panics and errors are trapped, recording collector status as `0` in `exporter_collector_up` and discarding partial invalid samples.

3. **Validation & Caps**:
   - Total samples collected during a scrape are capped at `MaxSamples` (default 100,000).
   - Samples emitting undeclared metric names, mismatched label counts, or duplicate label tuples are flagged as `ErrInvalidSample`.

4. **Text Formatting (`WriteText`)**:
   - Metric families and sample label values are sorted deterministically.
   - Values are formatted using standard Prometheus float conventions (`NaN`, `+Inf`, `-Inf`, and `strconv.FormatFloat(v, 'g', -1, 64)`).
   - Backslashes, quotes, and newlines are strictly escaped according to 0.0.4 specifications.

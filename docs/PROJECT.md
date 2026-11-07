# Project record

## Objective

Write Prometheus exporters without client_golang: collect-on-scrape collectors, byte-exact text format 0.0.4, per-collector timeouts and up gauges.

## Scope

- Implementing the public API as defined in the library package `exporterkit`.
- Handling Prometheus 0.0.4 text exposition formatting, including strict escaping and deterministic sorting.
- Managing collector lifecycle: registration, concurrent gathering with per-collector timeouts, and panic recovery.
- Validating metric/label names and sample integrity.

## Out of Scope

- Histograms and Summaries.
- OpenMetrics or Protocol Buffer (protobuf) formats.
- Push gateway integration.
- Automated process or hardware metrics.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

| Metric | Value |
|---|---|
| Tokens (input / output) | 649371 / 25354 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.1803 |

## Factory run u135 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | gemini-3.8-flash | yes | 4225 | 16981 | 0 | 0 | 0.0334 |
| fix-1 | gemini-3.8-flash | no | 81129 | 6761 | 0 | 501340 | 0.1238 |
| fix-2 | gemini-3.8-flash | no | 16544 | 108 | 0 | 12322 | 0.0137 |
| audit | gemini-3.1-flash-lite | no | 23717 | 69 | 0 | 6115 | 0.0062 |
| docs | gemini-3.1-flash-lite | no | 3979 | 1435 | 0 | 0 | 0.0031 |

Total: $0.1803. Prices: Anthropic list prices.

# Collect-on-scrape Prometheus exposition engine with per-collector timeouts

Status: accepted (generated, pending human review)

## Context

Small custom Prometheus exporters (for databases, queues, and third-party APIs) require collect-on-scrape semantics. Using client_golang pulls in a large dependency tree and lacks built-in per-collector deadlines and automatic collector up-gauge management, risking hung scrapes when backend systems stall.

## Decision

Implement exporterkit using standard library Go only. Each collector runs concurrently with isolated timeouts, emitting into local buffers with panics caught and reported via `exporter_collector_up`. Format text output strictly according to Prometheus exposition format 0.0.4 with deterministic sorting.

## Consequences

Exporters remain lightweight with zero external dependencies. If a single backend collector fails or times out, healthy metrics are preserved and Prometheus receives clear visibility into collector health.

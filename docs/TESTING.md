# Testing

## Running

```sh
go test -race -cover . 
```

## Policy

- **Coverage**: Statement coverage must remain at or above 80% for the `exporterkit` package.
- **Deterministic**: Tests must not rely on wall clock, network, or execution order. The only allowed timer dependency is the 20ms timeout used for testing `Gather` deadlines.
- **API Enforcement**: Tests reside in `exporterkit_test` and interact exclusively with the public API. They assert on return values, error types (`errors.Is`), and output strings.
- **Golden Files**: Formatting tests use golden strings in `testdata/` to verify byte-exact output against the 0.0.4 spec.
- **Bug Fixes**: Every regression must include a specific failing test case reproduced before the fix.

## Layout

Tests are colocated with the source code (`<file>_test.go`). `testdata/` contains fixtures for golden file comparisons.
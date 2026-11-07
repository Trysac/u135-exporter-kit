# exporter-kit

[![Go Reference](https://pkg.go.dev/badge/github.com/Trysac/u135-exporter-kit.svg)](https://pkg.go.dev/github.com/Trysac/u135-exporter-kit)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/Trysac/u135-exporter-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u135-exporter-kit/actions/workflows/ci.yml)

Write Prometheus exporters without client_golang: collect-on-scrape collectors, byte-exact text format 0.0.4, per-collector timeouts and up gauges

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
go get github.com/Trysac/u135-exporter-kit
```

Requires Go 1.23 or newer. No dependencies outside the standard library.

## Usage

```go
package main

import (
	"errors"
	"fmt"

	"github.com/Trysac/u135-exporter-kit"
)

func main() {
	// Normalize trims surrounding whitespace and lower-cases the text.
	out, _ := exporterkit.Normalize("  Hello World ")
	fmt.Println(out) // -> "hello world"

	// Blank input is rejected explicitly instead of returning an empty string.
	if _, err := exporterkit.Normalize("   "); errors.Is(err, exporterkit.ErrBlank) {
		fmt.Println("rejected:", err)
	}
}
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez

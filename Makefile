.PHONY: build run simulate dry-run test test-verbose bench clean install lint

# Build binary into bin/
build:
	go build -o bin/rebalancer ./cmd/rebalancer

# Run continuously against a real cluster (uses config.json)
run: build
	./bin/rebalancer --config config.json

# Run in simulation mode (no cluster required)
simulate: build
	./bin/rebalancer --simulate --cycles 3

# Dry-run against a real cluster (plans but no changes)
dry-run: build
	./bin/rebalancer --config config.json --dry-run --once

# Run all unit tests
test:
	go test ./...

# Run tests with verbose output
test-verbose:
	go test -v ./...

# Run benchmarks
bench:
	go test -bench=. -benchmem ./...

# Remove build artifacts
clean:
	rm -rf bin/

# Install binary to GOPATH/bin
install:
	go install ./cmd/rebalancer

# Run go vet (built-in linter)
lint:
	go vet ./...

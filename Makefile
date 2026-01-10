.PHONY: all build test clean benchmark cli help

# Default target
all: help

# Build the project
build:
	@echo "Building GoDB-3000..."
	@go build -o bin/godb main.go
	@go build -o bin/godb-cli cmd/godb/main.go
	@echo "✓ Build complete: bin/godb, bin/godb-cli"

# Run benchmarks
benchmark:
	@echo "Running performance benchmarks..."
	@go run examples/benchmark.go

# Start interactive CLI
cli:
	@echo "Starting interactive CLI..."
	@go run cmd/godb/main.go demo.db

# Run tests
test:
	@echo "Running tests..."
	@go test ./internal/... -v

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@echo "✓ Code formatted"

# Run linter
lint:
	@echo "Running linter..."
	@go vet ./...
	@echo "✓ Lint complete"

# Clean build artifacts and test databases
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f *.db
	@rm -f examples/*.db
	@rm -f cmd/godb/*.db
	@rm -f *.json *.txt *.tmp.*
	@echo "✓ Cleaned"

# Install dependencies
deps:
	@echo "Installing dependencies..."
	@go mod download
	@go mod tidy
	@echo "✓ Dependencies ready"


# Show statistics
stats:
	@echo "Project Statistics:"
	@echo "  Total Go files: $$(find . -name '*.go' -not -path './vendor/*' | wc -l)"
	@echo "  Total lines of code: $$(find . -name '*.go' -not -path './vendor/*' -exec cat {} \; | wc -l)"
	@echo "  Lines per package:"
	@echo "    Storage:     $$(find internal/storage -name '*.go' -exec cat {} \; | wc -l 2>/dev/null || echo 0)"
	@echo "    B+Tree:      $$(find internal/btree -name '*.go' -exec cat {} \; | wc -l 2>/dev/null || echo 0)"
	@echo "    KV Store:    $$(find internal/kv -name '*.go' -exec cat {} \; | wc -l 2>/dev/null || echo 0)"
	@echo "    Table:       $$(find internal/table -name '*.go' -exec cat {} \; | wc -l 2>/dev/null || echo 0)"
	@echo "    Index:       $$(find internal/index -name '*.go' -exec cat {} \; | wc -l 2>/dev/null || echo 0)"
	@echo "    Transaction: $$(find internal/transaction -name '*.go' -exec cat {} \; | wc -l 2>/dev/null || echo 0)"
	@echo "    SQL:         $$(find internal/sql -name '*.go' -exec cat {} \; | wc -l 2>/dev/null || echo 0)"

# Development workflow
dev: clean fmt lint build run

# Full check before commit
check: clean fmt lint test examples
	@echo "✓ All checks passed!"

# Help
help:
	@echo "GoDB-3000 - Database System"
	@echo ""
	@echo "Available targets:"
	@echo "  make benchmark  - Run performance benchmarks"
	@echo "  make cli        - Start interactive CLI"
	@echo "  make build      - Build binaries"
	@echo "  make test       - Run tests"
	@echo "  make fmt        - Format code"
	@echo "  make lint       - Run linter"
	@echo "  make clean      - Remove build artifacts"
	@echo "  make stats      - Show code statistics"`
	@echo "  make dev        - Development workflow (clean, fmt, lint, build, run)"
	@echo "  make check      - Full check before commit"
	@echo "  make help       - Show this help"
	@echo ""
	@echo "Quick Start:"
	@echo "  1. make cli           # Interactive database shell"
	@echo "  2. make benchmark     # Performance benchmarks"
	@echo ""

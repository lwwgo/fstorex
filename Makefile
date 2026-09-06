# FStoreX Makefile
# Distributed file storage with Raft consensus metadata server.

.PHONY: all build test lint fmt tidy clean

BINARY_DIR := bin
BINARIES := fstorex fstorex-mds fstorex-dn

all: lint test build

# ===== Build =====
build:
	@mkdir -p $(BINARY_DIR)
	@for bin in $(BINARIES); do \
		echo "Building $$bin..."; \
		go build -o $(BINARY_DIR)/$$bin ./cmd/$$bin; \
	done
	@echo "Build complete. Binaries in $(BINARY_DIR)/"

# ===== Test =====
test:
	go test -v -race -cover ./...

# ===== Lint =====
lint:
	@which golangci-lint > /dev/null 2>&1 && golangci-lint run ./... || (echo "golangci-lint not found, falling back to go vet" && go vet ./...)

# ===== Format =====
fmt:
	gofmt -s -w .

# ===== Tidy dependencies =====
tidy:
	go mod tidy

# ===== Clean =====
clean:
	rm -rf $(BINARY_DIR)/

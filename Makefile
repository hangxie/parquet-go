.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

BUILD_TIME  = $(shell date +%FT%T%z)
BUILD_DIR   = $(CURDIR)/build
GIT_HASH    = $(shell git rev-parse --short HEAD)
PKG_PREFIX  = github.com/hangxie/parquet-go
VERSION     = $(shell git describe --tags --always)

PAGES_DIR    = $(BUILD_DIR)/pages
COVERAGE_CSV ?= $(BUILD_DIR)/coverage.csv
COLLECT_ARGS ?=

TESTDATA_DIR     = $(BUILD_DIR)/testdata
PARQUET_TESTING  = https://raw.githubusercontent.com/apache/parquet-testing/master/data
TESTDATA_FILES   = $(TESTDATA_DIR)/datapage_v1-uncompressed-checksum.parquet \
                   $(TESTDATA_DIR)/datapage_v1-corrupt-checksum.parquet \
                   $(TESTDATA_DIR)/encrypt_columns_and_footer.parquet.encrypted \
                   $(TESTDATA_DIR)/encrypt_columns_and_footer_ctr.parquet.encrypted \
                   $(TESTDATA_DIR)/encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted \
                   $(TESTDATA_DIR)/encrypt_columns_plaintext_footer.parquet.encrypted \
                   $(TESTDATA_DIR)/uniform_encryption.parquet.encrypted

# go option
CGO_ENABLED := 0
GO          ?= go
PYTHON      ?= python3
GOBIN       = $(shell go env GOPATH)/bin
GOFLAGS     := -trimpath
GOSOURCES   := $(shell find . -type f -name '*.go')
LDFLAGS     := -w -s

.EXPORT_ALL_VARIABLES:

.PHONY: all
all: deps tools format lint test example  ## Build all common targets

.PHONY: format
format: tools  ## Format all go code
	@echo "==> Formatting all go code"
	@$(GOBIN)/gofumpt -w -extra $(GOSOURCES)
	@$(GOBIN)/goimports -w -local $(PKG_PREFIX) $(GOSOURCES)

.PHONY: lint
lint: tools  ## Run static code analysis
	@echo "==> Running static code analysis"
	@$(GOBIN)/golangci-lint cache clean
	@$(GOBIN)/golangci-lint run ./... \
		--timeout 5m \
		--enable gocognit

.PHONY: deps
deps:  ## Install prerequisite for build
	@echo "==> Installing prerequisite for build"
	@go mod tidy

.PHONY: tools
# golangci-lint is pinned because @latest broke CI the day v2.13 raised its Go
# floor. No single version spans the build matrix: v2.12 cannot analyse 1.27 and
# v2.13 cannot be built on 1.25, so lint runs on one recent Go instead.
tools:  ## Install build tools
	@echo "==> Installing build tools"
	@(cd /tmp; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1; \
		go install mvdan.cc/gofumpt@latest; \
		go install golang.org/x/tools/cmd/goimports@latest; \
	)

.PHONY: clean
clean:  ## Clean up the build dirs
	@echo "==> Cleaning up build dirs"
	@rm -rf $(BUILD_DIR) vendor .venv
	@find . -name *.parquet | xargs -r rm

.PHONY: testdata
testdata:  ## Download test data from apache/parquet-testing
	@mkdir -p $(TESTDATA_DIR)
	@for f in $(TESTDATA_FILES); do \
		if [ ! -f "$$f" ]; then \
			echo "    ==> Downloading $$(basename $$f)"; \
			curl -sSfL -o "$$f" "$(PARQUET_TESTING)/$$(basename $$f)"; \
		fi; \
	done

.PHONY: test
test: deps testdata  ## Run unit tests
	@echo "==> Running unit tests"
	@CGO_ENABLED=1 go test -race -count 1 -trimpath ./...

# Separate from test so the version matrix only pays for the tests themselves;
# the reports are consumed once, by the coverage badge at release.
.PHONY: coverage
coverage: deps testdata  ## Run unit tests and build coverage reports
	@echo "==> Running unit tests with coverage"
	@mkdir -p $(BUILD_DIR)/test
	@set -euo pipefail ; \
		cd $(BUILD_DIR)/test; \
		CGO_ENABLED=1 go test -race -count 1 -trimpath \
			-coverprofile=coverage.out.tmp $(CURDIR)/... ; \
		grep -v /parquet/ coverage.out.tmp > coverage.out; \
		go tool cover -html=coverage.out -o coverage.html ; \
		go tool cover -func=coverage.out -o coverage.txt ; \
		cat coverage.txt

.PHONY: pages
pages: pages-coverage  ## Generate all GitHub Pages content to build/pages/

.PHONY: pages-coverage
pages-coverage: deps testdata  ## Collect coverage history and build charts (COLLECT_ARGS="--start 2024-01-01 --end 2024-06-01")
	@echo "==> Generating coverage history page"
	@mkdir -p $(PAGES_DIR)
	@$(PYTHON) scripts/coverage-history.py $(COLLECT_ARGS) $(PAGES_DIR)/coverage-history.html $(COVERAGE_CSV)
	@echo "==> Generating Go coverage report"
	@mkdir -p $(BUILD_DIR)/test
	@set -euo pipefail ; \
		CGO_ENABLED=1 $(GO) test -parallel 4 -count 1 -trimpath \
			-coverprofile=$(BUILD_DIR)/test/coverage.out.tmp ./... ; \
		grep -v /parquet/ $(BUILD_DIR)/test/coverage.out.tmp > $(BUILD_DIR)/test/coverage.out ; \
		$(GO) tool cover -html=$(BUILD_DIR)/test/coverage.out -o $(PAGES_DIR)/coverage.html

.PHONY: example
example: deps  ## Run all examples
	@echo "==> Compiling all examples"
	@mkdir -p build/example
	@set -euo pipefail; \
	    for DIR in example/*; do \
	        (go build -tags example -o build/example/ ./$${DIR}); \
			echo "    ==> $${DIR}"; \
	    done

.PHONY: benchmark
benchmark:  ## Run benchmark
	@echo "==> Running benchmark"
	@go test -bench ^Benchmark -run=^$$ -count 1 -benchtime 3x -benchmem ./...

# Per-target fuzz duration. Override for deeper runs, e.g. make fuzz FUZZTIME=120s
FUZZTIME ?= 10s

.PHONY: fuzz
fuzz: deps  ## Run every fuzz test for FUZZTIME each (default 10s; runs nightly in CI)
	@echo "==> Running fuzz tests (FUZZTIME=$(FUZZTIME) each)"
	@rc=0; \
	for pkg in $$($(GO) list ./...); do \
		for fn in $$($(GO) test -list '^Fuzz' $$pkg 2>/dev/null | grep '^Fuzz'); do \
			echo "--> $$pkg $$fn"; \
			$(GO) test -run='^$$' -fuzz="^$$fn$$" -fuzztime=$(FUZZTIME) $$pkg 2>&1 | grep -vE "^(fuzz: |PASS$$|ok )"; \
			s=$${PIPESTATUS[0]}; \
			if [ $$s -ne 0 ]; then rc=$$s; fi; \
		done; \
	done; \
	exit $$rc

.PHONY: help
help:  ## Print list of Makefile targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

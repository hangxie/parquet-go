#!/usr/bin/env bash
# Compares BenchmarkReaderScan across the original reader (6ee845f), incremental
# decoding (d4cae47) and its numeric fast paths (60b1940) from PR #556, using the
# same benchmark code pinned at e17652e for all three.
#
# Usage: scripts/benchmark-reader-scan.sh [output-dir]   (default build/reader-scan)
#        benchstat <output-dir>/main.txt <output-dir>/incremental.txt <output-dir>/optimized.txt
#
# BENCH_COUNT (default 6) and BENCH_TIME (default 150ms) set samples per case and
# their duration. Apart from 6ee845f, the pinned commits are only reachable from the
# PR #556 head, so fetch it first if needed: git fetch origin pull/556/head
set -euo pipefail

repo_dir=$(git rev-parse --show-toplevel)
cd "$repo_dir"
output_dir=${1:-build/reader-scan}
mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
scratch_dir=$(mktemp -d "${TMPDIR:-/tmp}/reader-scan.XXXXXX")
trap 'rm -rf -- "$scratch_dir"' EXIT

labels=(main incremental optimized)
revisions=(6ee845f d4cae47 60b1940)
benchmark_revision=e17652e
rounds=${BENCH_COUNT:-6}
benchtime=${BENCH_TIME:-150ms}

# This pinned benchmark and its helpers compile unchanged on all three revisions.
cat > "$scratch_dir/reader_scan_bench_test.go" <<'GO'
package reader

import (
    "bytes"
    "context"
    "strconv"
    "testing"

    "github.com/stretchr/testify/require"

    "github.com/hangxie/parquet-go/v3/common"
    "github.com/hangxie/parquet-go/v3/parquet"
    "github.com/hangxie/parquet-go/v3/source/buffer"
    "github.com/hangxie/parquet-go/v3/source/writerfile"
    "github.com/hangxie/parquet-go/v3/writer"
)
GO
git show "$benchmark_revision:reader/reader_test.go" | sed -n '/^func BenchmarkReaderScan(/,$p' >> "$scratch_dir/reader_scan_bench_test.go"

: > "$output_dir/revisions.txt"
for i in 0 1 2; do
    label=${labels[$i]}
    revision=${revisions[$i]}
    snapshot="$scratch_dir/$label"
    mkdir -p "$snapshot"
    git archive "$revision" | tar -x -C "$snapshot"
    cp "$scratch_dir/reader_scan_bench_test.go" "$snapshot/reader/review_scan_bench_test.go"
    (cd "$snapshot" && go test -c ./reader -o "$scratch_dir/$label-bench")
    printf '%s %s\n' "$label" "$(git rev-parse "$revision")" >> "$output_dir/revisions.txt"
    : > "$output_dir/$label.txt"
done

# Compile first, then alternate revision order without concurrent benchmark jobs.
for ((round=1; round<=rounds; round++)); do
    if ((round % 2)); then order=(0 1 2); else order=(2 1 0); fi
    for i in "${order[@]}"; do
        label=${labels[$i]}
        "$scratch_dir/$label-bench" -test.run='^$' -test.bench='^BenchmarkReaderScan$' \
            -test.benchmem -test.benchtime="$benchtime" -test.count=1 -test.cpu=1 >> "$output_dir/$label.txt"
    done
done

printf 'Results: %s\n' "$output_dir"
printf 'Compare: benchstat %q %q %q\n' "$output_dir/main.txt" "$output_dir/incremental.txt" "$output_dir/optimized.txt"

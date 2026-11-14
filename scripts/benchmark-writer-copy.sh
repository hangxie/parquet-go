#!/usr/bin/env bash
# Compares BenchmarkWriteRowCopy between a base revision, which buffers the caller's
# rows by reference, and a head revision that copies them on Write (issue #601).
# The benchmark is taken from the head revision and added to the base snapshot.
#
# Usage: scripts/benchmark-writer-copy.sh [output-dir] [base] [head]
#        (defaults: build/writer-copy, origin/main, HEAD)
#        benchstat <output-dir>/base.txt <output-dir>/head.txt
#
# BENCH_COUNT (default 6) and BENCH_TIME (default 20x) set samples per case and
# their duration; BENCH_CPU (default 1) sets GOMAXPROCS, which decides whether
# garbage collection competes with the writer for the same core.
set -euo pipefail

repo_dir=$(git rev-parse --show-toplevel)
cd "$repo_dir"
output_dir=${1:-build/writer-copy}
base=${2:-origin/main}
head=${3:-HEAD}
mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
scratch_dir=$(mktemp -d "${TMPDIR:-/tmp}/writer-copy.XXXXXX")
trap 'rm -rf -- "$scratch_dir"' EXIT

labels=(base head)
revisions=("$base" "$head")
rounds=${BENCH_COUNT:-6}
benchtime=${BENCH_TIME:-20x}
cpu=${BENCH_CPU:-1}

# The base revision lacks the benchmark, so it gets its own file with the imports it needs.
# The benchmark and its helper close writer/writer_test.go, so everything from it on is taken.
cat > "$scratch_dir/writer_copy_bench_test.go" <<'GO'
package writer

import (
    "bytes"
    "context"
    "fmt"
    "runtime"
    "testing"

    "github.com/hangxie/parquet-go/v3/source/writerfile"
)

GO
git show "$head:writer/writer_test.go" | sed -n '/^\/\/ BenchmarkWriteRowCopy /,$p' >> "$scratch_dir/writer_copy_bench_test.go"

: > "$output_dir/revisions.txt"
for i in 0 1; do
    label=${labels[$i]}
    revision=${revisions[$i]}
    snapshot="$scratch_dir/$label"
    mkdir -p "$snapshot"
    git archive "$revision" | tar -x -C "$snapshot"
    if ! grep -q '^func BenchmarkWriteRowCopy(' "$snapshot/writer/writer_test.go"; then
        cp "$scratch_dir/writer_copy_bench_test.go" "$snapshot/writer/"
    fi
    (cd "$snapshot" && go test -c ./writer -o "$scratch_dir/$label-bench")
    printf '%s %s\n' "$label" "$(git rev-parse "$revision")" >> "$output_dir/revisions.txt"
    : > "$output_dir/$label.txt"
done

# Compile first, then alternate revision order without concurrent benchmark jobs.
for ((round=1; round<=rounds; round++)); do
    if ((round % 2)); then order=(0 1); else order=(1 0); fi
    for i in "${order[@]}"; do
        label=${labels[$i]}
        "$scratch_dir/$label-bench" -test.run='^$' -test.bench='^BenchmarkWriteRowCopy$' \
            -test.benchmem -test.benchtime="$benchtime" -test.count=1 -test.cpu="$cpu" >> "$output_dir/$label.txt"
    done
done

printf 'Results: %s\n' "$output_dir"
printf 'Compare: benchstat %q %q\n' "$output_dir/base.txt" "$output_dir/head.txt"

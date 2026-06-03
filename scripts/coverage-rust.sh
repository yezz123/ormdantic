#!/usr/bin/env bash

set -euo pipefail
set -x

mkdir -p target/coverage
cargo llvm-cov --workspace --exclude ormdantic-py --lcov --output-path target/coverage/rust.lcov

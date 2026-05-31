#!/usr/bin/env bash

set -e
set -x

# Build the docs
uv run --group docs mkdocs build -d build

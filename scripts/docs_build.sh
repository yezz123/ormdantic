#!/usr/bin/env bash

set -e
set -x

# Build the docs
mkdocs build -d build

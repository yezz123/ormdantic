#!/usr/bin/env bash

set -e
set -x

# Serve the docs
uv run --group docs mkdocs serve --livereload

#!/usr/bin/env bash

set -e
set -x

uv run --group linting pre-commit run --all-files --verbose --show-diff-on-failure

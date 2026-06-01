#!/usr/bin/env bash

set -e
set -x

uv run --group linting mypy --show-error-codes ormdantic

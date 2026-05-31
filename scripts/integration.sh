#!/usr/bin/env bash

set -e
set -x

echo "ENV=${ENV}"

export PYTHONPATH=.
export DATABASE_URL="${DATABASE_URL:-sqlite:///db.sqlite3}"
uv run --group testing pytest tests/integration

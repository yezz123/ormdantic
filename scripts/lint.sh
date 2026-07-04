#!/usr/bin/env bash

set -e
set -x

uv run --group linting ty check

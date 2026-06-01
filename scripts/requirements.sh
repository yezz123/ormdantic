#!/usr/bin/env bash

refresh-lockfiles() {
  echo "Syncing dependencies from pyproject.toml using uv"
  uv sync --all-groups
}

refresh-lockfiles

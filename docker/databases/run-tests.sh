#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yaml"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-ormdantic-databases}"

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" up -d --wait

export ORMDANTIC_TEST_POSTGRES_URL="${ORMDANTIC_TEST_POSTGRES_URL:-postgresql://postgres:postgres@localhost:5432/postgres}"
export ORMDANTIC_TEST_MYSQL_URL="${ORMDANTIC_TEST_MYSQL_URL:-mysql://root:mysql@localhost:3306/mysql}"
export ORMDANTIC_TEST_MARIADB_URL="${ORMDANTIC_TEST_MARIADB_URL:-mariadb://root:mariadb@localhost:3307/mariadb}"
export ORMDANTIC_TEST_MSSQL_URL="${ORMDANTIC_TEST_MSSQL_URL:-mssql://sa:Password123@localhost:1433/master?trust_cert=true}"
export ORMDANTIC_TEST_ORACLE_URL="${ORMDANTIC_TEST_ORACLE_URL:-oracle://system:oracle@localhost:1521/FREEPDB1}"

export ORMDANTIC_POSTGRES_URL="${ORMDANTIC_POSTGRES_URL:-${ORMDANTIC_TEST_POSTGRES_URL}}"
export ORMDANTIC_MYSQL_URL="${ORMDANTIC_MYSQL_URL:-${ORMDANTIC_TEST_MYSQL_URL}}"
export ORMDANTIC_MARIADB_URL="${ORMDANTIC_MARIADB_URL:-${ORMDANTIC_TEST_MARIADB_URL}}"
export ORMDANTIC_MSSQL_URL="${ORMDANTIC_MSSQL_URL:-${ORMDANTIC_TEST_MSSQL_URL}}"
export ORMDANTIC_ORACLE_URL="${ORMDANTIC_ORACLE_URL:-${ORMDANTIC_TEST_ORACLE_URL}}"

cd "${REPO_ROOT}"

uv run --group dev maturin develop
uv run --group testing pytest tests/integration/test_migrations_external.py -q
uv run --group testing pytest tests/integration/test_playground_external.py -q
uv run --group testing pytest tests/integration/test_relationship_loading_external.py -q
uv run --group testing pytest tests/integration/test_transactions_external.py -q
uv run --group testing pytest tests/integration/test_value_fidelity.py -q
(
  unset ORMDANTIC_MSSQL_URL
  unset ORMDANTIC_ORACLE_URL
  cargo test -p ormdantic-engine
)
cargo test -p ormdantic-engine --features mssql,oracle --test driver_matrix --test mssql_exec --test oracle_exec

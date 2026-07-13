# Database Test Matrix

This folder starts every server database currently supported by Ormdantic's native runtime:

- PostgreSQL
- MySQL
- MariaDB
- SQL Server
- Oracle Free

SQLite is covered by the normal test suite and does not need a container.

## Start The Matrix

```bash
docker compose -f docker/databases/docker-compose.yaml up -d --wait
```

## Run Ormdantic Checks

```bash
docker/databases/run-tests.sh
```

The runner exports both Python migration smoke-test URLs (`ORMDANTIC_TEST_*_URL`) and Rust engine URLs (`ORMDANTIC_*_URL`), then runs:

- `tests/integration/test_migrations_external.py`
- `tests/integration/test_relationship_loading_external.py`
- `cargo test -p ormdantic-engine`
- `cargo test -p ormdantic-engine --features mssql,oracle --test mssql_exec --test oracle_exec`

## Run Benchmark Smoke Checks

Start only the database services needed by the benchmark suite:

```bash
docker compose -p ormdantic-benchmark -f docker/databases/docker-compose.yaml up -d --wait postgres mysql
```

Then run backend-scoped smoke profiles:

```bash
uv run --group dev maturin develop --release
uv run --group benchmark python -m benchmark.run --backend postgres --profile smoke
uv run --group benchmark python -m benchmark.run --backend mysql --profile smoke
```

The benchmark runner drops only tables named `ormdantic_bench_*`; it does not
remove or reset Docker volumes.

## URLs

```bash
export ORMDANTIC_TEST_POSTGRES_URL=postgresql://postgres:postgres@localhost:5432/postgres
export ORMDANTIC_TEST_MYSQL_URL=mysql://root:mysql@localhost:3306/mysql
export ORMDANTIC_TEST_MARIADB_URL=mariadb://root:mariadb@localhost:3307/mariadb
export ORMDANTIC_TEST_MSSQL_URL='mssql://sa:Password123@localhost:1433/master?trust_cert=true'
export ORMDANTIC_TEST_ORACLE_URL=oracle://system:oracle@localhost:1521/FREEPDB1
```

## Stop And Reset

```bash
docker compose -f docker/databases/docker-compose.yaml down -v
```

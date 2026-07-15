# Cross-Database ORM Benchmarks

This folder contains the reproducible Ormdantic comparison suite used for docs,
release notes, and blog-post data. The suite compares Ormdantic, SQLAlchemy, and
SQLModel across SQLite, PostgreSQL, and MySQL.

The benchmark measures scoped cases instead of producing one global winner:

- schema create/drop;
- raw batch inserts and ORM inserts;
- filtered updates, mixed upserts, and filtered deletes;
- counts, filters, aggregation, scalar projections, point lookup, pagination, and ordering;
- flat hydration and simple/nested serialization;
- one-to-many, many-to-one, and nested relationship loading.

Setup and seed work are outside the timed section. Validation also runs outside
the timed section before cleanup, so each latency sample measures only the case
operation. Use a release-built native extension before recording report artifacts.

## Backend Matrix

| Backend | Runtime | URL source |
| --- | --- | --- |
| SQLite | local file per sample | generated temporary file |
| PostgreSQL | Docker Compose service | `ORMDANTIC_BENCH_POSTGRES_URL`, then `ORMDANTIC_TEST_POSTGRES_URL`, then `postgresql://postgres:postgres@localhost:5432/postgres` |
| MySQL | Docker Compose service | `ORMDANTIC_BENCH_MYSQL_URL`, then `ORMDANTIC_TEST_MYSQL_URL`, then `mysql://root:mysql@localhost:3306/mysql` |

Server runs use `docker/databases/docker-compose.yaml`. The runner never destroys
shared Docker volumes. Cleanup drops only benchmark-owned tables named
`ormdantic_bench_*`.

## Profiles

| Profile | Purpose | Read rows | Write rows | Lookups | Iterations | Warmups | Batch |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `smoke` | fast harness verification | 1,000 | 1,000 | 100 | 1 | 0 | 5,000 |
| `ci` | repeated pull-request comparison | 10,000 | 10,000 | 500 | 7 | 2 | 500 |
| `default` | local docs refresh | 20,000 | 20,000 | 1,000 | 5 | 1 | 5,000 |
| `million` | real large local workload | 1,000,000 | 1,000,000 | 10,000 | 1 | 0 | 5,000 |
| `large` | opt-in stress workload | 10,000,000 | 1,000,000 | 50,000 | 1 | 0 | 5,000 |
| `billion` | explicit scale workload | 1,000,000,000 | 10,000,000 | 100,000 | 1 | 0 | 5,000 |

The `billion` profile requires `--i-understand-this-may-be-expensive`. Use
`--planner-scale` when producing planner-scale artifacts that must not be mixed
into materialized latency charts.

## Run

Install benchmark dependencies and build the native extension:

```bash
uv sync --group dev --group benchmark
uv run --group dev maturin develop --release
```

Run SQLite smoke without Docker:

```bash
uv run --group benchmark python -m benchmark.run --backend sqlite --profile smoke
```

Run the repeated CI profile or isolate a hot path:

```bash
make benchmark-ci
make benchmark-target CASE="orm insert models"
uv run --group benchmark python -m benchmark.run \
  --backend sqlite --profile ci \
  --case "orm insert models" --case "orm delete filtered" \
  --orm ormdantic --orm sqlalchemy
```

`--case` and `--orm` are repeatable. The runner rotates ORM order between
rounds, retains every sample, and records median absolute deviation and each
sample's order position.

Run PostgreSQL and MySQL smoke with Docker:

```bash
docker compose -p ormdantic-benchmark -f docker/databases/docker-compose.yaml up -d --wait postgres mysql
uv run --group benchmark python -m benchmark.run --backend postgres --profile smoke
uv run --group benchmark python -m benchmark.run --backend mysql --profile smoke
```

Use `--allow-missing` to record unavailable dependencies or server connections
as skipped measurements in JSON instead of aborting the run.

## Artifacts

Each run writes backend/profile-scoped artifacts:

- raw JSON under `benchmark/results/`;
- CSV summaries under `benchmark/charts/<profile>/<backend>/`;
- SVG latency and speedup charts under `benchmark/charts/<profile>/<backend>/`;
- docs-ready SVG copies under `docs/assets/benchmarks/<profile>/<backend>/` unless `--docs-charts-dir ""` is passed.

The JSON payload records git SHA, dirty-worktree state, Python/platform details,
Ormdantic version, runtime capabilities, backend/server metadata, redacted
database URL, exact profile settings, setup time, measured latency samples,
median absolute deviation, timing order, validation results, skip reasons, and
methodology caveats.

## Compare base and head results

Generate the pull-request report locally from two result files:

```bash
make benchmark-compare \
  BASE=/tmp/base.json \
  HEAD=/tmp/head.json \
  REPORT_DIR=/tmp/ormdantic-report
```

The output directory contains `report.svg`, `report.md`, `report.csv`, and
`comparison.json`. Competitor ratios and base/head changes include deterministic
bootstrap confidence intervals. Diagnostic serialization cases remain visible
but are excluded from winner aggregates and regression gates.

## Pull-request publication

`ORM Benchmark Report` runs base and head sequentially on one runner with a
release native build and `contents: read` only. It uploads raw JSON and rendered
artifacts even when the regression gate fails.

`Publish ORM Benchmark Report` is a separate `workflow_run` workflow. It checks
out only the default branch, treats the downloaded ZIP as untrusted, enforces
path, link, file-count, and size limits, validates the pull-request number and
head SHA, and regenerates all rendered files with trusted code. It publishes an
immutable report to `benchmark-results` under `pull/<number>/<head-sha>/` and
creates or updates one marked pull-request comment.

The publisher workflow must already exist on the default branch before GitHub
will run it. A local run can verify the files and schema, but only a real pull
request can verify artifact download, branch publication, and inline image
rendering.

## Interpretation

Benchmark results are machine-sensitive. Acceptable claims are scoped to the
backend, profile, case, hardware, and commit that produced the artifact. Avoid
global statements such as "Ormdantic is always faster." Prefer wording like:
"On this machine, for SQLite smoke, this case measured X."

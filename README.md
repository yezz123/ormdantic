![Logo](https://raw.githubusercontent.com/yezz123/ormdantic/main/docs/logo.svg)

<p align="center">
  <em>A Rust-backed async ORM for Python applications that use Pydantic models.</em>
</p>

<p align="center">
<a href="https://github.com/yezz123/ormdantic/actions/workflows/ci.yml" target="_blank">
    <img src="https://github.com/yezz123/ormdantic/actions/workflows/ci.yml/badge.svg" alt="Test">
</a>
<a href="https://codecov.io/gh/yezz123/ormdantic">
    <img src="https://codecov.io/gh/yezz123/ormdantic/branch/main/graph/badge.svg" alt="Coverage">
</a>
<a href="https://pypi.org/project/ormdantic" target="_blank">
    <img src="https://img.shields.io/pypi/v/ormdantic?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/ormdantic" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/ormdantic.svg?color=%2334D058" alt="Supported Python versions">
</a>
<a href="https://app.codspeed.io//yezz123/ormdantic?utm_source=badge"><img src="https://img.shields.io/endpoint?url=https://app.codspeed.io//badge.json" alt="CodSpeed Badge"/></a>
</p>

Ormdantic lets you declare database tables with Pydantic v2 models and run async CRUD, relationship loading, migrations, reflection, and native SQL execution through a Rust-backed runtime.

The project is designed for applications that want Python model ergonomics without giving up SQL database features. Python owns the model and API surface; Rust owns SQL compilation, type conversion, and driver execution.

## What you get

| Area | What Ormdantic Provides |
| --- | --- |
| Models | Pydantic models decorated as database tables. |
| CRUD | Async insert, update, upsert, delete, find, count, and bulk update helpers. |
| Queries | Dictionary filters for ordinary cases and expression objects for advanced SQL. |
| Relationships | Explicit joined and select-in loaders, plus explicit relationship loading. |
| Transactions | Async transaction and session contexts. |
| Migrations | Snapshots, diffs, plans, migration artifacts, history, rollback, repair, and squash helpers. |
| Reflection | Live database inspection for tables, columns, indexes, constraints, schemas, views, sequences, and dialect metadata. |
| Drivers | SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle through the native runtime. |

## Benchmarks

The reproducible benchmark report in `benchmark/` compares Ormdantic,
SQLAlchemy, and SQLModel on local SQLite file databases. It includes read and
write cases, and the optional huge profile uses million-row datasets.

![Ormdantic speedup over SQLAlchemy and SQLModel](docs/assets/benchmarks/default/ormdantic-orm-benchmark-speedup.svg)

![Ormdantic, SQLAlchemy, and SQLModel median latency](docs/assets/benchmarks/default/ormdantic-orm-benchmark-latency.svg)

Million-row profile:

![Million-row Ormdantic speedup over SQLAlchemy and SQLModel](docs/assets/benchmarks/huge/ormdantic-orm-benchmark-speedup.svg)

## Install

```bash
uv add ormdantic
```

## First example

```python
from pydantic import BaseModel, Field

from ormdantic import Ormdantic

db = Ormdantic("sqlite:///app.sqlite3")


@db.table(pk="id", indexed=["name"])
class Flavor(BaseModel):
    id: str
    name: str = Field(min_length=2, max_length=63)
    rating: int = 0


async def main() -> None:
    await db.init()

    await db[Flavor].insert(Flavor(id="vanilla", name="Vanilla", rating=5))

    result = await db[Flavor].find_many(
        {"rating": {"gte": 4}},
        order_by=["name"],
    )

    for flavor in result.data:
        print(flavor.name)
```

## Learn the project

Start with the documentation when you are new:

- `docs/learning-path.md` tells new and advanced readers where to start.
- `docs/quickstart.md` walks through the first model, first table, first query, first session, and first migration preview.
- `docs/concepts/` explains tables, fields, relationships, querying, loading, sessions, migrations, events, and the native engine.
- `docs/drivers/` explains SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle behavior.
- `docs/examples/` contains task-focused how-to guides.
- `docs/api/` documents the Python API with generated references and usage notes.

## Migration CLI

Migration commands can read the database URL from `--url`, an exported
`DATABASE_URL`, or a local `.env` file:

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
uv run ormdantic migrations init
uv run ormdantic migrations apply-dir migrations
uv run ormdantic migrations current
```

The CLI prints redacted connection details and summaries such as
`Applied 2 migrations from migrations.` instead of only raw revision IDs.

## Development

Common commands:

```bash
bash scripts/lint.sh
bash scripts/test.sh
bash scripts/docs_build.sh
```

The docs use Zensical:

```bash
uv run --group docs zensical build
uv run --group docs zensical serve
```

Rust crates live under `rust/crates/` and are managed from the repository root `Cargo.toml`.

## Status

Ormdantic is evolving quickly. Prefer explicit migrations, review generated SQL before applying it in production, and check the driver-specific pages for dialect behavior.

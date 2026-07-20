![Logo](https://raw.githubusercontent.com/yezz123/ormdantic/main/docs/logo.svg)

<p align="center">
  <em>A Rust-backed async ORM for Python applications that use Pydantic models.</em>
</p>

| Project | Status |
|---------|--------|
| CI      | [![CI](https://github.com/yezz123/ormdantic/actions/workflows/ci.yml/badge.svg)](https://github.com/yezz123/ormdantic/actions/workflows/ci.yml) [![pre-commit.ci status](https://results.pre-commit.ci/badge/github/yezz123/ormdantic/main.svg)](https://results.pre-commit.ci/latest/github/yezz123/ormdantic/main) [![Codecov](https://codecov.io/gh/yezz123/ormdantic/graph/badge.svg)](https://codecov.io/gh/yezz123/ormdantic) |
| Meta    | [![Package version](https://img.shields.io/pypi/v/ormdantic?color=%2334D058&label=pypi%20package)](https://pypi.org/project/ormdantic) [![Downloads](https://static.pepy.tech/badge/ormdantic)](https://pepy.tech/project/ormdantic) [![Pydantic Version 2](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pydantic/pydantic/main/docs/badge/v2.json)](https://pydantic.dev) [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff) [![CodeSpeed](https://img.shields.io/endpoint?url=https://app.codspeed.io//badge.json)](https://app.codspeed.io//yezz123/ormdantic?utm_source=badge) |

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

- `[Quick Start](https://ormdantic.yezz.me/quickstart/)` walks through the first model, first table, first query, first session, and first migration preview.
- `[learning Path](https://ormdantic.yezz.me/learning-path)` tells new and advanced readers where to start.
- `[Concepts](https://ormdantic.yezz.me/concepts/)` explains tables, fields, relationships, querying, loading, sessions, migrations, events, and the native engine.
- `[Supported Drivers](https://ormdantic.yezz.me/drivers/)` explains SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle behavior.
- `[Examples](https://ormdantic.yezz.me/examples/existing-databases/)` contains task-focused how-to guides.
- `[API Documentation](https://ormdantic.yezz.me/api/reference/)` documents the Python API with generated references and usage notes.

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

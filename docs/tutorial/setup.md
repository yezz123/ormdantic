# Set up the reference application

By the end of this page, you will have the Todo package installed and its tests
running without creating a database server.

## Prerequisites

You need Python 3.10 or newer, `uv`, and a local checkout of Ormdantic. Docker is
only required when you reach the PostgreSQL chapter.

Install the example dependencies from the repository root:

```console
uv sync --extra examples --extra playground --group testing
```

The `examples` extra contains FastAPI, HTTPX, and Uvicorn. It is intentionally
separate from Ormdantic's core dependencies, so applications that only need the
ORM do not install a web framework.

## Inspect the package boundary

```text
examples/todo_app/
├── app/
│   ├── config.py
│   ├── database.py
│   ├── errors.py
│   ├── main.py
│   ├── models.py
│   ├── routes.py
│   ├── schemas.py
│   └── service.py
├── migrations/
│   ├── postgresql/
│   └── sqlite/
├── tests/
├── Dockerfile
├── docker-compose.yml
└── ormdantic.toml
```

Run the example suite:

```console
uv run --extra examples pytest examples/todo_app/tests -q
```

Tests create isolated SQLite files in pytest's temporary directory. They do not
write `todo-dev.sqlite3` into your checkout.

## Start the API with SQLite

```console
cd examples/todo_app
cp .env.example .env
uv run --extra examples ormdantic migrations apply-dir migrations/sqlite
uv run --extra examples uvicorn app.main:app --reload
```

Open <http://127.0.0.1:8000/docs>. A successful `GET /health` returns
`{"status":"ready","database":"sqlite"}`. The response never contains the
database URL.

Continue with [Configuration](configuration.md) before changing environments.
For package-wide installation choices, read [Installation](../installation.md).

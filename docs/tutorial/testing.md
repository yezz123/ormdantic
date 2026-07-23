# Test the application

By the end of this chapter, you can verify each layer independently and run an
optional live PostgreSQL contract with the same service behavior.

## Run the example suite

```console
uv run --extra examples pytest examples/todo_app/tests -q
```

The suite covers:

- configuration defaults, production requirements, and URL redaction;
- model registration, UUIDs, timestamps, constraints, and relationships;
- request validation, PATCH semantics, and OpenAPI schemas;
- real SQLite CRUD, typed filters, pagination, transactions, and loading;
- all HTTP routes and stable error responses;
- artifact checksums, dependencies, apply, rollback, and reapply;
- Dockerfile and Compose startup contracts.

API tests use HTTPX's in-process ASGI transport but enter the real FastAPI
lifespan. Service and migration tests use separate database files under pytest's
temporary directory. This keeps tests fast without replacing Ormdantic with a
mock.

## Run the PostgreSQL contract

Point the opt-in test at a disposable database:

```console
ORMDANTIC_TODO_POSTGRES_URL=postgresql://todo:todo@127.0.0.1:5432/todo \
  uv run --extra examples pytest \
  examples/todo_app/tests/test_postgresql.py -q
```

Use a database created for this test. The contract applies the checked-in
PostgreSQL history and runs the service against the native driver.

## Run the repository quality gates

```console
bash scripts/test.sh
bash scripts/lint.sh
bash scripts/docs_build.sh
```

The test script combines Python statement and branch coverage. A high number is
useful only when tests assert behavior; the example tests deliberately exercise
real persistence paths and failure responses.

See [Local development](../development/local-development.md) for the full
repository workflow.

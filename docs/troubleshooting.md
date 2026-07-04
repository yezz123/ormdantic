# Troubleshooting

Use this page when Ormdantic raises an exception or a database operation behaves differently from what you expected. Ormdantic raises typed exceptions for common runtime failures. Catch the most specific class you can, then inspect `error.context` for structured metadata.

```python
from ormdantic import QueryExecutionError

try:
    await db[Flavor].insert(flavor)
except QueryExecutionError as exc:
    print(exc)
    print(exc.context["table"], exc.context["operation"])
```

## Fix connection failures

`DatabaseConnectionError` means the native runtime could not open or use the configured database connection.

Check these items first:

- the URL scheme matches an installed backend, such as `sqlite`, `postgresql`, `mysql`, `mariadb`, `mssql`, or `oracle`
- network host, port, database name, and credentials are correct
- the Rust extension was built with the backend you are using
- `db.runtime_diagnostics()["capabilities"]` reports the expected backend as available

## Fix schema failures

`SchemaError` covers schema creation, table drops, and backend DDL failures. The context includes the backend and operation, and native details are preserved on `native_message`.

Common fixes:

- call `await db.init()` after all models are registered
- verify primary key and relationship fields are present on the Pydantic model
- check backend-specific metadata, such as index options, tablespaces, partitions, or enum support
- run `db.migrations.dry_run()` before applying schema changes to inspect generated SQL

## Fix query failures

`QueryCompilationError` means Ormdantic could not compile the query payload. `QueryExecutionError` means the SQL compiled but the database rejected or failed the execution.

Enable debug diagnostics to inspect generated SQL and bind names:

```python
db = Ormdantic("sqlite:///app.sqlite3", debug=True)
db.on_query(lambda **event: print(event["sql"], event["bind_names"]))
```

Debug event payloads redact sensitive values by bind name. Names containing tokens such as `password`, `secret`, `token`, or `api_key` show `<redacted>` instead of the original value.

## Fix migration and reflection failures

`MigrationError` wraps migration history, apply, rollback, and dirty-state failures. If the migration table is dirty after a failed apply, repair it intentionally:

```python
await db.migrations.repair(clear_dirty=True)
```

`ReflectionError` wraps live inspector calls. Use `before_reflection` and `after_reflection` handlers when you need timing data for schema inspection.

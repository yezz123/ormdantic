# Query Builder Removal Checkpoint

## Goal

Record the slice where Ormdantic removed the Python query-builder dependency and moved current SQL generation into Rust.

## Rust Now Owns

- Flat CRUD query compilation.
- Count query compilation for current equality filters.
- Depth-based joined select compilation.
- Current relationship alias generation using `table/relation\\column`.
- Dialect placeholder and identifier rendering.
- SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle dialect name recognition.

## Python Still Owns

- Pydantic model declarations and object construction.
- Relationship discovery from Pydantic model annotations.
- Async execution through SQLAlchemy.
- Nested serializer object folding for relationship results.

## Removed

- The Python query-builder dependency.
- Python fallback query construction for insert/update/upsert/find/delete.
- The old relationship join query builder in `ormdantic/generator/_field.py`.

## Remaining Work

The next migration target is Rust nested row folding. The serializer still reconstructs nested dictionaries and Pydantic models in Python, which is correct for now because it preserves behavior while query generation moves to Rust.

SQLAlchemy is still retained as the async execution layer. Replacing execution with native Rust drivers remains a separate, larger decision.

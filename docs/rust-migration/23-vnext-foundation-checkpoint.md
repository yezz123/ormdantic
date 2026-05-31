# vNext Foundation Checkpoint

## Goal

Capture the first breaking-vNext foundation slice after the initial Rust workspace kickoff.

## Implemented Foundation

The workspace now includes:

- `ormdantic-core` for shared identifiers, result aliases, and structured errors.
- `ormdantic-schema` for table metadata, column metadata, relationship descriptors, and schema registry validation.
- `ormdantic-hydrate` for flat hydration plans and joined result-shape planning.
- `ormdantic-dialects` for SQLite and PostgreSQL dialect basics.
- `ormdantic-sql` for a typed SQL AST and initial query compilation.
- `ormdantic-py` for private PyO3 bindings exposed as `ormdantic._ormdantic`.

## Python Bridge

Python now has private bridge modules:

- `ormdantic/generator/_rust_schema.py`
- `ormdantic/generator/_rust_query.py`
- `ormdantic/generator/_hydration.py`

These modules call Rust when the extension exposes the needed symbol and fall back safely when a local checkout has an older or unavailable extension.

## Supported Rust Query Shapes

The Rust compiler foundation supports:

- select by primary key
- insert
- delete by primary key
- count/update/upsert AST foundations in Rust
- SQLite placeholders
- PostgreSQL placeholders
- quoted identifiers

The existing PyPika paths remain in place until Rust output is integrated into CRUD execution and covered by behavior tests.

## Current Boundary

Rust now owns metadata validation, result-shape planning, and SQL compilation primitives. Python still owns:

- Pydantic model introspection
- async SQLAlchemy execution
- CRUD orchestration
- Pydantic object construction
- fallback query generation

## Next Step

The next migration slice should connect Rust query compilation into `OrmField` and `OrmQuery` behind a runtime fallback, then compare SQL behavior with existing PyPika queries before removing PyPika.

# ormdantic-py

PyO3 bindings for Ormdantic Rust internals.

`ormdantic-py` builds the Python extension module exposed as `ormdantic._ormdantic`. It is the boundary where Python passes schema metadata, query plans, values, and connection URLs into Rust.

## Python Module Surface

| Export                     | Purpose                                                     |
| -------------------------- | ----------------------------------------------------------- |
| `PyNativeConnection`       | Python wrapper around a persistent native connection.       |
| `hydrate_flat`             | Hydrates flat result rows into Python dictionaries.         |
| `hydrate_joined`           | Hydrates joined result rows into nested dictionaries/lists. |
| `plan_result_shape`        | Returns result-shape metadata for joined hydration.         |
| `validate_schema_tables`   | Registers table metadata and validates relationships.       |
| `compile_select_pk`        | Compiles a primary-key lookup.                              |
| `compile_find_many`        | Compiles a filtered/paginated select.                       |
| `compile_joined_find_many` | Compiles a joined relationship select.                      |
| `compile_count`            | Compiles a count query.                                     |
| `compile_insert`           | Compiles an insert query.                                   |
| `compile_update`           | Compiles an update query.                                   |
| `compile_upsert`           | Compiles an upsert query.                                   |
| `compile_delete_pk`        | Compiles a primary-key delete query.                        |
| `execute_native`           | Executes SQL through a native database URL.                 |
| `snake_case`               | Converts names to Ormdantic's database naming style.        |
| `sql_value`                | Converts Python values into SQL bind values.                |
| `compile_create_table_sql` | Compiles table DDL statements.                              |
| `compile_drop_table_sql`   | Compiles a table drop statement.                            |

## Dependencies

Internal dependencies:

- `ormdantic-core`
- `ormdantic-schema`
- `ormdantic-hydrate`
- `ormdantic-dialects`
- `ormdantic-engine`
- `ormdantic-sql`

External dependencies:

- `pyo3`

## Build

From the repository root:

```bash
cargo build -p ormdantic-py
```

The Python package build uses `maturin` and maps this crate to `ormdantic._ormdantic`.

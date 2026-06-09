# Refactor And Test Roadmap

This roadmap tracks the ongoing split of large Python and Rust modules into focused files, plus the test and benchmark coverage needed to keep that split safe.

## Completed Slices

The first completed slices focus on stable public facades with internals moved into focused modules:

- `ormdantic-engine` exposes a small facade from `src/lib.rs`.
- Engine runtime concerns live in focused modules for connections, statements, runtime helpers, reflection, migrations, values, and results.
- Driver-specific code remains under `src/drivers/`.
- Engine integration tests now have shared support helpers and a grouped driver matrix.
- The grouped driver matrix covers driver alias URL handling, dialect detection, backend-specific parameter binding, `DbValue` round-trips for nulls, integers, floats, text, and booleans, backend-specific numeric edge cases for integer widths, MySQL/MariaDB unsigned and decimal text values, MSSQL decimal values, and Oracle `NUMBER` values, CRUD result round-trips, PostgreSQL/MariaDB `RETURNING` and MSSQL `OUTPUT` statement rowsets, DDL add-column/index/drop-column lifecycle behavior, syntax-error mapping, unique constraint-error mapping, connection-failure mapping, live reflection smoke execution, and transaction/savepoint behavior across Postgres, MySQL, MariaDB, MSSQL, and Oracle feature gates.
- External migration integration tests now round-trip live autogenerate artifacts through disk, apply them, verify reflected schema changes, and roll them back across Docker-backed Postgres, MySQL, MariaDB, MSSQL, and Oracle URLs.
- Engine benchmarks cover statement-result conversion, SQLite execution, migration-store revision checks, and reflection query planning across dialects.
- Python migration SQL helpers are split into `ormdantic._migrations.sql` with unit coverage.
- Python migration model and document serialization helpers are split into `ormdantic._migrations.models` and `ormdantic._migrations.documents`.
- Python migration artifact serialization, checksums, file discovery, coercion, and contiguous-artifact validation are split into `ormdantic._migrations.artifacts`.
- Python migration history table storage, history row parsing/writing, dirty repair, locking, and migration operation execution are split into `ormdantic._migrations.history`.
- Python migration schema diffing, destructive-change checks, SQLite rebuild planning, SQL operation classification, artifact creation, and squash generation are split into `ormdantic._migrations.planning`.
- Python migration live snapshot reflection and backend-specific reflection SQL helpers are split into `ormdantic._migrations.reflection`.
- Python migration CLI command handlers are split into `ormdantic._migrations.cli`, with `ormdantic.cli` kept as the top-level command facade.
- `ormdantic-py` has self-contained modules for PyO3 binding registration, database/table handle classes, event bridge, session runtime, transaction options, runtime schema conversion, schema validation/diff bridge wrappers, DDL helper compilation, native connection/runtime/reflection helpers, migration revision execution helpers, query/filter helper parsing, compile query wrappers, select-in plan compilation, typed expression payload parsing, table-handle query methods, hydration bridge wrappers, and utility pyfunctions.
- PyO3 bridge benchmarks cover Python-facing scalar value conversion, query payload normalization and compilation, schema validation payloads, flat/joined hydration payloads, and select-in merge payloads through in-memory module registration.
- `ormdantic-sql` exposes a small facade from `src/lib.rs`, with AST/data types in `ast.rs`, filter/predicate conversion in `filters.rs`, and rendering/compilation in `compiler.rs`.
- SQL compiler tests are organized as integration test files by behavior, including facade, CRUD, expressions, filters, and joins.
- SQL benchmarks are split across general query compilation, expression compilation, and dedicated bulk/wide DML compilation targets.
- `ormdantic-schema` has focused modules for tables, columns, indexes, constraints, relationships, namespaces, column aliases, schema diffs, reflected schema conversion, and registry validation.
- Schema tests are organized as integration test files for aliases, columns, constraints, diffs, indexes, namespaces, reflection, registry behavior, relationships, and tables.
- Schema benchmarks cover registry validation and schema diff generation.
- `ormdantic-dialects` keeps dialect structs and the public trait in `src/lib.rs`, with parsing, identifiers, DDL rendering, reflection query metadata, and transaction rendering split into focused modules.
- Dialect tests are organized by behavior for parsing, identifiers, capabilities, DDL, reflection SQL, transactions, and upsert clauses.
- Dialect benchmarks cover identifier and placeholder rendering, create-table rendering, and transaction SQL rendering.
- `ormdantic-hydrate` exposes a small facade from `src/lib.rs`, with result columns/shapes, flat plans, hydration keys, relationship graphs, row helpers, and select-in merging split into focused modules.
- Hydration tests now cover result-column parsing, flat plans, keys, graph deduplication, relationship nodes, select-in parent keys, and collection/scalar select-in merges.
- Hydration benchmarks cover result-shape planning, flat-plan construction, select-in merge behavior, nested select-in hydration for scalar and collection relationships, and graph duplicate folding.

## Next Slices

1. Continue Rust crate cleanup as new broad modules appear.

   The broad Rust crates in this roadmap now expose facade-style public modules for their completed slices. Future Rust cleanup should be driven by new behavior gaps, ownership boundaries, or modules that grow broad again.

2. Grow Rust tests by responsibility.

   Test folders should mirror behavior, not implementation files:

   - `tests/sql/` for compiler output, bind order, filters, joins, pagination, and upsert.
   - `tests/dialects/` for placeholder styles, identifier quoting, DDL syntax, reflection SQL, and transaction SQL.
   - `tests/schema/` for table registration, key validation, constraints, indexes, and relationship metadata.
   - `tests/hydrate/` for flat rows, nested rows, optional relationships, lists, alias parsing, and duplicate folding.
   - `tests/engine/` for runtime capabilities, connection state, statement results, migration store, and reflection.
   - `tests/drivers/` for Docker-backed database behavior.

3. Expand driver integration tests.

   Remaining backend-specific coverage should include:

   - Exact decimal/unsigned overflow representation once `DbValue` grows a decimal or big-integer variant, including direct PostgreSQL `NUMERIC` decoding and unsigned values outside the `i64` range.
   - Oracle out-bind `RETURNING INTO` behavior once the engine exposes an out-parameter API.
   - Additional backend-specific edge cases as new driver features land.

4. Add benchmark groups.

   Benchmark folders should measure stable boundaries:

   - SQL compiler benchmarks for additional backend-specific rendering paths as the compiler grows.
   - Dialect rendering benchmarks for identifier quoting, placeholder allocation, and DDL generation.
   - Hydration benchmarks for additional loader shapes as hydration APIs grow.
   - Engine benchmarks for additional connection-state and batch-execution paths as those APIs grow.
   - PyO3 bridge benchmarks for additional runtime/session payload conversion as those bridge paths grow.

## Rules

- Keep public imports stable when splitting Python modules.
- Keep PyO3 code out of core Rust crates.
- Keep SQL generation separate from execution.
- Keep driver tests explicit about backend-specific parameter typing.
- Prefer small shared test helpers over broad test harness abstractions.
- Run Docker-backed driver tests after changing connection dispatch, parameter conversion, transaction behavior, or reflection SQL.

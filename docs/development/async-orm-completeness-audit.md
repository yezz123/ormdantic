# Async ORM Completeness Audit

**Date:** 2026-07-20

## Conclusion

Ormdantic already has an unusually broad alpha surface: Pydantic table metadata, async CRUD facades, typed expressions, relationship loaders, sessions, transactions, events, reflection, schema diffs, migration artifacts, six dialects, and a Rust-native runtime. Its migration and schema modeling are more developed than many young ORMs.

It is not yet production-complete for concurrent async applications. The highest-risk gap is connection ownership: an `Ormdantic` runtime shares one persistent native connection between table handles, sessions, and transaction helpers. Blocking driver calls are moved to worker threads for normal table operations, but the shared connection serializes work and transaction state is not bound to the calling async task. Several async migration methods also execute blocking work directly on the event-loop thread.

“Complete” should mean a documented, production-safe contract for concurrent web services. It should not mean duplicating the entire SQLAlchemy ecosystem.

## Verification baseline

The following checks passed locally on Python 3.10.19:

- `bash scripts/test.sh`: 1,168 passed, 79 skipped.
- Python coverage: 99.68% statement coverage, 97.33% branch coverage, and
  99.02% combined coverage. The configured gate is 99.00%.
- `bash scripts/lint.sh`: all Ty checks passed.
- Documentation example, navigation, image, diagram, and completeness-matrix
  checks passed. The Zensical documentation build also passed.
- `cargo test --workspace`: passed, including Rust documentation tests.
- `uv build`: the source distribution and CPython 3.10 macOS ARM64 wheel built
  successfully. The wheel contains the optional Playground package and stylesheet;
  FastAPI and the other example-only dependencies remain in the `examples` extra.
- The Todo Compose stack built a Linux ARM64 wheel, started PostgreSQL 17, applied
  both PostgreSQL migrations, and reported a healthy API response. The containers
  and network were removed afterward while the named demonstration volume was kept.

The 79 Python skips are primarily external-database tests without configured service
URLs, including Playground contracts for PostgreSQL, MySQL, MariaDB, SQL Server, and
Oracle. The Todo PostgreSQL deployment was exercised through Compose, but the
environment-gated pytest contract was not run against a host-exposed PostgreSQL URL.
Rust external driver tests also return successfully when their environment URL is
absent, so a local green run is not evidence that every external server was
exercised. The CI coverage job starts the Docker database matrix and provides those
URLs.

## Reference application and documentation status

The repository now has one production-shaped Todo reference application under
`examples/todo_app/`. It uses FastAPI, SQLite for local development, PostgreSQL for
the Compose deployment, separate checked migration chains for both dialects, stable
domain errors, transactions, filtering, pagination, and health reporting.

The progressive tutorial covers configuration, models, services, API routes,
migrations, the Playground, deployment, testing, and troubleshooting. It uses
tested source includes, five Mermaid diagrams, and real OpenAPI and Playground
screenshots. The documentation matrix maps every exported top-level API group,
migration CLI command, supported driver, and Playground workflow to a canonical
page. Automated tests enforce that inventory and navigation.

## What is already strong

- Pydantic-first table registration and extensive backend metadata.
- Async table CRUD, bulk writes, filtered updates/deletes, counts, and typed projections.
- Explicit joined and select-in relationship loading without hidden attribute I/O.
- Unit-of-work session with identity map, dirty detection, dependency ordering, and savepoint snapshots.
- Transaction isolation options, savepoints, lifecycle events, typed errors, and redacted diagnostics.
- Snapshot, live reflection, diff, plan, artifact, checksum, history, rollback, dirty repair, dependency, and squash migration APIs.
- Rich DDL coverage for SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle.
- High automated coverage and broad Python/Rust test suites.
- Multi-OS and Python 3.10-3.14 CI and release-wheel workflows.

## Playground delivery status

The optional `ormdantic playground` package now provides the migration-focused operator experience requested by this audit:

- Live model-file and database-schema watching with debounced, generation-safe refreshes.
- Schema, drift, migration, history, settings, help, and overview screens.
- Full embedded TOML/JSON artifact and SQL editing with validation, checksum updates, atomic saves, and crash drafts.
- Generate, apply, rollback, repair, and squash workflows.
- Risk summaries, exact typed confirmations, stronger production phrases, and mandatory confirmations for history rewrites.
- Redacted connection diagnostics and configuration through TOML, environment variables, or CLI overrides.
- Isolated model inspection with timeouts and cancellation.
- A complete playground documentation section, tested configuration examples, CLI/API reference, troubleshooting, and safety guidance.

This is a complete migration playground surface, but it intentionally does not hide or solve the connection ownership, lifecycle, execution, TLS, and concurrency gaps below. Those remain framework-level work required before Ormdantic can claim production-complete async ORM semantics.

## Priority 0: production blockers

### Connection pooling and task-local ownership

`PyDatabase` owns one `Arc<Mutex<NativeConnection>>`, and every table handle clones that shared connection. Concurrent queries therefore serialize on one connection. More importantly, `db.transaction()` and `db.session()` mutate transaction state on that shared connection, so unrelated async tasks can accidentally execute inside another task's transaction.

Required outcome:

- A configurable pool per `Ormdantic` instance.
- One leased connection for each standalone operation.
- One connection pinned to the current transaction/session using explicit context ownership.
- Nested transaction behavior defined per task.
- Pool size, acquisition timeout, idle timeout, lifetime, and recycling controls.
- Tests proving isolation between concurrent tasks and sessions.

### Lifecycle management

There is no public database `close()`/`dispose()` lifecycle, and `NativeEngine` opens its connection in synchronous construction. Applications cannot deterministically drain a pool or release connections during framework shutdown.

Required outcome:

- Explicit `connect()` and `close()` methods.
- `async with Ormdantic(...)` lifecycle support.
- Idempotent startup/shutdown and graceful in-flight draining.
- Health check/ping and stale-connection recycling.
- Framework lifespan documentation.

### Async execution contract

PostgreSQL, MySQL, and SQLite use synchronous Rust drivers. Normal table calls use `asyncio.to_thread`, which keeps the Python loop responsive but is not native asynchronous I/O. Several `MigrationManager` methods are declared `async` yet open connections and execute history/migration work directly without `to_thread`.

Required outcome:

- Define whether Ormdantic promises native async drivers or a bounded blocking executor.
- If retaining blocking drivers, own a bounded executor rather than consuming the process-wide default without backpressure.
- Move every blocking migration, reflection, connection-open, and transaction operation off the event loop.
- Add query, connect, pool-acquisition, and shutdown timeouts.
- Define cancellation semantics, including what happens to the underlying database operation.
- Test event-loop responsiveness and cancellation races for every API family.

### TLS and production connection options

The PostgreSQL driver uses `NoTls`. A production-ready driver contract needs verifiable TLS behavior and backend connection options rather than relying on URL normalization alone.

Required outcome:

- TLS modes, certificate roots, hostname verification, and client certificate support where applicable.
- Backend-specific connection-option models with safe URL/environment integration.
- Documentation and live tests for encrypted connections.

### Concurrency and failure testing

There are responsiveness tests for table execution, but no tests using `asyncio.gather` to prove query concurrency or transaction isolation. Pool exhaustion, cancellation, shutdown, stale connection, server restart, and reconnect behavior are untested because those features do not yet exist.

Required outcome:

- Deterministic concurrent task and transaction tests.
- Connection-loss and server-restart tests.
- Deadlock, serialization retry guidance, timeout, cancellation, and pool-exhaustion tests.
- Mandatory live matrix tests for supported production drivers.

## Priority 1: expected ORM capabilities

### Composite primary keys

The public table decorator and Python metadata accept one `pk: str`. Rust hydration can reason about composite keys, and table-level composite foreign keys exist, but the public ORM path cannot declare or use a composite primary key.

Required outcome:

- `pk: str | Sequence[str]` through metadata, schema, SQL compilation, CRUD, hydration, identity maps, sessions, loaders, reflection, and migrations.
- Tuple/mapping primary-key lookup APIs with stable typing.

### Relationship parity

Rust schema metadata defines one-to-one, one-to-many, many-to-one, many-to-many, secondary tables, cascades, and loader strategies. The Python bridge does not wire `secondary_table` or direction/cascade metadata into the runtime. The current Python helper labeled “many-to-many” represents a direct back-reference and behaves as one-to-many.

Required outcome:

- Explicit one-to-one cardinality.
- Association-table many-to-many mappings and loading.
- Configurable save/update, merge, delete, delete-orphan, refresh/expire, and expunge cascades.
- Relationship configuration represented consistently in Python, Rust, migrations, and docs.

### Public query surface

The Rust SQL AST supports explicit joins and advanced select shapes, but the Python `SelectExpressionQuery` does not expose arbitrary joins. Set operations and row-locking clauses are also absent from the public API.

Required outcome:

- Typed inner/outer/cross/lateral join construction.
- `UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` where supported.
- `FOR UPDATE`, `NOWAIT`, and `SKIP LOCKED` capability-aware APIs.
- Public returning/output abstractions with explicit backend support.
- Transaction-aware raw SQL on `Ormdantic` and `Session`, not only a separate engine.

### Streaming and memory bounds

Driver execution currently materializes complete result sets into vectors/lists before hydration. Large reads cannot use async iteration or bounded fetch sizes.

Required outcome:

- Async row/model streaming with explicit cursor lifetime.
- Configurable fetch size and backpressure.
- Cancellation and connection-return behavior for partially consumed streams.
- Streaming relationship-loading limitations documented and tested.

### Session semantics and optimistic concurrency

The session has a useful unit-of-work core but a small public surface. It lacks session-bound query helpers, public detached/expired state rules, and version-column conflict detection.

Required outcome:

- Session-bound `get`, select, execute, and stream paths that share the pinned transaction.
- Explicit expiration, detachment, refresh, and commit behavior.
- Configurable version columns and a typed stale-write error.
- Clear bulk-operation interaction with identity-map state.

## Priority 2: release maturity

### Initialization semantics

`await db.init()` builds the runtime and also calls `create_all()`. This mixes connection/runtime initialization with schema mutation and encourages redundant calls in tests and examples.

Required outcome:

- Separate runtime connection/metadata initialization from `create_all()`.
- Preserve a compatibility path with deprecation guidance.
- Production docs should default to migrations, not implicit table creation.

### Packaging claims

`pyproject.toml` advertises PyPy and GraalPy, while CI and release builds cover CPython versions. A compiled PyO3 package should claim only interpreters and platforms that are built and smoke-tested.

Required outcome:

- Add real PyPy/GraalPy build and smoke jobs or remove those classifiers.
- Publish an explicit wheel/platform/architecture matrix.
- Add supported manylinux/musllinux and architecture coverage according to the release policy.

### Documentation correctness

The known `columns={...}` examples have been corrected to `column_options={...}`, playground TOML examples are parsed in tests, and the documentation navigation is checked automatically. The broader docs still blur `init()` and `create_all()` responsibilities, and not every core ORM example is executable in CI.

Required outcome:

- Executable documentation snippets or doctest-style smoke coverage.
- One backend capability contract linking ORM, migration, reflection, and runtime support.
- Compatibility, deprecation, and semantic-versioning policy.
- Accurate limitations for returning rows, transactional DDL, TLS, and optional drivers.

### Observability and integration

Events and query timing already provide a good base. Production maturity would benefit from standardized integration without making a telemetry vendor mandatory.

Required outcome:

- Stable structured event schema.
- Optional OpenTelemetry spans/metrics adapter.
- Pool metrics and connection lifecycle events.
- FastAPI/Starlette and other ASGI lifespan recipes.

## Recommended sequence

1. Pool and task-local transaction ownership.
2. Lifecycle, bounded execution, timeouts, cancellation, and TLS.
3. Concurrency and failure matrix tests.
4. Composite primary keys and relationship parity.
5. Streaming, session-query integration, optimistic concurrency, and public query expansion.
6. Initialization cleanup, packaging accuracy, documentation enforcement, and compatibility policy.

The first three steps are the gate for describing Ormdantic as a production-ready async ORM. The remaining steps determine how broad the ORM contract should be while retaining its focused Pydantic-first identity.

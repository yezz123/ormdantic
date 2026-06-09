# SQLAlchemy Parity

This matrix tracks Ormdantic vNext against SQLAlchemy-style capabilities.

| Area                                  | Status  | Notes                                                                                                                                                    |
| ------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Core CRUD                             | Full    | Insert, update, upsert, delete, find one, find many, count compile through Rust.                                                                         |
| Bind parameters                       | Full    | Queries use driver placeholders and ordered bind values.                                                                                                 |
| SQLite runtime                        | Full    | Native Rust execution with `rusqlite`.                                                                                                                   |
| PostgreSQL runtime                    | Full    | Native Rust support with gated CI integration.                                                                                                           |
| MySQL runtime                         | Full    | Native Rust support with gated CI integration.                                                                                                           |
| MariaDB runtime                       | Full    | Native Rust support via MySQL protocol with gated CI integration.                                                                                        |
| SQL Server runtime                    | Full    | Native Rust support through the pure Rust TDS driver compiled into default Python wheels.                                                                |
| Oracle runtime                        | Full    | Native Rust support through the pure Rust TNS driver compiled into default Python wheels.                                                                |
| Query expressions                     | Partial | Comparisons, `IN`, `LIKE`, null checks, and a Python expression facade are implemented; subqueries/CTEs/window functions remain future work.             |
| DDL                                   | Partial | Tables, keys, indexes, schema-diff SQL, and generated SQLite table rebuilds are covered; advanced DDL object parity remains future work.                 |
| Relationships                         | Partial | Rust-owned joined/depth loading, select-in loading, explicit lazy loading, `noload`, loader filters, and loader ordering are available.                  |
| Transactions                          | Partial | Begin/commit/rollback and savepoint primitives are available; isolation options remain planned.                                                          |
| Sessions/unit of work                 | Partial | Identity map, add, flush, commit, rollback, refresh, delete staging, merge, and expire are available; automatic dirty tracking and cascades are planned. |
| Events                                | Partial | CRUD, flush events, handler removal, and handler clearing are available; transaction/session lifecycle events are planned.                               |
| Migrations                            | Full    | Artifact V2 checksums, durable history, explicit rollback, status/history/current/repair/check CLI commands, live autogenerate, and SQLite rebuild plans. |
| Reflection                            | Partial | Runtime inspection and live autogenerate cover supported database backends for core table metadata; advanced objects and expressions remain planned.      |
| Association proxy / hybrid attributes | Partial | Python descriptors are available; Rust expression lowering for hybrid attributes is planned.                                                             |

Ormdantic intentionally differs from SQLAlchemy where async safety requires explicit behavior, such as explicit relationship loading instead of hidden synchronous lazy loads.

## Remaining Work For A Clean, Fast ORM

Clean means the public API stays small, explicit, typed, and predictable. Fast means hot paths stay Rust-owned, benchmarked, and free of hidden round trips. The main gaps before Ormdantic feels complete are:

| Track                       | Remaining work                                                                                                                                         | Why it matters                                                                                      |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| Query power                 | Add typed subquery predicates, CTE builders, window-function builders, relationship-aware predicates, and relation aggregate ordering.                  | Covers advanced application queries without falling back to raw SQL or expanding the Python surface. |
| Relationship loading        | Add parameter-limit chunking, batch-size controls, and broader cross-dialect coverage for large select-in loads and deeply nested loader graphs.        | Keeps eager loading predictable and fast on real production-sized result sets.                       |
| Sessions/unit of work       | Add automatic dirty tracking, relationship cascades, dependency-aware flush ordering, and clearer detached/expired object behavior.                     | Makes the session API feel complete while preserving explicit async behavior.                        |
| Transactions                | Add isolation-level options, read-only/deferrable transaction flags where supported, and transaction/session lifecycle events.                          | Gives users production-grade control without dropping to driver-specific APIs.                       |
| DDL and schema objects      | Expand advanced defaults, generated/computed columns, functional and partial indexes, richer constraints, enums, sequences, and view-like objects.       | Lets schema and migration workflows represent more database-native features.                         |
| Reflection and migrations   | Reflect advanced schema objects and expression metadata, then feed them into autogenerate with stable diffs and clear downgrade behavior.               | Keeps live database inspection aligned with the DDL and migration story.                             |
| Association-style helpers   | Lower hybrid attributes and association proxies into Rust expression payloads where possible.                                                           | Preserves a clean Python model API while keeping query compilation native.                           |
| Dialect hardening           | Promote SQL Server and Oracle integration from optional/manual coverage to regular CI where feasible, and add backend-specific edge-case suites.        | Makes the "full runtime" claim durable across all advertised backends.                              |
| Value fidelity              | Add first-class decimal and large unsigned integer representations in `DbValue` and test backend-specific numeric decoding.                             | Avoids lossy conversion for financial and high-range identifiers.                                    |
| Performance guardrails      | Keep benchmark groups for query compilation, hydration, select-in merging, relationship loading, migrations, and driver execution in the release gates. | Prevents the ORM from becoming clean at the API layer but slow in the runtime layer.                 |

Non-goals remain important: Ormdantic should not become a full SQLAlchemy clone, hide synchronous lazy loads behind attribute access, or move query compilation and hydration hot paths back into Python.

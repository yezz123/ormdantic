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
| Relationships                         | Partial | Rust-owned joined/depth loading and explicit lazy loading are available; true select-in batching remains planned.                                        |
| Transactions                          | Partial | Begin/commit/rollback and savepoint primitives are available; isolation options remain planned.                                                          |
| Sessions/unit of work                 | Partial | Identity map, add, flush, commit, rollback, refresh, delete staging, merge, and expire are available; automatic dirty tracking and cascades are planned. |
| Events                                | Partial | CRUD, flush events, handler removal, and handler clearing are available; transaction/session lifecycle events are planned.                               |
| Migrations                            | Full    | Artifact V2 checksums, durable history, explicit rollback, status/history/current/repair/check CLI commands, live autogenerate, and SQLite rebuild plans. |
| Reflection                            | Partial | Runtime inspection and live autogenerate cover supported database backends for core table metadata; advanced objects and expressions remain planned.      |
| Association proxy / hybrid attributes | Partial | Python descriptors are available; Rust expression lowering for hybrid attributes is planned.                                                             |

Ormdantic intentionally differs from SQLAlchemy where async safety requires explicit behavior, such as explicit relationship loading instead of hidden synchronous lazy loads.

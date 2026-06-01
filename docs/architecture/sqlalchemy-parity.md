# SQLAlchemy Parity

This matrix tracks Ormdantic vNext against SQLAlchemy-style capabilities.

| Area | Status | Notes |
| --- | --- | --- |
| Core CRUD | Full | Insert, update, upsert, delete, find one, find many, count compile through Rust. |
| Bind parameters | Full | Queries use driver placeholders and ordered bind values. |
| SQLite runtime | Full | Native Rust execution with `rusqlite`. |
| PostgreSQL runtime | Full | Native Rust support with gated CI integration. |
| MySQL runtime | Full | Native Rust support with gated CI integration. |
| MariaDB runtime | Full | Native Rust support via MySQL protocol with gated CI integration. |
| SQL Server runtime | Full | Native Rust support through the pure Rust TDS driver compiled into default Python wheels. |
| Oracle runtime | Full | Native Rust support through the pure Rust TNS driver compiled into default Python wheels. |
| Query expressions | Partial | Comparisons, `IN`, `LIKE`, null checks, and a Python expression facade are implemented; subqueries/CTEs/window functions remain future work. |
| DDL | Partial | Tables, primary keys, foreign keys, indexes, unique/check constraints, and migration operation execution are covered; advanced defaults and schema diffs are planned. |
| Relationships | Partial | Rust-owned joined/depth loading and explicit lazy loading are available; true select-in batching remains planned. |
| Transactions | Partial | Begin/commit/rollback and savepoint primitives are available; isolation options remain planned. |
| Sessions/unit of work | Partial | Identity map, add, flush, commit, rollback, refresh, delete staging, merge, and expire are available; automatic dirty tracking and cascades are planned. |
| Events | Partial | CRUD, flush events, handler removal, and handler clearing are available; transaction/session lifecycle events are planned. |
| Migrations | Partial | Native revision table plus apply/rollback SQL plans are available; generation and schema diffing are planned. |
| Reflection | Partial | Runtime SQLite table/column reflection is available; full cross-dialect introspection is planned. |
| Association proxy / hybrid attributes | Partial | Python descriptors are available; Rust expression lowering for hybrid attributes is planned. |

Ormdantic intentionally differs from SQLAlchemy where async safety requires explicit behavior, such as explicit relationship loading instead of hidden synchronous lazy loads.

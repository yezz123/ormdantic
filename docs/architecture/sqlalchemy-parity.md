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
| Query expressions | Partial | Comparisons, `IN`, `LIKE`, null checks are implemented; subqueries/CTEs/window functions are planned. |
| DDL | Partial | Tables, primary keys, foreign keys, indexes, unique/check constraints are covered; advanced defaults and migrations are planned. |
| Relationships | Partial | Joined/depth loading and explicit lazy loading are available; select-in planning is being expanded. |
| Transactions | Partial | Basic begin/commit/rollback are available. |
| Sessions/unit of work | Partial | Identity map, add, flush, commit, rollback, refresh are available; advanced dirty tracking is planned. |
| Events | Partial | Basic before/after CRUD and flush events are available. |
| Migrations | Planned | Native migrations or Alembic-style interop remain future work. |
| Reflection | Planned | Runtime database introspection remains future work. |
| Association proxy / hybrid attributes | Planned | Not yet implemented. |

Ormdantic intentionally differs from SQLAlchemy where async safety requires explicit behavior, such as explicit relationship loading instead of hidden synchronous lazy loads.

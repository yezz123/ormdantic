# Start here

Use this page to choose the shortest path through the docs. Ormdantic has two main audiences:

- new readers who want to create tables, insert rows, and query data without learning every internal detail first
- advanced readers who need migrations, reflection, dialect behavior, performance tradeoffs, and lower-level SQL control

You do not need to read every page before writing code. Start with the path that matches your current task.

## If you are new to Ormdantic

Start here if you are new to Ormdantic or new to object-relational mappers (ORMs). This path teaches one idea at a time.

1. Read [Why Ormdantic](why-ormdantic.md) to understand the problem it solves.
2. Read [Installation](installation.md) to install the package and check available drivers.
3. Follow [First steps](quickstart.md) with SQLite.
4. Read [Database and tables](concepts/database-and-tables.md) to understand `Ormdantic`, `@db.table`, and `db[Model]`.
5. Read [Field types and metadata](concepts/field-types-and-metadata.md) to understand how Python annotations become database columns.
6. Read [Querying](concepts/querying.md) for `find_one`, `find_many`, filters, expressions, counts, and result objects.
7. Read [Relationships](concepts/relationships.md) and [Loading Strategies](concepts/loading-strategies.md) before modeling related data.

Then build the [Todo reference application](tutorial/index.md). It combines CRUD,
relationships, transactions, migrations, testing, and deployment in one project.

## If your app has more than one table

Read these when your application has relationships, transactions, database changes, or more than one deployment environment.

1. [Transactions and sessions](concepts/transactions-and-sessions.md) explains when to use direct table methods and when to use a unit of work.
2. [Migrations and reflection](concepts/migrations-and-reflection.md) explains snapshots, diffs, dry runs, migration files, history, and live database inspection.
3. [Events](concepts/events.md) explains lifecycle hooks.
4. [Native Engine](concepts/native-engine.md) explains the Rust runtime boundary.
5. [Performance](performance.md) explains where the runtime helps and where query design still matters.

Use the tutorial chapters on [typed queries](tutorial/crud-and-queries.md),
[migrations](tutorial/migrations.md), and [PostgreSQL](tutorial/postgresql.md).

## If you are designing production schema

Read these when you are designing production schemas, cross-dialect tests, migration tooling, or backend-specific behavior.

1. Pick your driver page in [Drivers](drivers/index.md).
2. Review backend-specific DDL and reflection behavior before relying on generated SQL.
3. Read [Rust Core](architecture/rust-core.md) to understand the Python-to-Rust split.
4. Read [Dialect Support](architecture/dialect-support.md) to understand what is normalized and what is intentionally backend-specific.
5. Use [Python API](api/reference.md) when you need exact method signatures, metadata models, and generated API references.

## How to use the API reference

The concept pages explain ideas. The API pages list exact Python objects.

Use the API reference when you need:

- the full `Ormdantic.table(...)` decorator signature
- the full `Table` CRUD and query handle surface
- metadata model fields for columns, indexes, constraints, namespaces, sequences, and views
- query expression helper signatures
- loader option methods such as `filter`, `sorted_by`, and `batched`
- migration manager methods and snapshot models
- reflection inspector methods
- native engine helpers and error types

## Rule of thumb

Use the narrowest API that explains your intent:

- one row by primary key: `find_one`
- filtered lists: `find_many` with dictionary filters
- composed SQL: expression helpers and `select_query`
- several writes that must succeed together: `db.transaction()` or `db.session()`
- existing production database changes: migrations, not `create_all()`
- backend-specific schema features: driver pages plus metadata models

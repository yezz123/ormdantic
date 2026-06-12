# Learning Path

Ormdantic has two audiences:

- beginners who want to create tables, insert rows, and query data without learning every internal detail first;
- advanced users who need migrations, reflection, dialect behavior, performance tradeoffs, and lower-level SQL control.

This page shows the order to read the docs.

## Beginner Path

Start here if you are new to Ormdantic or new to ORMs.

1. Read [Why Ormdantic](why-ormdantic.md) to understand the problem it solves.
2. Read [Installation](installation.md) and install the package.
3. Follow [Quickstart](quickstart.md) and run the example with SQLite.
4. Read [Database And Tables](concepts/database-and-tables.md) to understand `Ormdantic`, `@db.table`, and `db[Model]`.
5. Read [Field Types And Metadata](concepts/field-types-and-metadata.md) to understand how Python annotations become database columns.
6. Read [Querying](concepts/querying.md) for `find_one`, `find_many`, filters, expressions, counts, and result objects.
7. Read [Relationships](concepts/relationships.md) and [Loading Strategies](concepts/loading-strategies.md) before modeling related data.

After that, use the guides:

- [Basic CRUD](examples/basic-crud.md);
- [Relationships](examples/relationships.md);
- [Transactions And Sessions](examples/transactions-sessions.md).

## Intermediate Path

Read these when your application has more than one table or more than one deployment environment.

1. [Transactions And Sessions](concepts/transactions-and-sessions.md) explains when to use direct table methods and when to use a unit of work.
2. [Migrations And Reflection](concepts/migrations-and-reflection.md) explains snapshots, diffs, dry runs, migration files, history, and live database inspection.
3. [Events](concepts/events.md) explains lifecycle hooks.
4. [Native Engine](concepts/native-engine.md) explains the Rust runtime boundary.
5. [Performance](performance.md) explains where the runtime helps and where query design still matters.

Use these guides:

- [Query Expressions](examples/query-expressions.md);
- [Migrations And Reflection](examples/migrations-reflection.md);
- [Enterprise Dialects](examples/enterprise-dialects.md).

## Advanced Path

Read these when you are designing production schemas, cross-dialect tests, or migration tooling.

1. Pick your driver page in [Drivers](drivers/index.md).
2. Review backend-specific DDL and reflection behavior before relying on generated SQL.
3. Read [Rust Core](architecture/rust-core.md) to understand the Python-to-Rust split.
4. Read [Dialect Support](architecture/dialect-support.md) to understand what is normalized and what is intentionally backend-specific.
5. Use [Python API](api/reference.md) when you need exact method signatures, metadata models, and generated API references.

## How To Use The API Reference

The concept pages explain ideas. The API pages list exact Python objects.

Use the API reference when you need:

- the full `Ormdantic.table(...)` decorator signature;
- the full `Table` CRUD and query handle surface;
- metadata model fields for columns, indexes, constraints, namespaces, sequences, and views;
- query expression helper signatures;
- loader option methods such as `filter`, `sorted_by`, and `batched`;
- migration manager methods and snapshot models;
- reflection inspector methods;
- native engine helpers and error types.

## Rule Of Thumb

Use the simplest API that explains your intent:

- simple row lookup: `find_one`;
- simple filtered lists: `find_many` with dictionary filters;
- advanced SQL: expression helpers and `select_query`;
- several writes that must succeed together: `db.transaction()` or `db.session()`;
- existing production database changes: migrations, not `create_all()`;
- backend-specific schema features: driver pages plus metadata models.

# Concepts

Read these pages when you want to understand how Ormdantic works before you use the API reference. Each page explains one part of the mental model.

If you have not run the first example yet, read [First steps](../quickstart.md) first. The concept pages make more sense after you have seen `Ormdantic`, `@db.table(...)`, and `db[Model]` in code.

## The mental model

Ormdantic has a small set of concepts:

- an `Ormdantic` database instance owns a connection URL and registered schema metadata
- Pydantic models become tables through `@db.table(...)`
- `Table` handles perform CRUD, query, count, select, and bulk update operations
- relationships are explicit metadata derived from model fields and foreign keys
- loaders decide how related models are fetched
- sessions group writes in a unit of work
- migrations compare model snapshots with live database reflection
- events let application code observe lifecycle points

## Recommended order

Follow this order if you are learning the project:

1. [Database and tables](database-and-tables.md)
2. [Field types and metadata](field-types-and-metadata.md)
3. [Relationships](relationships.md)
4. [Querying](querying.md)
5. [Loading Strategies](loading-strategies.md)
6. [Transactions and sessions](transactions-and-sessions.md)
7. [Migrations and reflection](migrations-and-reflection.md)
8. [Drivers](../drivers/index.md)

Advanced readers can jump directly to the page for the part they are tuning, then use the [API reference](../api/reference.md) for exact signatures.

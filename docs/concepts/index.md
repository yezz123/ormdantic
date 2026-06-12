# Concepts

Ormdantic has a small number of concepts:

- an `Ormdantic` database instance owns a connection URL and registered schema metadata;
- Pydantic models become tables through `@db.table(...)`;
- `Table` handles perform CRUD, query, count, select, and bulk update operations;
- relationships are explicit metadata derived from model fields and foreign keys;
- loaders decide how related models are fetched;
- sessions group writes in a unit of work;
- migrations compare snapshots and live database reflection;
- events let application code observe lifecycle points.

Read these pages when you need to understand how the pieces fit together before jumping into API reference.

## Recommended Order

1. [Database And Tables](database-and-tables.md)
2. [Field Types And Metadata](field-types-and-metadata.md)
3. [Relationships](relationships.md)
4. [Querying](querying.md)
5. [Loading Strategies](loading-strategies.md)
6. [Transactions And Sessions](transactions-and-sessions.md)
7. [Migrations And Reflection](migrations-and-reflection.md)
8. [Drivers](../drivers/index.md)

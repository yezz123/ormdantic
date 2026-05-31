# API Reference

The API reference is generated from Ormdantic's Python source with mkdocstrings.

## Public API

- [Ormdantic](ormdantic.md): database registration, table decorators, schema lifecycle, sessions, transactions, and relationship loading.
- [Native Engine](engine.md): the Rust-backed execution wrapper and transaction context.
- [Session](session.md): async unit-of-work helpers.
- [Events](events.md): event registration and dispatch.
- [Loaders](loaders.md): relationship loader options.
- [Errors](errors.md): Ormdantic-specific exceptions.

## ORM Internals

- [CRUD](crud.md): table-scoped CRUD orchestration.
- [Table](table.md): schema creation orchestration.
- [Field](field.md): select, count, delete, and relationship query planning.
- [Query](query.md): insert/update/upsert query planning.
- [Serializer](serializer.md): result payload conversion into Pydantic models.
- [Rust Bridge Internals](internals.md): private Python modules that call the Rust extension.

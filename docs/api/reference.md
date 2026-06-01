# API Reference

The API reference is generated from Ormdantic's Python source with mkdocstrings.

## Public API

- [Ormdantic](ormdantic.md): database registration, table decorators, schema lifecycle, sessions, transactions, and relationship loading.
- [Native Engine](engine.md): the Rust-backed execution wrapper and transaction context.
- [Session](session.md): async unit-of-work helpers.
- [Events](events.md): event registration and dispatch.
- [Loaders](loaders.md): relationship loader options.
- [Errors](errors.md): Ormdantic-specific exceptions.
- [Table](table.md): Rust-backed table CRUD facade.
- `ormdantic.column`: query expression helper.
- `ormdantic.association_proxy` and `ormdantic.hybrid_property`: descriptor helpers for proxy and computed attributes.
- `Ormdantic.inspect()` and `Ormdantic.migrations`: reflection and migration facades.

## ORM Internals

- [Serializer](serializer.md): result payload conversion into Pydantic models.
- [Runtime Internals](internals.md): schema and hydration helpers used by the Rust-first facade.

# Why Ormdantic

Python teams often end up with three overlapping models for the same data:

1. request and response models;
2. ORM models;
3. database migration metadata.

Ormdantic starts from a different premise: if Pydantic is already the shape your application trusts, the database layer should understand those models directly.

## The problem

Traditional Python ORM stacks are powerful, but they often make you choose between:

- a large SQL toolkit with a separate ORM model layer
- a small mapper that cannot express production database details
- hand-written SQL and ad hoc serialization
- async APIs that still hide blocking or implicit relationship work

Ormdantic exists for applications that want a narrower contract:

- Pydantic models are the public object model
- Database schema metadata is explicit Python data
- SQL and hydration hot paths run in Rust
- Async behavior stays visible at the call site

## When Ormdantic is a good fit

Use Ormdantic when:

- your application already uses Pydantic v2
- you want an async ORM without hidden lazy database access on attribute reads
- you need typed CRUD, relationship loading, migrations, and reflection
- you want native drivers compiled into the package instead of managing separate Python driver stacks
- you need backend-specific DDL options but do not want to model every SQLAlchemy concept

## When it is not the right tool

Use a lower-level SQL toolkit or SQLAlchemy when:

- you need arbitrary SQL expression coverage and vendor features beyond Ormdantic's modeled surface
- you need the SQLAlchemy ecosystem, plugins, or declarative mapper features
- you prefer hand-written SQL as the primary interface
- you cannot use the Rust extension in your deployment environment

## The core tradeoff

Ormdantic deliberately trades infinite ORM flexibility for a smaller, explicit, Pydantic-first surface. The library should make common application persistence predictable and fast, while still exposing enough database metadata for serious schemas.

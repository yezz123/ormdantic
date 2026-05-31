# Rust Query Runtime Checkpoint

## Goal

Record the first runtime slice where Ormdantic executes Rust-compiled SQL for supported CRUD paths.

## Implemented

Rust now compiles and Python can execute these shallow query shapes when `ormdantic._ormdantic` is available:

- find one by primary key
- find many with equality filters, ordering, limit, and offset
- count with equality filters
- insert
- update by primary key
- upsert by primary key
- delete by primary key

The runtime bridge binds values in the order returned by the Rust compiler and uses SQLAlchemy driver SQL execution for Rust queries. This keeps SQLite `?` placeholders and PostgreSQL `$n` placeholders under dialect control instead of interpolating values into SQL strings.

## Retired From The Hot Path

For `depth <= 0`, `OrmField` and `OrmQuery` now prefer Rust query compilation and only fall back to PyPika when the extension is unavailable or does not expose the required symbol. This retires PyPika from the installed-extension hot path for flat CRUD and count operations.

## Still Kept

PyPika remains for relationship joins and extension-less local checkouts.

Relationship loading still depends on Python join construction and alias expansion. Removing PyPika completely would be unsafe until Rust owns join planning, relationship path aliases, and nested result-shape execution for the current `table/relation\column` contract.

## Next Migration Work

The next slice should move relationship select planning into Rust:

1. Add relationship graph metadata to `ormdantic-schema`.
2. Generate join aliases in `ormdantic-sql`.
3. Compile left joins for current one-to-one, many-to-one, and one-to-many shapes.
4. Move nested row folding out of Python once Rust can describe the full result shape.

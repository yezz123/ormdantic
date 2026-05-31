# ormdantic-core

Shared primitives for the Ormdantic Rust workspace.

`ormdantic-core` is intentionally small and dependency-free. It defines the common error type, result alias, and typed identifiers used by the schema, dialect, SQL, hydration, engine, and PyO3 crates.

## Public API

| Item | Purpose |
| --- | --- |
| `OrmdanticResult<T>` | Workspace-wide alias for `Result<T, OrmdanticError>`. |
| `TableId` | Opaque table registry identifier. |
| `ColumnId` | Opaque column registry identifier. |
| `RelationshipId` | Opaque relationship registry identifier. |
| `OrmdanticError` | Shared validation, dialect, SQL compilation, and hydration error enum. |

## Dependencies

This crate has no internal or external dependencies.

## Tests

The crate tests typed ID copy semantics and display strings for `OrmdanticError`.

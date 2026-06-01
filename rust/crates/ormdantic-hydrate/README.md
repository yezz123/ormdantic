# ormdantic-hydrate

Row hydration planning for Ormdantic.

`ormdantic-hydrate` describes how flat database rows map back into Python dictionaries and nested relationship payloads before Pydantic models are rebuilt in Python.

## Public API

| Item | Purpose |
| --- | --- |
| `FlatHydrationPlan` | Maps result aliases to model column names and tracks the primary-key column index. |
| `ResultColumn` | Parsed table path and column name from a result alias. |
| `ResultShape` | Root table, selected columns, relationship paths, and array paths for joined results. |

## Dependencies

Internal dependencies:

- `ormdantic-core`
- `ormdantic-schema`

External dependencies: none.

## Tests

The crate tests flat hydration plans, alias parsing, primary-key index detection, nested relationship paths, and array path handling.

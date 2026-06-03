# ormdantic-schema

Schema metadata structures for Ormdantic.

`ormdantic-schema` models tables, columns, indexes, unique constraints, and relationships before they are compiled into SQL or exposed through the Python extension.

## Public API

| Item                      | Purpose                                                                                     |
| ------------------------- | ------------------------------------------------------------------------------------------- |
| `TableDef`                | Table definition with columns, primary key, indexes, unique constraints, and relationships. |
| `ColumnDef`               | Column metadata used by validation and DDL compilation.                                     |
| `FieldKind`               | Logical field kind derived from Python/Pydantic fields.                                     |
| `IndexDef`                | Index metadata for generated DDL.                                                           |
| `UniqueConstraintDef`     | Single-column and multi-column uniqueness metadata.                                         |
| `RelationshipDef`         | Relationship metadata between tables.                                                       |
| `RelationshipCardinality` | Relationship direction/cardinality marker.                                                  |
| `SchemaRegistry`          | Registers tables and validates relationship targets.                                        |
| `ColumnAlias`             | Parses aliases such as `table\\column` from result sets.                                    |

## Dependencies

Internal dependencies:

- `ormdantic-core`

External dependencies: none.

## Tests

The crate tests registry validation, duplicate metadata detection, alias parsing, and relationship target errors.

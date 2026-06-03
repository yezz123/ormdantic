# ormdantic-sql

SQL AST and query compiler for Ormdantic.

`ormdantic-sql` turns structured query operations into dialect-aware SQL strings and ordered bind parameter names. It is used by the PyO3 bridge to compile CRUD, count, filter, ordering, pagination, and joined relationship queries.

## Public API

| Item                 | Purpose                                                                                     |
| -------------------- | ------------------------------------------------------------------------------------------- |
| `QueryOperation`     | Describes the compiled operation kind.                                                      |
| `CompiledQuery`      | SQL text, ordered parameter names, and operation kind.                                      |
| `TableRef`           | Table reference used by query AST nodes.                                                    |
| `SelectColumn`       | Selected column and alias metadata.                                                         |
| `JoinedSelectColumn` | Selected column metadata for joined relationship result aliases.                            |
| `Filter`             | Predicate AST for equality, comparison, `IN`, `LIKE`, null checks, and boolean composition. |
| `SortDirection`      | Ascending or descending ordering.                                                           |
| `OrderBy`            | Ordered column expression.                                                                  |
| `JoinSpec`           | LEFT JOIN metadata for relationship queries.                                                |
| `QueryAst`           | Insert, update, upsert, delete, select, and count AST with `compile`.                       |

## Dependencies

Internal dependencies:

- `ormdantic-core`
- `ormdantic-dialects`

External dependencies: none.

## Tests

The crate tests CRUD compilation, dialect-specific SQL, filters, null checks, `IN` predicates, and joined select shape.

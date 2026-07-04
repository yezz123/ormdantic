# Query expressions

Expression helpers build serializable SQL AST fragments consumed by the Rust compiler.

Use these helpers when dictionary filters are not enough for the query you need.

## Core classes

::: ormdantic.expressions.QueryExpression
::: ormdantic.expressions.RelationExpression
::: ormdantic.expressions.ColumnExpression
::: ormdantic.expressions.SelectExpressionQuery
::: ormdantic.expressions.UpdateExpressionQuery

## Helper functions

::: ormdantic.expressions.column
::: ormdantic.expressions.projection
::: ormdantic.expressions.assignment
::: ormdantic.expressions.select_query
::: ormdantic.expressions.update_query
::: ormdantic.expressions.literal
::: ormdantic.expressions.raw_sql_safe
::: ormdantic.expressions.case
::: ormdantic.expressions.cast
::: ormdantic.expressions.tuple_
::: ormdantic.expressions.cte
::: ormdantic.expressions.count
::: ormdantic.expressions.sum
::: ormdantic.expressions.avg
::: ormdantic.expressions.min
::: ormdantic.expressions.max
::: ormdantic.expressions.exists
::: ormdantic.expressions.not_exists
::: ormdantic.expressions.not_
::: ormdantic.expressions.group
::: ormdantic.expressions.over
::: ormdantic.expressions.subquery

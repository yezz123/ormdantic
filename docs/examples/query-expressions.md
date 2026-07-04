# Query expressions

Dictionary filters cover ordinary CRUD screens. Expression helpers are for grouping, aggregates, subqueries, window functions, and reusable SQL fragments.

Use this guide after [Querying](../concepts/querying.md).

## What the example covers

- `column(...)` expressions
- comparison operators
- aggregate helpers
- select query objects
- expression-backed table `select` calls

```python
--8<-- "examples/query_expressions.py"
```

Run it locally:

```console
python examples/query_expressions.py
```

## Rule of thumb

Start with dictionary filters for ordinary CRUD screens. Move to expression helpers when you need grouping, aggregates, subqueries, window functions, or reusable expression objects.

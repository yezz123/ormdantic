# Query Expressions

Dictionary filters cover simple cases. Expression helpers are for composed SQL.

## What The Example Covers

- `column(...)` expressions;
- comparison operators;
- aggregate helpers;
- select query objects;
- expression-backed table `select` calls.

```python
--8<-- "examples/query_expressions.py"
```

Run it locally:

```console
python examples/query_expressions.py
```

## Rule Of Thumb

Start with dictionary filters for ordinary CRUD screens. Move to expression helpers when you need grouping, aggregates, subqueries, window functions, or reusable expression objects.

# Query Expressions

Ormdantic accepts dictionary filters and typed expression helpers. Dictionary filters remain useful for simple cases:

```python
await database[Flavor].find_many(where={"name": "mocha"})
await database[Flavor].find_many(where={"strength__gt": 2})
await database[Flavor].find_many(where={"name__like": "mo%"})
await database[Flavor].find_many(where={"id__in": [first_id, second_id]})
await database[Flavor].find_many(where={"strength__is_not_null": True})
```

Supported filter operators:

- `eq` (default)
- `ne`
- `lt`, `le`, `gt`, `ge`
- `like`, `ilike`
- `in`, `not_in`
- `is_null`, `is_not_null`

The same predicates can be built with `column()`:

```python
from ormdantic import column

await database[Flavor].find_many(
    where=column("strength").ge(2) & column("name").startswith("mo")
)

await database[Flavor].find_many(
    where=(column("name") == "mocha") | (column("name") == "latte")
)
```

Predicate helpers include `between`, `not_between`, `in_`, `not_in`, `contains`, `startswith`, `endswith`, `icontains`, `istartswith`, `iendswith`, `is_null`, `is_not_null`, and `not_`.

Typed expression queries serialize to the Rust SQL AST for deterministic compilation:

```python
from ormdantic import column, count, select_query, sum

total = sum(column("total"))
result = await database[Order].select(
    column("customer_id"),
    total.as_("total_sum"),
    count().as_("row_count"),
    where=column("status").in_(["paid", "refunded"]),
    group_by=[column("customer_id")],
    having=total > 100,
    order_by=[total.desc(nulls="last")],
)
```

Use `select_query()` when you want to build a reusable query object and pass it as `query=`.

Typed expression helpers also cover scalar SQL AST nodes:

```python
from ormdantic import case, cast, column, literal, tuple_

result = await database[Customer].select(
    column("id"),
    cast(column("created_at"), "TEXT").as_("created_at_text"),
    case(
        (column("tier") == "gold", literal("priority")),
        else_=literal("standard"),
    ).as_("service_level"),
    tuple_(column("country"), column("city")).as_("location_key"),
    where=(column("tier") == "gold") & column("deleted_at").is_null(),
)
```

Arithmetic expressions can be projected or used inside functions:

```python
from ormdantic import avg, column, select_query

query = select_query(
    "orders",
    avg(column("total") + 2).as_("adjusted_average"),
    where=column("total").between(10, 100),
)
```

Typed subqueries can be used as scalar projections and predicates:

```python
from ormdantic import column, count, exists, not_exists, select_query, subquery

order_count = select_query(
    "orders",
    count(),
    where=column("customer_id", table="orders") == column("id", table="customers"),
)
paid_customer_ids = select_query(
    "orders",
    column("customer_id"),
    where=column("status") == "paid",
)
banned_customers = select_query(
    "bans",
    column("customer_id"),
    where=column("customer_id", table="bans") == column("id", table="customers"),
)

query = select_query(
    "customers",
    column("id"),
    subquery(order_count).as_("order_count"),
    where=exists(paid_customer_ids)
    & column("id").in_query(paid_customer_ids)
    & not_exists(banned_customers),
)
```

Registered relationships can build correlated predicates and relation aggregate
ordering without raw SQL:

```python
posts = database.relation(Author, "posts")
published = posts.column("status") == "published"

authors = await database[Author].find_many(
    where=posts.any(published),
    order_by=[posts.count(published).desc(), "name"],
)

authors_without_drafts = await database[Author].find_many(
    where=posts.none(posts.column("status") == "draft"),
)

authors_where_every_post_is_published = await database[Author].find_many(
    where=posts.every(published),
)
```

Use `any()` and `every()` on collection relationships, `none()` on either
collection or scalar relationships, and `has()` on scalar relationships:

```python
author = database.relation(Post, "author")
posts_by_alice = await database[Post].find_many(
    where=author.has(author.column("name") == "alice"),
)
```

The relation helper emits typed `EXISTS`, `NOT EXISTS`, and scalar `COUNT(*)`
subqueries. `find_many()` and `count()` route expression-only relation
predicates through the typed compiler and still hydrate normal model instances.

CTEs and window expressions are supported by the same typed compiler:

```python
from ormdantic import column, cte, over, select_query, sum

paid_orders = select_query(
    "orders",
    column("customer_id"),
    sum(column("total")).as_("paid_total"),
    where=column("status") == "paid",
    group_by=[column("customer_id")],
)

query = select_query(
    "paid_orders",
    column("customer_id"),
    column("paid_total"),
    over(
        sum(column("paid_total")),
        order_by=[column("paid_total").desc()],
    ).as_("running_total"),
    with_=[cte("paid_orders", paid_orders)],
    where=column("paid_total") > 100,
    order_by=[column("paid_total").desc()],
)
```

Hybrid attributes and association proxies can expose class-level expressions when
you attach an explicit expression hook:

```python
from ormdantic import association_proxy, column, hybrid_property


class CustomerView:
    account_name = association_proxy("account", "name")

    @account_name.expression
    def account_name(cls):
        return column("account_name")

    @hybrid_property
    def status_label(self) -> str:
        return f"status:{self.status}"

    @status_label.expression
    def status_label(cls):
        return column("status")


query = select_query(
    "customers",
    CustomerView.account_name.as_("account"),
    where=CustomerView.status_label == "active",
)
```

Typed update expressions can assign bound values or computed expressions:

```python
await database[Order].update_where(
    column("total").set(column("total") + 5),
    column("status").set("archived"),
    where=column("customer_id") == "alice",
)
```

Case-insensitive matching uses `ILIKE` where the expression compiler is used:

```python
query = select_query(
    "flavors",
    column("id"),
    column("name"),
    where=column("name").icontains("mocha"),
    order_by=[column("name").asc(nulls="first")],
)
```

Use `raw_sql_safe()` only for trusted fragments that cannot be represented by typed helpers:

```python
from ormdantic import raw_sql_safe, select_query

query = select_query(
    "flavors",
    raw_sql_safe("LOWER(name)").as_("normalized_name"),
    order_by=[raw_sql_safe("LOWER(name)").asc()],
)
```

`raw_sql_safe()` is an explicit escape hatch. User input should be passed through normal expression values so the Rust compiler can emit bind parameters in stable traversal order.

The typed API is intentionally not a full SQLAlchemy clone. Nested `include`/`select` shapes remain loader concerns instead of being folded into the query-expression surface.

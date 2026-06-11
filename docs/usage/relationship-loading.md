# Relationship Loading

Existing `depth` loading remains supported:

```python
coffee = await database[Coffee].find_one(coffee_id, depth=1)
```

Loader options provide a clearer API and can target a selected relationship
branch:

```python
from ormdantic import joinedload, lazyload, load, noload, selectinload

coffee = await database[Coffee].find_one(coffee_id, load=[joinedload("flavor")])
coffee = await database[Coffee].find_one(coffee_id, load=[selectinload("flavor")])
coffee = await database[Coffee].find_one(coffee_id, load=[lazyload("flavor")])
coffee = await database[Coffee].find_one(coffee_id, load=[noload("flavor")])
flavor = await database.load(coffee, "flavor")
```

Nested paths load only the requested branch:

```python
article = await database[Article].find_one(
    article_id,
    load=[joinedload("author.country")],
)
```

Relationship loaders can filter target rows by equality and order collections by
target columns. Prefix a column with `-` for descending order:

```python
author = await database[Author].find_one(
    author_id,
    load=[joinedload("posts").filter(status="published").sorted_by("-created_at")],
)
```

Select-in loaders automatically chunk large `IN` lists and can set an explicit
batch size for a path:

```python
authors = await database[Author].find_many(
    load=[selectinload("posts").batched(250)],
)
```

Joined and select-in strategies can be composed in one query:

```python
author = await database[Author].find_one(
    author_id,
    load=[joinedload("posts"), selectinload("posts.comments")],
)
```

After `database.init()` installs relationship path descriptors, class-level paths can
be used as well:

```python
article = await database[Article].find_one(
    article_id,
    load=[load(Article.author.country)],
)
```

Current behavior:

- `joinedload()` maps to Rust-compiled joined loading.
- `selectinload()` runs the root query first, then batches each selected relationship branch with chunked `IN` filters.
- `lazyload()` and `noload()` keep the initial result shallow for that path and require explicit async loading if the relationship value is needed later.
- Repeated rows for the same loaded table and primary key reuse the same Python object within one hydrated result graph.
- Invalid paths raise `ValueError` messages that name the missing relationship and available relationships at that point.

The older `joined()`, `selectin()`, and `lazy()` helper names remain available as
aliases.

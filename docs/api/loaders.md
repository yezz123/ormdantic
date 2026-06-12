# Loaders

Loader options describe how relationship paths should be loaded.

Use these helpers in `find_one(..., load=[...])` and `find_many(..., load=[...])`:

```python
from ormdantic import joinedload, selectinload

await db[Flavor].find_many(load=[joinedload("supplier")])
await db[Supplier].find_many(
    load=[selectinload("flavors").sorted_by("name").batched(100)]
)
```

Use `joinedload` for small related row sets where one joined query is acceptable. Use `selectinload` for collections or larger graphs where batched secondary queries are easier to control.

::: ormdantic.loaders.LoaderStrategy
::: ormdantic.loaders.LoaderPath
::: ormdantic.loaders.LoaderOption
::: ormdantic.loaders.load
::: ormdantic.loaders.joinedload
::: ormdantic.loaders.selectinload
::: ormdantic.loaders.lazyload
::: ormdantic.loaders.noload
::: ormdantic.loaders.joined
::: ormdantic.loaders.selectin
::: ormdantic.loaders.lazy
::: ormdantic.loaders.loader_depth

# Relationships

Relationship fields can reference another Pydantic model or its primary-key type. Rust plans the joined query and Python rebuilds the final Pydantic objects.

```python
--8<-- "examples/relationships.py"
```

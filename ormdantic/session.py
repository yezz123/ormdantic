"""Async unit-of-work session helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, TypeGuard

from pydantic import BaseModel


class Session:
    """Minimal async unit-of-work session for Ormdantic models."""

    def __init__(
        self, database: Any, *, transaction_options: Any | None = None
    ) -> None:
        """Create a session bound to an `Ormdantic` database instance."""
        self._database = database
        self._transaction_options = transaction_options
        self._new: list[BaseModel] = []
        self._dirty: list[BaseModel] = []
        self._deleted: list[BaseModel] = []
        self._identity_map: dict[tuple[type[BaseModel], Any], BaseModel] = {}
        self._snapshots: dict[tuple[type[BaseModel], Any], dict[str, Any]] = {}
        self._closed = False

    async def __aenter__(self) -> Session:
        """Begin a native transaction and return the session."""
        self._ensure_open()
        await self._database._begin(self._transaction_options)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Commit on success and roll back on error."""
        if self._closed:
            return
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()

    def add(self, model: BaseModel) -> None:
        """Stage a new model for insertion on flush."""
        self._ensure_open()
        self._cascade_add(model, set())

    def mark_dirty(self, model: BaseModel) -> None:
        """Stage an existing model for update on flush."""
        self._ensure_open()
        if model not in self._dirty:
            self._dirty.append(model)

    def delete(self, model: BaseModel) -> None:
        """Stage an existing model for deletion on flush."""
        self._ensure_open()
        self._cascade_delete(model, set())

    def merge(self, model: BaseModel) -> BaseModel:
        """Merge a detached model into the identity map and stage it as dirty."""
        self._ensure_open()
        table = self._database._table_map.model_to_data[type(model)]
        key = (type(model), getattr(model, table.pk))
        if cached := self._identity_map.get(key):
            for field, value in model.__dict__.items():
                setattr(cached, field, value)
            self.mark_dirty(cached)
            return cached
        self._remember(model)
        self.mark_dirty(model)
        return model

    def expire(self, model: BaseModel) -> None:
        """Remove a model from the identity map."""
        self._ensure_open()
        key = self._identity_key(model)
        self._identity_map.pop(key, None)
        self._snapshots.pop(key, None)

    async def flush(self) -> None:
        """Write staged inserts and updates without ending the transaction."""
        self._ensure_open()
        await self._database._events.dispatch("before_flush", session=self)
        for model in self._dependency_ordered(self._new):
            stored = await self._database[type(model)].insert(model)
            self._remember(stored)
        self._new.clear()

        for model in self._detect_dirty_models():
            self.mark_dirty(model)

        for model in list(self._dirty):
            stored = await self._database[type(model)].update(model)
            self._remember(stored)
        self._dirty.clear()

        for model in reversed(self._dependency_ordered(self._deleted)):
            key = self._identity_key(model)
            pk = key[1]
            await self._database[type(model)].delete(pk)
            self._identity_map.pop(key, None)
            self._snapshots.pop(key, None)
        self._deleted.clear()
        await self._database._events.dispatch("after_flush", session=self)

    async def commit(self) -> None:
        """Flush changes and commit the active transaction."""
        if self._closed:
            return
        await self.flush()
        await self._database._commit()
        self._closed = True

    async def rollback(self) -> None:
        """Discard staged changes and roll back the active transaction."""
        if self._closed:
            return
        self._new.clear()
        self._dirty.clear()
        self._deleted.clear()
        self._snapshots.clear()
        await self._database._rollback()
        self._closed = True

    async def refresh(self, model: BaseModel, *, depth: int = 0) -> BaseModel | None:
        """Reload a model by primary key and remember the refreshed instance."""
        self._ensure_open()
        table = self._database._table_map.model_to_data[type(model)]
        refreshed = await self._database[type(model)].find_one(
            getattr(model, table.pk), depth=depth
        )
        if refreshed is not None:
            self._remember(refreshed)
        return refreshed

    def get_cached(self, model_type: type[BaseModel], pk: Any) -> BaseModel | None:
        """Return a model from the identity map if it has been remembered."""
        return self._identity_map.get((model_type, pk))

    async def get(
        self, model_type: type[BaseModel], pk: Any, *, depth: int = 0
    ) -> BaseModel | None:
        """Return a cached model or load it by primary key."""
        self._ensure_open()
        if cached := self.get_cached(model_type, pk):
            return cached
        loaded = await self._database[model_type].find_one(pk, depth=depth)
        if loaded is not None:
            self._remember(loaded)
        return loaded

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("session is closed")

    def _remember(self, model: BaseModel) -> None:
        """Store a model and loaded related models in the identity map."""
        self._remember_graph(model, set(), overwrite_existing=True)

    def _remember_graph(
        self, model: BaseModel, seen: set[int], *, overwrite_existing: bool
    ) -> None:
        if id(model) in seen:
            return
        seen.add(id(model))
        key = self._identity_key(model)
        if overwrite_existing or key not in self._identity_map:
            self._identity_map[key] = model
            self._snapshots[key] = self._snapshot(model)
        for related in self._scalar_related_models(model):
            self._remember_graph(related, seen, overwrite_existing=False)
        for child, _back_reference in self._collection_related_models(model):
            self._remember_graph(child, seen, overwrite_existing=False)

    def _cascade_add(self, model: BaseModel, seen: set[int]) -> None:
        """Stage a model and reachable transient relationship objects."""
        if id(model) in seen:
            return
        seen.add(id(model))

        for related in self._scalar_related_models(model):
            self._cascade_add(related, seen)

        if not self._is_remembered(model) and model not in self._new:
            self._new.append(model)

        for child, back_reference in self._collection_related_models(model):
            if getattr(child, back_reference, None) is None:
                setattr(child, back_reference, model)
            self._cascade_add(child, seen)

    def _cascade_delete(self, model: BaseModel, seen: set[int]) -> None:
        """Stage a model and loaded relationship collections for deletion."""
        if id(model) in seen:
            return
        seen.add(id(model))

        for child, _back_reference in self._collection_related_models(model):
            self._cascade_delete(child, seen)

        if model not in self._deleted:
            self._deleted.append(model)

    def _identity_key(self, model: BaseModel) -> tuple[type[BaseModel], Any]:
        """Return the identity-map key for a managed model."""
        table = self._database._table_map.model_to_data[type(model)]
        return (type(model), getattr(model, table.pk))

    def _is_remembered(self, model: BaseModel) -> bool:
        """Return whether this model identity is already managed."""
        return self._identity_key(model) in self._identity_map

    def _snapshot(self, model: BaseModel) -> dict[str, Any]:
        """Return comparable persisted-column values for dirty detection."""
        table = self._database._table_map.model_to_data[type(model)]
        return {
            column: self._snapshot_value(getattr(model, column))
            for column in table.columns
        }

    def _snapshot_value(self, value: Any) -> Any:
        """Return a comparable persisted value without relationship cycles."""
        if self._is_registered_model(value):
            related_table = self._database._table_map.model_to_data[type(value)]
            return getattr(value, related_table.pk)
        return deepcopy(value)

    def _detect_dirty_models(self) -> list[BaseModel]:
        """Find remembered models whose persisted-column values changed."""
        staged = {id(model) for model in (*self._new, *self._dirty, *self._deleted)}
        changed = []
        for key, model in self._identity_map.items():
            if id(model) in staged:
                continue
            if self._snapshots.get(key) != self._snapshot(model):
                changed.append(model)
        return changed

    def _scalar_related_models(self, model: BaseModel) -> list[BaseModel]:
        """Return registered model values referenced by scalar relationships."""
        table = self._database._table_map.model_to_data[type(model)]
        related = []
        for field_name, relationship in table.relationships.items():
            if relationship.back_references is not None:
                continue
            value = getattr(model, field_name, None)
            if self._is_registered_model(value):
                related.append(value)
        return related

    def _collection_related_models(
        self, model: BaseModel
    ) -> list[tuple[BaseModel, str]]:
        """Return loaded collection children and their back-reference fields."""
        table = self._database._table_map.model_to_data[type(model)]
        related = []
        for field_name, relationship in table.relationships.items():
            if relationship.back_references is None:
                continue
            value = getattr(model, field_name, None)
            if not isinstance(value, list):
                continue
            for item in value:
                if self._is_registered_model(item):
                    related.append((item, relationship.back_references))
        return related

    def _dependency_ordered(self, models: list[BaseModel]) -> list[BaseModel]:
        """Return staged models with referenced parents before dependents."""
        ordered: list[BaseModel] = []
        permanent: set[int] = set()
        temporary: set[int] = set()
        model_by_id = {id(model): model for model in models}
        model_by_key = {self._identity_key(model): model for model in models}

        def visit(model: BaseModel) -> None:
            marker = id(model)
            if marker in permanent:
                return
            if marker in temporary:
                return
            temporary.add(marker)
            for dependency in self._dependencies_for(model, model_by_key):
                if id(dependency) in model_by_id:
                    visit(dependency)
            temporary.remove(marker)
            permanent.add(marker)
            ordered.append(model)

        for model in models:
            visit(model)
        return ordered

    def _dependencies_for(
        self,
        model: BaseModel,
        model_by_key: dict[tuple[type[BaseModel], Any], BaseModel],
    ) -> list[BaseModel]:
        """Return staged models that must be inserted before this model."""
        table = self._database._table_map.model_to_data[type(model)]
        dependencies = []
        for field_name, relationship in table.relationships.items():
            if relationship.back_references is not None:
                continue
            value = getattr(model, field_name, None)
            if self._is_registered_model(value):
                dependencies.append(value)
                continue
            related_table = self._database._table_map.name_to_data[
                relationship.foreign_table
            ]
            dependency = model_by_key.get((related_table.model, value))
            if dependency is not None:
                dependencies.append(dependency)
        return dependencies

    def _is_registered_model(self, value: Any) -> TypeGuard[BaseModel]:
        return (
            isinstance(value, BaseModel)
            and type(value) in self._database._table_map.model_to_data
        )

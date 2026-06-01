"""Async unit-of-work session helpers."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class Session:
    """Minimal async unit-of-work session for Ormdantic models."""

    def __init__(self, database: Any) -> None:
        """Create a session bound to an `Ormdantic` database instance."""
        self._database = database
        self._new: list[BaseModel] = []
        self._dirty: list[BaseModel] = []
        self._deleted: list[BaseModel] = []
        self._identity_map: dict[tuple[type[BaseModel], Any], BaseModel] = {}
        self._closed = False

    async def __aenter__(self) -> Session:
        """Begin a native transaction and return the session."""
        await self._database._begin()
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
        if model not in self._new:
            self._new.append(model)

    def mark_dirty(self, model: BaseModel) -> None:
        """Stage an existing model for update on flush."""
        if model not in self._dirty:
            self._dirty.append(model)

    def delete(self, model: BaseModel) -> None:
        """Stage an existing model for deletion on flush."""
        if model not in self._deleted:
            self._deleted.append(model)

    def merge(self, model: BaseModel) -> BaseModel:
        """Merge a detached model into the identity map and stage it as dirty."""
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
        table = self._database._table_map.model_to_data[type(model)]
        self._identity_map.pop((type(model), getattr(model, table.pk)), None)

    async def flush(self) -> None:
        """Write staged inserts and updates without ending the transaction."""
        await self._database._events.dispatch("before_flush", session=self)
        for model in list(self._new):
            stored = await self._database[type(model)].insert(model)
            self._remember(stored)
        self._new.clear()

        for model in list(self._dirty):
            stored = await self._database[type(model)].update(model)
            self._remember(stored)
        self._dirty.clear()

        for model in list(self._deleted):
            table = self._database._table_map.model_to_data[type(model)]
            pk = getattr(model, table.pk)
            await self._database[type(model)].delete(pk)
            self._identity_map.pop((type(model), pk), None)
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
        await self._database._rollback()
        self._closed = True

    async def refresh(self, model: BaseModel, *, depth: int = 0) -> BaseModel | None:
        """Reload a model by primary key and remember the refreshed instance."""
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
        if cached := self.get_cached(model_type, pk):
            return cached
        loaded = await self._database[model_type].find_one(pk, depth=depth)
        if loaded is not None:
            self._remember(loaded)
        return loaded

    def _remember(self, model: BaseModel) -> None:
        """Store a model in the identity map by model type and primary key."""
        table = self._database._table_map.model_to_data[type(model)]
        self._identity_map[(type(model), getattr(model, table.pk))] = model

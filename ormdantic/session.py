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
        self._identity_map: dict[tuple[type[BaseModel], Any], BaseModel] = {}

    async def __aenter__(self) -> Session:
        """Begin a native transaction and return the session."""
        await self._database._native_engine.begin()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Commit on success and roll back on error."""
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
        await self._database._events.dispatch("after_flush", session=self)

    async def commit(self) -> None:
        """Flush changes and commit the active transaction."""
        await self.flush()
        await self._database._native_engine.commit()

    async def rollback(self) -> None:
        """Discard staged changes and roll back the active transaction."""
        self._new.clear()
        self._dirty.clear()
        await self._database._native_engine.rollback()

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

    def _remember(self, model: BaseModel) -> None:
        """Store a model in the identity map by model type and primary key."""
        table = self._database._table_map.model_to_data[type(model)]
        self._identity_map[(type(model), getattr(model, table.pk))] = model

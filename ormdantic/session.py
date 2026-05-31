from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class Session:
    """Minimal async unit-of-work session for Ormdantic models."""

    def __init__(self, database: Any) -> None:
        self._database = database
        self._new: list[BaseModel] = []
        self._dirty: list[BaseModel] = []
        self._identity_map: dict[tuple[type[BaseModel], Any], BaseModel] = {}

    async def __aenter__(self) -> Session:
        await self._database._native_engine.begin()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()

    def add(self, model: BaseModel) -> None:
        if model not in self._new:
            self._new.append(model)

    def mark_dirty(self, model: BaseModel) -> None:
        if model not in self._dirty:
            self._dirty.append(model)

    async def flush(self) -> None:
        for model in list(self._new):
            stored = await self._database[type(model)].insert(model)
            self._remember(stored)
        self._new.clear()

        for model in list(self._dirty):
            stored = await self._database[type(model)].update(model)
            self._remember(stored)
        self._dirty.clear()

    async def commit(self) -> None:
        await self.flush()
        await self._database._native_engine.commit()

    async def rollback(self) -> None:
        self._new.clear()
        self._dirty.clear()
        await self._database._native_engine.rollback()

    async def refresh(self, model: BaseModel, *, depth: int = 0) -> BaseModel | None:
        table = self._database._table_map.model_to_data[type(model)]
        refreshed = await self._database[type(model)].find_one(
            getattr(model, table.pk), depth=depth
        )
        if refreshed is not None:
            self._remember(refreshed)
        return refreshed

    def get_cached(self, model_type: type[BaseModel], pk: Any) -> BaseModel | None:
        return self._identity_map.get((model_type, pk))

    def _remember(self, model: BaseModel) -> None:
        table = self._database._table_map.model_to_data[type(model)]
        self._identity_map[(type(model), getattr(model, table.pk))] = model

"""Async unit-of-work session helpers."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from types import TracebackType
from typing import Any, TypeGuard

from pydantic import BaseModel
from typing_extensions import Self


@dataclass(frozen=True)
class _UnitOfWorkSnapshot:
    new: list[BaseModel]
    dirty: list[BaseModel]
    deleted: list[BaseModel]
    identity_map: dict[tuple[type[BaseModel], Any], BaseModel]
    snapshots: dict[tuple[type[BaseModel], Any], dict[str, Any]]
    failed_flush_error: Exception | None


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
        self._failed_flush_error: Exception | None = None
        self._savepoint_sequence = 0
        self._closed = False

    async def __aenter__(self) -> Self:
        """Begin a native transaction and return the session."""
        self._ensure_open()
        await self._database._begin(self._transaction_options)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Commit on success and roll back on error."""
        if self._closed:
            return
        if exc_type is None:
            try:
                await self.commit()
            except Exception:
                if not self._closed:
                    await self.rollback()
                raise
        else:
            await self.rollback()

    def add(self, model: BaseModel) -> None:
        """Stage a new model for insertion on flush."""
        self._ensure_usable()
        self._cascade_add(model, set())

    def mark_dirty(self, model: BaseModel) -> None:
        """Stage an existing model for update on flush."""
        self._ensure_usable()
        if model not in self._new and model not in self._dirty:
            self._dirty.append(model)

    def delete(self, model: BaseModel) -> None:
        """Stage an existing model for deletion on flush."""
        self._ensure_usable()
        self._cascade_delete(model, set())

    def merge(self, model: BaseModel) -> BaseModel:
        """Merge a detached model into the identity map and stage it as dirty."""
        self._ensure_usable()
        table = self._database._table_map.model_to_data[type(model)]
        key = (type(model), getattr(model, table.pk))
        if cached := self._identity_map.get(key):
            for field, value in model.__dict__.items():
                setattr(cached, field, value)
            self.mark_dirty(cached)
            return cached
        if staged := self._staged_model_for_key(key):
            for field, value in model.__dict__.items():
                setattr(staged, field, value)
            if staged not in self._new:
                self.mark_dirty(staged)
            return staged
        self._remember(model)
        self.mark_dirty(model)
        return model

    def expire(self, model: BaseModel) -> None:
        """Remove a model from the identity map."""
        self._ensure_usable()
        key = self._identity_key(model)
        self._identity_map.pop(key, None)
        self._snapshots.pop(key, None)

    async def flush(self) -> None:
        """Write staged inserts and updates without ending the transaction."""
        self._ensure_usable()
        state = self._capture_state()
        try:
            await self._database._events.dispatch("before_flush", session=self)
            self._detect_relationship_changes()

            for model in self._detect_dirty_models():
                self.mark_dirty(model)

            inserted = self._dependency_ordered(list(self._new))
            for batch in self._model_batches(inserted):
                for model in batch:
                    stored = await self._database[type(model)].insert(model)
                    self._remember(stored)
            self._new.clear()

            updated = list(self._dirty)
            for batch in self._model_batches(updated):
                for model in batch:
                    stored = await self._database[type(model)].update(model)
                    self._remember(stored)
            self._dirty.clear()

            deleted = list(reversed(self._dependency_ordered(list(self._deleted))))
            for batch in self._model_batches(deleted):
                for model in batch:
                    key = self._identity_key(model)
                    pk = key[1]
                    await self._database[type(model)].delete(pk)
                    self._identity_map.pop(key, None)
                    self._snapshots.pop(key, None)
            self._deleted.clear()
        except Exception as exc:
            self._restore_state(state)
            self._failed_flush_error = exc
            raise
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
        try:
            await self._database._rollback()
        finally:
            self._new.clear()
            self._dirty.clear()
            self._deleted.clear()
            self._identity_map.clear()
            self._snapshots.clear()
            self._failed_flush_error = None
            self._closed = True

    def savepoint(self, name: str | None = None) -> Any:
        """Open a nested session savepoint that restores unit-of-work state."""
        self._ensure_usable()
        if name is None:
            self._savepoint_sequence += 1
            name = f"session_sp_{self._savepoint_sequence}"
        return _SessionSavepoint(self, name)

    async def refresh(self, model: BaseModel, *, depth: int = 0) -> BaseModel | None:
        """Reload a model by primary key and remember the refreshed instance."""
        self._ensure_usable()
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
        self._ensure_usable()
        if cached := self.get_cached(model_type, pk):
            return cached
        loaded = await self._database[model_type].find_one(pk, depth=depth)
        if loaded is not None:
            self._remember(loaded)
        return loaded

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("session is closed")

    def _ensure_usable(self) -> None:
        self._ensure_open()
        if self._failed_flush_error is not None:
            raise RuntimeError("session flush failed; rollback required") from (
                self._failed_flush_error
            )

    def _capture_state(self) -> _UnitOfWorkSnapshot:
        return _UnitOfWorkSnapshot(
            new=list(self._new),
            dirty=list(self._dirty),
            deleted=list(self._deleted),
            identity_map=dict(self._identity_map),
            snapshots={key: deepcopy(value) for key, value in self._snapshots.items()},
            failed_flush_error=self._failed_flush_error,
        )

    def _restore_state(self, state: _UnitOfWorkSnapshot) -> None:
        self._new = list(state.new)
        self._dirty = list(state.dirty)
        self._deleted = list(state.deleted)
        self._identity_map = dict(state.identity_map)
        self._snapshots = {
            key: deepcopy(value) for key, value in state.snapshots.items()
        }
        self._failed_flush_error = state.failed_flush_error

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

        self._raise_for_identity_conflict(model)

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

        if model in self._new:
            self._new.remove(model)
            return
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

    def _detect_relationship_changes(self) -> None:
        """Stage reachable relationship objects added to managed models."""
        for model in list(self._identity_map.values()):
            self._cascade_relationship_changes(model, set())

    def _cascade_relationship_changes(self, model: BaseModel, seen: set[int]) -> None:
        if id(model) in seen:
            return
        seen.add(id(model))

        for related in self._scalar_related_models(model):
            if not self._is_pending_or_remembered(related):
                self._cascade_add(related, seen)
            else:
                self._cascade_relationship_changes(related, seen)

        for child, back_reference in self._collection_related_models(model):
            if getattr(child, back_reference, None) is None:
                setattr(child, back_reference, model)
            if not self._is_pending_or_remembered(child):
                self._cascade_add(child, seen)
            else:
                self._cascade_relationship_changes(child, seen)

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

    def _model_batches(self, models: list[BaseModel]) -> list[list[BaseModel]]:
        """Group adjacent models by table while preserving dependency order."""
        batches: list[list[BaseModel]] = []
        for model in models:
            if batches and type(batches[-1][0]) is type(model):
                batches[-1].append(model)
            else:
                batches.append([model])
        return batches

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

    def _is_pending_or_remembered(self, model: BaseModel) -> bool:
        return (
            model in self._new
            or model in self._dirty
            or model in self._deleted
            or self._is_remembered(model)
        )

    def _staged_model_for_key(
        self, key: tuple[type[BaseModel], Any]
    ) -> BaseModel | None:
        if key[1] is None:
            return None
        for staged in (*self._new, *self._dirty, *self._deleted):
            if self._identity_key(staged) == key:
                return staged
        return None

    def _raise_for_identity_conflict(self, model: BaseModel) -> None:
        key = self._identity_key(model)
        if key[1] is None:
            return

        cached = self._identity_map.get(key)
        if cached is not None and cached is not model:
            raise ValueError(
                f"{type(model).__name__} primary key {key[1]!r} is already "
                "present in this session; use merge() for detached state"
            )

        staged = self._staged_model_for_key(key)
        if staged is not None and staged is not model:
            raise ValueError(
                f"{type(model).__name__} primary key {key[1]!r} is already "
                "staged in this session"
            )


class _SessionSavepoint:
    def __init__(self, session: Session, name: str) -> None:
        self._session = session
        self._name = name
        self._state: _UnitOfWorkSnapshot | None = None

    async def __aenter__(self) -> Self:
        self._session._ensure_usable()
        self._state = self._session._capture_state()
        await self._session._database._savepoint(self._name)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._state is None:
            return
        if exc_type is not None:
            try:
                await self._session._database._rollback_to_savepoint(self._name)
            finally:
                self._session._restore_state(self._state)
        else:
            await self._session._database._release_savepoint(self._name)

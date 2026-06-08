"""Relationship loader option helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Union

LoaderStrategy = Literal["joined", "selectin", "lazy", "noload"]
LoaderPathLike = Union[str, "LoaderPath"]


@dataclass(frozen=True)
class LoaderPath:
    """A dotted relationship path built from model class attributes."""

    parts: tuple[str, ...]

    def __getattr__(self, name: str) -> "LoaderPath":
        if name.startswith("_"):
            raise AttributeError(name)
        return LoaderPath((*self.parts, name))

    def __str__(self) -> str:
        return ".".join(self.parts)

    @property
    def path(self) -> str:
        """Return the dotted path string."""
        return str(self)


class RelationshipPathDescriptor:
    """Non-data descriptor used for class-level relationship loader paths."""

    def __init__(self, field_name: str) -> None:
        self.field_name = field_name

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            return LoaderPath((self.field_name,))
        try:
            return instance.__dict__[self.field_name]
        except KeyError as exc:
            raise AttributeError(self.field_name) from exc


@dataclass(frozen=True)
class LoaderOption:
    """Relationship loading strategy for a dotted model path."""

    path: str
    strategy: LoaderStrategy
    filter_by: dict[str, Any] | None = None
    order_by: tuple[str, ...] = ()

    @property
    def depth(self) -> int:
        """Return the relationship depth implied by the path."""
        return len(path_parts(self.path))

    def filter(self, **criteria: Any) -> "LoaderOption":
        """Return a copy with relationship-local filter criteria attached."""
        return LoaderOption(
            path=self.path,
            strategy=self.strategy,
            filter_by={**(self.filter_by or {}), **criteria},
            order_by=self.order_by,
        )

    def sorted_by(self, *columns: str) -> "LoaderOption":
        """Return a copy with relationship-local ordering attached."""
        return LoaderOption(
            path=self.path,
            strategy=self.strategy,
            filter_by=self.filter_by,
            order_by=tuple(columns),
        )


def path_parts(path: LoaderPathLike) -> tuple[str, ...]:
    """Normalize a public loader path into relationship field parts."""
    if isinstance(path, LoaderPath):
        raw = path.path
    else:
        raw = path
    parts = tuple(part for part in raw.replace("/", ".").split(".") if part)
    if not parts:
        raise ValueError("loader path must not be empty")
    return parts


def install_relationship_path_descriptor(model: type[Any], field_name: str) -> None:
    """Install a class-level descriptor for relationship path expressions."""
    setattr(model, field_name, RelationshipPathDescriptor(field_name))


def load(path: LoaderPathLike, *, strategy: LoaderStrategy = "joined") -> LoaderOption:
    """Load a relationship path with the requested strategy."""
    return LoaderOption(path=".".join(path_parts(path)), strategy=strategy)


def joinedload(path: LoaderPathLike) -> LoaderOption:
    """Load a relationship path with the joined strategy."""
    return load(path, strategy="joined")


def selectinload(path: LoaderPathLike) -> LoaderOption:
    """Load a relationship path with the select-in strategy."""
    return load(path, strategy="selectin")


def lazyload(path: LoaderPathLike) -> LoaderOption:
    """Mark a relationship path for explicit lazy loading."""
    return load(path, strategy="lazy")


def noload(path: LoaderPathLike) -> LoaderOption:
    """Prevent eager loading for a relationship path."""
    return load(path, strategy="noload")


def joined(path: LoaderPathLike) -> LoaderOption:
    """Backward-compatible alias for :func:`joinedload`."""
    return joinedload(path)


def selectin(path: LoaderPathLike) -> LoaderOption:
    """Backward-compatible alias for :func:`selectinload`."""
    return selectinload(path)


def lazy(path: LoaderPathLike) -> LoaderOption:
    """Backward-compatible alias for :func:`lazyload`."""
    return lazyload(path)


def loader_depth(load: list[LoaderOption] | None) -> int:
    """Return the maximum eager-loading depth from loader options."""
    if not load:
        return 0
    return max(
        (option.depth for option in load if option.strategy not in {"lazy", "noload"}),
        default=0,
    )

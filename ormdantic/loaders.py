"""Relationship loader option helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

LoaderStrategy = Literal["joined", "selectin", "lazy"]


@dataclass(frozen=True)
class LoaderOption:
    """Relationship loading strategy for a dotted model path."""

    path: str
    strategy: LoaderStrategy

    @property
    def depth(self) -> int:
        """Return the relationship depth implied by the path."""
        return len([part for part in self.path.split(".") if part])


def joined(path: str) -> LoaderOption:
    """Load a relationship path with the joined strategy."""
    return LoaderOption(path=path, strategy="joined")


def selectin(path: str) -> LoaderOption:
    """Load a relationship path with the select-in strategy."""
    return LoaderOption(path=path, strategy="selectin")


def lazy(path: str) -> LoaderOption:
    """Mark a relationship path for explicit lazy loading."""
    return LoaderOption(path=path, strategy="lazy")


def loader_depth(load: list[LoaderOption] | None) -> int:
    """Return the maximum eager-loading depth from loader options."""
    if not load:
        return 0
    return max(
        (option.depth for option in load if option.strategy != "lazy"), default=0
    )

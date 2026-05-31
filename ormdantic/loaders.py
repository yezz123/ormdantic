from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

LoaderStrategy = Literal["joined", "selectin", "lazy"]


@dataclass(frozen=True)
class LoaderOption:
    path: str
    strategy: LoaderStrategy

    @property
    def depth(self) -> int:
        return len([part for part in self.path.split(".") if part])


def joined(path: str) -> LoaderOption:
    return LoaderOption(path=path, strategy="joined")


def selectin(path: str) -> LoaderOption:
    return LoaderOption(path=path, strategy="selectin")


def lazy(path: str) -> LoaderOption:
    return LoaderOption(path=path, strategy="lazy")


def loader_depth(load: list[LoaderOption] | None) -> int:
    if not load:
        return 0
    return max(
        (option.depth for option in load if option.strategy != "lazy"), default=0
    )

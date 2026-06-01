"""Association proxy and hybrid attribute descriptors."""

from __future__ import annotations

from typing import Any, Callable


class association_proxy:
    """Descriptor that proxies through a relationship attribute."""

    def __init__(self, relationship: str, attribute: str) -> None:
        self.relationship = relationship
        self.attribute = attribute

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            return self
        target = getattr(instance, self.relationship)
        if isinstance(target, list):
            return [getattr(item, self.attribute) for item in target]
        return getattr(target, self.attribute)


class hybrid_attribute:
    """Descriptor for Python-computed attributes that may later lower to Rust expressions."""

    def __init__(self, func: Callable[[Any], Any]) -> None:
        self.func = func
        self.__name__ = getattr(func, "__name__", "hybrid_attribute")

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            return self
        return self.func(instance)


def hybrid_property(func: Callable[[Any], Any]) -> hybrid_attribute:
    """Create a hybrid attribute descriptor."""
    return hybrid_attribute(func)

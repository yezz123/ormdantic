"""Association proxy and hybrid attribute descriptors."""

from __future__ import annotations

from typing import Any, Callable


class association_proxy:
    """Descriptor that proxies through a relationship attribute."""

    def __init__(self, relationship: str, attribute: str) -> None:
        self.relationship = relationship
        self.attribute = attribute
        self.expression_func: Callable[[type[Any]], Any] | None = None

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            if self.expression_func is not None:
                return self.expression_func(owner)
            return self
        target = getattr(instance, self.relationship)
        if isinstance(target, list):
            return [getattr(item, self.attribute) for item in target]
        return getattr(target, self.attribute)

    def __set__(self, instance: Any, value: Any) -> None:
        target = getattr(instance, self.relationship)
        if isinstance(target, list):
            raise TypeError("cannot assign scalar value through a collection proxy")
        setattr(target, self.attribute, value)

    def expression(self, func: Callable[[type[Any]], Any]) -> "association_proxy":
        """Attach a class-level query expression for this proxy."""
        self.expression_func = func
        return self


class hybrid_attribute:
    """Descriptor for Python-computed attributes with optional query expressions."""

    def __init__(self, func: Callable[[Any], Any]) -> None:
        self.func = func
        self.expression_func: Callable[[type[Any]], Any] | None = None
        self.__name__ = getattr(func, "__name__", "hybrid_attribute")

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            if self.expression_func is not None:
                return self.expression_func(owner)
            return self
        return self.func(instance)

    def expression(self, func: Callable[[type[Any]], Any]) -> "hybrid_attribute":
        self.expression_func = func
        return self


def hybrid_property(func: Callable[[Any], Any]) -> hybrid_attribute:
    """Create a hybrid attribute descriptor."""
    return hybrid_attribute(func)

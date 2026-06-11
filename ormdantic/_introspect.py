"""Pydantic v2 model introspection helpers used by the ORM runtime."""

from __future__ import annotations

from dataclasses import dataclass
from types import NoneType, UnionType
from typing import Any, Type, Union, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from ormdantic.types import ModelType


@dataclass(frozen=True)
class FieldMetadata:
    """Small compatibility wrapper around a Pydantic v2 field."""

    name: str
    info: FieldInfo

    @property
    def annotation(self) -> Any:
        """Return the field annotation."""
        return self.info.annotation

    @property
    def origin(self) -> Any:
        """Return `typing.get_origin` for the field annotation."""
        return get_origin(self.annotation)

    @property
    def args(self) -> tuple[Any, ...]:
        """Return `typing.get_args` for the field annotation."""
        return get_args(self.annotation)

    @property
    def required(self) -> bool:
        """Return whether the field is required by Pydantic."""
        return self.info.is_required()

    @property
    def nullable(self) -> bool:
        """Return whether the field annotation accepts `None`."""
        return annotation_allows_none(self.annotation)

    @property
    def default(self) -> Any:
        """Return the field default value."""
        return self.info.default

    @property
    def default_factory(self) -> Any:
        """Return the field default factory."""
        return self.info.default_factory

    def constraint(self, name: str) -> Any:
        """Return a Pydantic field constraint by name when present."""
        if hasattr(self.info, name):
            value = getattr(self.info, name)
            if value is not None:
                return value
        for metadata in self.info.metadata:
            if hasattr(metadata, name):
                value = getattr(metadata, name)
                if value is not None:
                    return value
        return None

    @property
    def min_length(self) -> int | None:
        """Return the minimum length constraint."""
        return self.constraint("min_length")

    @property
    def max_length(self) -> int | None:
        """Return the maximum length constraint."""
        return self.constraint("max_length")

    @property
    def pattern(self) -> Any:
        """Return the string pattern constraint."""
        return self.constraint("pattern")

    @property
    def multiple_of(self) -> Any:
        """Return the multiple-of constraint."""
        return self.constraint("multiple_of")

    @property
    def max_digits(self) -> int | None:
        """Return the maximum digit count for Decimal fields."""
        return self.constraint("max_digits")

    @property
    def decimal_places(self) -> int | None:
        """Return the fixed decimal-place count for Decimal fields."""
        return self.constraint("decimal_places")

    @property
    def ge(self) -> Any:
        """Return the greater-than-or-equal constraint."""
        return self.constraint("ge")

    @property
    def gt(self) -> Any:
        """Return the greater-than constraint."""
        return self.constraint("gt")

    @property
    def le(self) -> Any:
        """Return the less-than-or-equal constraint."""
        return self.constraint("le")

    @property
    def lt(self) -> Any:
        """Return the less-than constraint."""
        return self.constraint("lt")

    @property
    def min_items(self) -> int | None:
        """Return the minimum item count constraint."""
        return self.min_length

    @property
    def max_items(self) -> int | None:
        """Return the maximum item count constraint."""
        return self.max_length


def model_fields(model: Type[ModelType]) -> dict[str, FieldMetadata]:
    """Return Ormdantic field metadata for a Pydantic model."""
    return {
        name: FieldMetadata(name=name, info=field)
        for name, field in model.model_fields.items()
    }


def model_field(model: Type[ModelType], name: str) -> FieldMetadata:
    """Return field metadata for one field on a Pydantic model."""
    return FieldMetadata(name=name, info=model.model_fields[name])


def annotation_allows_none(annotation: Any) -> bool:
    """Return whether an annotation accepts `None`."""
    if annotation is NoneType:
        return True
    return NoneType in get_args(annotation)


def is_union_annotation(annotation: Any) -> bool:
    """Return whether an annotation is a PEP 604 or typing union."""
    return get_origin(annotation) in {UnionType, Union}


def is_list_annotation(annotation: Any) -> bool:
    """Return whether an annotation is a concrete list annotation."""
    return get_origin(annotation) is list


def contains_list_annotation(annotation: Any) -> bool:
    """Return whether an annotation contains a list at any nesting level."""
    if is_list_annotation(annotation):
        return True
    return any(contains_list_annotation(arg) for arg in get_args(annotation))


def is_dict_annotation(annotation: Any) -> bool:
    """Return whether an annotation is a dictionary."""
    return get_origin(annotation) is dict or annotation is dict


def first_model_arg(
    annotation: Any, table_models: set[type[BaseModel]]
) -> type[BaseModel] | None:
    """Return the first registered table model found in an annotation."""
    try:
        if annotation in table_models:
            return annotation
    except TypeError:
        pass
    for arg in get_args(annotation):
        try:
            if arg in table_models:
                return arg
        except TypeError:
            pass
        if nested := first_model_arg(arg, table_models):
            return nested
    return None


def rebuild_model(model: Type[ModelType]) -> None:
    """Rebuild a Pydantic model after forward references are available."""
    model.model_rebuild()

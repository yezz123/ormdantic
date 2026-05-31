from __future__ import annotations

from dataclasses import dataclass
from types import NoneType, UnionType
from typing import Any, Type, Union, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from ormdantic.types import ModelType


@dataclass(frozen=True)
class FieldMetadata:
    name: str
    info: FieldInfo

    @property
    def annotation(self) -> Any:
        return self.info.annotation

    @property
    def origin(self) -> Any:
        return get_origin(self.annotation)

    @property
    def args(self) -> tuple[Any, ...]:
        return get_args(self.annotation)

    @property
    def required(self) -> bool:
        return self.info.is_required()

    @property
    def nullable(self) -> bool:
        return annotation_allows_none(self.annotation)

    @property
    def default(self) -> Any:
        return self.info.default

    @property
    def default_factory(self) -> Any:
        return self.info.default_factory

    def constraint(self, name: str) -> Any:
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
        return self.constraint("min_length")

    @property
    def max_length(self) -> int | None:
        return self.constraint("max_length")

    @property
    def multiple_of(self) -> Any:
        return self.constraint("multiple_of")

    @property
    def ge(self) -> Any:
        return self.constraint("ge")

    @property
    def gt(self) -> Any:
        return self.constraint("gt")

    @property
    def le(self) -> Any:
        return self.constraint("le")

    @property
    def lt(self) -> Any:
        return self.constraint("lt")

    @property
    def min_items(self) -> int | None:
        return self.min_length

    @property
    def max_items(self) -> int | None:
        return self.max_length


def model_fields(model: Type[ModelType]) -> dict[str, FieldMetadata]:
    return {
        name: FieldMetadata(name=name, info=field)
        for name, field in model.model_fields.items()
    }


def model_field(model: Type[ModelType], name: str) -> FieldMetadata:
    return FieldMetadata(name=name, info=model.model_fields[name])


def annotation_allows_none(annotation: Any) -> bool:
    if annotation is NoneType:
        return True
    return NoneType in get_args(annotation)


def is_union_annotation(annotation: Any) -> bool:
    return get_origin(annotation) in {UnionType, Union}


def is_list_annotation(annotation: Any) -> bool:
    return get_origin(annotation) is list


def contains_list_annotation(annotation: Any) -> bool:
    if is_list_annotation(annotation):
        return True
    return any(contains_list_annotation(arg) for arg in get_args(annotation))


def is_dict_annotation(annotation: Any) -> bool:
    return get_origin(annotation) is dict or annotation is dict


def first_model_arg(annotation: Any, table_models: set[type[BaseModel]]) -> type[BaseModel] | None:
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
    model.model_rebuild()

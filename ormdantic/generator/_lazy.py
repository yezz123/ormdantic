import contextlib
import datetime
import random
import types
import typing
from enum import Enum
from typing import Any, Type
from uuid import UUID, uuid4

import pydantic
from pydantic import BaseModel
from pydantic.fields import ModelField

from ormdantic.handler import (
    GetTargetLength,
    RandomDatetimeValue,
    RandomDateValue,
    RandomNumberValue,
    RandomStrValue,
    RandomTimedeltaValue,
    RandomTimeValue,
)
from ormdantic.types import ModelType


def generate(
    model_type: Type[ModelType],
    use_default_values: bool = True,
    optionals_use_none: bool = False,
    **kwargs: Any,
) -> ModelType:
    """
    This is a function that generates an instance of a Pydantic model with random values.
    It takes in a model type, and optional parameters use_default_values, `optionals_use_none`, and kwargs.
    If `use_default_values` is True, the function will use the model's default values for fields that have them.
    If `optionals_use_none` is True, the function will set optional fields to None instead of generating a random value for them.
    `kwargs()` is a dictionary of attributes to set on the model instance, which will override any randomly generated values.

    The function iterates through all the fields of the model and checks if a value has been provided in kwargs or if the field has a default value or default factory.
    If none of these conditions are met, the function calls the `_get_value` function to generate a random value for the field based on its type annotation.
    Finally, the function returns a new instance of the model type with the generated or provided field values.
    """
    for field_name, model_field in model_type.__fields__.items():
        if field_name in kwargs:
            continue
        if (
            model_field.default is not None or model_field.default_factory is not None
        ) and use_default_values:
            continue

        kwargs[field_name] = _get_value(
            model_field.annotation, model_field, use_default_values, optionals_use_none
        )
    return model_type(**kwargs)


def _get_value(
    type_: Type,  # type: ignore
    model_field: ModelField,
    use_default_values: bool,
    optionals_use_none: bool,
) -> Any:
    """
    This is a helper function that generates a random value for a given type.
    It takes in four parameters: `type_`, `model_field`, `use_default_values`, and `optionals_use_none.type_` is the type of the value to generate,
    `model_field` is an instance of a Pydantic model field, `use_default_values` is a boolean indicating whether to use default values, and `optionals_use_none` is a boolean indicating whether to set optional fields to None.

    The function first checks if type_ is a dictionary, list, or union type, and generates random values accordingly.

    If model_field is an optional field and optionals_use_none is True, the function returns None.

    - `type_ is` a string or constrained string, the function returns a random string using the RandomStrValue class.
    - `type_ is` a number or constrained number, the function returns a random number using the RandomNumberValue class.
    - `type_ is` a boolean, the function returns a random boolean value.
    - `type_ is` a Pydantic model, the function recursively generates a random instance of that model.
    - `type_ is` an enum, the function returns a random value from the enum.
    - `type_ is` a UUID, the function returns a randomly generated UUID.
    - `type_ is` a date, time, timedelta, or datetime, the function returns a random value using the corresponding Random*Value class.

    If none of these conditions are met, the function returns a default value for the given type.
    """
    origin = typing.get_origin(type_)
    if origin is dict:
        k_type, v_type = typing.get_args(type_)
        return {
            _get_value(
                k_type, model_field, use_default_values, optionals_use_none
            ): _get_value(v_type, model_field, use_default_values, optionals_use_none)
            for _ in range(random.randint(1, 100))
        }
    with contextlib.suppress(TypeError):
        if origin is list or issubclass(type_, pydantic.types.ConstrainedList):
            return _get_list_values(
                type_, model_field, use_default_values, optionals_use_none
            )
    if origin and issubclass(origin, types.UnionType):
        type_choices = [
            it for it in typing.get_args(type_) if not issubclass(it, types.NoneType)
        ]
        chosen_union_type = random.choice(type_choices)
        return _get_value(
            chosen_union_type, model_field, use_default_values, optionals_use_none
        )
    if model_field.allow_none and optionals_use_none:
        return None
    if type_ == str or issubclass(type_, pydantic.types.ConstrainedStr):
        return RandomStrValue(model_field)
    if type_ in [int, float] or isinstance(type_, pydantic.types.ConstrainedNumberMeta):
        return RandomNumberValue(model_field)
    if type_ == bool:
        return random.random() > 0.5
    if issubclass(type_, types.NoneType):
        return None
    if issubclass(type_, BaseModel):
        return generate(type_, use_default_values, optionals_use_none)
    if issubclass(type_, Enum):
        return random.choice(list(type_))
    if type_ == UUID:
        return uuid4()
    if type_ == datetime.date:
        return RandomDateValue()
    if type_ == datetime.time:
        return RandomTimeValue()
    if type_ == datetime.timedelta:
        return RandomTimedeltaValue()
    return RandomDatetimeValue() if type_ == datetime.datetime else type_()


def _get_list_values(
    type_: Type | pydantic.types.ConstrainedList,  # type: ignore
    model_field: ModelField,
    use_default_values: bool = True,
    optionals_use_none: bool = False,
) -> list[Any]:
    target_length = GetTargetLength(
        model_field.field_info.min_items, model_field.field_info.max_items
    )
    items: list = []  # type: ignore
    if issubclass(type_, pydantic.types.ConstrainedList):  # type: ignore
        list_types = typing.get_args(type_.item_type) or [
            type_.item_type
        ]  # pragma: no cover
    else:
        list_types = typing.get_args(type_)
    while len(items) < target_length:
        for arg in list_types:
            value = _get_value(arg, model_field, use_default_values, optionals_use_none)
            if model_field.field_info.unique_items and value in items:
                continue  # pragma: no cover
            items.append(value)
    return items

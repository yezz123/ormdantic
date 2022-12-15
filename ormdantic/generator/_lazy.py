"""Generate random instances of the given Pydantic model type."""

import datetime
import math
import random
import string
from enum import Enum
from typing import Any, Type, TypeVar
from uuid import UUID, uuid4

import pydantic
from pydantic import BaseModel
from pydantic.fields import ModelField

ModelType = TypeVar("ModelType", bound=BaseModel)


def generate(
    model_type: Type[ModelType],
    use_default_values: bool = True,
    optionals_use_none: bool = False,
    **kwargs: Any,
) -> ModelType:
    """Generate an instance of a Pydantic model with random values.

    Any values provided in `kwargs` will be used as model field values
    instead of randomly generating them.

    :param model_type: Model type to generate an instance of.
    :param use_default_values: Whether to use model default values.
    :param optionals_use_none: How to handle optional fields.
    :param kwargs: Attributes to set on the model instance.
    :return: A randomly generated instance of the provided model type.
    """
    for field_name, model_field in model_type.__fields__.items():
        if field_name in kwargs:
            continue
        if (
            model_field.default is not None or model_field.default_factory is not None
        ) and use_default_values:
            continue

        kwargs[field_name] = _get_value(
            model_field, use_default_values, optionals_use_none
        )
    return model_type(**kwargs)


def _get_value(
    model_field: ModelField, use_default_values: bool, optionals_use_none: bool
) -> Any:
    """Get a random value for the given model field."""
    if model_field.allow_none and optionals_use_none:
        return None
    if model_field.type_ == str:
        return _random_str()
    if model_field.type_ == int or isinstance(
        model_field.type_, pydantic.types.ConstrainedNumberMeta
    ):
        return _random_int(model_field)
    if model_field.type_ == float:
        return random.random() * 100
    if model_field.type_ == bool:
        return random.random() > 0.5
    if issubclass(model_field.type_, BaseModel):
        return generate(model_field.type_, use_default_values, optionals_use_none)
    if issubclass(model_field.type_, Enum):
        return random.choices(list(model_field.type_))[0]
    if model_field.type_ == UUID:
        return uuid4()
    if model_field.type_ == datetime.datetime:
        return datetime.datetime.now()


def _random_str() -> str:
    """Get a random string."""
    return "".join(random.choices(string.ascii_letters, k=5))


def _random_int(model_field: ModelField) -> int:
    """Get a random integer."""
    default_max_difference = 256
    iter_size = model_field.field_info.multiple_of or 1
    # Determine lower bound.
    lower = 0
    if ge := model_field.field_info.ge:
        while lower < ge:
            lower += iter_size
    if gt := model_field.field_info.gt:
        while lower <= gt:
            lower += iter_size
    # Determine upper bound.
    upper = lower + iter_size * default_max_difference
    if le := model_field.field_info.le:
        while upper > le:
            upper -= iter_size
    if lt := model_field.field_info.lt:
        while upper >= lt:
            upper -= iter_size
    # Re-evaluate lower bound in case ge/gt unset and upper is negative.
    if (
        not model_field.field_info.ge
        and not model_field.field_info.gt
        and lower > upper
    ):
        lower = upper - iter_size * default_max_difference
    # Find a random int within determined range.
    if not model_field.field_info.multiple_of:
        return random.randint(lower, upper)
    max_iter_distance = abs(math.floor((upper - lower) / iter_size))
    return lower + iter_size * random.randint(1, max_iter_distance)

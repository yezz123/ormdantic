from numbers import Number
from typing import TypeAlias, TypeVar

from pydantic import BaseModel

# ModelType is a TypeVar that is bound to BaseModel, so it can only be used
# with subclasses of BaseModel.
ModelType = TypeVar("ModelType", bound=BaseModel)

# SerializedType is a TypeVar that is bound to dict, so it can only be used
# with subclasses of dict.
SerializedType = TypeVar("SerializedType")

# AnyNumber is a TypeAlias that is bound to Number, so it can only be used
# with subclasses of Number.
AnyNumber: TypeAlias = Number | float

# This is the default maximum length for strings generated by the generator.
default_max_length = 5

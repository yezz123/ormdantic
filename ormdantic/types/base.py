from typing import TypeVar

from pydantic import BaseModel

# ModelType is a TypeVar that is bound to BaseModel, so it can only be used
# with subclasses of BaseModel.
ModelType = TypeVar("ModelType", bound=BaseModel)

# SerializedType is a TypeVar that is bound to dict, so it can only be used
# with subclasses of dict.
SerializedType = TypeVar("SerializedType")


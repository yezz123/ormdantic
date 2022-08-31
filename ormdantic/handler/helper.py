"""Utility functions used throughout the project."""
from typing import Type

from pydantic import BaseModel

from ormdantic.models.models import Map
from ormdantic.types import ModelType


def TableName_From_Model(model: Type[ModelType], table_map: Map) -> str:
    """Get a tablename from the model and schema."""
    return [
        tablename
        for tablename, data in table_map.name_to_data.items()
        if data.model == model
    ][0]


def Model_Instance(model: BaseModel, table_map: Map) -> str:
    """Get a tablename from a model instance."""
    return [k for k, v in table_map.name_to_data.items() if isinstance(model, v.model)][
        0
    ]

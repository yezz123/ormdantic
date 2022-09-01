"""Utility functions used throughout the project."""
import json
from typing import Any, Type
from uuid import UUID

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


def py_type_to_sql(table_map: Map, value: Any) -> Any:
    """Get value as SQL compatible type."""
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, BaseModel) and type(value) in table_map.model_to_data:
        tablename = Model_Instance(value, table_map)
        return py_type_to_sql(
            table_map, value.__dict__[table_map.name_to_data[tablename].pk]
        )
    return value.json() if isinstance(value, BaseModel) else value

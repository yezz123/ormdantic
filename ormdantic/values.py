"""Value conversion helpers for the Rust runtime."""

import importlib
import json
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from ormdantic.models.models import Map

_ormdantic = importlib.import_module("ormdantic._ormdantic")


def model_instance_table(model: BaseModel, table_map: Map) -> str:
    """Return the table name for a registered model instance."""
    return [k for k, v in table_map.name_to_data.items() if isinstance(model, v.model)][
        0
    ]


def py_type_to_sql(table_map: Map, value: Any) -> Any:
    """Convert a Python value to a SQL-compatible value for Rust binding."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, BaseModel) and type(value) in table_map.model_to_data:
        tablename = model_instance_table(value, table_map)
        return py_type_to_sql(
            table_map, value.__dict__[table_map.name_to_data[tablename].pk]
        )
    if isinstance(value, BaseModel):
        value = value.model_dump_json()
    if isinstance(value, UUID):
        value = str(value)
    return _ormdantic.sql_value(value)

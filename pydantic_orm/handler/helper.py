"""Utility functions used throughout the project."""
from typing import Any, Type

from pydantic_orm.types import ModelType


def TableName_From_Model(model: Type[ModelType], schema: dict[str, Any]) -> str:
    """Get a tablename from the model and schema."""
    return [tablename for tablename, data in schema.items() if data.model == model][0]


def Get_M2M_TableName(
    table: str, column: str, other_table: str, other_column: str
) -> str:
    """Get the name of a table joining two tables in an ManyToMany relation."""
    return f"{table}.{column}-to-{other_table}.{other_column}"

"""Deserialize a result set into Python models."""

from __future__ import annotations

from typing import Generic

from pydantic import BaseModel, Field
from sqlalchemy.engine import CursorResult

from ormdantic.models import Map, OrmTable
from ormdantic.types import ModelType, SerializedType


class ResultSchema(BaseModel):
    """Model to describe the schema of a model result."""

    table_data: OrmTable  # type: ignore
    field_name: str | None
    is_array: bool
    references: dict[str, ResultSchema] = Field(default_factory=lambda: [])
    models: list[ModelType] = Field(default_factory=lambda: [])  # type: ignore


class OrmSerializer(Generic[SerializedType]):
    """Generate Python models from a table map and result set."""

    def __init__(
        self,
        table_data: OrmTable,  # type: ignore
        table_map: Map,
        result_set: CursorResult,
        is_array: bool,
        depth: int,
        result_schema: ResultSchema | None = None,
    ) -> None:
        """Generate Python models from a table map and result set.

        :param table_data: Table data for the returned model type.
        :param table_map: Map of tablenames and models.
        :param result_set: SQL Alchemy cursor result.
        :param is_array: Deserialize as a model or a list of models?
        :param depth: Model tree depth.
        :param result_schema: What the result schema will be.
        """
        self._table_data = table_data
        self._table_map = table_map
        self._result_set = result_set
        self._is_array = is_array
        self._depth = depth
        self._result_schema = result_schema or self._get_result_schema(
            table_data, depth, is_array
        )
        self._columns = [it[0] for it in self._result_set.cursor.description]
        self._return_dict = {}  # type: ignore

    def deserialize(self) -> SerializedType:
        """Deserialize the result set into Python models."""
        for row in self._result_set:
            last_pk = None
            for column_idx, column_tree in enumerate(self._columns):
                node = self._return_dict
                schema = self._result_schema
                for branch_idx, branch in enumerate(column_tree.split("/")):
                    column: str | None = None
                    if "\\" in branch:
                        branch, column = branch.split("\\")
                    if branch_idx != 0:
                        schema = schema.references[branch]  # type: ignore
                    if node.get(branch) is None and branch_idx != 0:
                        node[branch] = {}
                    if branch_idx != 0:
                        if schema.is_array:  # type: ignore
                            if column == schema.table_data.pk:  # type: ignore
                                last_pk = row[column_idx]
                                node[branch][last_pk] = node[branch].get(last_pk) or {}
                            node = node[branch].get(last_pk)
                            if node is None:
                                break
                        else:
                            node = node[branch]
                    if column is not None:
                        node[column] = row[column_idx]
        return self._table_data.model(**self._return_dict)

    def _get_result_schema(
        self,
        table_data: OrmTable,  # type: ignore
        depth: int,
        is_array: bool,
        field_name: str | None = None,
    ) -> ResultSchema | None:
        if depth < 0:
            return None
        return ResultSchema(
            table_data=table_data,
            field_name=field_name,
            is_array=is_array,
            references={
                column: schema
                for column, rel in table_data.relationships.items()
                if (
                    schema := self._get_result_schema(
                        table_data=self._table_map.name_to_data[rel.foreign_table],
                        depth=depth - 1,
                        field_name=column,
                        is_array=rel.back_references is not None,
                    )
                )
                is not None
            },
        )

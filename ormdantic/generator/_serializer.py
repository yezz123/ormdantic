import json
from types import NoneType
from typing import Any, Generic, cast, get_args

from pydantic import BaseModel, Field

from ormdantic._introspect import is_dict_annotation, is_list_annotation, model_field
from ormdantic.generator._hydration import hydrate_flat_payload, hydrate_joined_payload
from ormdantic.models import Map, OrmTable
from ormdantic.types import ModelType, SerializedType


class ResultSchema(BaseModel):
    """Model to describe the schema of a model result."""

    table_data: OrmTable | None = None  # type: ignore
    is_array: bool
    references: dict[str, "ResultSchema"] = Field(default_factory=lambda: {})


class OrmSerializer(Generic[SerializedType]):
    """Generate Python models from a table map and result set."""

    def __init__(
        self,
        table_data: OrmTable,  # type: ignore
        table_map: Map,
        result_set: Any,
        is_array: bool,
        depth: int,
    ) -> None:
        """Generate Python models from a table map and result set.

        :param table_data: Table data for the returned model type.
        :param table_map: Map of tablenames and models.
        :param result_set: SQL Alchemy cursor result.
        :param is_array: Deserialize as a model or a list of models?
        :param depth: Model tree depth.
        """
        self._table_data = table_data
        self._table_map = table_map
        self._result_set = result_set
        self._is_array = is_array
        self._depth = depth
        self._result_schema = ResultSchema(
            is_array=is_array,
            references={
                table_data.tablename: self._get_result_schema(
                    table_data, depth, is_array
                )
            },
        )
        self._columns = [it[0] for it in self._result_set.cursor.description]
        self._return_dict: dict[str, Any] = {}

    def deserialize(self) -> SerializedType:
        """Deserialize the result set into Python models."""
        if self._can_use_flat_hydration():
            return self._deserialize_flat()
        self._return_dict = (
            hydrate_joined_payload(
                columns=self._columns,
                rows=[tuple(row) for row in self._result_set],
                path_pks=self._path_pks(self._result_schema),
                array_paths=self._array_paths(self._result_schema),
            )
            or {}
        )
        if not self._return_dict:
            return None  # type: ignore
        if self._result_schema.is_array:
            return [
                self._table_data.model(**record)
                for record in self._prep_result(self._return_dict, self._result_schema)[
                    self._table_data.tablename
                ]  # type: ignore
            ]
        return self._table_data.model(
            **self._prep_result(self._return_dict, self._result_schema)[
                self._table_data.tablename
            ]
        )

    def _can_use_flat_hydration(self) -> bool:
        return self._depth <= 0

    def _deserialize_flat(self) -> SerializedType:
        rows = [tuple(row) for row in self._result_set]
        payload = hydrate_flat_payload(
            tablename=self._table_data.tablename,
            pk=self._table_data.pk,
            columns=self._columns,
            rows=rows,
            is_array=self._is_array,
        )
        if payload is None:
            return None  # type: ignore
        if self._is_array:
            records = cast(list[dict[str, Any]], payload)
            return [
                self._table_data.model(**self._prep_flat_result(record))
                for record in records
            ]  # type: ignore
        record = cast(dict[str, Any], payload)
        return self._table_data.model(**self._prep_flat_result(record))

    def _prep_flat_result(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            column: self._sql_type_to_py(self._table_data.model, column, value)
            for column, value in record.items()
        }

    def _path_pks(
        self, schema: ResultSchema, prefix: str | None = None
    ) -> list[tuple[str, str]]:
        paths = []
        for name, reference in schema.references.items():
            path = name if prefix is None else f"{prefix}/{name}"
            if reference.table_data is not None:
                paths.append((path, reference.table_data.pk))
            paths.extend(self._path_pks(reference, path))
        return paths

    def _array_paths(
        self, schema: ResultSchema, prefix: str | None = None
    ) -> list[str]:
        paths = []
        for name, reference in schema.references.items():
            path = name if prefix is None else f"{prefix}/{name}"
            if reference.is_array:
                paths.append(path)
            paths.extend(self._array_paths(reference, path))
        return paths

    def _prep_result(
        self, node: dict[Any, Any], schema: ResultSchema
    ) -> dict[str, Any]:
        for key, val in node.items():
            if key in schema.references:
                ref_schema = schema.references[key]
                if ref_schema.is_array:
                    node[key] = [
                        self._prep_result(v, ref_schema) for v in node[key].values()
                    ]
                else:
                    node[key] = self._prep_result(node[key], ref_schema)
                continue
            if td := schema.table_data:
                node[key] = self._sql_type_to_py(td.model, key, val)
        return node

    def _get_result_schema(
        self,
        table_data: OrmTable,  # type: ignore
        depth: int,
        is_array: bool,
    ) -> ResultSchema | None:
        if depth < 0:
            return None
        return ResultSchema(
            table_data=table_data,
            is_array=is_array,
            references={
                column: schema
                for column, rel in table_data.relationships.items()
                if (
                    schema := self._get_result_schema(
                        table_data=self._table_map.name_to_data[rel.foreign_table],
                        depth=depth - 1,
                        is_array=rel.back_references is not None,
                    )
                )
                is not None
            },
        )

    @staticmethod
    def _sql_type_to_py(model_type: type[ModelType], column: str, value: Any) -> Any:
        field = model_field(model_type, column)
        annotation = field.annotation
        if is_dict_annotation(annotation):
            return {} if value is None else json.loads(value)
        if is_list_annotation(annotation) or annotation is list:
            return [] if value is None else json.loads(value)
        if value is None:
            return None
        if get_args(annotation):
            for arg in get_args(annotation):
                if arg is NoneType:
                    continue
                try:
                    return arg(value)
                except (AttributeError, TypeError):
                    continue
        try:
            if issubclass(annotation, BaseModel):
                return json.loads(value)
        except TypeError:
            return value
        return value

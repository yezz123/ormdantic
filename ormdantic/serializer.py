"""Pydantic model construction from Rust row payloads."""

import json
from types import NoneType
from typing import Any, Generic, cast, get_args

from pydantic import BaseModel, Field

from ormdantic._introspect import is_dict_annotation, is_list_annotation, model_field
from ormdantic.hydration import hydrate_joined_payload
from ormdantic.loaders import LoaderOption, path_parts
from ormdantic.models import Map, OrmTable
from ormdantic.types import ModelType, SerializedType


class ResultSchema(BaseModel):
    """Model to describe the schema of a model result."""

    table_data: OrmTable[Any] | None = None
    is_array: bool
    references: dict[str, "ResultSchema"] = Field(default_factory=lambda: {})


class OrmSerializer(Generic[SerializedType]):
    """Generate Python models from a table map and result set."""

    def __init__(
        self,
        table_data: OrmTable[Any],
        table_map: Map,
        result_set: Any,
        is_array: bool,
        depth: int,
        load_paths: tuple[str, ...] | None = None,
        load_options: tuple[LoaderOption, ...] = (),
    ) -> None:
        self._table_data = table_data
        self._table_map = table_map
        self._result_set = result_set
        self._is_array = is_array
        self._depth = depth
        self._load_paths = load_paths
        self._load_options = load_options
        self._identity_map: dict[tuple[type[BaseModel], Any], BaseModel] = {}
        self._building_identities: set[tuple[type[BaseModel], Any]] = set()
        result_schema = (
            self._get_path_result_schema(
                table_data, self._path_tree(load_paths), is_array
            )
            if load_paths is not None
            else self._get_result_schema(table_data, depth, is_array)
        )
        if result_schema is None:
            result_schema = ResultSchema(table_data=table_data, is_array=is_array)
        self._result_schema = ResultSchema(
            is_array=is_array,
            references={table_data.tablename: result_schema},
        )
        self._columns = [it[0] for it in self._result_set.cursor.description]
        self._flat_columns = [column.rsplit("\\", 1)[-1] for column in self._columns]
        self._flat_conversion_columns = (
            {
                column
                for column in self._flat_columns
                if self._requires_preparation(table_data.model, column)
            }
            if depth <= 0 and load_paths is None
            else set()
        )
        self._return_dict: dict[str, Any] = {}

    def deserialize(self) -> SerializedType:
        """Deserialize the result set into Python models."""
        if self._depth <= 0 and self._load_paths is None:
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
        root_schema = self._result_schema.references[self._table_data.tablename]
        prepared = self._prep_result(self._return_dict, self._result_schema)[
            self._table_data.tablename
        ]
        if self._result_schema.is_array:
            result = [
                self._build_model(record, root_schema, cache_identity=False)
                for record in prepared
            ]
            return cast(SerializedType, self._apply_loader_options(result))
        model = self._build_model(prepared, root_schema, cache_identity=False)
        return cast(SerializedType, self._apply_loader_options(model))

    def _deserialize_flat(self) -> SerializedType:
        rows = [tuple(row) for row in self._result_set]
        if not rows:
            return [] if self._is_array else None  # type: ignore
        if self._is_array:
            primary_key_index = self._flat_columns.index(self._table_data.pk)
            seen = set()
            models = []
            for row in rows:
                primary_key = row[primary_key_index]
                if primary_key in seen:
                    continue
                seen.add(primary_key)
                record = dict(zip(self._flat_columns, row, strict=True))
                models.append(self._table_data.model(**self._prep_flat_result(record)))
            return models  # type: ignore
        record = dict(zip(self._flat_columns, rows[0], strict=True))
        return self._table_data.model(**self._prep_flat_result(record))

    def _prep_flat_result(self, record: dict[str, Any]) -> dict[str, Any]:
        if not self._flat_conversion_columns:
            return record
        prepared = dict(record)
        for column in self._flat_conversion_columns:
            if column in prepared:
                prepared[column] = self._sql_type_to_py(
                    self._table_data.model, column, prepared[column]
                )
        return prepared

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
            if table_data := schema.table_data:
                node[key] = self._sql_type_to_py(table_data.model, key, val)
        for key, ref_schema in schema.references.items():
            if key not in node and not ref_schema.is_array:
                node[key] = None
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

    def _get_path_result_schema(
        self,
        table_data: OrmTable,  # type: ignore
        path_tree: dict[str, Any],
        is_array: bool,
    ) -> ResultSchema:
        references = {}
        for column, subtree in path_tree.items():
            rel = table_data.relationships.get(column)
            if rel is None:
                continue
            references[column] = self._get_path_result_schema(
                table_data=self._table_map.name_to_data[rel.foreign_table],
                path_tree=subtree,
                is_array=rel.back_references is not None,
            )
        return ResultSchema(
            table_data=table_data,
            is_array=is_array,
            references=references,
        )

    @staticmethod
    def _path_tree(load_paths: tuple[str, ...]) -> dict[str, Any]:
        tree: dict[str, Any] = {}
        for path in load_paths:
            node = tree
            for part in path.replace("/", ".").split("."):
                if not part:
                    continue
                node = node.setdefault(part, {})
        return tree

    def _build_model(
        self,
        record: dict[str, Any],
        schema: ResultSchema,
        *,
        cache_identity: bool = True,
    ) -> BaseModel:
        table_data = schema.table_data
        if table_data is None:
            raise ValueError("result schema node is missing table metadata")
        if not cache_identity and self._is_collection_tree(schema):
            return table_data.model(**record)

        identity = self._identity_for(record, table_data)
        cached = self._identity_map.get(identity) if identity is not None else None
        if cached is not None:
            if identity in self._building_identities:
                cache_identity = False
            else:
                self._merge_relationships(cached, record, schema)
                return cached

        model = table_data.model(**record)
        if identity is not None and cache_identity:
            self._identity_map[identity] = model
            self._building_identities.add(identity)
        try:
            self._merge_relationships(model, record, schema)
        finally:
            if identity is not None and cache_identity:
                self._building_identities.discard(identity)
        return model

    @classmethod
    def _is_collection_tree(cls, schema: ResultSchema) -> bool:
        return all(
            reference.is_array and cls._is_collection_tree(reference)
            for reference in schema.references.values()
        )

    def _merge_relationships(
        self, model: BaseModel, record: dict[str, Any], schema: ResultSchema
    ) -> None:
        for key, ref_schema in schema.references.items():
            if key not in record:
                continue
            value = record[key]
            if value is None:
                object.__setattr__(model, key, None)
            elif ref_schema.is_array:
                object.__setattr__(
                    model,
                    key,
                    [self._build_model(item, ref_schema) for item in value],
                )
            else:
                object.__setattr__(model, key, self._build_model(value, ref_schema))

    @staticmethod
    def _identity_for(
        record: dict[str, Any], table_data: OrmTable[Any]
    ) -> tuple[type[BaseModel], Any] | None:
        value = record.get(table_data.pk)
        if value is None:
            return None
        return (table_data.model, value)

    def _apply_loader_options(self, result: Any) -> Any:
        if not self._load_options:
            return result
        roots = result if isinstance(result, list) else [result]
        for option in self._load_options:
            if not option.filter_by and not option.order_by:
                continue
            parts = path_parts(option.path)
            for root in roots:
                self._apply_loader_option(root, parts, option)
        return result

    def _apply_loader_option(
        self, model: Any, parts: tuple[str, ...], option: LoaderOption
    ) -> None:
        if model is None or not parts:
            return
        relationship = parts[0]
        value = getattr(model, relationship, None)
        if len(parts) == 1:
            object.__setattr__(
                model,
                relationship,
                self._filter_and_order_relationship(value, option),
            )
            return
        if isinstance(value, list):
            for item in value:
                self._apply_loader_option(item, parts[1:], option)
        else:
            self._apply_loader_option(value, parts[1:], option)

    def _filter_and_order_relationship(self, value: Any, option: LoaderOption) -> Any:
        if isinstance(value, list):
            items = [
                item for item in value if self._matches_loader_filter(item, option)
            ]
            for column in reversed(option.order_by):
                descending = column.startswith("-")
                key = column[1:] if descending else column
                items.sort(
                    key=lambda item: (
                        getattr(item, key, None) is None,
                        getattr(item, key, None),
                    ),
                    reverse=descending,
                )
            return items
        if value is None:
            return None
        return value if self._matches_loader_filter(value, option) else None

    @staticmethod
    def _matches_loader_filter(value: Any, option: LoaderOption) -> bool:
        for column, expected in (option.filter_by or {}).items():
            actual = getattr(value, column, None)
            if actual != expected and str(actual) != str(expected):
                return False
        return True

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

    @staticmethod
    def _requires_preparation(model_type: type[ModelType], column: str) -> bool:
        annotation = model_field(model_type, column).annotation
        if (
            is_dict_annotation(annotation)
            or is_list_annotation(annotation)
            or annotation is list
        ):
            return True
        candidates = get_args(annotation) or (annotation,)
        for candidate in candidates:
            try:
                if issubclass(candidate, BaseModel):
                    return True
            except TypeError:
                continue
        return False

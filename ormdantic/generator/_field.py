"""Module for building queries from field data."""

from enum import Enum
from typing import Any

from ormdantic.generator._rust_query import (
    CompiledQuery,
    FilterSpec,
    RustQuery,
    bind_compiled_query,
    compile_count,
    compile_delete_pk,
    compile_find_many,
    compile_joined_find_many,
)
from ormdantic.handler import py_type_to_sql
from ormdantic.models import Map, OrmTable


class Order(Enum):
    asc = "asc"
    desc = "desc"


FILTER_OPERATORS = {
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "like",
    "ilike",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
}


class OrmField:
    """Build SQL queries from field information."""

    def __init__(
        self, table_data: OrmTable[Any], table_map: Map, dialect: str
    ) -> None:
        """Build CRUD queries from tablename and field info.

        :param table_data: Meta data of target table for SQL script.
        :param table_map: Map of tablenames and models.
        """
        self._table_data = table_data
        self._table_map = table_map
        self._dialect = dialect

    def get_find_one_query(self, pk: Any, depth: int = 1) -> RustQuery:
        """Get query to find one model."""
        return bind_compiled_query(
            self._compile_select(
                where=[(self._table_data.pk, "eq", [self._table_data.pk])],
                order_by=[],
                order=Order.asc,
                limit=None,
                offset=None,
                depth=depth,
            ),
            {self._table_data.pk: py_type_to_sql(self._table_map, pk)},
        )

    def get_find_many_query(
        self,
        where: dict[str, Any] | None,
        order_by: list[str] | None,
        order: Order,
        limit: int,
        offset: int,
        depth: int,
    ) -> RustQuery:
        """Get find query for many records.

        :param where: Dictionary of column name to desired value.
        :param order_by: Columns to order by.
        :param order: Order results by ascending or descending.
        :param limit: Number of records to return.
        :param offset: Number of records to offset by.
        :param depth: Depth of relations to populate.
        :return: A list of models representing table records.
        """
        where = where or {}
        order_by = order_by or []
        filters, filter_values = self._parse_where(where)
        return bind_compiled_query(
            self._compile_select(
                where=filters,
                order_by=order_by,
                order=order,
                limit=limit or None,
                offset=offset or None,
                depth=depth,
            ),
            filter_values,
        )

    def get_delete_query(self, pk: Any) -> RustQuery:
        """Get a `delete` query.

        :param pk: Primary key of the record to delete.
        :return: Query to delete a record.
        """
        return bind_compiled_query(
            compile_delete_pk(
                dialect=self._dialect,
                table=self._table_data.tablename,
                primary_key=self._table_data.pk,
            ),
            {self._table_data.pk: py_type_to_sql(self._table_map, pk)},
        )

    def get_count_query(
        self,
        where: dict[str, Any] | None,
        depth: int,
    ) -> RustQuery:
        """Get a `count` query.

        :param where: Dictionary of column name to desired value.
        :param depth: Depth of relations to populate.
        :return: Query to count records.
        """
        where = where or {}
        filters, filter_values = self._parse_where(where)
        return bind_compiled_query(
            compile_count(
                dialect=self._dialect,
                table=self._table_data.tablename,
                filter_columns=filters,
            ),
            filter_values,
        )

    def _compile_select(
        self,
        where: list[FilterSpec],
        order_by: list[str],
        order: Order,
        limit: int | None,
        offset: int | None,
        depth: int,
    ) -> CompiledQuery:
        if depth <= 0:
            return compile_find_many(
                dialect=self._dialect,
                table=self._table_data.tablename,
                columns=self._flat_column_names(depth),
                filter_columns=where,
                order_columns=order_by,
                order_direction=order.value,
                limit=limit,
                offset=offset,
                aliases=self._flat_column_aliases(depth),
            )

        columns = self._joined_column_specs(self._table_data, depth)
        joins = self._join_specs(self._table_data, depth)
        return compile_joined_find_many(
            dialect=self._dialect,
            table=self._table_data.tablename,
            columns=columns,
            joins=joins,
            filter_columns=where,
            order_columns=order_by,
            order_direction=order.value,
            limit=limit,
            offset=offset,
        )

    def _joined_column_specs(
        self,
        table_data: OrmTable[Any],
        depth: int,
        table_tree: str | None = None,
    ) -> list[tuple[str, str, str]]:
        table_tree = table_tree or table_data.tablename
        columns = [
            (table_tree, column, f"{table_tree}\\{column}")
            for column in table_data.columns
            if depth <= 0 or column not in table_data.relationships
        ]
        if depth <= 0:
            return columns

        next_depth = depth - 1
        for field_name, relation in table_data.relationships.items():
            relation_name = f"{table_tree}/{field_name}"
            rel_table_data = self._table_map.name_to_data[relation.foreign_table]
            columns.extend(
                self._joined_column_specs(rel_table_data, next_depth, relation_name)
            )
        return columns

    def _join_specs(
        self,
        table_data: OrmTable[Any],
        depth: int,
        table_tree: str | None = None,
    ) -> list[tuple[str, str, str, str, str, str]]:
        if depth <= 0 or not table_data.relationships:
            return []

        table_tree = table_tree or table_data.tablename
        next_depth = depth - 1
        joins = []
        for field_name, relation in table_data.relationships.items():
            relation_name = f"{table_tree}/{field_name}"
            rel_table_data = self._table_map.name_to_data[relation.foreign_table]
            if relation.back_references is not None:
                joins.append(
                    (
                        relation.foreign_table,
                        relation_name,
                        table_tree,
                        table_data.pk,
                        relation_name,
                        relation.back_references,
                    )
                )
            else:
                joins.append(
                    (
                        relation.foreign_table,
                        relation_name,
                        table_tree,
                        field_name,
                        relation_name,
                        rel_table_data.pk,
                    )
                )
            joins.extend(self._join_specs(rel_table_data, next_depth, relation_name))
        return joins

    def _flat_column_names(self, depth: int) -> list[str]:
        return [
            column
            for column in self._table_data.columns
            if depth <= 0 or column not in self._table_data.relationships
        ]

    def _flat_column_aliases(self, depth: int) -> list[str]:
        return [
            f"{self._table_data.tablename}\\{column}"
            for column in self._flat_column_names(depth)
        ]

    def _parse_where(
        self, where: dict[str, Any]
    ) -> tuple[list[FilterSpec], dict[str, Any]]:
        filters: list[FilterSpec] = []
        values = {}
        for key, value in where.items():
            column, operator = self._split_filter_key(key)
            if operator in {"is_null", "is_not_null"}:
                filters.append((column, operator, []))
                continue
            if operator in {"in", "not_in"}:
                param_names = []
                for idx, item in enumerate(value):
                    param_name = f"{column}__{operator}_{idx}"
                    param_names.append(param_name)
                    values[param_name] = py_type_to_sql(self._table_map, item)
                filters.append((column, operator, param_names))
                continue
            param_name = key
            filters.append((column, operator, [param_name]))
            values[param_name] = py_type_to_sql(self._table_map, value)
        return filters, values

    @staticmethod
    def _split_filter_key(key: str) -> tuple[str, str]:
        if "__" not in key:
            return key, "eq"
        column, operator = key.rsplit("__", 1)
        if operator not in FILTER_OPERATORS:
            return key, "eq"
        return column, operator

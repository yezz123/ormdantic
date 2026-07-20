"""Normalized schema tree widget."""

from __future__ import annotations

from typing import Any

from textual.widgets import Tree

from ormdantic.migrations import SchemaSnapshot


class SchemaTree(Tree[Any]):
    """Display a migration snapshot with stable, qualified node data."""

    def __init__(self, label: str, *, id: str | None = None) -> None:
        super().__init__(label, id=id)
        self._root_label = label
        self.root.expand()

    def update_snapshot(self, snapshot: SchemaSnapshot | None) -> None:
        """Replace tree data while retaining a clear unavailable state."""
        self.reset(self._root_label)
        self.root.expand()
        if snapshot is None:
            return
        for table in sorted(
            snapshot.tables,
            key=lambda item: ((item.schema or ""), item.name),
        ):
            qualified = f"{table.schema}.{table.name}" if table.schema else table.name
            table_node = self.root.add(
                f"▦  {qualified}",
                data={"kind": "table", **table.to_dict()},
                expand=True,
            )
            for column in table.columns:
                flags = []
                if column.primary_key:
                    flags.append("PK")
                if column.foreign_table:
                    flags.append("FK")
                if not column.nullable:
                    flags.append("required")
                suffix = f"  [{' · '.join(flags)}]" if flags else ""
                table_node.add(
                    f"  {column.name}: {column.kind}{suffix}",
                    data={"kind": "column", **column.to_dict()},
                )
            for index in table.indexes:
                table_node.add(
                    f"  index {index.name}",
                    data={"kind": "index", **index.to_dict()},
                )
            for constraint in table.check_constraints:
                table_node.add(
                    f"  check {constraint.name or constraint.expression}",
                    data={"kind": "check", **constraint.to_dict()},
                )
        for view in sorted(snapshot.views, key=lambda item: item.name):
            qualified = f"{view.schema}.{view.name}" if view.schema else view.name
            self.root.add(
                f"◫  {qualified}",
                data={"kind": "view", **view.to_dict()},
            )
        for enum_type in sorted(snapshot.enum_types, key=lambda item: item.name):
            self.root.add(
                f"◇  enum {enum_type.name}",
                data={"kind": "enum", **enum_type.to_dict()},
            )

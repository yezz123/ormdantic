"""Native migration facade."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MigrationOperation:
    """A SQL migration operation."""

    sql: str
    values: tuple[Any, ...] = ()


@dataclass
class MigrationPlan:
    """A generated migration plan."""

    operations: list[MigrationOperation] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.operations


class MigrationManager:
    """Apply and roll back simple SQL migration plans."""

    def __init__(self, database: Any) -> None:
        self._database = database

    async def ensure_revision_table(self) -> None:
        """Create the migration revision table when missing."""
        self._database._ensure_runtime().ensure_revision_table()

    async def applied_revisions(self) -> list[str]:
        """Return applied migration revisions."""
        return list(self._database._ensure_runtime().applied_revisions())

    async def apply(self, revision: str, plan: MigrationPlan) -> None:
        """Apply a migration plan and record its revision."""
        self._database._ensure_runtime().apply_migration(
            revision, _operation_payload(plan)
        )

    async def rollback(self, revision: str, plan: MigrationPlan) -> None:
        """Run rollback SQL and remove a migration revision."""
        self._database._ensure_runtime().rollback_migration(
            revision, _operation_payload(plan)
        )


def _operation_payload(plan: MigrationPlan) -> list[tuple[str, tuple[Any, ...]]]:
    return [(operation.sql, operation.values) for operation in plan.operations]

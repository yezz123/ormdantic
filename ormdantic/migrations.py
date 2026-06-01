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
        await self._database._native_engine.execute(
            "CREATE TABLE IF NOT EXISTS ormdantic_migrations (revision TEXT PRIMARY KEY)",
            (),
        )

    async def applied_revisions(self) -> list[str]:
        """Return applied migration revisions."""
        await self.ensure_revision_table()
        result = await self._database._native_engine.execute(
            "SELECT revision FROM ormdantic_migrations ORDER BY revision",
            (),
        )
        return [row[0] for row in result]

    async def apply(self, revision: str, plan: MigrationPlan) -> None:
        """Apply a migration plan and record its revision."""
        await self.ensure_revision_table()
        async with self._database.transaction():
            for operation in plan.operations:
                await self._database._native_engine.execute(
                    operation.sql, operation.values
                )
            await self._database._native_engine.execute(
                "INSERT INTO ormdantic_migrations (revision) VALUES (?)",
                (revision,),
            )

    async def rollback(self, revision: str, plan: MigrationPlan) -> None:
        """Run rollback SQL and remove a migration revision."""
        await self.ensure_revision_table()
        async with self._database.transaction():
            for operation in plan.operations:
                await self._database._native_engine.execute(
                    operation.sql, operation.values
                )
            await self._database._native_engine.execute(
                "DELETE FROM ormdantic_migrations WHERE revision = ?",
                (revision,),
            )

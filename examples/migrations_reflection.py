"""Migration and reflection example."""

import asyncio
from uuid import uuid4

from pydantic import BaseModel

from ormdantic import Ormdantic
from ormdantic.migrations import MigrationOperation, MigrationPlan

db = Ormdantic("sqlite:///examples_migrations_reflection.sqlite3")


@db.table(pk="id")
class Flavor(BaseModel):
    id: str
    name: str


async def main() -> None:
    await db.init()

    inspector = db.inspect()
    print(await inspector.table_names())
    print(await inspector.columns("flavor"))

    revision = f"example-{uuid4()}"
    await db.migrations.apply(
        revision,
        MigrationPlan(
            [MigrationOperation("CREATE TABLE IF NOT EXISTS extra (id TEXT)")]
        ),
    )
    assert revision in await db.migrations.applied_revisions()


if __name__ == "__main__":
    asyncio.run(main())

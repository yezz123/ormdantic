from __future__ import annotations

from enum import Enum
from typing import Any

import pytest
from pydantic import BaseModel

import ormdantic.orm as orm_module
from ormdantic import Ormdantic
from ormdantic.errors import SchemaError, TransactionError
from ormdantic.models import (
    Map,
    OrmTable,
    Relationship,
    TableCheck,
    TableIndex,
    TableUnique,
)
from ormdantic.orm import _drop_runtime_enum_type_sql


class FacadeKind(str, Enum):
    sweet = "sweet"
    bitter = "bitter"


class FacadeModel(BaseModel):
    id: int
    name: str | None = None
    kind: FacadeKind | None = None


class FacadeOtherModel(BaseModel):
    id: int
    name: str | None = None


class RecordingRuntime:
    def __init__(self) -> None:
        self.begin_options = []
        self.commits = 0
        self.rollbacks = 0

    def begin(self, options=None) -> None:
        self.begin_options.append(options)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class FailingRuntime(RecordingRuntime):
    def __init__(self, fail_on: str) -> None:
        super().__init__()
        self.fail_on = fail_on

    def _maybe_fail(self, operation: str) -> None:
        if self.fail_on == operation:
            raise RuntimeError(f"{operation} exploded")

    def create_all(self) -> None:
        self._maybe_fail("create_all")

    def drop_all(self) -> None:
        self._maybe_fail("drop_all")

    def commit(self) -> None:
        self._maybe_fail("commit")
        super().commit()

    def rollback(self) -> None:
        self._maybe_fail("rollback")
        super().rollback()

    def savepoint(self, name: str) -> None:
        self._maybe_fail("savepoint")

    def rollback_to_savepoint(self, name: str) -> None:
        self._maybe_fail("rollback_to_savepoint")

    def release_savepoint(self, name: str) -> None:
        self._maybe_fail("release_savepoint")


class RecordingNative:
    class PyTransactionOptions:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

    def __init__(self) -> None:
        self.executed: list[tuple[str, str, list[Any]]] = []

    def execute_native(
        self, connection: str, sql: str, params: list[Any]
    ) -> dict[str, Any]:
        self.executed.append((connection, sql, params))
        return {"rows": []}

    def compile_drop_table_sql(self, connection: str, tablename: str) -> str:
        del connection
        return f"DROP TABLE {tablename}"

    def runtime_capabilities(self) -> dict[str, str]:
        return {"native": "fake"}


def facade_table(
    tablename: str = "facade",
    model: type[BaseModel] = FacadeModel,
    **overrides: Any,
) -> OrmTable[Any]:
    data: dict[str, Any] = {
        "model": model,
        "tablename": tablename,
        "pk": "id",
        "indexed": [],
        "unique": [],
        "unique_constraints": [],
        "columns": ["id", "name", "kind"],
        "relationships": {},
        "back_references": {},
    }
    data.update(overrides)
    return OrmTable[Any](**data)


async def test_transaction_passes_native_options_to_runtime() -> None:
    db = Ormdantic("sqlite:///:memory:")
    runtime = RecordingRuntime()
    db._runtime = runtime

    async with db.transaction(
        isolation_level="serializable", read_only=True, deferrable=False
    ):
        pass

    assert len(runtime.begin_options) == 1
    assert runtime.begin_options[0] is not None
    assert type(runtime.begin_options[0]).__name__ == "PyTransactionOptions"
    assert runtime.commits == 1
    assert runtime.rollbacks == 0


async def test_session_passes_native_options_to_runtime() -> None:
    db = Ormdantic("sqlite:///:memory:")
    runtime = RecordingRuntime()
    db._runtime = runtime

    async with db.session(isolation_level="read_committed", read_only=True):
        pass

    assert len(runtime.begin_options) == 1
    assert runtime.begin_options[0] is not None
    assert type(runtime.begin_options[0]).__name__ == "PyTransactionOptions"
    assert runtime.commits == 1
    assert runtime.rollbacks == 0


async def test_transaction_rejects_unknown_isolation_level() -> None:
    db = Ormdantic("sqlite:///:memory:")

    with pytest.raises(ValueError, match="unsupported isolation level 'bogus'"):
        async with db.transaction(isolation_level="bogus"):
            pass


async def test_orm_facade_helpers_cover_lazy_relation_events_and_load_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = RecordingNative()
    monkeypatch.setattr(orm_module, "_ormdantic", native)
    db = Ormdantic("postgres://localhost/app", debug=True, log_queries=True)
    table = facade_table(relationships={})
    db._table_map = Map(name_to_data={"facade": table})
    db._table_map.model_to_data[FacadeModel] = table

    assert db._backend() == "postgresql"
    assert db.runtime_diagnostics()["capabilities"] == {"native": "fake"}

    def handler(**payload: object) -> None:
        del payload

    assert db.on_query(handler) is handler

    monkeypatch.setattr(
        db,
        "get",
        lambda table_data: {
            "supplier": Relationship(foreign_table=table_data.tablename)
        },
    )
    monkeypatch.setattr(
        orm_module,
        "relation_expression",
        lambda *args, **kwargs: ("relation", args, kwargs),
    )
    relation = db.relation(FacadeModel, "supplier", outer_alias="outer")
    assert relation[0] == "relation"
    assert table.relationships == {"supplier": Relationship(foreign_table="facade")}

    class MissingTable:
        def __init__(self) -> None:
            self.calls: list[tuple[object, object]] = []

        async def find_one(self, key: object, *, load: object) -> None:
            self.calls.append((key, load))
            return None

    missing_table = MissingTable()
    monkeypatch.setattr(Ormdantic, "__getitem__", lambda self, item: missing_table)
    assert await db.load(FacadeModel(id=1), "supplier") is None
    assert missing_table.calls[0][0] == 1


async def test_schema_create_and_drop_errors_are_wrapped() -> None:
    create_db = Ormdantic("sqlite:///:memory:")
    create_db._runtime = FailingRuntime("create_all")
    with pytest.raises(SchemaError, match="schema creation failed"):
        await create_db.create_all()

    drop_db = Ormdantic("sqlite:///:memory:")
    drop_db._runtime = FailingRuntime("drop_all")
    with pytest.raises(SchemaError, match="schema drop failed"):
        await drop_db.drop_all()


@pytest.mark.parametrize(
    ("method_name", "fail_on", "event_name", "message"),
    [
        ("_commit", "commit", "after_commit", "transaction commit failed"),
        ("_rollback", "rollback", "after_rollback", "transaction rollback failed"),
        ("_savepoint", "savepoint", "after_savepoint", "savepoint 'sp1' failed"),
        (
            "_rollback_to_savepoint",
            "rollback_to_savepoint",
            "after_rollback_to_savepoint",
            "rollback to savepoint 'sp1' failed",
        ),
        (
            "_release_savepoint",
            "release_savepoint",
            "after_release_savepoint",
            "release savepoint 'sp1' failed",
        ),
    ],
)
async def test_transaction_helper_errors_dispatch_wrapped_failures(
    method_name: str,
    fail_on: str,
    event_name: str,
    message: str,
) -> None:
    db = Ormdantic("sqlite:///:memory:")
    db._runtime = FailingRuntime(fail_on)
    seen: list[object | None] = []

    async def record(**payload: object) -> None:
        seen.append(payload.get("error"))

    db._events.on(event_name, record)
    method = getattr(db, method_name)
    args = ("sp1",) if "savepoint" in method_name else ()

    with pytest.raises(TransactionError, match=message):
        await method(*args)

    assert len(seen) == 1
    assert isinstance(seen[0], TransactionError)


def test_runtime_enum_drop_sql_quotes_identifiers() -> None:
    assert (
        _drop_runtime_enum_type_sql(("flavor", ["a"])) == 'DROP TYPE IF EXISTS "flavor"'
    )
    assert (
        _drop_runtime_enum_type_sql(('flavor"type', ["a"], 'inventory"schema'))
        == 'DROP TYPE IF EXISTS "inventory""schema"."flavor""type"'
    )


async def test_registered_backend_metadata_helpers_execute_and_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = RecordingNative()
    monkeypatch.setattr(orm_module, "_ormdantic", native)

    drop_db = Ormdantic("postgresql://localhost/app")
    monkeypatch.setattr(
        drop_db,
        "_runtime_enum_type_specs",
        lambda: [("facade_kind", ["sweet"], None, None)],
    )
    await drop_db.drop_all()
    assert any("DROP TYPE" in sql for _connection, sql, _params in native.executed)

    metadata_db = Ormdantic("postgresql://localhost/app")
    metadata_table = facade_table(
        indexes=[
            TableIndex(name="plain_idx", columns=["id"]),
            TableIndex(name="commented_idx", columns=["name"], comment="Name index"),
            TableIndex(
                name="ops_idx",
                columns=["name"],
                comment="Ops index",
                postgres_ops={"name": "text_pattern_ops"},
            ),
            TableIndex(
                name="spaced_idx",
                columns=["name"],
                postgres_tablespace="fastspace",
            ),
        ],
        named_unique_constraints=[
            TableUnique(name="plain_uq", columns=["id"]),
            TableUnique(name="commented_uq", columns=["name"], comment="Unique name"),
        ],
        check_constraints=[
            TableCheck(name="plain_ck", expression="id > 0"),
            TableCheck(
                name="commented_ck",
                expression="name IS NOT NULL",
                comment="Name required",
            ),
        ],
    )
    metadata_db._table_map = Map(name_to_data={"facade": metadata_table})
    metadata_db._create_registered_index_comments()
    metadata_db._create_registered_constraint_comments()
    metadata_db._create_registered_index_tablespaces()
    metadata_db.sequence("facade_seq", comment="Sequence comment")
    metadata_db._create_registered_sequences()
    monkeypatch.setattr(
        metadata_db,
        "_runtime_enum_type_specs",
        lambda: [("facade_kind", ["sweet"], None, "Kind comment")],
    )
    metadata_db._create_registered_enum_type_comments()
    executed_sql = [sql for _connection, sql, _params in native.executed]
    assert any("COMMENT ON INDEX" in sql for sql in executed_sql)
    assert any("COMMENT ON CONSTRAINT" in sql for sql in executed_sql)
    assert any("SET TABLESPACE" in sql for sql in executed_sql)
    assert any("CREATE SEQUENCE" in sql for sql in executed_sql)
    assert any("COMMENT ON TYPE" in sql for sql in executed_sql)

    mysql_visible_guard = Ormdantic("mariadb://localhost/app")
    mysql_visible_guard._table_map = Map(
        name_to_data={
            "facade": facade_table(
                indexes=[
                    TableIndex(
                        name="visible_idx",
                        columns=["name"],
                        mysql_visible=False,
                    )
                ],
            )
        }
    )
    with pytest.raises(ValueError, match="visibility only supports MySQL"):
        mysql_visible_guard._create_registered_mysql_index_options()

    unique_guard = Ormdantic("sqlite:///:memory:")
    unique_guard._table_map = Map(
        name_to_data={
            "facade": facade_table(
                named_unique_constraints=[
                    TableUnique(
                        name="uq_name",
                        columns=["name"],
                        postgres_include=["id"],
                    )
                ],
            )
        }
    )
    with pytest.raises(ValueError, match="only support PostgreSQL"):
        unique_guard._create_registered_postgres_unique_options()

    postgres_index_guard = Ormdantic("sqlite:///:memory:")
    postgres_index_guard._table_map = Map(
        name_to_data={
            "facade": facade_table(
                indexes=[
                    TableIndex(
                        name="ops_idx",
                        columns=["name"],
                        postgres_ops={"name": "text_pattern_ops"},
                    )
                ],
            )
        }
    )
    with pytest.raises(ValueError, match="only support PostgreSQL"):
        postgres_index_guard._create_registered_postgres_index_options()

    import ormdantic._migrations.planning as planning

    monkeypatch.setattr(
        planning,
        "_compile_unique_constraint_recreate_sql",
        lambda *args, **kwargs: [{"sql": "ALTER UNIQUE fake"}],
    )
    monkeypatch.setattr(
        planning,
        "_compile_index_recreate_sql",
        lambda *args, **kwargs: [{"sql": "ALTER INDEX fake"}],
    )

    postgres_unique_db = Ormdantic("postgresql://localhost/app")
    postgres_unique_db._table_map = Map(
        name_to_data={
            "facade": facade_table(
                named_unique_constraints=[
                    TableUnique(name="plain_uq", columns=["id"]),
                    TableUnique(
                        name="include_uq",
                        columns=["name"],
                        postgres_include=["id"],
                    ),
                ],
            )
        }
    )
    postgres_unique_db._create_registered_postgres_unique_options()

    postgres_index_db = Ormdantic("postgresql://localhost/app")
    postgres_index_db._table_map = Map(
        name_to_data={
            "facade": facade_table(
                indexes=[
                    TableIndex(name="plain_idx", columns=["id"]),
                    TableIndex(
                        name="ops_idx",
                        columns=["name"],
                        postgres_ops={"name": "text_pattern_ops"},
                    ),
                ],
            )
        }
    )
    postgres_index_db._create_registered_postgres_index_options()

    mssql_db = Ormdantic("mssql://localhost/app")
    mssql_db._table_map = Map(
        name_to_data={
            "mssql_facade": facade_table(
                "mssql_facade",
                FacadeOtherModel,
                indexes=[
                    TableIndex(name="plain_idx", columns=["id"]),
                    TableIndex(
                        name="mssql_idx",
                        columns=["name"],
                        mssql_filegroup="fastspace",
                    ),
                ],
            )
        }
    )
    mssql_db._create_registered_mssql_index_options()

    oracle_db = Ormdantic("oracle://localhost/app")
    oracle_db._table_map = Map(
        name_to_data={
            "plain_oracle": facade_table(
                "plain_oracle",
                FacadeModel,
                indexes=[TableIndex(name="plain_oracle_idx", columns=["id"])],
            ),
            "oracle_facade": facade_table(
                "oracle_facade",
                FacadeOtherModel,
                indexes=[
                    TableIndex(name="plain_idx", columns=["id"]),
                    TableIndex(
                        name="oracle_idx",
                        columns=["name"],
                        oracle_tablespace="fastspace",
                    ),
                ],
            ),
        }
    )
    oracle_db._create_registered_oracle_index_options()
    executed_sql = [sql for _connection, sql, _params in native.executed]
    assert "ALTER UNIQUE fake" in executed_sql
    assert executed_sql.count("ALTER INDEX fake") >= 2

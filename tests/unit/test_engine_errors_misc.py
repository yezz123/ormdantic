from __future__ import annotations

from types import SimpleNamespace

import pytest
import typer

import ormdantic.engine as engine_module
import ormdantic.naming as naming_module
from ormdantic import cli as root_cli
from ormdantic.association import association_proxy, hybrid_property
from ormdantic.engine import NativeEngine, NativeResult, runtime_capabilities
from ormdantic.errors import (
    REDACTED_VALUE,
    ConfigurationError,
    DatabaseConnectionError,
    MigrationError,
    NativeExtensionError,
    QueryCompilationError,
    QueryExecutionError,
    ReflectionError,
    SchemaError,
    TransactionError,
    TypeConversionError,
    classify_native_error,
    is_sensitive_parameter,
    raise_with_context,
    redact_parameter_values,
)
from ormdantic.events import EventRegistry
from ormdantic.loaders import (
    LoaderOption,
    install_relationship_path_descriptor,
    lazy,
    lazyload,
    load,
    loader_depth,
    noload,
    path_parts,
    selectin,
    selectinload,
)
from ormdantic.naming import _split_words_on_regex, get_words, snake_case


class FakeConnection:
    fail_on: str | None = None
    instances: list["FakeConnection"] = []

    def __init__(self, url: str) -> None:
        if self.fail_on == "connect":
            raise RuntimeError("could not connect to backend")
        self.url = url
        self.calls: list[tuple[str, object]] = []
        self.instances.append(self)

    def execute(self, sql: str, values: list[object]) -> dict[str, object]:
        self.calls.append(("execute", sql, tuple(values)))
        if self.fail_on == "execute":
            raise RuntimeError("unsupported filter compile failed")
        return {"columns": ["id", "name"], "rows": [(1, "dark")]}

    def begin(self) -> None:
        self.calls.append(("begin", None))
        if self.fail_on == "begin":
            raise RuntimeError("transaction error: begin")

    def commit(self) -> None:
        self.calls.append(("commit", None))
        if self.fail_on == "commit":
            raise RuntimeError("transaction error: commit")

    def rollback(self) -> None:
        self.calls.append(("rollback", None))
        if self.fail_on == "rollback":
            raise RuntimeError("savepoint rollback failed")


@pytest.fixture(autouse=True)
def reset_fake_connection() -> None:
    FakeConnection.fail_on = None
    FakeConnection.instances = []


def install_fake_native(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_native = SimpleNamespace(
        PyNativeConnection=FakeConnection,
        runtime_capabilities=lambda: {"sqlite": True, "postgresql": False},
    )
    monkeypatch.setattr(engine_module, "_ormdantic", fake_native)


def test_runtime_capabilities_and_native_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(engine_module, "_ormdantic", None)
    unavailable = runtime_capabilities()
    assert unavailable
    assert set(unavailable.values()) == {False}

    install_fake_native(monkeypatch)
    assert runtime_capabilities() == {"sqlite": True, "postgresql": False}

    result = NativeResult(["id", "name"], [(1, "dark"), (2, "light")])
    assert result.cursor.description == [("id",), ("name",)]
    assert list(result) == [(1, "dark"), (2, "light")]
    assert result.scalar() == 1
    assert NativeResult(["id"], []).scalar() is None
    assert NativeResult(["id"], [()]).scalar() is None


def test_native_engine_initialization_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(engine_module, "_ormdantic", object())
    with pytest.raises(NativeExtensionError):
        NativeEngine("sqlite:///db.sqlite3")

    install_fake_native(monkeypatch)
    FakeConnection.fail_on = "connect"
    with pytest.raises(DatabaseConnectionError) as exc_info:
        NativeEngine("postgres://localhost/db")
    assert exc_info.value.context["operation"] == "connect"
    assert exc_info.value.context["backend"] == "postgresql"


@pytest.mark.asyncio
async def test_native_engine_executes_and_manages_transactions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_fake_native(monkeypatch)
    engine = NativeEngine("sqlite:///db.sqlite3")

    result = await engine.execute("SELECT id, name FROM flavor WHERE id = ?", (1,))
    assert result.scalar() == 1
    assert FakeConnection.instances[-1].calls[0] == (
        "execute",
        "SELECT id, name FROM flavor WHERE id = ?",
        (1,),
    )

    async with engine.transaction():
        pass
    assert ("begin", None) in FakeConnection.instances[-1].calls
    assert ("commit", None) in FakeConnection.instances[-1].calls

    with pytest.raises(RuntimeError):
        async with engine.transaction():
            raise RuntimeError("boom")
    assert ("rollback", None) in FakeConnection.instances[-1].calls


@pytest.mark.asyncio
async def test_native_engine_wraps_execution_and_transaction_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_fake_native(monkeypatch)

    FakeConnection.fail_on = "execute"
    engine = NativeEngine("sqlite:///db.sqlite3")
    with pytest.raises(QueryCompilationError) as execute_error:
        await engine.execute("SELECT * FROM t", ())
    assert execute_error.value.context["operation"] == "execute"

    for operation, method_name in [
        ("begin", "begin"),
        ("commit", "commit"),
        ("rollback", "rollback"),
    ]:
        FakeConnection.fail_on = operation
        failing_engine = NativeEngine("sqlite:///db.sqlite3")
        with pytest.raises(TransactionError) as exc_info:
            await getattr(failing_engine, method_name)()
        assert exc_info.value.context["operation"] == operation


def test_error_context_redaction_and_classification() -> None:
    assert is_sensitive_parameter("api-key")
    assert not is_sensitive_parameter("flavor")
    assert redact_parameter_values(None) is None
    assert redact_parameter_values({"password": "secret", "name": "dark"}) == {
        "password": REDACTED_VALUE,
        "name": "dark",
    }
    assert redact_parameter_values(
        ["secret", "dark"], bind_names=["token", "name"]
    ) == {
        "token": REDACTED_VALUE,
        "name": "dark",
    }
    assert redact_parameter_values(("secret", "dark")) == ["secret", "dark"]

    existing = QueryExecutionError("failed")
    assert (
        raise_with_context(
            existing, QueryExecutionError, "ignored", context={"table": "t"}
        )
        is existing
    )
    assert existing.context["table"] == "t"
    converted = raise_with_context(ValueError("bad"), SchemaError, "schema failed")
    assert isinstance(converted, SchemaError)
    assert converted.native_error_type == "ValueError"

    cases = [
        (
            RuntimeError("network down"),
            {},
            DatabaseConnectionError,
            "database connection failed",
        ),
        (
            RuntimeError("transaction error"),
            {},
            TransactionError,
            "transaction operation failed",
        ),
        (RuntimeError("reflection error"), {}, ReflectionError, "reflection failed"),
        (RuntimeError("migration error"), {}, MigrationError, "migration failed"),
        (RuntimeError("schema diff error"), {}, SchemaError, "schema operation failed"),
        (
            RuntimeError("unsupported filter"),
            {},
            QueryCompilationError,
            "QueryCompilationError",
        ),
        (
            RuntimeError("boom"),
            {"operation": "apply", "table": "flavor", "backend": "sqlite"},
            QueryExecutionError,
            "apply failed for table 'flavor' on sqlite",
        ),
    ]
    for error, context, expected_type, expected_message in cases:
        classified = classify_native_error(error, context=context)
        assert isinstance(classified, expected_type)
        assert expected_message in str(classified)

    native_error = MigrationError("native migration")
    assert (
        classify_native_error(native_error, context={"operation": "upgrade"})
        is native_error
    )
    assert native_error.context["operation"] == "upgrade"
    assert isinstance(
        classify_native_error(RuntimeError("boom"), default=ReflectionError),
        ReflectionError,
    )


def test_configuration_error_messages_are_stable() -> None:
    assert str(ConfigurationError("bad config")) == "bad config"
    assert "not supported" in str(TypeConversionError(bytes))


def test_association_proxy_and_hybrid_descriptors() -> None:
    class Related:
        def __init__(self, name: str) -> None:
            self.name = name

    class Owner:
        child_name = association_proxy("child", "name")
        children_names = association_proxy("children", "name")

        def __init__(self) -> None:
            self.child = Related("dark")
            self.children = [Related("dark"), Related("light")]

        @hybrid_property
        def label(self) -> str:
            return self.child.name.upper()

    @Owner.child_name.expression
    def child_name_expr(owner: type[Owner]) -> str:
        return owner.__name__

    @Owner.label.expression
    def label_expr(owner: type[Owner]) -> str:
        return f"{owner.__name__}.label"

    owner = Owner()
    assert Owner.child_name == "Owner"
    assert owner.child_name == "dark"
    owner.child_name = "medium"
    assert owner.child.name == "medium"
    assert owner.children_names == ["dark", "light"]
    with pytest.raises(TypeError):
        owner.children_names = "invalid"
    assert owner.label == "MEDIUM"
    assert Owner.label == "Owner.label"


def test_naming_helpers_cover_regex_splits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        naming_module,
        "_ormdantic",
        SimpleNamespace(snake_case=lambda value: f"snake::{value.lower()}"),
    )
    assert snake_case("CamelCase") == "snake::camelcase"
    assert get_words("HTTPServer2Port") == ["HTTP", "Server2", "Port"]
    assert _split_words_on_regex(["abcDef"], r"(?<=[a-z])(?=[A-Z])") == [
        "abc",
        "Def",
    ]


def test_loader_options_descriptors_and_aliases() -> None:
    class Model:
        pass

    install_relationship_path_descriptor(Model, "children")
    path = Model.children.parent
    assert str(path) == "children.parent"
    assert path.path == "children.parent"
    with pytest.raises(AttributeError):
        _ = path._private

    descriptor = Model.__dict__["children"]
    instance = Model()
    instance.children = ["loaded"]
    assert descriptor.__get__(instance, Model) == ["loaded"]
    missing = Model()
    with pytest.raises(AttributeError):
        descriptor.__get__(missing, Model)

    joined = load(path)
    filtered = joined.filter(active=True)
    sorted_option = filtered.sorted_by("name")
    batched = selectinload("children/parent").batched(10)
    assert joined.strategy == "joined"
    assert filtered.filter_by == {"active": True}
    assert sorted_option.order_by == ("name",)
    assert batched.path == "children.parent"
    assert batched.batch_size == 10
    assert lazyload("children").strategy == "lazy"
    assert noload("children").strategy == "noload"
    assert lazy("children").strategy == "lazy"
    assert selectin("children").strategy == "selectin"
    assert loader_depth(None) == 0
    assert loader_depth([joined, lazyload("children"), noload("children")]) == 2
    with pytest.raises(ValueError):
        LoaderOption("children", "selectin", batch_size=0)
    with pytest.raises(ValueError):
        path_parts("")


@pytest.mark.asyncio
async def test_event_registry_clear_off_and_async_dispatch() -> None:
    registry = EventRegistry()
    seen: list[tuple[str, int]] = []

    def sync_handler(value: int) -> None:
        seen.append(("sync", value))

    async def async_handler(value: int) -> None:
        seen.append(("async", value))

    assert registry.on("created", sync_handler) is sync_handler
    registry.on("created", async_handler)
    assert registry.has_handlers("created")
    await registry.dispatch("created", value=3)
    assert seen == [("sync", 3), ("async", 3)]

    registry.off("created", sync_handler)
    seen.clear()
    await registry.dispatch("created", value=4)
    assert seen == [("async", 4)]
    registry.clear("created")
    assert not registry.has_handlers("created")
    registry.clear()


def test_root_cli_main_maps_runtime_exceptions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(root_cli, "app", lambda **_kwargs: 7)
    assert root_cli.main(["anything"]) == 7

    monkeypatch.setattr(root_cli, "app", lambda **_kwargs: None)
    assert root_cli.main(["anything"]) == 0

    def aborting_app(**_kwargs):
        raise typer.Abort()

    def exiting_app(**_kwargs):
        raise typer.Exit(3)

    def failing_app(**_kwargs):
        raise ValueError("bad cli")

    monkeypatch.setattr(root_cli, "app", aborting_app)
    assert root_cli.main(["anything"]) == 1
    monkeypatch.setattr(root_cli, "app", exiting_app)
    assert root_cli.main(["anything"]) == 3
    monkeypatch.setattr(root_cli, "app", failing_app)
    assert root_cli.main(["anything"]) == 1

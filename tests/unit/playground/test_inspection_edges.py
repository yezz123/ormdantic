from __future__ import annotations

import asyncio
import io
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from ormdantic import Ormdantic
from ormdantic.playground import inspect_worker, inspection, launcher


class Process:
    def __init__(
        self,
        payload: Any = None,
        *,
        hang_wait_once: bool = False,
        returncode: int = 0,
    ) -> None:
        self.payload = payload or {}
        self.returncode = returncode
        self.terminated = False
        self.killed = False
        self.hang_wait_once = hang_wait_once

    async def communicate(self) -> tuple[bytes, bytes]:
        return json.dumps(self.payload).encode(), b"worker warning"

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        if self.hang_wait_once:
            self.hang_wait_once = False
            await asyncio.Event().wait()
        return self.returncode


def test_worker_loads_targets_and_returns_success_or_safe_errors(monkeypatch) -> None:
    database = Ormdantic("sqlite:///:memory:")
    module = SimpleNamespace(db=database, other=object())
    monkeypatch.setattr(inspect_worker.importlib, "import_module", lambda _name: module)

    success = inspect_worker.inspect_target("app:db")
    invalid = inspect_worker.inspect_target("missing-colon")
    wrong_type = inspect_worker.inspect_target("app:other")

    assert success["ok"] is True
    assert invalid["error"]["type"] == "ValueError"
    assert wrong_type["error"]["type"] == "TypeError"


def test_worker_captures_import_output_and_main_protocol(monkeypatch) -> None:
    database = Ormdantic("sqlite:///:memory:")

    def import_with_output(_name: str) -> SimpleNamespace:
        print("model stdout")
        print("model stderr", file=inspect_worker.sys.stderr)
        return SimpleNamespace(db=database)

    monkeypatch.setattr(inspect_worker.importlib, "import_module", import_with_output)
    result = inspect_worker.inspect_target("app:db")
    assert result["diagnostics"][0]["code"] == "model.import_output"
    assert "model stdout" in result["diagnostics"][0]["message"]

    output = io.StringIO()
    monkeypatch.setattr(inspect_worker.sys, "__stdout__", output)
    assert inspect_worker.main([]) == 0
    assert json.loads(output.getvalue())["error"]["type"] == "ValueError"

    output.seek(0)
    output.truncate()
    monkeypatch.setattr(inspect_worker.sys, "argv", ["worker", "app:db"])
    assert inspect_worker.main() == 0
    assert json.loads(output.getvalue())["ok"] is True


async def test_termination_escalates_to_kill_when_process_ignores_terminate() -> None:
    process = Process(hang_wait_once=True)

    await inspection._terminate_process(process)

    assert process.terminated is True
    assert process.killed is True


@pytest.mark.parametrize(
    "payload",
    [
        ["not", "an", "object"],
        {"protocol": 2, "ok": True, "snapshot": {}},
        {"protocol": 1, "ok": True},
        {"protocol": 1, "ok": False, "error": None},
        {"protocol": 1, "ok": True, "snapshot": {}, "diagnostics": {}},
        {"protocol": 1, "ok": True, "snapshot": {}, "diagnostics": [1]},
    ],
)
async def test_inspection_rejects_invalid_protocol_shapes(
    payload: Any,
    tmp_path: Path,
) -> None:
    async def spawn(*_args: Any, **_kwargs: Any) -> Process:
        return Process(payload)

    result = await inspection.inspect_models(
        "app:db",
        cwd=tmp_path,
        process_factory=spawn,
    )

    assert result.snapshot is None
    assert result.diagnostics[0].code == "model.protocol_error"
    assert "worker warning" in result.diagnostics[0].message


async def test_inspection_normalizes_worker_diagnostics(tmp_path: Path) -> None:
    payload = {
        "protocol": 1,
        "ok": True,
        "snapshot": {"version": 1, "tables": []},
        "diagnostics": [
            {
                "severity": "unknown",
                "code": "model.note",
                "message": "loaded",
                "source": "models.py",
                "hint": "review",
            }
        ],
    }

    async def spawn(*_args: Any, **_kwargs: Any) -> Process:
        return Process(payload)

    result = await inspection.inspect_models(
        "app:db",
        cwd=tmp_path,
        process_factory=spawn,
    )

    diagnostic = result.diagnostics[0]
    assert diagnostic.severity.value == "info"
    assert diagnostic.source == "models.py"
    assert diagnostic.hint == "review"


async def test_worker_exit_without_stderr_gets_a_diagnostic(tmp_path: Path) -> None:
    async def spawn(*_args: Any, **_kwargs: Any) -> Process:
        process = Process(returncode=7)

        async def communicate() -> tuple[bytes, bytes]:
            return b"", b""

        process.communicate = communicate  # type: ignore[method-assign]
        return process

    result = await inspection.inspect_models(
        "app:db",
        cwd=tmp_path,
        process_factory=spawn,
    )

    assert "exited with 7" in result.diagnostics[0].message


def test_launcher_runs_app_and_maps_only_missing_textual(monkeypatch) -> None:
    calls: list[object] = []

    class App:
        @classmethod
        def from_cli(cls, **kwargs: object) -> App:
            calls.append(kwargs)
            return cls()

        def run(self) -> None:
            calls.append("run")

    monkeypatch.setattr(
        launcher,
        "import_module",
        lambda _name: SimpleNamespace(PlaygroundApp=App),
    )
    launcher.run_playground(
        config_path=None,
        environment="development",
        target=None,
        migrations_dir=None,
    )
    assert calls[-1] == "run"

    def missing_textual(_name: str) -> None:
        error = ImportError("missing textual")
        error.name = "textual.widgets"
        raise error

    monkeypatch.setattr(launcher, "import_module", missing_textual)
    with pytest.raises(launcher.PlaygroundDependencyError, match="playground"):
        launcher.run_playground(
            config_path=None,
            environment=None,
            target=None,
            migrations_dir=None,
        )

    def unrelated(_name: str) -> None:
        error = ImportError("missing project")
        error.name = "project"
        raise error

    monkeypatch.setattr(launcher, "import_module", unrelated)
    with pytest.raises(ImportError, match="missing project"):
        launcher.run_playground(
            config_path=None,
            environment=None,
            target=None,
            migrations_dir=None,
        )

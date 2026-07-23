from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from ormdantic.playground.inspection import inspect_models

FIXTURE_PROJECT = (
    Path(__file__).resolve().parents[2] / "fixtures" / "playground_project"
)


async def test_inspect_models_returns_registered_schema_from_isolated_worker() -> None:
    result = await inspect_models("app:db", cwd=FIXTURE_PROJECT)

    assert result.snapshot is not None
    assert [table.name for table in result.snapshot.tables] == ["users"]
    assert result.error is None
    assert any(item.code == "model.import_output" for item in result.diagnostics)
    assert "fixture import output" in result.diagnostics[0].message


async def test_inspect_models_returns_actionable_import_error() -> None:
    result = await inspect_models("missing_module:db", cwd=FIXTURE_PROJECT)

    assert result.snapshot is None
    assert result.error is not None
    assert result.error.type == "ModuleNotFoundError"
    assert result.diagnostics[0].code == "model.import_failed"


async def test_inspect_models_rejects_missing_attribute() -> None:
    result = await inspect_models("app:missing", cwd=FIXTURE_PROJECT)

    assert result.snapshot is None
    assert result.error is not None
    assert result.error.type == "AttributeError"


async def test_inspect_models_rejects_non_database_target() -> None:
    result = await inspect_models("app:not_a_database", cwd=FIXTURE_PROJECT)

    assert result.snapshot is None
    assert result.error is not None
    assert result.error.type == "TypeError"
    assert "Ormdantic" in result.error.message


class FakeProcess:
    def __init__(
        self,
        *,
        stdout: bytes = b"",
        stderr: bytes = b"",
        returncode: int = 0,
        hangs: bool = False,
    ) -> None:
        self._stdout = stdout
        self._stderr = stderr
        self.returncode = returncode
        self.hangs = hangs
        self.terminated = False
        self.killed = False

    async def communicate(self) -> tuple[bytes, bytes]:
        if self.hangs:
            await asyncio.Event().wait()
        return self._stdout, self._stderr

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    async def wait(self) -> int:
        return self.returncode


async def test_inspect_models_handles_malformed_worker_output(tmp_path: Path) -> None:
    process = FakeProcess(stdout=b"not json")

    async def spawn(*args: Any, **kwargs: Any) -> FakeProcess:
        return process

    result = await inspect_models("app:db", cwd=tmp_path, process_factory=spawn)

    assert result.snapshot is None
    assert result.diagnostics[0].code == "model.protocol_error"


async def test_inspect_models_handles_nonzero_worker_exit(tmp_path: Path) -> None:
    process = FakeProcess(stderr=b"worker crashed", returncode=3)

    async def spawn(*args: Any, **kwargs: Any) -> FakeProcess:
        return process

    result = await inspect_models("app:db", cwd=tmp_path, process_factory=spawn)

    assert result.snapshot is None
    assert result.diagnostics[0].code == "model.worker_failed"
    assert "worker crashed" in result.diagnostics[0].message


async def test_inspect_models_terminates_a_timed_out_worker(tmp_path: Path) -> None:
    process = FakeProcess(hangs=True)

    async def spawn(*args: Any, **kwargs: Any) -> FakeProcess:
        return process

    result = await inspect_models(
        "app:db",
        cwd=tmp_path,
        timeout=0.01,
        process_factory=spawn,
    )

    assert result.snapshot is None
    assert result.diagnostics[0].code == "model.timeout"
    assert process.terminated is True


async def test_inspect_models_terminates_worker_when_caller_is_cancelled(
    tmp_path: Path,
) -> None:
    process = FakeProcess(hangs=True)

    async def spawn(*args: Any, **kwargs: Any) -> FakeProcess:
        return process

    task = asyncio.create_task(
        inspect_models("app:db", cwd=tmp_path, process_factory=spawn)
    )
    await asyncio.sleep(0)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    assert process.terminated is True


async def test_inspect_models_accepts_versioned_protocol_payload(
    tmp_path: Path,
) -> None:
    payload = {
        "protocol": 1,
        "ok": True,
        "snapshot": {"version": 1, "tables": []},
        "diagnostics": [],
    }
    process = FakeProcess(stdout=json.dumps(payload).encode())

    async def spawn(*args: Any, **kwargs: Any) -> FakeProcess:
        return process

    result = await inspect_models("app:db", cwd=tmp_path, process_factory=spawn)

    assert result.snapshot is not None
    assert result.snapshot.tables == []

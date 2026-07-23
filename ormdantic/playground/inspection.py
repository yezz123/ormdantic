"""Async client for isolated model inspection."""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from ormdantic.migrations import SchemaSnapshot
from ormdantic.playground.diagnostics import Diagnostic, Severity

PROTOCOL_VERSION = 1


class ProcessLike(Protocol):
    """Subset of asyncio subprocess behavior used by inspection."""

    returncode: int | None

    async def communicate(self) -> tuple[bytes, bytes]: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    async def wait(self) -> int: ...


ProcessFactory = Callable[..., Awaitable[ProcessLike]]


@dataclass(frozen=True)
class InspectionError:
    """Serializable worker error."""

    type: str
    message: str


@dataclass(frozen=True)
class InspectionResult:
    """Snapshot or error returned from a model-inspection worker."""

    snapshot: SchemaSnapshot | None
    diagnostics: tuple[Diagnostic, ...] = ()
    error: InspectionError | None = None


async def inspect_models(
    target: str,
    *,
    cwd: Path,
    timeout: float = 15.0,
    process_factory: ProcessFactory | None = None,
) -> InspectionResult:
    """Inspect models in a subprocess without polluting the TUI process."""
    spawn = process_factory or cast(ProcessFactory, asyncio.create_subprocess_exec)
    process = await spawn(
        sys.executable,
        "-m",
        "ormdantic.playground.inspect_worker",
        target,
        cwd=str(cwd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
    except asyncio.CancelledError:
        await asyncio.shield(_terminate_process(process))
        raise
    except asyncio.TimeoutError:
        await _terminate_process(process)
        message = f"Model inspection exceeded {timeout:g} seconds"
        return InspectionResult(
            snapshot=None,
            error=InspectionError("TimeoutError", message),
            diagnostics=(
                Diagnostic.create(
                    Severity.ERROR,
                    "model.timeout",
                    message,
                    hint="Fix slow import-time work or increase the inspection timeout.",
                ),
            ),
        )

    stdout_text = stdout.decode("utf-8", errors="replace").strip()
    stderr_text = stderr.decode("utf-8", errors="replace").strip()
    if process.returncode not in {0, None}:
        message = stderr_text or f"Inspection worker exited with {process.returncode}"
        return InspectionResult(
            snapshot=None,
            error=InspectionError("WorkerError", message),
            diagnostics=(
                Diagnostic.create(
                    Severity.ERROR,
                    "model.worker_failed",
                    message,
                    details={"returncode": process.returncode},
                ),
            ),
        )
    try:
        payload = json.loads(stdout_text)
        if not isinstance(payload, Mapping):
            raise TypeError("worker payload must be a JSON object")
        if payload.get("protocol") != PROTOCOL_VERSION:
            raise ValueError(f"unsupported worker protocol {payload.get('protocol')!r}")
        diagnostics = _diagnostics(payload.get("diagnostics", []))
        if payload.get("ok") is True:
            snapshot_payload = payload.get("snapshot")
            if not isinstance(snapshot_payload, Mapping):
                raise TypeError("successful worker payload has no snapshot")
            return InspectionResult(
                snapshot=SchemaSnapshot.from_dict(snapshot_payload),
                diagnostics=diagnostics,
            )
        error = _inspection_error(payload.get("error"))
        return InspectionResult(
            snapshot=None,
            diagnostics=(
                Diagnostic.create(
                    Severity.ERROR,
                    "model.import_failed",
                    f"{error.type}: {error.message}",
                    hint="Fix the target or its import error, then refresh.",
                ),
                *diagnostics,
            ),
            error=error,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        message = f"Invalid model-inspection response: {exc}"
        if stderr_text:
            message = f"{message}. Worker stderr: {stderr_text}"
        return InspectionResult(
            snapshot=None,
            error=InspectionError(type(exc).__name__, str(exc)),
            diagnostics=(
                Diagnostic.create(
                    Severity.ERROR,
                    "model.protocol_error",
                    message,
                    hint="Reinstall matching Ormdantic package versions.",
                ),
            ),
        )


async def _terminate_process(process: ProcessLike) -> None:
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), 0.5)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()


def _inspection_error(value: Any) -> InspectionError:
    if not isinstance(value, Mapping):
        raise TypeError("failed worker payload has no error object")
    return InspectionError(
        type=str(value.get("type", "ImportError")),
        message=str(value.get("message", "Model import failed")),
    )


def _diagnostics(value: Any) -> tuple[Diagnostic, ...]:
    if not isinstance(value, list):
        raise TypeError("worker diagnostics must be an array")
    diagnostics: list[Diagnostic] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise TypeError("worker diagnostic must be an object")
        severity_value = str(item.get("severity", "info"))
        try:
            severity = Severity(severity_value)
        except ValueError:
            severity = Severity.INFO
        diagnostics.append(
            Diagnostic.create(
                severity,
                str(item.get("code", "model.worker")),
                str(item.get("message", "")),
                source=(
                    str(item["source"]) if item.get("source") is not None else None
                ),
                hint=str(item["hint"]) if item.get("hint") is not None else None,
            )
        )
    return tuple(diagnostics)

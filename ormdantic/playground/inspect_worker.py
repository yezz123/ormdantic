"""Short-lived worker that snapshots a project's registered models."""

from __future__ import annotations

import importlib
import io
import json
import sys
from collections.abc import Sequence
from contextlib import redirect_stderr, redirect_stdout
from typing import Any

from ormdantic import Ormdantic

PROTOCOL_VERSION = 1


def inspect_target(target: str) -> dict[str, Any]:
    """Import one ``module:object`` target and return a protocol payload."""
    stdout = io.StringIO()
    stderr = io.StringIO()
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            database = _load_database(target)
            snapshot = database.migrations.snapshot().to_dict()
    except Exception as exc:
        return {
            "protocol": PROTOCOL_VERSION,
            "ok": False,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
            "diagnostics": _output_diagnostics(stdout.getvalue(), stderr.getvalue()),
        }
    return {
        "protocol": PROTOCOL_VERSION,
        "ok": True,
        "snapshot": snapshot,
        "diagnostics": _output_diagnostics(stdout.getvalue(), stderr.getvalue()),
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Run the inspection protocol on stdout."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 1:
        payload = {
            "protocol": PROTOCOL_VERSION,
            "ok": False,
            "error": {
                "type": "ValueError",
                "message": "expected exactly one module:object target",
            },
            "diagnostics": [],
        }
    else:
        payload = inspect_target(arguments[0])
    print(json.dumps(payload, sort_keys=True), file=sys.__stdout__, flush=True)
    return 0


def _load_database(target: str) -> Ormdantic:
    module_name, separator, object_name = target.partition(":")
    if not separator or not module_name or not object_name:
        raise ValueError("target must use module:object syntax")
    module = importlib.import_module(module_name)
    database = getattr(module, object_name)
    if not isinstance(database, Ormdantic):
        raise TypeError(f"{target} does not resolve to an Ormdantic database")
    return database


def _output_diagnostics(stdout: str, stderr: str) -> list[dict[str, str]]:
    diagnostics: list[dict[str, str]] = []
    combined = "\n".join(part.strip() for part in (stdout, stderr) if part.strip())
    if combined:
        diagnostics.append(
            {
                "severity": "info",
                "code": "model.import_output",
                "message": combined,
                "source": "model import",
            }
        )
    return diagnostics


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

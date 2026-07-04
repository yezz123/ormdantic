"""Native extension loading and diagnostics."""

from __future__ import annotations

import importlib
from collections.abc import Iterable
from types import ModuleType
from typing import NoReturn

from ormdantic.errors import NativeExtensionError

NATIVE_EXTENSION_MODULE = "ormdantic._ormdantic"
RUNTIME_CAPABILITY_KEYS = (
    "sqlite",
    "postgresql",
    "mysql",
    "mariadb",
    "mssql",
    "oracle",
)


def unavailable_runtime_capabilities() -> dict[str, bool]:
    """Return the diagnostic shape used when the extension is unavailable."""
    return dict.fromkeys(RUNTIME_CAPABILITY_KEYS, False)


def import_native_extension(
    *,
    context: str,
    required_symbols: Iterable[str] = (),
) -> ModuleType:
    """Import ``ormdantic._ormdantic`` and verify required symbols exist."""
    symbols = tuple(required_symbols)
    try:
        module = importlib.import_module(NATIVE_EXTENSION_MODULE)
    except (ImportError, OSError) as exc:
        _raise_native_extension_error(context, symbols, cause=exc)

    missing = [symbol for symbol in symbols if not hasattr(module, symbol)]
    if missing:
        _raise_native_extension_error(context, missing)
    return module


def raise_native_extension_unavailable(
    *,
    context: str,
    required_symbols: Iterable[str] = (),
    cause: BaseException | None = None,
) -> NoReturn:
    """Raise an actionable error for delayed native-extension requirements."""
    _raise_native_extension_error(context, tuple(required_symbols), cause=cause)


def _raise_native_extension_error(
    context: str,
    missing_symbols: Iterable[str],
    *,
    cause: BaseException | None = None,
) -> NoReturn:
    missing = list(missing_symbols)
    message = _native_extension_error_message(
        context=context,
        missing_symbols=missing,
        cause=cause,
    )
    raise NativeExtensionError(
        message,
        context={
            "extension_module": NATIVE_EXTENSION_MODULE,
            "context": context,
            "missing_symbols": missing,
        },
        cause=cause,
    )


def _native_extension_error_message(
    *,
    context: str,
    missing_symbols: list[str],
    cause: BaseException | None,
) -> str:
    if cause is None:
        reason = "The loaded extension is incomplete"
    else:
        reason = "The extension could not be imported"

    missing = ""
    if missing_symbols:
        missing = f"; missing required symbol(s): {', '.join(missing_symbols)}"
    return (
        f"Ormdantic requires the Rust extension `{NATIVE_EXTENSION_MODULE}` for "
        f"{context}. {reason}{missing}. Install a wheel compatible with this "
        "Python version and platform, or rebuild from a source checkout with "
        "`uv sync --group dev` and `uv run --group dev maturin develop`. "
        "If this started after changing Python versions or upgrading Ormdantic, "
        "run `pip install --force-reinstall ormdantic`."
    )

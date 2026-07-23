"""Structured, secret-safe diagnostics for the playground."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Severity(str, Enum):
    """Diagnostic importance."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


_CREDENTIAL_URL = re.compile(
    r"(?P<scheme>[a-z][a-z0-9+.-]*://)"
    r"(?P<user>[^\s/:@]+):(?P<password>[^\s/@]+)@",
    re.IGNORECASE,
)
_SECRET_PARAMETER = re.compile(
    r"(?P<prefix>[?&](?:access_token|api_key|password|secret|token)=)"
    r"[^&#\s]+",
    re.IGNORECASE,
)
_SECRET_KEYS = {
    "access_token",
    "api_key",
    "database_url",
    "password",
    "secret",
    "token",
}


def redact_text(value: str) -> str:
    """Redact credentials embedded in URLs or secret query parameters."""
    value = _CREDENTIAL_URL.sub(
        lambda match: f"{match.group('scheme')}{match.group('user')}:<redacted>@",
        value,
    )
    return _SECRET_PARAMETER.sub(
        lambda match: f"{match.group('prefix')}<redacted>",
        value,
    )


def redact_value(value: Any, *, key: str | None = None) -> Any:
    """Recursively redact values carried by diagnostics and logs."""
    if key is not None and key.casefold() in _SECRET_KEYS:
        return "<redacted>"
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        return {
            str(item_key): redact_value(item, key=str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        return tuple(redact_value(item) for item in value)
    return value


@dataclass(frozen=True)
class Diagnostic:
    """One actionable and already-redacted diagnostic."""

    severity: Severity
    code: str
    message: str
    source: str | None = None
    hint: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        severity: Severity,
        code: str,
        message: str,
        *,
        source: str | None = None,
        hint: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> Diagnostic:
        """Construct a diagnostic after removing secret values."""
        return cls(
            severity=severity,
            code=code,
            message=redact_text(message),
            source=redact_text(source) if source is not None else None,
            hint=redact_text(hint) if hint is not None else None,
            details=redact_value(details or {}),
        )

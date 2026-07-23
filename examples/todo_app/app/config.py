"""Validated runtime configuration for the Todo example."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

Environment = Literal["development", "test", "production"]
_ENVIRONMENTS = {"development", "test", "production"}
_DEVELOPMENT_DATABASE_URL = "sqlite:///todo-dev.sqlite3"
_SENSITIVE_QUERY_KEYS = {
    "access_token",
    "api_key",
    "passwd",
    "password",
    "secret",
    "token",
}


class ConfigurationError(ValueError):
    """Raised when required application configuration is invalid or absent."""


@dataclass(frozen=True)
class Settings:
    """Validated, immutable application settings."""

    environment: Environment
    database_url: str = field(repr=False)

    @property
    def safe_database_url(self) -> str:
        """Return a diagnostic URL with credentials and query secrets removed."""
        parsed = urlsplit(self.database_url)
        netloc = parsed.netloc
        authority_redacted = False
        if "@" in netloc:
            _credentials, host = netloc.rsplit("@", 1)
            netloc = f"***@{host}"
            authority_redacted = True

        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        query_redacted = any(
            key.casefold() in _SENSITIVE_QUERY_KEYS for key, _value in query_pairs
        )
        if not authority_redacted and not query_redacted:
            return self.database_url

        query = parsed.query
        if query_redacted:
            query = urlencode(
                [
                    (
                        key,
                        "***" if key.casefold() in _SENSITIVE_QUERY_KEYS else value,
                    )
                    for key, value in query_pairs
                ],
                safe="*",
            )
        safe_url = urlunsplit(parsed._replace(netloc=netloc, query=query))
        scheme_prefix = f"{parsed.scheme}://"
        if (
            not parsed.netloc
            and self.database_url.startswith(scheme_prefix)
            and not safe_url.startswith(scheme_prefix)
        ):
            safe_url = scheme_prefix + safe_url[len(parsed.scheme) + 1 :]
        return safe_url


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    """Load and validate settings from a supplied mapping or the process environment."""
    source = os.environ if environ is None else environ
    raw_environment = source.get("APP_ENV", "development")
    normalized_environment = raw_environment.strip().casefold()
    if normalized_environment not in _ENVIRONMENTS:
        raise ConfigurationError(
            f"APP_ENV must be development, test, or production; got {raw_environment!r}"
        )

    database_url = source.get("DATABASE_URL", "").strip()
    if not database_url:
        if normalized_environment == "development":
            database_url = _DEVELOPMENT_DATABASE_URL
        else:
            raise ConfigurationError(
                f"DATABASE_URL is required when APP_ENV={normalized_environment}"
            )

    return Settings(
        environment=cast(Environment, normalized_environment),
        database_url=database_url,
    )

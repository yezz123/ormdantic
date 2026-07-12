from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit, urlunsplit

SQLITE_SENTINEL_URL = "sqlite:///:benchmark:"


@dataclass(frozen=True)
class BackendDefinition:
    """Resolved benchmark backend connection details."""

    name: str
    url: str
    sqlalchemy_url: str
    redacted_url: str
    docker_image: str | None = None


def resolve_backend(
    backend: str,
    *,
    env: Mapping[str, str] | None = None,
) -> BackendDefinition:
    """Resolve a backend URL from benchmark env vars, test env vars, or defaults."""
    source = env if env is not None else os.environ
    normalized = _normalize_backend(backend)
    if normalized == "sqlite":
        return BackendDefinition(
            name="sqlite",
            url=SQLITE_SENTINEL_URL,
            sqlalchemy_url="sqlite+aiosqlite:///:benchmark:",
            redacted_url=SQLITE_SENTINEL_URL,
        )
    if normalized == "postgres":
        url = (
            source.get("ORMDANTIC_BENCH_POSTGRES_URL")
            or source.get("ORMDANTIC_TEST_POSTGRES_URL")
            or "postgresql://postgres:postgres@localhost:5432/postgres"
        )
        return BackendDefinition(
            name="postgres",
            url=url,
            sqlalchemy_url=_sqlalchemy_url(url, "postgresql+asyncpg"),
            redacted_url=redact_url(url),
            docker_image="postgres:16",
        )
    if normalized == "mysql":
        url = (
            source.get("ORMDANTIC_BENCH_MYSQL_URL")
            or source.get("ORMDANTIC_TEST_MYSQL_URL")
            or "mysql://root:mysql@localhost:3306/mysql"
        )
        return BackendDefinition(
            name="mysql",
            url=url,
            sqlalchemy_url=_sqlalchemy_url(url, "mysql+aiomysql"),
            redacted_url=redact_url(url),
            docker_image="mysql:8",
        )
    raise ValueError(f"unsupported benchmark backend: {backend}")


def redact_url(url: str) -> str:
    """Mask URL passwords while preserving backend, user, host, path, and query."""
    split = urlsplit(url)
    query = urlencode(
        [
            (key, "***" if _is_sensitive_query_key(key) else value)
            for key, value in parse_qsl(split.query, keep_blank_values=True)
        ],
        safe="*",
    )
    if split.password is None:
        if query == split.query:
            return url
        return urlunsplit(
            SplitResult(
                scheme=split.scheme,
                netloc=split.netloc,
                path=split.path,
                query=query,
                fragment=split.fragment,
            )
        )
    username = split.username or ""
    host = split.hostname or ""
    auth = f"{username}:***"
    if split.port is not None:
        host = f"{host}:{split.port}"
    netloc = f"{auth}@{host}"
    return urlunsplit(
        SplitResult(
            scheme=split.scheme,
            netloc=netloc,
            path=split.path,
            query=query,
            fragment=split.fragment,
        )
    )


def _is_sensitive_query_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(
        token in normalized
        for token in ("password", "passwd", "token", "secret", "key")
    )


def sqlite_urls(db_path: str) -> tuple[str, str]:
    """Return Ormdantic and SQLAlchemy URLs for a SQLite benchmark file."""
    return f"sqlite:///{db_path}", f"sqlite+aiosqlite:///{db_path}"


def _sqlalchemy_url(url: str, async_scheme: str) -> str:
    split = urlsplit(url)
    scheme = split.scheme.split("+", 1)[0]
    if scheme in {"postgres", "postgresql"}:
        scheme = async_scheme
    elif scheme == "mysql":
        scheme = async_scheme
    return urlunsplit(
        SplitResult(
            scheme=scheme,
            netloc=split.netloc,
            path=split.path,
            query=split.query,
            fragment=split.fragment,
        )
    )


def _normalize_backend(backend: str) -> str:
    normalized = backend.lower()
    if normalized in {"postgresql", "postgres"}:
        return "postgres"
    return normalized

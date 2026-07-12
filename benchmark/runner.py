from __future__ import annotations

import asyncio
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter_ns
from typing import Any

from pydantic import BaseModel

from benchmark.backends import BackendDefinition, resolve_backend, sqlite_urls
from benchmark.cases import CaseDefinition, case_matrix
from benchmark.charts import ORMDANTIC, SQLALCHEMY, SQLMODEL, BenchmarkMeasurement
from benchmark.config import BenchmarkConfig, validate_config
from benchmark.datasets import (
    SCORE_THRESHOLD,
    batched_rows,
    lookup_ids,
    row_dict,
)
from benchmark.models import (
    BENCH_ITEM_TABLE,
    SQLALCHEMY_IMPORT_ERROR,
    SQLMODEL_IMPORT_ERROR,
    NestedChildPayload,
    NestedLeafPayload,
    NestedParentPayload,
    OrmdanticModels,
    register_ormdantic_models,
)
from ormdantic import Order, Ormdantic, assignment, column, selectinload
from ormdantic import count as orm_count
from ormdantic import sum as orm_sum
from ormdantic.engine import NativeEngine, runtime_capabilities

try:
    from sqlalchemy import delete, func, insert, select, update
    from sqlalchemy.ext.asyncio import (
        async_sessionmaker,
        create_async_engine,
    )
    from sqlalchemy.orm import joinedload as sqlalchemy_joinedload
    from sqlalchemy.orm import selectinload as sqlalchemy_selectinload
except ModuleNotFoundError as exc:  # pragma: no cover - exercised by CLI setup errors
    _SQLALCHEMY_RUNTIME_IMPORT_ERROR: ModuleNotFoundError | None = exc
else:
    _SQLALCHEMY_RUNTIME_IMPORT_ERROR = None

try:
    from sqlmodel import SQLModel
except ModuleNotFoundError as exc:  # pragma: no cover - exercised by CLI setup errors
    _SQLMODEL_RUNTIME_IMPORT_ERROR: ModuleNotFoundError | None = exc
else:
    _SQLMODEL_RUNTIME_IMPORT_ERROR = None

from benchmark import models as bm

ProgressCallback = Callable[[str], None]
OperationFactory = Callable[[], Awaitable["Operation"]]
RUNNER_VERSION = "cross-db-v1"


@dataclass(frozen=True)
class Operation:
    """Prepared benchmark operation."""

    run: Callable[[], Awaitable[None]]
    validate: Callable[[], Awaitable[int]]
    cleanup: Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class OrmdanticContext:
    """Prepared Ormdantic benchmark context."""

    backend: str
    url: str
    directory: Path | None
    db: Ormdantic
    models: OrmdanticModels


@dataclass(frozen=True)
class SqlAlchemyContext:
    """Prepared SQLAlchemy benchmark context."""

    backend: str
    url: str
    directory: Path | None
    engine: Any
    session_factory: Any


@dataclass(frozen=True)
class SqlModelContext:
    """Prepared SQLModel benchmark context."""

    backend: str
    url: str
    directory: Path | None
    engine: Any
    session_factory: Any


def run_from_config(
    config: BenchmarkConfig,
    *,
    allow_missing: bool = False,
    progress: ProgressCallback | None = None,
) -> list[BenchmarkMeasurement]:
    """Run benchmarks synchronously for CLI callers."""
    return asyncio.run(
        run_from_config_async(config, allow_missing=allow_missing, progress=progress)
    )


async def run_from_config_async(
    config: BenchmarkConfig,
    *,
    allow_missing: bool = False,
    progress: ProgressCallback | None = None,
) -> list[BenchmarkMeasurement]:
    """Run the cross-database benchmark matrix."""
    validate_config(config)
    backend = resolve_backend(config.backend)
    measurements: list[BenchmarkMeasurement] = []
    for case in case_matrix():
        expected = case.expected(
            read_rows=config.rows,
            write_rows=config.write_rows,
            lookup_count=config.lookup_count,
            category=config.category,
        )
        row_count = case.rows(
            read_rows=config.rows,
            write_rows=config.write_rows,
            lookup_count=config.lookup_count,
        )
        for orm_name in (ORMDANTIC, SQLALCHEMY, SQLMODEL):
            if not case.supports(orm_name, backend.name):
                measurements.append(
                    _skipped_measurement(
                        config, case, row_count, orm_name, "unsupported combination"
                    )
                )
                continue
            if config.planner_scale:
                measurements.append(
                    _skipped_measurement(
                        config,
                        case,
                        row_count,
                        orm_name,
                        "planner-scale mode records metadata only; materialized latency is not measured",
                    )
                )
                continue
            missing = _dependency_skip_reason(orm_name)
            if missing is not None:
                if not allow_missing:
                    raise RuntimeError(missing)
                measurements.append(
                    _skipped_measurement(config, case, row_count, orm_name, missing)
                )
                continue
            if progress is not None:
                progress(f"{backend.name} {config.profile} {case.name}: {orm_name}")
            try:
                measurement = await _measure_case(
                    config=config,
                    backend=backend,
                    case=case,
                    rows=row_count,
                    orm_name=orm_name,
                    expected=expected,
                )
            except Exception as exc:
                if not allow_missing:
                    raise
                measurements.append(
                    _skipped_measurement(config, case, row_count, orm_name, str(exc))
                )
            else:
                measurements.append(measurement)
    return measurements


def build_result_payload(
    *,
    config: BenchmarkConfig,
    backend: BackendDefinition,
    measurements: Iterable[BenchmarkMeasurement],
    git_commit: str | None = None,
    git_dirty: bool | None = None,
    server_version: str | None = None,
    docker_image: str | None = None,
) -> dict[str, object]:
    """Build the reproducible JSON benchmark payload."""
    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "git_commit": git_commit if git_commit is not None else _git_commit(),
            "git_dirty": git_dirty if git_dirty is not None else _git_dirty(),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "ormdantic_version": _ormdantic_version(),
            "runtime_capabilities": runtime_capabilities(),
            "runner": "benchmark.runner",
            "runner_version": RUNNER_VERSION,
            "backend": backend.name,
            "server_version": server_version,
            "database_url": backend.redacted_url,
            "docker_image": docker_image
            if docker_image is not None
            else backend.docker_image,
            "profile": config.profile,
            "materialized": config.materialized,
            "planner_scale": config.planner_scale,
            "methodology": [
                "Setup and seed work are measured separately from operation latency.",
                "Validation runs after each timed operation and before cleanup.",
                "Server database cleanup drops only benchmark-owned tables.",
                "Billion planner-scale artifacts must not be merged into materialized latency charts.",
            ],
        },
        "config": {
            "backend": config.backend,
            "profile": config.profile,
            "rows": config.rows,
            "write_rows": config.write_rows,
            "lookup_count": config.lookup_count,
            "iterations": config.iterations,
            "warmups": config.warmups,
            "batch_size": config.batch_size,
            "category": config.category,
            "materialized": config.materialized,
            "planner_scale": config.planner_scale,
        },
        "measurements": [measurement.as_dict() for measurement in measurements],
    }


async def backend_server_version(backend: BackendDefinition) -> str | None:
    """Return a best-effort server version string."""
    if backend.name == "sqlite":
        return "SQLite file database"
    engine = NativeEngine(backend.url)
    sql = "SELECT version()" if backend.name == "postgres" else "SELECT VERSION()"
    result = await engine.execute(sql, ())
    value = result.scalar()
    return str(value) if value is not None else None


async def _measure_case(
    *,
    config: BenchmarkConfig,
    backend: BackendDefinition,
    case: CaseDefinition,
    rows: int,
    orm_name: str,
    expected: int,
) -> BenchmarkMeasurement:
    samples: list[float] = []
    setup_samples: list[float] = []
    total_runs = config.iterations + config.warmups
    for run_index in range(total_runs):
        setup_start = perf_counter_ns()
        operation = await _operation_factory(config, backend, case.name, orm_name)()
        setup_ms = (perf_counter_ns() - setup_start) / 1_000_000
        start = perf_counter_ns()
        try:
            try:
                await operation.run()
            finally:
                elapsed_ms = (perf_counter_ns() - start) / 1_000_000
            actual = await operation.validate()
        finally:
            await operation.cleanup()
        if actual != expected:
            raise AssertionError(
                "validation mismatch for "
                f"case={case.name!r} backend={backend.name!r} orm={orm_name!r}: "
                f"expected {expected}, actual {actual}"
            )
        if run_index >= config.warmups:
            samples.append(elapsed_ms)
            setup_samples.append(setup_ms)
    return BenchmarkMeasurement(
        backend=backend.name,
        profile=config.profile,
        case=case.name,
        rows=rows,
        orm=orm_name,
        median_ms=statistics.median(samples),
        samples_ms=tuple(samples),
        setup_ms=statistics.median(setup_samples) if setup_samples else None,
        validation={"expected": expected, "actual": expected},
    )


def _operation_factory(
    config: BenchmarkConfig,
    backend: BackendDefinition,
    case_name: str,
    orm_name: str,
) -> OperationFactory:
    factories: dict[
        str, Callable[[BenchmarkConfig, BackendDefinition, str], OperationFactory]
    ] = {
        ORMDANTIC: _ormdantic_operation_factory,
        SQLALCHEMY: _sqlalchemy_operation_factory,
        SQLMODEL: _sqlmodel_operation_factory,
    }
    return factories[orm_name](config, backend, case_name)


def _ormdantic_operation_factory(
    config: BenchmarkConfig, backend: BackendDefinition, case_name: str
) -> OperationFactory:
    async def factory() -> Operation:
        if case_name == "schema create/drop":
            context = await _prepare_ormdantic_context(backend, create_schema=False)
            return _ormdantic_schema_operation(context)
        if case_name in {"serialize simple payloads", "serialize nested payloads"}:
            return _serialization_operation(config, case_name, ORMDANTIC)
        if case_name in _RELATIONSHIP_CASES:
            context = await _prepare_ormdantic_context(backend, create_schema=True)
            await _seed_ormdantic_relationships(context, config.rows)
            return _ormdantic_relationship_operation(context, config, case_name)

        context = await _prepare_ormdantic_context(backend, create_schema=True)
        if case_name not in {"raw batch insert", "orm insert models"}:
            await _seed_items_native(
                context.url, backend.name, config.rows, config.batch_size
            )
        return _ormdantic_item_operation(context, config, backend.name, case_name)

    return factory


def _sqlalchemy_operation_factory(
    config: BenchmarkConfig, backend: BackendDefinition, case_name: str
) -> OperationFactory:
    async def factory() -> Operation:
        if case_name == "schema create/drop":
            context = await _prepare_sqlalchemy_context(backend, create_schema=False)
            return _sqlalchemy_schema_operation(context)
        if case_name in {"serialize simple payloads", "serialize nested payloads"}:
            return _serialization_operation(config, case_name, SQLALCHEMY)
        if case_name in _RELATIONSHIP_CASES:
            context = await _prepare_sqlalchemy_context(backend, create_schema=True)
            await _seed_sqlalchemy_relationships(context, config.rows)
            return _sqlalchemy_relationship_operation(context, config, case_name)

        context = await _prepare_sqlalchemy_context(backend, create_schema=True)
        if case_name not in {"raw batch insert", "orm insert models"}:
            await _seed_sqlalchemy_items(context, config.rows, config.batch_size)
        return _sqlalchemy_item_operation(context, config, case_name)

    return factory


def _sqlmodel_operation_factory(
    config: BenchmarkConfig, backend: BackendDefinition, case_name: str
) -> OperationFactory:
    async def factory() -> Operation:
        if case_name == "schema create/drop":
            context = await _prepare_sqlmodel_context(backend, create_schema=False)
            return _sqlmodel_schema_operation(context)
        if case_name in {"serialize simple payloads", "serialize nested payloads"}:
            return _serialization_operation(config, case_name, SQLMODEL)
        if case_name in _RELATIONSHIP_CASES:
            context = await _prepare_sqlmodel_context(backend, create_schema=True)
            await _seed_sqlmodel_relationships(context, config.rows)
            return _sqlmodel_relationship_operation(context, config, case_name)

        context = await _prepare_sqlmodel_context(backend, create_schema=True)
        if case_name not in {"raw batch insert", "orm insert models"}:
            await _seed_sqlmodel_items(context, config.rows, config.batch_size)
        return _sqlmodel_item_operation(context, config, case_name)

    return factory


async def _prepare_ormdantic_context(
    backend: BackendDefinition, *, create_schema: bool
) -> OrmdanticContext:
    directory, url, _sqlalchemy_url = _urls_for_sample(backend)
    db = Ormdantic(url)
    models = register_ormdantic_models(db)
    await db.init()
    await db.drop_all()
    if create_schema:
        await db.create_all()
        await _create_ormdantic_item_indexes(url, backend.name)
    return OrmdanticContext(
        backend=backend.name, url=url, directory=directory, db=db, models=models
    )


async def _prepare_sqlalchemy_context(
    backend: BackendDefinition, *, create_schema: bool
) -> SqlAlchemyContext:
    _ensure_sqlalchemy()
    directory, _url, sqlalchemy_url = _urls_for_sample(backend)
    engine = create_async_engine(sqlalchemy_url)
    async with engine.begin() as connection:
        await connection.run_sync(bm.SqlAlchemyBase.metadata.drop_all)
        if create_schema:
            await connection.run_sync(bm.SqlAlchemyBase.metadata.create_all)
    return SqlAlchemyContext(
        backend=backend.name,
        url=sqlalchemy_url,
        directory=directory,
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )


async def _prepare_sqlmodel_context(
    backend: BackendDefinition, *, create_schema: bool
) -> SqlModelContext:
    _ensure_sqlmodel()
    directory, _url, sqlalchemy_url = _urls_for_sample(backend)
    engine = create_async_engine(sqlalchemy_url)
    async with engine.begin() as connection:
        tables = bm.sqlmodel_benchmark_tables()
        await connection.run_sync(
            lambda sync: SQLModel.metadata.drop_all(sync, tables=tables)
        )
        if create_schema:
            await connection.run_sync(
                lambda sync: SQLModel.metadata.create_all(sync, tables=tables)
            )
    return SqlModelContext(
        backend=backend.name,
        url=sqlalchemy_url,
        directory=directory,
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )


def _urls_for_sample(backend: BackendDefinition) -> tuple[Path | None, str, str]:
    if backend.name != "sqlite":
        return None, backend.url, backend.sqlalchemy_url
    directory = Path(tempfile.mkdtemp(prefix="ormdantic-benchmark-"))
    db_path = directory / "bench.sqlite3"
    url, sqlalchemy_url = sqlite_urls(str(db_path))
    return directory, url, sqlalchemy_url


def _ormdantic_schema_operation(context: OrmdanticContext) -> Operation:
    async def run() -> None:
        await context.db.create_all()
        await _create_ormdantic_item_indexes(context.url, context.backend)

    async def validate() -> int:
        return await context.db[context.models.item].count()

    async def cleanup() -> None:
        await _cleanup_ormdantic(context)

    return Operation(run=run, validate=validate, cleanup=cleanup)


def _sqlalchemy_schema_operation(context: SqlAlchemyContext) -> Operation:
    async def run() -> None:
        async with context.engine.begin() as connection:
            await connection.run_sync(bm.SqlAlchemyBase.metadata.create_all)

    async def validate() -> int:
        async with context.session_factory() as session:
            value = await session.scalar(
                select(func.count()).select_from(bm.SqlAlchemyBenchItem)
            )
            return int(value or 0)

    async def cleanup() -> None:
        await _cleanup_sqlalchemy(context)

    return Operation(run=run, validate=validate, cleanup=cleanup)


def _sqlmodel_schema_operation(context: SqlModelContext) -> Operation:
    async def run() -> None:
        async with context.engine.begin() as connection:
            tables = bm.sqlmodel_benchmark_tables()
            await connection.run_sync(
                lambda sync: SQLModel.metadata.create_all(sync, tables=tables)
            )

    async def validate() -> int:
        async with context.session_factory() as session:
            value = await session.scalar(
                select(func.count()).select_from(bm.SQLModelBenchItem)
            )
            return int(value or 0)

    async def cleanup() -> None:
        await _cleanup_sqlmodel(context)

    return Operation(run=run, validate=validate, cleanup=cleanup)


def _ormdantic_item_operation(
    context: OrmdanticContext,
    config: BenchmarkConfig,
    backend_name: str,
    case_name: str,
) -> Operation:
    actual = 0
    item_model = context.models.item

    async def run() -> None:
        nonlocal actual
        table = context.db[item_model]
        if case_name == "raw batch insert":
            await _seed_items_native(
                context.url,
                backend_name,
                config.write_rows,
                config.batch_size,
                prefix="write",
            )
            actual = config.write_rows
        elif case_name == "orm insert models":
            for batch in batched_rows(
                config.write_rows, config.batch_size, prefix="write"
            ):
                for values in batch:
                    await table.insert(item_model(**values))
            actual = config.write_rows
        elif case_name == "orm update filtered":
            await table.update_where(
                assignment("score", 9_999),
                where=column("category") == config.category,
            )
            actual = await table.count(column("score") == 9_999)
        elif case_name == "orm upsert mixed":
            ids = lookup_ids(config.rows, config.lookup_count)
            for item_id in ids:
                index = int(item_id.rsplit("-", 1)[1])
                await table.upsert(item_model(**{**row_dict(index), "score": 8_888}))
            for index in range(len(ids)):
                await table.upsert(
                    item_model(**row_dict(config.rows + index, prefix="upsert"))
                )
            actual = len(ids)
        elif case_name == "orm delete filtered":
            rows = await table.find_many(where={"category": config.category})
            for item in rows.data:
                await table.delete(item.id)
            actual = await table.count()
        elif case_name == "count all rows":
            actual = await table.count()
        elif case_name == "count equality filter":
            actual = await table.count({"category": config.category})
        elif case_name == "count range filter":
            actual = await table.count(column("score") >= SCORE_THRESHOLD)
        elif case_name == "aggregate filtered rows":
            result = await table.select(
                orm_count().as_("row_count"),
                orm_sum(column("score")).as_("score_sum"),
                where=column("category") == config.category,
            )
            rows = list(result)
            actual = int(rows[0][0]) if rows else 0
        elif case_name == "scalar projection read":
            result = await table.select(
                column("id"),
                column("score"),
                where=column("category") == config.category,
                order_by=[column("id").asc()],
            )
            actual = len(list(result))
        elif case_name == "batched primary-key lookup":
            found = 0
            for item_id in lookup_ids(config.rows, config.lookup_count):
                if await table.find_one(item_id) is not None:
                    found += 1
            actual = found
        elif case_name == "paginated find_many":
            result = await table.find_many(limit=min(config.lookup_count, 1_000))
            actual = len(result.data)
        elif case_name == "ordered find_many":
            result = await table.find_many(
                order_by=["category", "score"],
                order=Order.asc,
                limit=min(config.lookup_count, 1_000),
            )
            actual = len(result.data)
        elif case_name == "hydrate flat rows":
            result = await table.find_many(limit=min(config.lookup_count, 1_000))
            actual = len(result.data)
        elif case_name == "hydrate relationship results":
            actual = await _ormdantic_load_parent_count(context, config, case_name)
        else:
            raise ValueError(f"unknown Ormdantic benchmark case: {case_name}")

    async def validate() -> int:
        return actual

    async def cleanup() -> None:
        await _cleanup_ormdantic(context)

    return Operation(run=run, validate=validate, cleanup=cleanup)


def _sqlalchemy_item_operation(
    context: SqlAlchemyContext,
    config: BenchmarkConfig,
    case_name: str,
) -> Operation:
    actual = 0

    async def run() -> None:
        nonlocal actual
        if case_name == "raw batch insert":
            await _seed_sqlalchemy_items(
                context, config.write_rows, config.batch_size, prefix="write"
            )
            actual = config.write_rows
        elif case_name == "orm insert models":
            async with context.session_factory() as session:
                for batch in batched_rows(
                    config.write_rows, config.batch_size, prefix="write"
                ):
                    session.add_all(
                        bm.SqlAlchemyBenchItem(**values) for values in batch
                    )
                    await session.commit()
            actual = config.write_rows
        elif case_name == "orm update filtered":
            async with context.session_factory() as session:
                result = await session.execute(
                    update(bm.SqlAlchemyBenchItem)
                    .where(bm.SqlAlchemyBenchItem.category == config.category)
                    .values(score=9_999)
                )
                await session.commit()
                actual = int(result.rowcount or 0)
        elif case_name == "orm upsert mixed":
            async with context.session_factory() as session:
                ids = lookup_ids(config.rows, config.lookup_count)
                for item_id in ids:
                    index = int(item_id.rsplit("-", 1)[1])
                    await session.merge(
                        bm.SqlAlchemyBenchItem(**{**row_dict(index), "score": 8_888})
                    )
                for index in range(len(ids)):
                    await session.merge(
                        bm.SqlAlchemyBenchItem(
                            **row_dict(config.rows + index, prefix="upsert")
                        )
                    )
                await session.commit()
                actual = len(ids)
        elif case_name == "orm delete filtered":
            async with context.session_factory() as session:
                await session.execute(
                    delete(bm.SqlAlchemyBenchItem).where(
                        bm.SqlAlchemyBenchItem.category == config.category
                    )
                )
                await session.commit()
                value = await session.scalar(
                    select(func.count()).select_from(bm.SqlAlchemyBenchItem)
                )
                actual = int(value or 0)
        else:
            actual = await _sqlalchemy_read_case(context, config, case_name)

    async def validate() -> int:
        return actual

    async def cleanup() -> None:
        await _cleanup_sqlalchemy(context)

    return Operation(run=run, validate=validate, cleanup=cleanup)


def _sqlmodel_item_operation(
    context: SqlModelContext,
    config: BenchmarkConfig,
    case_name: str,
) -> Operation:
    actual = 0

    async def run() -> None:
        nonlocal actual
        if case_name == "raw batch insert":
            await _seed_sqlmodel_items(
                context, config.write_rows, config.batch_size, prefix="write"
            )
            actual = config.write_rows
        elif case_name == "orm insert models":
            async with context.session_factory() as session:
                for batch in batched_rows(
                    config.write_rows, config.batch_size, prefix="write"
                ):
                    session.add_all(bm.SQLModelBenchItem(**values) for values in batch)
                    await session.commit()
            actual = config.write_rows
        elif case_name == "orm update filtered":
            async with context.session_factory() as session:
                result = await session.execute(
                    update(bm.SQLModelBenchItem)
                    .where(bm.SQLModelBenchItem.category == config.category)
                    .values(score=9_999)
                )
                await session.commit()
                actual = int(result.rowcount or 0)
        elif case_name == "orm upsert mixed":
            async with context.session_factory() as session:
                ids = lookup_ids(config.rows, config.lookup_count)
                for item_id in ids:
                    index = int(item_id.rsplit("-", 1)[1])
                    await session.merge(
                        bm.SQLModelBenchItem(**{**row_dict(index), "score": 8_888})
                    )
                for index in range(len(ids)):
                    await session.merge(
                        bm.SQLModelBenchItem(
                            **row_dict(config.rows + index, prefix="upsert")
                        )
                    )
                await session.commit()
                actual = len(ids)
        elif case_name == "orm delete filtered":
            async with context.session_factory() as session:
                await session.execute(
                    delete(bm.SQLModelBenchItem).where(
                        bm.SQLModelBenchItem.category == config.category
                    )
                )
                await session.commit()
                value = await session.scalar(
                    select(func.count()).select_from(bm.SQLModelBenchItem)
                )
                actual = int(value or 0)
        else:
            actual = await _sqlmodel_read_case(context, config, case_name)

    async def validate() -> int:
        return actual

    async def cleanup() -> None:
        await _cleanup_sqlmodel(context)

    return Operation(run=run, validate=validate, cleanup=cleanup)


async def _sqlalchemy_read_case(
    context: SqlAlchemyContext, config: BenchmarkConfig, case_name: str
) -> int:
    async with context.session_factory() as session:
        if case_name == "count all rows":
            value = await session.scalar(
                select(func.count()).select_from(bm.SqlAlchemyBenchItem)
            )
            return int(value or 0)
        if case_name == "count equality filter":
            value = await session.scalar(
                select(func.count())
                .select_from(bm.SqlAlchemyBenchItem)
                .where(bm.SqlAlchemyBenchItem.category == config.category)
            )
            return int(value or 0)
        if case_name == "count range filter":
            value = await session.scalar(
                select(func.count())
                .select_from(bm.SqlAlchemyBenchItem)
                .where(bm.SqlAlchemyBenchItem.score >= SCORE_THRESHOLD)
            )
            return int(value or 0)
        if case_name == "aggregate filtered rows":
            row = await session.execute(
                select(func.count(), func.sum(bm.SqlAlchemyBenchItem.score)).where(
                    bm.SqlAlchemyBenchItem.category == config.category
                )
            )
            return int(row.one()[0])
        if case_name == "scalar projection read":
            result = await session.execute(
                select(bm.SqlAlchemyBenchItem.id, bm.SqlAlchemyBenchItem.score)
                .where(bm.SqlAlchemyBenchItem.category == config.category)
                .order_by(bm.SqlAlchemyBenchItem.id)
            )
            return len(result.all())
        if case_name == "batched primary-key lookup":
            found = 0
            for item_id in lookup_ids(config.rows, config.lookup_count):
                if await session.get(bm.SqlAlchemyBenchItem, item_id) is not None:
                    found += 1
            return found
        if case_name in {"paginated find_many", "hydrate flat rows"}:
            result = await session.execute(
                select(bm.SqlAlchemyBenchItem).limit(min(config.lookup_count, 1_000))
            )
            return len(result.scalars().all())
        if case_name == "ordered find_many":
            result = await session.execute(
                select(bm.SqlAlchemyBenchItem)
                .order_by(bm.SqlAlchemyBenchItem.category, bm.SqlAlchemyBenchItem.score)
                .limit(min(config.lookup_count, 1_000))
            )
            return len(result.scalars().all())
        raise ValueError(f"unknown SQLAlchemy benchmark case: {case_name}")


async def _sqlmodel_read_case(
    context: SqlModelContext, config: BenchmarkConfig, case_name: str
) -> int:
    async with context.session_factory() as session:
        if case_name == "count all rows":
            value = await session.scalar(
                select(func.count()).select_from(bm.SQLModelBenchItem)
            )
            return int(value or 0)
        if case_name == "count equality filter":
            value = await session.scalar(
                select(func.count())
                .select_from(bm.SQLModelBenchItem)
                .where(bm.SQLModelBenchItem.category == config.category)
            )
            return int(value or 0)
        if case_name == "count range filter":
            value = await session.scalar(
                select(func.count())
                .select_from(bm.SQLModelBenchItem)
                .where(bm.SQLModelBenchItem.score >= SCORE_THRESHOLD)
            )
            return int(value or 0)
        if case_name == "aggregate filtered rows":
            row = await session.execute(
                select(func.count(), func.sum(bm.SQLModelBenchItem.score)).where(
                    bm.SQLModelBenchItem.category == config.category
                )
            )
            return int(row.one()[0])
        if case_name == "scalar projection read":
            result = await session.execute(
                select(bm.SQLModelBenchItem.id, bm.SQLModelBenchItem.score)
                .where(bm.SQLModelBenchItem.category == config.category)
                .order_by(bm.SQLModelBenchItem.id)
            )
            return len(result.all())
        if case_name == "batched primary-key lookup":
            found = 0
            for item_id in lookup_ids(config.rows, config.lookup_count):
                if await session.get(bm.SQLModelBenchItem, item_id) is not None:
                    found += 1
            return found
        if case_name in {"paginated find_many", "hydrate flat rows"}:
            result = await session.execute(
                select(bm.SQLModelBenchItem).limit(min(config.lookup_count, 1_000))
            )
            return len(result.scalars().all())
        if case_name == "ordered find_many":
            result = await session.execute(
                select(bm.SQLModelBenchItem)
                .order_by(bm.SQLModelBenchItem.category, bm.SQLModelBenchItem.score)
                .limit(min(config.lookup_count, 1_000))
            )
            return len(result.scalars().all())
        raise ValueError(f"unknown SQLModel benchmark case: {case_name}")


def _ormdantic_relationship_operation(
    context: OrmdanticContext, config: BenchmarkConfig, case_name: str
) -> Operation:
    actual = 0

    async def run() -> None:
        nonlocal actual
        if case_name in {
            "one-to-many relationship loading",
            "nested relationship loading",
            "hydrate relationship results",
        }:
            actual = await _ormdantic_load_parent_count(context, config, case_name)
        elif case_name == "many-to-one relationship loading":
            result = await context.db[context.models.child].find_many(
                order_by=["name"],
                load=[selectinload("parent")],
            )
            actual = len(result.data)
        else:
            raise ValueError(f"unknown Ormdantic relationship case: {case_name}")

    async def validate() -> int:
        return actual

    async def cleanup() -> None:
        await _cleanup_ormdantic(context)

    return Operation(run=run, validate=validate, cleanup=cleanup)


def _sqlalchemy_relationship_operation(
    context: SqlAlchemyContext, config: BenchmarkConfig, case_name: str
) -> Operation:
    actual = 0

    async def run() -> None:
        nonlocal actual
        async with context.session_factory() as session:
            plan = _relationship_loader_plan(case_name)
            if plan == "children":
                result = await session.execute(
                    select(bm.SqlAlchemyBenchParent)
                    .options(sqlalchemy_selectinload(bm.SqlAlchemyBenchParent.children))
                    .order_by(bm.SqlAlchemyBenchParent.name)
                )
                actual = len(result.scalars().all())
            elif plan == "children.leaves":
                result = await session.execute(
                    select(bm.SqlAlchemyBenchParent)
                    .options(
                        sqlalchemy_selectinload(
                            bm.SqlAlchemyBenchParent.children
                        ).selectinload(bm.SqlAlchemyBenchChild.leaves)
                    )
                    .order_by(bm.SqlAlchemyBenchParent.name)
                )
                actual = len(result.scalars().all())
            elif plan == "joined-depth":
                result = await session.execute(
                    select(bm.SqlAlchemyBenchParent)
                    .options(
                        sqlalchemy_joinedload(
                            bm.SqlAlchemyBenchParent.children
                        ).joinedload(bm.SqlAlchemyBenchChild.leaves)
                    )
                    .order_by(bm.SqlAlchemyBenchParent.name)
                )
                actual = len(result.unique().scalars().all())
            elif case_name == "many-to-one relationship loading":
                result = await session.execute(
                    select(bm.SqlAlchemyBenchChild)
                    .options(sqlalchemy_selectinload(bm.SqlAlchemyBenchChild.parent))
                    .order_by(bm.SqlAlchemyBenchChild.name)
                )
                actual = len(result.scalars().all())
            else:
                raise ValueError(f"unknown SQLAlchemy relationship case: {case_name}")

    async def validate() -> int:
        return actual

    async def cleanup() -> None:
        await _cleanup_sqlalchemy(context)

    return Operation(run=run, validate=validate, cleanup=cleanup)


def _sqlmodel_relationship_operation(
    context: SqlModelContext, config: BenchmarkConfig, case_name: str
) -> Operation:
    actual = 0

    async def run() -> None:
        nonlocal actual
        async with context.session_factory() as session:
            plan = _relationship_loader_plan(case_name)
            if plan == "children":
                result = await session.execute(
                    select(bm.SQLModelBenchParent)
                    .options(sqlalchemy_selectinload(bm.SQLModelBenchParent.children))
                    .order_by(bm.SQLModelBenchParent.name)
                )
                actual = len(result.scalars().all())
            elif plan == "children.leaves":
                result = await session.execute(
                    select(bm.SQLModelBenchParent)
                    .options(
                        sqlalchemy_selectinload(
                            bm.SQLModelBenchParent.children
                        ).selectinload(bm.SQLModelBenchChild.leaves)
                    )
                    .order_by(bm.SQLModelBenchParent.name)
                )
                actual = len(result.scalars().all())
            elif plan == "joined-depth":
                result = await session.execute(
                    select(bm.SQLModelBenchParent)
                    .options(
                        sqlalchemy_joinedload(
                            bm.SQLModelBenchParent.children
                        ).joinedload(bm.SQLModelBenchChild.leaves)
                    )
                    .order_by(bm.SQLModelBenchParent.name)
                )
                actual = len(result.unique().scalars().all())
            elif case_name == "many-to-one relationship loading":
                result = await session.execute(
                    select(bm.SQLModelBenchChild)
                    .options(sqlalchemy_selectinload(bm.SQLModelBenchChild.parent))
                    .order_by(bm.SQLModelBenchChild.name)
                )
                actual = len(result.scalars().all())
            else:
                raise ValueError(f"unknown SQLModel relationship case: {case_name}")

    async def validate() -> int:
        return actual

    async def cleanup() -> None:
        await _cleanup_sqlmodel(context)

    return Operation(run=run, validate=validate, cleanup=cleanup)


async def _ormdantic_load_parent_count(
    context: OrmdanticContext, config: BenchmarkConfig, case_name: str
) -> int:
    plan = _relationship_loader_plan(case_name)
    if plan == "joined-depth":
        result = await context.db[context.models.parent].find_many(
            order_by=["name"],
            depth=2,
        )
    elif plan == "children":
        result = await context.db[context.models.parent].find_many(
            order_by=["name"],
            load=[selectinload("children").batched(_relationship_batch(config.rows))],
        )
    elif plan == "children.leaves":
        result = await context.db[context.models.parent].find_many(
            order_by=["name"],
            load=[
                selectinload("children").batched(_relationship_batch(config.rows)),
                selectinload("children.leaves").batched(
                    _relationship_batch(config.rows)
                ),
            ],
        )
    else:
        raise ValueError(f"unknown relationship loader plan for {case_name!r}")
    return len(result.data)


def _relationship_loader_plan(case_name: str) -> str:
    if case_name == "one-to-many relationship loading":
        return "children"
    if case_name == "nested relationship loading":
        return "children.leaves"
    if case_name == "hydrate relationship results":
        return "joined-depth"
    return ""


def _serialization_operation(
    config: BenchmarkConfig, case_name: str, orm_name: str
) -> Operation:
    count = min(config.lookup_count, 1_000)
    actual = 0

    async def run() -> None:
        nonlocal actual
        if case_name == "serialize simple payloads":
            payloads = _simple_serialized_payloads(orm_name, count)
            actual = sum(1 for payload in payloads if payload.get("id"))
        elif case_name == "serialize nested payloads":
            parents = _nested_serialized_payloads(orm_name, config.rows)
            actual = sum(1 for parent in parents if parent.get("children"))
        else:
            raise ValueError(f"unknown serialization benchmark case: {case_name}")

    async def validate() -> int:
        return actual

    async def cleanup() -> None:
        return None

    return Operation(run=run, validate=validate, cleanup=cleanup)


def _simple_serialized_payloads(orm_name: str, count: int) -> list[dict[str, Any]]:
    if orm_name == ORMDANTIC:
        db = Ormdantic("sqlite:///:benchmark-serialization:")
        models = register_ormdantic_models(db)
        return [models.item(**row_dict(index)).model_dump() for index in range(count)]
    if orm_name == SQLMODEL:
        return [
            bm.SQLModelBenchItem(**row_dict(index)).model_dump()
            for index in range(count)
        ]
    if orm_name == SQLALCHEMY:
        return [
            _sqlalchemy_item_dict(bm.SqlAlchemyBenchItem(**row_dict(index)))
            for index in range(count)
        ]
    raise ValueError(f"unknown serialization ORM: {orm_name}")


def _nested_serialized_payloads(orm_name: str, read_rows: int) -> list[dict[str, Any]]:
    if orm_name == ORMDANTIC:
        return [parent.model_dump() for parent in _ormdantic_nested_payloads(read_rows)]
    if orm_name == SQLMODEL:
        return [
            _sqlmodel_parent_dict(parent)
            for parent in _sqlmodel_nested_payloads(read_rows)
        ]
    if orm_name == SQLALCHEMY:
        return [
            _sqlalchemy_parent_dict(parent)
            for parent in _sqlalchemy_nested_payloads(read_rows)
        ]
    raise ValueError(f"unknown serialization ORM: {orm_name}")


async def _seed_items_native(
    url: str,
    backend_name: str,
    row_count: int,
    batch_size: int,
    *,
    prefix: str = "item",
) -> None:
    engine = NativeEngine(url)
    async with engine.transaction():
        for batch in batched_rows(row_count, batch_size, prefix=prefix):
            sql, values = _insert_sql(BENCH_ITEM_TABLE, backend_name, batch)
            await engine.execute(sql, tuple(values))


async def _create_ormdantic_item_indexes(url: str, backend_name: str) -> None:
    engine = NativeEngine(url)
    for sql in _ormdantic_item_index_sql(backend_name):
        await engine.execute(sql, ())


def _ormdantic_item_index_sql(backend_name: str) -> list[str]:
    if backend_name == "mysql":
        return [
            "CREATE INDEX ormdantic_bench_items_category_idx ON "
            "ormdantic_bench_items (category(24))",
            "CREATE INDEX ormdantic_bench_items_score_idx ON "
            "ormdantic_bench_items (score)",
        ]
    return [
        "CREATE INDEX ormdantic_bench_items_category_idx ON "
        "ormdantic_bench_items (category)",
        "CREATE INDEX ormdantic_bench_items_score_idx ON ormdantic_bench_items (score)",
    ]


async def _seed_sqlalchemy_items(
    context: SqlAlchemyContext,
    row_count: int,
    batch_size: int,
    *,
    prefix: str = "item",
) -> None:
    async with context.session_factory() as session:
        for batch in batched_rows(row_count, batch_size, prefix=prefix):
            await session.execute(insert(bm.SqlAlchemyBenchItem), batch)
            await session.commit()


async def _seed_sqlmodel_items(
    context: SqlModelContext,
    row_count: int,
    batch_size: int,
    *,
    prefix: str = "item",
) -> None:
    async with context.session_factory() as session:
        for batch in batched_rows(row_count, batch_size, prefix=prefix):
            await session.execute(insert(bm.SQLModelBenchItem), batch)
            await session.commit()


async def _seed_ormdantic_relationships(
    context: OrmdanticContext, read_rows: int
) -> None:
    parents, children_per_parent, leaves_per_child = _relationship_shape(read_rows)
    parent_model = context.models.parent
    child_model = context.models.child
    leaf_model = context.models.leaf
    for parent_index in range(parents):
        parent = await context.db[parent_model].insert(
            parent_model(
                id=f"parent-{parent_index:08d}", name=f"parent-{parent_index:04d}"
            )
        )
        for child_index in range(children_per_parent):
            child = await context.db[child_model].insert(
                child_model(
                    id=f"child-{parent_index:08d}-{child_index:04d}",
                    name=f"child-{parent_index:04d}-{child_index:04d}",
                    parent=parent,
                )
            )
            for leaf_index in range(leaves_per_child):
                await context.db[leaf_model].insert(
                    leaf_model(
                        id=f"leaf-{parent_index:08d}-{child_index:04d}-{leaf_index:04d}",
                        name=f"leaf-{parent_index:04d}-{child_index:04d}-{leaf_index:04d}",
                        child=child,
                    )
                )


async def _seed_sqlalchemy_relationships(
    context: SqlAlchemyContext, read_rows: int
) -> None:
    parents, children_per_parent, leaves_per_child = _relationship_shape(read_rows)
    async with context.session_factory() as session:
        for parent_index in range(parents):
            parent = bm.SqlAlchemyBenchParent(
                id=f"parent-{parent_index:08d}", name=f"parent-{parent_index:04d}"
            )
            for child_index in range(children_per_parent):
                child = bm.SqlAlchemyBenchChild(
                    id=f"child-{parent_index:08d}-{child_index:04d}",
                    name=f"child-{parent_index:04d}-{child_index:04d}",
                    parent=parent,
                )
                for leaf_index in range(leaves_per_child):
                    child.leaves.append(
                        bm.SqlAlchemyBenchLeaf(
                            id=f"leaf-{parent_index:08d}-{child_index:04d}-{leaf_index:04d}",
                            name=f"leaf-{parent_index:04d}-{child_index:04d}-{leaf_index:04d}",
                        )
                    )
            session.add(parent)
        await session.commit()


async def _seed_sqlmodel_relationships(
    context: SqlModelContext, read_rows: int
) -> None:
    parents, children_per_parent, leaves_per_child = _relationship_shape(read_rows)
    async with context.session_factory() as session:
        for parent_index in range(parents):
            parent = bm.SQLModelBenchParent(
                id=f"parent-{parent_index:08d}", name=f"parent-{parent_index:04d}"
            )
            for child_index in range(children_per_parent):
                child = bm.SQLModelBenchChild(
                    id=f"child-{parent_index:08d}-{child_index:04d}",
                    name=f"child-{parent_index:04d}-{child_index:04d}",
                    parent=parent,
                    parent_id=parent.id,
                )
                for leaf_index in range(leaves_per_child):
                    child.leaves.append(
                        bm.SQLModelBenchLeaf(
                            id=f"leaf-{parent_index:08d}-{child_index:04d}-{leaf_index:04d}",
                            name=f"leaf-{parent_index:04d}-{child_index:04d}-{leaf_index:04d}",
                            child=child,
                            child_id=child.id,
                        )
                    )
            session.add(parent)
        await session.commit()


def _insert_sql(
    table_name: str, backend_name: str, batch: list[dict[str, str | int]]
) -> tuple[str, list[str | int]]:
    columns = ("id", "category", "name", "score", "payload")
    values: list[str | int] = []
    rows_sql = []
    param_index = 1
    for row in batch:
        placeholders = []
        for column_name in columns:
            values.append(row[column_name])
            if backend_name == "postgres":
                placeholders.append(f"${param_index}")
                param_index += 1
            else:
                placeholders.append("?")
        rows_sql.append(f"({', '.join(placeholders)})")
    sql = (
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES {', '.join(rows_sql)}"
    )
    return sql, values


async def _cleanup_ormdantic(context: OrmdanticContext) -> None:
    try:
        await context.db.drop_all()
    finally:
        _cleanup_directory(context.directory)


async def _cleanup_sqlalchemy(context: SqlAlchemyContext) -> None:
    try:
        async with context.engine.begin() as connection:
            await connection.run_sync(bm.SqlAlchemyBase.metadata.drop_all)
    finally:
        await context.engine.dispose()
        _cleanup_directory(context.directory)


async def _cleanup_sqlmodel(context: SqlModelContext) -> None:
    try:
        async with context.engine.begin() as connection:
            tables = bm.sqlmodel_benchmark_tables()
            await connection.run_sync(
                lambda sync: SQLModel.metadata.drop_all(sync, tables=tables)
            )
    finally:
        await context.engine.dispose()
        _cleanup_directory(context.directory)


def _cleanup_directory(directory: Path | None) -> None:
    if directory is not None:
        shutil.rmtree(directory, ignore_errors=True)


def _relationship_shape(read_rows: int) -> tuple[int, int, int]:
    parents = min(max(read_rows // 250, 4), 200)
    children_per_parent = 3 if read_rows < 1_000_000 else 5
    leaves_per_child = 2 if read_rows < 1_000_000 else 3
    return parents, children_per_parent, leaves_per_child


def _relationship_batch(read_rows: int) -> int:
    return 25 if read_rows < 1_000_000 else 100


def _nested_payloads(read_rows: int) -> list[NestedParentPayload]:
    parents, children_per_parent, leaves_per_child = _relationship_shape(read_rows)
    payloads = []
    for parent_index in range(parents):
        children = []
        for child_index in range(children_per_parent):
            leaves = [
                NestedLeafPayload(
                    id=f"leaf-{parent_index:08d}-{child_index:04d}-{leaf_index:04d}",
                    name=f"leaf-{parent_index:04d}-{child_index:04d}-{leaf_index:04d}",
                )
                for leaf_index in range(leaves_per_child)
            ]
            children.append(
                NestedChildPayload(
                    id=f"child-{parent_index:08d}-{child_index:04d}",
                    name=f"child-{parent_index:04d}-{child_index:04d}",
                    leaves=leaves,
                )
            )
        payloads.append(
            NestedParentPayload(
                id=f"parent-{parent_index:08d}",
                name=f"parent-{parent_index:04d}",
                children=children,
            )
        )
    return payloads


def _ormdantic_nested_payloads(read_rows: int) -> list[BaseModel]:
    db = Ormdantic("sqlite:///:benchmark-serialization:")
    models = register_ormdantic_models(db)
    parents, children_per_parent, leaves_per_child = _relationship_shape(read_rows)
    payloads = []
    for parent_index in range(parents):
        children = []
        for child_index in range(children_per_parent):
            child_id = f"child-{parent_index:08d}-{child_index:04d}"
            leaves = [
                models.leaf(
                    id=f"leaf-{parent_index:08d}-{child_index:04d}-{leaf_index:04d}",
                    name=f"leaf-{parent_index:04d}-{child_index:04d}-{leaf_index:04d}",
                    child=child_id,
                )
                for leaf_index in range(leaves_per_child)
            ]
            children.append(
                models.child(
                    id=child_id,
                    name=f"child-{parent_index:04d}-{child_index:04d}",
                    parent=f"parent-{parent_index:08d}",
                    leaves=leaves,
                )
            )
        payloads.append(
            models.parent(
                id=f"parent-{parent_index:08d}",
                name=f"parent-{parent_index:04d}",
                children=children,
            )
        )
    return payloads


def _sqlmodel_nested_payloads(read_rows: int) -> list[Any]:
    parents, children_per_parent, leaves_per_child = _relationship_shape(read_rows)
    payloads = []
    for parent_index in range(parents):
        parent = bm.SQLModelBenchParent(
            id=f"parent-{parent_index:08d}", name=f"parent-{parent_index:04d}"
        )
        for child_index in range(children_per_parent):
            child = bm.SQLModelBenchChild(
                id=f"child-{parent_index:08d}-{child_index:04d}",
                name=f"child-{parent_index:04d}-{child_index:04d}",
                parent_id=parent.id,
            )
            for leaf_index in range(leaves_per_child):
                child.leaves.append(
                    bm.SQLModelBenchLeaf(
                        id=f"leaf-{parent_index:08d}-{child_index:04d}-{leaf_index:04d}",
                        name=f"leaf-{parent_index:04d}-{child_index:04d}-{leaf_index:04d}",
                        child_id=child.id,
                    )
                )
            parent.children.append(child)
        payloads.append(parent)
    return payloads


def _sqlalchemy_nested_payloads(read_rows: int) -> list[Any]:
    parents, children_per_parent, leaves_per_child = _relationship_shape(read_rows)
    payloads = []
    for parent_index in range(parents):
        parent = bm.SqlAlchemyBenchParent(
            id=f"parent-{parent_index:08d}", name=f"parent-{parent_index:04d}"
        )
        for child_index in range(children_per_parent):
            child = bm.SqlAlchemyBenchChild(
                id=f"child-{parent_index:08d}-{child_index:04d}",
                name=f"child-{parent_index:04d}-{child_index:04d}",
                parent_id=parent.id,
            )
            for leaf_index in range(leaves_per_child):
                child.leaves.append(
                    bm.SqlAlchemyBenchLeaf(
                        id=f"leaf-{parent_index:08d}-{child_index:04d}-{leaf_index:04d}",
                        name=f"leaf-{parent_index:04d}-{child_index:04d}-{leaf_index:04d}",
                        child_id=child.id,
                    )
                )
            parent.children.append(child)
        payloads.append(parent)
    return payloads


def _sqlalchemy_item_dict(item: Any) -> dict[str, Any]:
    return {
        "id": item.id,
        "category": item.category,
        "name": item.name,
        "score": item.score,
        "payload": item.payload,
    }


def _sqlmodel_parent_dict(parent: Any) -> dict[str, Any]:
    return {
        "id": parent.id,
        "name": parent.name,
        "children": [
            {
                "id": child.id,
                "name": child.name,
                "parent_id": child.parent_id,
                "leaves": [
                    {"id": leaf.id, "name": leaf.name, "child_id": leaf.child_id}
                    for leaf in child.leaves
                ],
            }
            for child in parent.children
        ],
    }


def _sqlalchemy_parent_dict(parent: Any) -> dict[str, Any]:
    return {
        "id": parent.id,
        "name": parent.name,
        "children": [
            {
                "id": child.id,
                "name": child.name,
                "parent_id": child.parent_id,
                "leaves": [
                    {"id": leaf.id, "name": leaf.name, "child_id": leaf.child_id}
                    for leaf in child.leaves
                ],
            }
            for child in parent.children
        ],
    }


def _dependency_skip_reason(orm_name: str) -> str | None:
    if orm_name == SQLALCHEMY:
        error = SQLALCHEMY_IMPORT_ERROR or _SQLALCHEMY_RUNTIME_IMPORT_ERROR
        if error is not None:
            return (
                "SQLAlchemy benchmark dependencies are missing. "
                "Run `uv sync --group benchmark`."
            )
    if orm_name == SQLMODEL:
        sqlalchemy_error = SQLALCHEMY_IMPORT_ERROR or _SQLALCHEMY_RUNTIME_IMPORT_ERROR
        sqlmodel_error = SQLMODEL_IMPORT_ERROR or _SQLMODEL_RUNTIME_IMPORT_ERROR
        if sqlalchemy_error is not None or sqlmodel_error is not None:
            return (
                "SQLModel benchmark dependencies are missing. "
                "Run `uv sync --group benchmark`."
            )
    return None


def _ensure_sqlalchemy() -> None:
    reason = _dependency_skip_reason(SQLALCHEMY)
    if reason is not None:
        raise RuntimeError(reason)


def _ensure_sqlmodel() -> None:
    reason = _dependency_skip_reason(SQLMODEL)
    if reason is not None:
        raise RuntimeError(reason)


def _skipped_measurement(
    config: BenchmarkConfig,
    case: CaseDefinition,
    rows: int,
    orm_name: str,
    reason: str,
) -> BenchmarkMeasurement:
    return BenchmarkMeasurement(
        backend=config.backend,
        profile=config.profile,
        case=case.name,
        rows=rows,
        orm=orm_name,
        median_ms=None,
        samples_ms=(),
        skip_reason=reason,
    )


def _git_commit() -> str | None:
    return _git_output(["git", "rev-parse", "HEAD"])


def _git_dirty() -> bool:
    status = _git_output(["git", "status", "--porcelain"])
    return bool(status)


def _git_output(command: list[str]) -> str | None:
    try:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _ormdantic_version() -> str:
    import ormdantic

    return ormdantic.__version__


_RELATIONSHIP_CASES = {
    "hydrate relationship results",
    "one-to-many relationship loading",
    "many-to-one relationship loading",
    "nested relationship loading",
}

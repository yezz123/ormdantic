from __future__ import annotations

import asyncio
import shutil
import sqlite3
import statistics
import tempfile
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Awaitable, Callable, Iterable, Iterator

from pydantic import BaseModel

from benchmark.charts import (
    ORMDANTIC,
    SQLALCHEMY,
    SQLMODEL,
    BenchmarkMeasurement,
)
from ormdantic import Ormdantic, column
from ormdantic import count as orm_count
from ormdantic import sum as orm_sum
from ormdantic.engine import NativeEngine

try:
    from sqlalchemy import Integer, String, func, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
except ModuleNotFoundError as exc:  # pragma: no cover - exercised by CLI messaging
    _SQLALCHEMY_IMPORT_ERROR: ModuleNotFoundError | None = exc
else:
    _SQLALCHEMY_IMPORT_ERROR = None

    class SqlAlchemyBase(DeclarativeBase):
        """SQLAlchemy declarative base used by the comparison suite."""

    class SqlAlchemyBenchItem(SqlAlchemyBase):
        """SQLAlchemy model that matches the Ormdantic benchmark model."""

        __tablename__ = "bench_items"

        id: Mapped[str] = mapped_column(String(32), primary_key=True)
        category: Mapped[str] = mapped_column(String(24), index=True)
        name: Mapped[str] = mapped_column(String(96))
        score: Mapped[int] = mapped_column(Integer, index=True)
        payload: Mapped[str] = mapped_column(String(160))


try:
    from sqlmodel import Field as SQLModelField
    from sqlmodel import SQLModel
except ModuleNotFoundError as exc:  # pragma: no cover - exercised by CLI messaging
    _SQLMODEL_IMPORT_ERROR: ModuleNotFoundError | None = exc
else:
    _SQLMODEL_IMPORT_ERROR = None

    class SQLModelBenchItem(SQLModel, table=True):
        """SQLModel table used by the comparison suite."""

        __tablename__ = "bench_items"

        id: str = SQLModelField(primary_key=True, max_length=32)
        category: str = SQLModelField(index=True, max_length=24)
        name: str = SQLModelField(max_length=96)
        score: int = SQLModelField(index=True)
        payload: str = SQLModelField(max_length=160)


ProgressCallback = Callable[[str], None]
SCORE_THRESHOLD = 500
DEFAULT_BATCH_SIZE = 5_000


@dataclass(frozen=True)
class BenchmarkConfig:
    """Configuration for the ORM comparison run."""

    rows: int = 20_000
    write_rows: int = 20_000
    lookup_count: int = 1_000
    iterations: int = 5
    warmups: int = 1
    batch_size: int = DEFAULT_BATCH_SIZE
    category: str = "cat-3"
    profile: str = "default"

    @classmethod
    def for_profile(cls, profile: str) -> "BenchmarkConfig":
        """Return the default settings for a named benchmark profile."""
        normalized = profile.lower()
        if normalized == "default":
            return cls(profile="default")
        if normalized == "huge":
            return cls(
                rows=1_000_000,
                write_rows=1_000_000,
                lookup_count=10_000,
                iterations=1,
                warmups=0,
                batch_size=DEFAULT_BATCH_SIZE,
                profile="huge",
            )
        raise ValueError("profile must be 'default' or 'huge'")

    @property
    def filtered_count(self) -> int:
        """Return rows matching the configured category."""
        return sum(1 for row in _row_tuples(self.rows) if row[1] == self.category)

    @property
    def range_count(self) -> int:
        """Return rows matching the score range benchmark."""
        return sum(1 for row in _row_tuples(self.rows) if row[3] >= SCORE_THRESHOLD)

    @property
    def lookup_ids(self) -> tuple[str, ...]:
        """Return stable primary keys for point lookup benchmarks."""
        count = min(self.rows, self.lookup_count)
        if count <= 0:
            return ()
        step = max(self.rows // count, 1)
        return tuple(
            _row_id(min(index * step, self.rows - 1)) for index in range(count)
        )


@dataclass(frozen=True)
class _OrmdanticContext:
    directory: Path
    db_path: Path
    db: Ormdantic
    model: type[BaseModel]


@dataclass(frozen=True)
class _SqlAlchemyContext:
    directory: Path
    db_path: Path
    engine: Any
    session_factory: Any


@dataclass(frozen=True)
class _SqlModelContext:
    directory: Path
    db_path: Path
    engine: Any
    session_factory: Any


@dataclass(frozen=True)
class _Operation:
    run: Callable[[], Awaitable[None]]
    validate: Callable[[], Awaitable[int]]
    cleanup: Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class _Case:
    name: str
    rows: Callable[[BenchmarkConfig], int]
    expected: Callable[[BenchmarkConfig], int]
    factories: dict[
        str, Callable[[BenchmarkConfig], Callable[[], Awaitable[_Operation]]]
    ]


def benchmark_orm_names() -> tuple[str, str, str]:
    """Return the ORM order used by the report."""
    return (ORMDANTIC, SQLALCHEMY, SQLMODEL)


def benchmark_case_names(config: BenchmarkConfig) -> tuple[str, ...]:
    """Return benchmark case names for a config."""
    return tuple(case.name for case in _cases(config))


async def run_benchmarks(
    config: BenchmarkConfig,
    *,
    progress: ProgressCallback | None = None,
) -> list[BenchmarkMeasurement]:
    """Run the Ormdantic, SQLAlchemy, and SQLModel comparison suite."""
    _validate_config(config)
    _ensure_dependencies()

    measurements = []
    for case in _cases(config):
        expected = case.expected(config)
        row_count = case.rows(config)
        for orm_name in benchmark_orm_names():
            if progress is not None:
                progress(f"{config.profile} {case.name}: {orm_name}")
            measurement = await _measure_case(
                case_name=case.name,
                rows=row_count,
                orm_name=orm_name,
                operation_factory=case.factories[orm_name](config),
                expected=expected,
                iterations=config.iterations,
                warmups=config.warmups,
            )
            measurements.append(measurement)
    return measurements


def run_benchmarks_sync(
    config: BenchmarkConfig,
    *,
    progress: ProgressCallback | None = None,
) -> list[BenchmarkMeasurement]:
    """Synchronous wrapper for CLI callers."""
    return asyncio.run(run_benchmarks(config, progress=progress))


def _cases(config: BenchmarkConfig) -> tuple[_Case, ...]:
    return (
        _Case(
            name="write insert batches",
            rows=lambda settings: settings.write_rows,
            expected=lambda settings: settings.write_rows,
            factories={
                ORMDANTIC: _ormdantic_write_case,
                SQLALCHEMY: _sqlalchemy_write_case,
                SQLMODEL: _sqlmodel_write_case,
            },
        ),
        _Case(
            name="count all rows",
            rows=lambda settings: settings.rows,
            expected=lambda settings: settings.rows,
            factories={
                ORMDANTIC: _ormdantic_count_all_case,
                SQLALCHEMY: _sqlalchemy_count_all_case,
                SQLMODEL: _sqlmodel_count_all_case,
            },
        ),
        _Case(
            name="count filtered",
            rows=lambda settings: settings.rows,
            expected=lambda settings: settings.filtered_count,
            factories={
                ORMDANTIC: _ormdantic_count_case,
                SQLALCHEMY: _sqlalchemy_count_case,
                SQLMODEL: _sqlmodel_count_case,
            },
        ),
        _Case(
            name="count score range",
            rows=lambda settings: settings.rows,
            expected=lambda settings: settings.range_count,
            factories={
                ORMDANTIC: _ormdantic_count_range_case,
                SQLALCHEMY: _sqlalchemy_count_range_case,
                SQLMODEL: _sqlmodel_count_range_case,
            },
        ),
        _Case(
            name="aggregate filtered",
            rows=lambda settings: settings.rows,
            expected=lambda settings: settings.filtered_count,
            factories={
                ORMDANTIC: _ormdantic_aggregate_case,
                SQLALCHEMY: _sqlalchemy_aggregate_case,
                SQLMODEL: _sqlmodel_aggregate_case,
            },
        ),
        _Case(
            name="scalar projection read",
            rows=lambda settings: settings.rows,
            expected=lambda settings: settings.filtered_count,
            factories={
                ORMDANTIC: _ormdantic_projection_read_case,
                SQLALCHEMY: _sqlalchemy_projection_read_case,
                SQLMODEL: _sqlmodel_projection_read_case,
            },
        ),
        _Case(
            name="point lookup batch",
            rows=lambda settings: settings.lookup_count,
            expected=lambda settings: len(settings.lookup_ids),
            factories={
                ORMDANTIC: _ormdantic_point_lookup_case,
                SQLALCHEMY: _sqlalchemy_point_lookup_case,
                SQLMODEL: _sqlmodel_point_lookup_case,
            },
        ),
    )


def _validate_config(config: BenchmarkConfig) -> None:
    if config.rows <= 0:
        raise ValueError("rows must be greater than zero")
    if config.write_rows <= 0:
        raise ValueError("write_rows must be greater than zero")
    if config.lookup_count <= 0:
        raise ValueError("lookup_count must be greater than zero")
    if config.iterations <= 0:
        raise ValueError("iterations must be greater than zero")
    if config.warmups < 0:
        raise ValueError("warmups cannot be negative")
    if config.batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")


def _ensure_dependencies() -> None:
    if _SQLALCHEMY_IMPORT_ERROR is not None:
        raise RuntimeError(
            "SQLAlchemy benchmark dependencies are missing. "
            "Run `uv sync --group benchmark` or "
            "`uv run --group benchmark python -m benchmark.run`."
        ) from _SQLALCHEMY_IMPORT_ERROR
    if _SQLMODEL_IMPORT_ERROR is not None:
        raise RuntimeError(
            "SQLModel benchmark dependencies are missing. "
            "Run `uv sync --group benchmark` or "
            "`uv run --group benchmark python -m benchmark.run`."
        ) from _SQLMODEL_IMPORT_ERROR


async def _measure_case(
    *,
    case_name: str,
    rows: int,
    orm_name: str,
    operation_factory: Callable[[], Awaitable[_Operation]],
    expected: int,
    iterations: int,
    warmups: int,
) -> BenchmarkMeasurement:
    samples = []
    total_runs = iterations + warmups
    for run_index in range(total_runs):
        operation = await operation_factory()
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
                f"{case_name} {orm_name} returned {actual}, expected {expected}"
            )
        if run_index >= warmups:
            samples.append(elapsed_ms)
    return BenchmarkMeasurement(
        case=case_name,
        rows=rows,
        orm=orm_name,
        median_ms=statistics.median(samples),
        samples_ms=tuple(samples),
    )


def _ormdantic_write_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_raw_context("ormdantic-write")
        engine = NativeEngine(f"sqlite:///{context.db_path}")

        async def run() -> None:
            async with engine.transaction():
                for sql, values in _insert_statements(
                    config.write_rows, config.batch_size
                ):
                    await engine.execute(sql, tuple(values))

        async def validate() -> int:
            return _sqlite_count(context.db_path)

        async def cleanup() -> None:
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlalchemy_write_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlalchemy_raw_context("sqlalchemy-write")

        async def run() -> None:
            async with context.engine.begin() as connection:
                for sql, values in _insert_statements(
                    config.write_rows, config.batch_size
                ):
                    await connection.exec_driver_sql(sql, tuple(values))

        async def validate() -> int:
            return _sqlite_count(context.db_path)

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlmodel_write_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlmodel_raw_context("sqlmodel-write")

        async def run() -> None:
            async with context.engine.begin() as connection:
                for sql, values in _insert_statements(
                    config.write_rows, config.batch_size
                ):
                    await connection.exec_driver_sql(sql, tuple(values))

        async def validate() -> int:
            return _sqlite_count(context.db_path)

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _ormdantic_count_all_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_ormdantic_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            actual = await context.db[context.model].count()

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlalchemy_count_all_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlalchemy_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                value = await session.scalar(
                    select(func.count()).select_from(SqlAlchemyBenchItem)
                )
                actual = int(value or 0)

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlmodel_count_all_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlmodel_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                value = await session.scalar(
                    select(func.count()).select_from(SQLModelBenchItem)
                )
                actual = int(value or 0)

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _ormdantic_count_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_ormdantic_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            actual = await context.db[context.model].count(
                {"category": config.category}
            )

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlalchemy_count_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlalchemy_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                value = await session.scalar(
                    select(func.count())
                    .select_from(SqlAlchemyBenchItem)
                    .where(SqlAlchemyBenchItem.category == config.category)
                )
                actual = int(value or 0)

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlmodel_count_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlmodel_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                value = await session.scalar(
                    select(func.count())
                    .select_from(SQLModelBenchItem)
                    .where(SQLModelBenchItem.category == config.category)
                )
                actual = int(value or 0)

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _ormdantic_count_range_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_ormdantic_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            actual = await context.db[context.model].count(
                column("score") >= SCORE_THRESHOLD
            )

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlalchemy_count_range_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlalchemy_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                value = await session.scalar(
                    select(func.count())
                    .select_from(SqlAlchemyBenchItem)
                    .where(SqlAlchemyBenchItem.score >= SCORE_THRESHOLD)
                )
                actual = int(value or 0)

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlmodel_count_range_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlmodel_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                value = await session.scalar(
                    select(func.count())
                    .select_from(SQLModelBenchItem)
                    .where(SQLModelBenchItem.score >= SCORE_THRESHOLD)
                )
                actual = int(value or 0)

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _ormdantic_aggregate_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_ormdantic_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            result = await context.db[context.model].select(
                orm_count().as_("row_count"),
                orm_sum(column("score")).as_("score_sum"),
                where=column("category") == config.category,
            )
            rows = list(result)
            actual = int(rows[0][0]) if rows else 0

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlalchemy_aggregate_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlalchemy_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                row = await session.execute(
                    select(func.count(), func.sum(SqlAlchemyBenchItem.score)).where(
                        SqlAlchemyBenchItem.category == config.category
                    )
                )
                actual = int(row.one()[0])

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlmodel_aggregate_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlmodel_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                row = await session.execute(
                    select(func.count(), func.sum(SQLModelBenchItem.score)).where(
                        SQLModelBenchItem.category == config.category
                    )
                )
                actual = int(row.one()[0])

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _ormdantic_projection_read_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_ormdantic_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            result = await context.db[context.model].select(
                column("id"),
                column("score"),
                where=column("category") == config.category,
                order_by=[column("id").asc()],
            )
            actual = len(list(result))

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlalchemy_projection_read_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlalchemy_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                result = await session.execute(
                    select(SqlAlchemyBenchItem.id, SqlAlchemyBenchItem.score)
                    .where(SqlAlchemyBenchItem.category == config.category)
                    .order_by(SqlAlchemyBenchItem.id)
                )
                actual = len(result.all())

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlmodel_projection_read_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    async def factory() -> _Operation:
        context = await _prepare_sqlmodel_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            async with context.session_factory() as session:
                result = await session.execute(
                    select(SQLModelBenchItem.id, SQLModelBenchItem.score)
                    .where(SQLModelBenchItem.category == config.category)
                    .order_by(SQLModelBenchItem.id)
                )
                actual = len(result.all())

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _ormdantic_point_lookup_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    ids = config.lookup_ids

    async def factory() -> _Operation:
        context = await _prepare_ormdantic_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            found = 0
            for item_id in ids:
                if await context.db[context.model].find_one(item_id) is not None:
                    found += 1
            actual = found

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlalchemy_point_lookup_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    ids = config.lookup_ids

    async def factory() -> _Operation:
        context = await _prepare_sqlalchemy_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            found = 0
            async with context.session_factory() as session:
                for item_id in ids:
                    if await session.get(SqlAlchemyBenchItem, item_id) is not None:
                        found += 1
            actual = found

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


def _sqlmodel_point_lookup_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[_Operation]]:
    ids = config.lookup_ids

    async def factory() -> _Operation:
        context = await _prepare_sqlmodel_context(seed_rows=config.rows)
        actual = 0

        async def run() -> None:
            nonlocal actual
            found = 0
            async with context.session_factory() as session:
                for item_id in ids:
                    if await session.get(SQLModelBenchItem, item_id) is not None:
                        found += 1
            actual = found

        async def validate() -> int:
            return actual

        async def cleanup() -> None:
            await context.engine.dispose()
            _cleanup_directory(context.directory)

        return _Operation(run=run, validate=validate, cleanup=cleanup)

    return factory


async def _prepare_ormdantic_context(seed_rows: int) -> _OrmdanticContext:
    directory = Path(tempfile.mkdtemp(prefix="ormdantic-benchmark-"))
    db_path = directory / "bench.sqlite3"
    db = Ormdantic(f"sqlite:///{db_path}")

    @db.table("bench_items", pk="id", indexed=["category", "score"])
    class OrmdanticBenchItem(BaseModel):
        id: str
        category: str
        name: str
        score: int
        payload: str

    await _init_ormdantic_schema(db)
    if seed_rows:
        _seed_sqlite(db_path, seed_rows)
    return _OrmdanticContext(
        directory=directory,
        db_path=db_path,
        db=db,
        model=OrmdanticBenchItem,
    )


async def _prepare_sqlalchemy_context(seed_rows: int) -> _SqlAlchemyContext:
    context = await _prepare_sqlalchemy_raw_context("sqlalchemy-benchmark")
    async with context.engine.begin() as connection:
        await connection.run_sync(SqlAlchemyBase.metadata.drop_all)
        await connection.run_sync(SqlAlchemyBase.metadata.create_all)
    if seed_rows:
        _seed_sqlite(context.db_path, seed_rows)
    return _SqlAlchemyContext(
        directory=context.directory,
        db_path=context.db_path,
        engine=context.engine,
        session_factory=async_sessionmaker(context.engine, expire_on_commit=False),
    )


async def _prepare_sqlmodel_context(seed_rows: int) -> _SqlModelContext:
    context = await _prepare_sqlmodel_raw_context("sqlmodel-benchmark")
    async with context.engine.begin() as connection:
        await connection.run_sync(SQLModel.metadata.drop_all)
        await connection.run_sync(SQLModel.metadata.create_all)
    if seed_rows:
        _seed_sqlite(context.db_path, seed_rows)
    return _SqlModelContext(
        directory=context.directory,
        db_path=context.db_path,
        engine=context.engine,
        session_factory=async_sessionmaker(context.engine, expire_on_commit=False),
    )


async def _prepare_raw_context(prefix: str) -> _OrmdanticContext:
    directory = Path(tempfile.mkdtemp(prefix=f"{prefix}-"))
    db_path = directory / "bench.sqlite3"
    _create_sqlite_schema(db_path)
    return _OrmdanticContext(
        directory=directory, db_path=db_path, db=None, model=BaseModel
    )  # type: ignore[arg-type]


async def _prepare_sqlalchemy_raw_context(prefix: str) -> _SqlAlchemyContext:
    directory = Path(tempfile.mkdtemp(prefix=f"{prefix}-"))
    db_path = directory / "bench.sqlite3"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    _create_sqlite_schema(db_path)
    return _SqlAlchemyContext(
        directory=directory,
        db_path=db_path,
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )


async def _prepare_sqlmodel_raw_context(prefix: str) -> _SqlModelContext:
    directory = Path(tempfile.mkdtemp(prefix=f"{prefix}-"))
    db_path = directory / "bench.sqlite3"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    _create_sqlite_schema(db_path)
    return _SqlModelContext(
        directory=directory,
        db_path=db_path,
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )


async def _init_ormdantic_schema(db: Ormdantic) -> None:
    await db.init()
    await db.drop_all()
    await db.create_all()


def _create_sqlite_schema(db_path: Path) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            DROP TABLE IF EXISTS bench_items;
            CREATE TABLE bench_items (
                id TEXT PRIMARY KEY,
                category TEXT NOT NULL,
                name TEXT NOT NULL,
                score INTEGER NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE INDEX ix_bench_items_category ON bench_items(category);
            CREATE INDEX ix_bench_items_score ON bench_items(score);
            """
        )
        connection.commit()
    finally:
        connection.close()


def _seed_sqlite(db_path: Path, row_count: int) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.executemany(
            """
            INSERT INTO bench_items (id, category, name, score, payload)
            VALUES (?, ?, ?, ?, ?)
            """,
            _row_tuples(row_count),
        )
        connection.commit()
    finally:
        connection.close()


def _sqlite_count(db_path: Path) -> int:
    connection = sqlite3.connect(db_path)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM bench_items").fetchone()[0])
    finally:
        connection.close()


def _insert_statements(
    row_count: int,
    batch_size: int,
) -> Iterator[tuple[str, list[str | int]]]:
    columns_per_row = 5
    for start in range(0, row_count, batch_size):
        current_size = min(batch_size, row_count - start)
        placeholders = ", ".join(["(?, ?, ?, ?, ?)"] * current_size)
        sql = (
            "INSERT INTO bench_items (id, category, name, score, payload) VALUES "
            f"{placeholders}"
        )
        values: list[str | int] = []
        for row in _row_tuples(current_size, start=start, prefix="write"):
            values.extend(row)
        assert len(values) == current_size * columns_per_row
        yield sql, values


def _row_tuples(
    count: int,
    *,
    start: int = 0,
    prefix: str = "item",
) -> Iterable[tuple[str, str, str, int, str]]:
    for offset in range(count):
        index = start + offset
        yield (
            _row_id(index, prefix=prefix),
            f"cat-{index % 10}",
            f"{prefix}-{index}",
            index % 1_000,
            f"payload-{index % 97}-{index % 13}",
        )


def _row_id(index: int, *, prefix: str = "item") -> str:
    return f"{prefix}-{index:08d}"


def _cleanup_directory(directory: Path) -> None:
    shutil.rmtree(directory, ignore_errors=True)

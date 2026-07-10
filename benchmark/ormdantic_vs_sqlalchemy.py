from __future__ import annotations

import asyncio
import shutil
import sqlite3
import statistics
import tempfile
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Awaitable, Callable, Iterable

from pydantic import BaseModel

from benchmark.charts import ORMDANTIC, SQLALCHEMY, BenchmarkMeasurement
from ormdantic import Ormdantic, column
from ormdantic import count as orm_count
from ormdantic import sum as orm_sum

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


ProgressCallback = Callable[[str], None]
SCORE_THRESHOLD = 500


@dataclass(frozen=True)
class BenchmarkConfig:
    """Configuration for the SQLAlchemy comparison run."""

    rows: int = 20_000
    lookup_count: int = 1_000
    iterations: int = 5
    warmups: int = 1
    category: str = "cat-3"

    @property
    def filtered_count(self) -> int:
        """Return rows matching the configured category."""
        return sum(
            1 for row in _row_dicts(self.rows) if row["category"] == self.category
        )

    @property
    def range_count(self) -> int:
        """Return rows matching the score range benchmark."""
        return sum(
            1 for row in _row_dicts(self.rows) if row["score"] >= SCORE_THRESHOLD
        )

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
    db: Ormdantic
    model: type[BaseModel]


@dataclass(frozen=True)
class _SqlAlchemyContext:
    directory: Path
    engine: Any
    session_factory: Any


async def run_benchmarks(
    config: BenchmarkConfig,
    *,
    progress: ProgressCallback | None = None,
) -> list[BenchmarkMeasurement]:
    """Run the Ormdantic vs SQLAlchemy comparison suite."""
    _validate_config(config)
    _ensure_sqlalchemy()

    measurements = []
    cases: tuple[
        tuple[
            str,
            int,
            Callable[[BenchmarkConfig], int],
            Callable[[BenchmarkConfig], Callable[[], Awaitable[Any]]],
            Callable[[BenchmarkConfig], Callable[[], Awaitable[Any]]],
        ],
        ...,
    ] = (
        (
            "count all rows",
            config.rows,
            lambda settings: settings.rows,
            _ormdantic_count_all_case,
            _sqlalchemy_count_all_case,
        ),
        (
            "count filtered",
            config.rows,
            lambda settings: settings.filtered_count,
            _ormdantic_count_case,
            _sqlalchemy_count_case,
        ),
        (
            "count score range",
            config.rows,
            lambda settings: settings.range_count,
            _ormdantic_count_range_case,
            _sqlalchemy_count_range_case,
        ),
        (
            "aggregate filtered",
            config.rows,
            lambda settings: settings.filtered_count,
            _ormdantic_aggregate_case,
            _sqlalchemy_aggregate_case,
        ),
        (
            "point lookup batch",
            config.lookup_count,
            lambda settings: len(settings.lookup_ids),
            _ormdantic_point_lookup_case,
            _sqlalchemy_point_lookup_case,
        ),
    )

    for (
        case_name,
        row_count,
        expected_factory,
        ormdantic_case,
        sqlalchemy_case,
    ) in cases:
        expected = expected_factory(config)
        for orm_name, case_factory in (
            (ORMDANTIC, ormdantic_case),
            (SQLALCHEMY, sqlalchemy_case),
        ):
            if progress is not None:
                progress(f"{case_name}: {orm_name}")
            measurement = await _measure_case(
                case_name=case_name,
                rows=row_count,
                orm_name=orm_name,
                operation_factory=case_factory(config),
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


def _validate_config(config: BenchmarkConfig) -> None:
    if config.rows <= 0:
        raise ValueError("rows must be greater than zero")
    if config.lookup_count <= 0:
        raise ValueError("lookup_count must be greater than zero")
    if config.iterations <= 0:
        raise ValueError("iterations must be greater than zero")
    if config.warmups < 0:
        raise ValueError("warmups cannot be negative")


def _ensure_sqlalchemy() -> None:
    if _SQLALCHEMY_IMPORT_ERROR is not None:
        raise RuntimeError(
            "SQLAlchemy benchmark dependencies are missing. "
            "Run `uv sync --group benchmark` or "
            "`uv run --group benchmark python -m benchmark.run`."
        ) from _SQLALCHEMY_IMPORT_ERROR


async def _measure_case(
    *,
    case_name: str,
    rows: int,
    orm_name: str,
    operation_factory: Callable[[], Awaitable[Any]],
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


@dataclass(frozen=True)
class _Operation:
    run: Callable[[], Awaitable[None]]
    validate: Callable[[], Awaitable[int]]
    cleanup: Callable[[], Awaitable[None]]


def _ormdantic_count_case(config: BenchmarkConfig) -> Callable[[], Awaitable[Any]]:
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


def _ormdantic_count_all_case(config: BenchmarkConfig) -> Callable[[], Awaitable[Any]]:
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


def _sqlalchemy_count_all_case(config: BenchmarkConfig) -> Callable[[], Awaitable[Any]]:
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


def _sqlalchemy_count_case(config: BenchmarkConfig) -> Callable[[], Awaitable[Any]]:
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


def _ormdantic_count_range_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[Any]]:
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
) -> Callable[[], Awaitable[Any]]:
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


def _ormdantic_aggregate_case(config: BenchmarkConfig) -> Callable[[], Awaitable[Any]]:
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


def _sqlalchemy_aggregate_case(config: BenchmarkConfig) -> Callable[[], Awaitable[Any]]:
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


def _ormdantic_point_lookup_case(
    config: BenchmarkConfig,
) -> Callable[[], Awaitable[Any]]:
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
) -> Callable[[], Awaitable[Any]]:
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
        _seed_sqlite(db_path, _row_dicts(seed_rows))
    return _OrmdanticContext(directory=directory, db=db, model=OrmdanticBenchItem)


async def _init_ormdantic_schema(db: Ormdantic) -> None:
    await db.init()
    await db.drop_all()
    await db.create_all()


async def _prepare_sqlalchemy_context(seed_rows: int) -> _SqlAlchemyContext:
    directory = Path(tempfile.mkdtemp(prefix="sqlalchemy-benchmark-"))
    db_path = directory / "bench.sqlite3"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as connection:
        await connection.run_sync(SqlAlchemyBase.metadata.drop_all)
        await connection.run_sync(SqlAlchemyBase.metadata.create_all)
    if seed_rows:
        _seed_sqlite(db_path, _row_dicts(seed_rows))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    return _SqlAlchemyContext(
        directory=directory,
        engine=engine,
        session_factory=session_factory,
    )


def _seed_sqlite(db_path: Path, rows: Iterable[dict[str, str | int]]) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.executemany(
            """
            INSERT INTO bench_items (id, category, name, score, payload)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                (
                    row["id"],
                    row["category"],
                    row["name"],
                    row["score"],
                    row["payload"],
                )
                for row in rows
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _row_dicts(
    count: int,
    *,
    prefix: str = "item",
) -> Iterable[dict[str, str | int]]:
    for index in range(count):
        yield {
            "id": _row_id(index, prefix=prefix),
            "category": f"cat-{index % 10}",
            "name": f"{prefix}-{index}",
            "score": index % 1_000,
            "payload": f"payload-{index % 97}-{index % 13}",
        }


def _row_id(index: int, *, prefix: str = "item") -> str:
    return f"{prefix}-{index:08d}"


def _cleanup_directory(directory: Path) -> None:
    shutil.rmtree(directory, ignore_errors=True)

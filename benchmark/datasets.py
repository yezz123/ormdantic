from __future__ import annotations

from collections.abc import Iterable

SCORE_THRESHOLD = 500
CATEGORY_COUNT = 10
SCORE_CYCLE = 1_000


def row_id(index: int, *, prefix: str = "item") -> str:
    """Return a stable benchmark primary key."""
    return f"{prefix}-{index:08d}"


def row_tuple(index: int, *, prefix: str = "item") -> tuple[str, str, str, int, str]:
    """Return one deterministic benchmark row."""
    return (
        row_id(index, prefix=prefix),
        f"cat-{index % CATEGORY_COUNT}",
        f"{prefix}-{index}",
        index % SCORE_CYCLE,
        f"payload-{index % 97}-{index % 13}",
    )


def row_tuples(
    count: int,
    *,
    start: int = 0,
    prefix: str = "item",
) -> Iterable[tuple[str, str, str, int, str]]:
    """Yield deterministic benchmark rows without holding them all in memory."""
    for offset in range(count):
        yield row_tuple(start + offset, prefix=prefix)


def row_dict(index: int, *, prefix: str = "item") -> dict[str, str | int]:
    """Return one deterministic benchmark row as a mapping."""
    item_id, category, name, score, payload = row_tuple(index, prefix=prefix)
    return {
        "id": item_id,
        "category": category,
        "name": name,
        "score": score,
        "payload": payload,
    }


def row_dicts(
    count: int,
    *,
    start: int = 0,
    prefix: str = "item",
) -> Iterable[dict[str, str | int]]:
    """Yield deterministic benchmark row dictionaries."""
    for offset in range(count):
        yield row_dict(start + offset, prefix=prefix)


def expected_category_count(rows: int, category: str) -> int:
    """Return the count for a deterministic category without iterating rows."""
    try:
        category_index = int(category.removeprefix("cat-"))
    except ValueError:
        return 0
    if category_index < 0 or category_index >= CATEGORY_COUNT:
        return 0
    full_cycles, remainder = divmod(rows, CATEGORY_COUNT)
    return full_cycles + (1 if category_index < remainder else 0)


def expected_score_range_count(rows: int, *, threshold: int = SCORE_THRESHOLD) -> int:
    """Return count of rows whose deterministic score is >= threshold."""
    if threshold <= 0:
        return rows
    if threshold >= SCORE_CYCLE:
        return 0
    full_cycles, remainder = divmod(rows, SCORE_CYCLE)
    per_cycle = SCORE_CYCLE - threshold
    return full_cycles * per_cycle + max(0, remainder - threshold)


def lookup_ids(rows: int, lookup_count: int) -> tuple[str, ...]:
    """Return stable primary keys for point lookup cases."""
    count = min(rows, lookup_count)
    if count <= 0:
        return ()
    step = max(rows // count, 1)
    return tuple(row_id(min(index * step, rows - 1)) for index in range(count))


def batched_rows(
    count: int,
    batch_size: int,
    *,
    prefix: str = "item",
) -> Iterable[list[dict[str, str | int]]]:
    """Yield row dictionaries in bounded batches."""
    for start in range(0, count, batch_size):
        size = min(batch_size, count - start)
        yield list(row_dicts(size, start=start, prefix=prefix))

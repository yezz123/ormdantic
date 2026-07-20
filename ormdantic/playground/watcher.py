"""Dependency-free schema file and database polling."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class WatchReason(str, Enum):
    """Cause of a requested schema refresh."""

    INITIAL = "initial"
    FILES = "files"
    DATABASE = "database"
    RESUMED = "resumed"
    MANUAL = "manual"


@dataclass(frozen=True)
class WatchEvent:
    """One coalesced request for a new refresh generation."""

    generation: int
    reasons: tuple[WatchReason, ...]
    paths: tuple[Path, ...] = ()


Fingerprint = tuple[int, int]


class SchemaWatcher:
    """Poll watched files and live-schema cadence without blocking the event loop."""

    def __init__(
        self,
        *,
        root: Path,
        patterns: tuple[str, ...],
        database_poll_seconds: float,
        debounce_milliseconds: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.root = root.resolve()
        self.patterns = patterns
        self.database_poll_seconds = database_poll_seconds
        self.debounce_seconds = debounce_milliseconds / 1000
        self._clock = clock
        self._fingerprints: dict[Path, Fingerprint] = {}
        self._initialized = False
        self._generation = 0
        self._paused = False
        self._resume_pending = False
        self._stopped = False
        self._pending_paths: set[Path] = set()
        self._pending_since: float | None = None
        self._next_database_poll = clock() + database_poll_seconds

    @property
    def paused(self) -> bool:
        """Whether automatic polling is paused."""
        return self._paused

    @property
    def stopped(self) -> bool:
        """Whether the run loop has been asked to stop."""
        return self._stopped

    def pause(self) -> None:
        """Pause file and database polling."""
        self._paused = True

    def resume(self) -> None:
        """Resume polling and request one fresh generation."""
        if self._paused:
            self._paused = False
            self._resume_pending = True

    async def stop(self) -> None:
        """Request clean run-loop shutdown."""
        self._stopped = True

    def poll(self) -> WatchEvent | None:
        """Perform one non-blocking metadata poll."""
        if self._stopped or self._paused:
            return None
        now = self._clock()
        current = self._scan()
        if not self._initialized:
            self._initialized = True
            self._fingerprints = current
            self._next_database_poll = now + self.database_poll_seconds
            return self._event(
                (WatchReason.INITIAL,),
                tuple(sorted(current)),
            )
        if self._resume_pending:
            self._resume_pending = False
            self._fingerprints = current
            self._pending_paths.clear()
            self._pending_since = None
            self._next_database_poll = now + self.database_poll_seconds
            return self._event((WatchReason.RESUMED,), ())

        changed = _changed_paths(self._fingerprints, current)
        self._fingerprints = current
        if changed:
            self._pending_paths.update(changed)
            if self._pending_since is None:
                self._pending_since = now

        reasons: list[WatchReason] = []
        if (
            self._pending_since is not None
            and now - self._pending_since >= self.debounce_seconds
        ):
            reasons.append(WatchReason.FILES)
        if now >= self._next_database_poll:
            reasons.append(WatchReason.DATABASE)
            while self._next_database_poll <= now:
                self._next_database_poll += self.database_poll_seconds
        if not reasons:
            return None

        paths = (
            tuple(sorted(self._pending_paths)) if WatchReason.FILES in reasons else ()
        )
        if WatchReason.FILES in reasons:
            self._pending_paths.clear()
            self._pending_since = None
        return self._event(tuple(reasons), paths)

    async def run(
        self,
        emit: Callable[[WatchEvent], Awaitable[None]],
    ) -> None:
        """Poll until stopped and publish every coalesced event."""
        while not self._stopped:
            event = self.poll()
            if event is not None:
                await emit(event)
            if self._stopped:
                break
            await asyncio.sleep(self._sleep_interval())

    def _event(
        self,
        reasons: tuple[WatchReason, ...],
        paths: tuple[Path, ...],
    ) -> WatchEvent:
        self._generation += 1
        return WatchEvent(self._generation, reasons, paths)

    def _scan(self) -> dict[Path, Fingerprint]:
        fingerprints: dict[Path, Fingerprint] = {}
        for pattern in self.patterns:
            candidate_pattern = Path(pattern)
            if candidate_pattern.is_absolute():
                continue
            for candidate in self.root.glob(pattern):
                if not candidate.is_file():
                    continue
                try:
                    relative = candidate.resolve().relative_to(self.root)
                except ValueError:
                    continue
                if _is_ignored(relative):
                    continue
                try:
                    stat = candidate.stat()
                except FileNotFoundError:
                    continue
                fingerprints[relative] = (stat.st_mtime_ns, stat.st_size)
        return fingerprints

    def _sleep_interval(self) -> float:
        candidates = [0.1, self.database_poll_seconds]
        if self.debounce_seconds > 0:
            candidates.append(self.debounce_seconds)
        return max(0.01, min(candidates))


def _changed_paths(
    previous: Mapping[Path, Fingerprint],
    current: Mapping[Path, Fingerprint],
) -> set[Path]:
    paths = set(previous) | set(current)
    return {path for path in paths if previous.get(path) != current.get(path)}


def _is_ignored(path: Path) -> bool:
    parts = path.parts
    if any(part in {".git", ".venv", "__pycache__"} for part in parts):
        return True
    return len(parts) >= 2 and parts[0] == ".ormdantic" and parts[1] == "drafts"

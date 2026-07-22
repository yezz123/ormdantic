from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from ormdantic.playground.watcher import SchemaWatcher, WatchReason


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


def watcher(tmp_path: Path, clock: Clock, *, debounce: int = 0) -> SchemaWatcher:
    return SchemaWatcher(
        root=tmp_path,
        patterns=("**/*.py", "/absolute/*.py"),
        database_poll_seconds=1.0,
        debounce_milliseconds=debounce,
        clock=clock,
    )


def test_properties_stop_before_run_and_resume_when_running(tmp_path: Path) -> None:
    clock = Clock()
    instance = watcher(tmp_path, clock)
    assert instance.paused is False
    assert instance.stopped is False
    instance.resume()
    assert instance.poll().reasons == (WatchReason.INITIAL,)  # type: ignore[union-attr]


async def test_run_can_be_stopped_before_first_poll(tmp_path: Path) -> None:
    instance = watcher(tmp_path, Clock())
    await instance.stop()
    await instance.run(lambda _event: asyncio.sleep(0))
    assert instance.stopped is True


def test_file_and_multiple_database_intervals_coalesce(tmp_path: Path) -> None:
    clock = Clock()
    instance = watcher(tmp_path, clock, debounce=100)
    instance.poll()
    model = tmp_path / "models.py"
    model.write_text("one")
    assert instance.poll() is None
    clock.value = 3.5

    event = instance.poll()

    assert event is not None
    assert event.reasons == (WatchReason.FILES, WatchReason.DATABASE)
    assert event.paths == (Path("models.py"),)


def test_scan_skips_directories_ignored_paths_and_external_symlinks(
    tmp_path: Path,
) -> None:
    clock = Clock()
    package = tmp_path / "package.py"
    package.mkdir()
    ignored = tmp_path / ".git"
    ignored.mkdir()
    (ignored / "ignored.py").write_text("")
    outside = tmp_path.parent / "outside.py"
    outside.write_text("")
    link = tmp_path / "outside-link.py"
    link.symlink_to(outside)
    instance = watcher(tmp_path, clock)

    event = instance.poll()

    assert event is not None
    assert event.paths == ()
    assert instance._sleep_interval() == 0.1

    debounced = watcher(tmp_path, clock, debounce=500)
    assert debounced._sleep_interval() == 0.1


@pytest.mark.parametrize(
    "pattern",
    (
        "/absolute/*.py",
        "C:/absolute/*.py",
        "C:drive-relative/*.py",
        "//server/share/*.py",
    ),
)
def test_scan_skips_non_relative_patterns_on_every_platform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pattern: str,
) -> None:
    def unexpected_glob(_root: Path, _pattern: str):
        raise AssertionError("non-relative pattern reached Path.glob")

    monkeypatch.setattr(Path, "glob", unexpected_glob)
    instance = SchemaWatcher(
        root=tmp_path,
        patterns=(pattern,),
        database_poll_seconds=1.0,
        debounce_milliseconds=0,
        clock=Clock(),
    )

    event = instance.poll()

    assert event is not None
    assert event.paths == ()


async def test_run_sleeps_after_empty_poll_then_stops(
    tmp_path: Path,
    monkeypatch,
) -> None:
    instance = watcher(tmp_path, Clock())
    instance.poll()
    slept: list[float] = []

    async def stop_after_sleep(delay: float) -> None:
        slept.append(delay)
        await instance.stop()

    monkeypatch.setattr(asyncio, "sleep", stop_after_sleep)
    await instance.run(lambda _event: asyncio.sleep(0))
    assert slept == [0.1]


def test_scan_tolerates_a_file_disappearing_during_stat(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model = tmp_path / "models.py"
    model.write_text("")
    instance = watcher(tmp_path, Clock())
    original_stat = Path.stat

    def missing_on_stat(path: Path, *args, **kwargs):
        if path.name == "models.py":
            raise FileNotFoundError(path)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", missing_on_stat)
    assert instance.poll().paths == ()  # type: ignore[union-attr]


def test_scan_tolerates_disappearance_after_file_type_check(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model = tmp_path / "models.py"
    model.write_text("")
    instance = watcher(tmp_path, Clock())
    original_stat = Path.stat
    original_is_file = Path.is_file

    def file_before_disappearance(path: Path) -> bool:
        if path.name == "models.py":
            return True
        return original_is_file(path)

    def missing_on_stat(path: Path, *args, **kwargs):
        if path.name == "models.py":
            raise FileNotFoundError(path)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "is_file", file_before_disappearance)
    monkeypatch.setattr(Path, "stat", missing_on_stat)
    assert instance.poll().paths == ()  # type: ignore[union-attr]

from __future__ import annotations

from pathlib import Path

from ormdantic.playground.watcher import SchemaWatcher, WatchReason


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def test_initial_poll_emits_all_matching_files(tmp_path: Path) -> None:
    model = tmp_path / "app" / "models.py"
    model.parent.mkdir()
    model.write_text("class User: ...\n")
    watcher = SchemaWatcher(
        root=tmp_path,
        patterns=("app/**/*.py",),
        database_poll_seconds=5.0,
        debounce_milliseconds=100,
        clock=FakeClock(),
    )

    event = watcher.poll()

    assert event is not None
    assert event.generation == 1
    assert event.reasons == (WatchReason.INITIAL,)
    assert event.paths == (Path("app/models.py"),)


def test_file_changes_are_debounced_and_coalesced(tmp_path: Path) -> None:
    clock = FakeClock()
    first = tmp_path / "app" / "models.py"
    first.parent.mkdir()
    first.write_text("one")
    watcher = SchemaWatcher(
        root=tmp_path,
        patterns=("app/**/*.py",),
        database_poll_seconds=10.0,
        debounce_milliseconds=200,
        clock=clock,
    )
    watcher.poll()

    first.write_text("updated content")
    assert watcher.poll() is None
    second = tmp_path / "app" / "accounts.py"
    second.write_text("created")
    clock.advance(0.1)
    assert watcher.poll() is None
    first.unlink()
    clock.advance(0.11)

    event = watcher.poll()

    assert event is not None
    assert event.generation == 2
    assert event.reasons == (WatchReason.FILES,)
    assert event.paths == (Path("app/accounts.py"), Path("app/models.py"))


def test_watcher_ignores_unmatched_and_generated_paths(tmp_path: Path) -> None:
    clock = FakeClock()
    watcher = SchemaWatcher(
        root=tmp_path,
        patterns=("**/*.py", "migrations/**/*.toml"),
        database_poll_seconds=10.0,
        debounce_milliseconds=0,
        clock=clock,
    )
    watcher.poll()

    (tmp_path / "notes.txt").write_text("ignored")
    cache = tmp_path / "app" / "__pycache__"
    cache.mkdir(parents=True)
    (cache / "models.py").write_text("ignored")
    drafts = tmp_path / ".ormdantic" / "drafts"
    drafts.mkdir(parents=True)
    (drafts / "001.toml").write_text("ignored")

    assert watcher.poll() is None


def test_database_polling_has_an_independent_cadence(tmp_path: Path) -> None:
    clock = FakeClock()
    watcher = SchemaWatcher(
        root=tmp_path,
        patterns=("**/*.py",),
        database_poll_seconds=2.0,
        debounce_milliseconds=100,
        clock=clock,
    )
    watcher.poll()

    clock.advance(1.9)
    assert watcher.poll() is None
    clock.advance(0.1)
    event = watcher.poll()

    assert event is not None
    assert event.reasons == (WatchReason.DATABASE,)
    assert event.generation == 2


def test_pause_and_resume_emit_one_fresh_generation(tmp_path: Path) -> None:
    clock = FakeClock()
    watcher = SchemaWatcher(
        root=tmp_path,
        patterns=("**/*.py",),
        database_poll_seconds=1.0,
        debounce_milliseconds=0,
        clock=clock,
    )
    watcher.poll()
    watcher.pause()
    (tmp_path / "models.py").write_text("changed")
    clock.advance(2.0)

    assert watcher.poll() is None

    watcher.resume()
    event = watcher.poll()

    assert event is not None
    assert event.generation == 2
    assert event.reasons == (WatchReason.RESUMED,)


async def test_run_stops_cleanly_after_stop_is_requested(tmp_path: Path) -> None:
    watcher = SchemaWatcher(
        root=tmp_path,
        patterns=("**/*.py",),
        database_poll_seconds=5.0,
        debounce_milliseconds=0,
    )
    events = []

    async def emit(event: object) -> None:
        events.append(event)
        await watcher.stop()

    await watcher.run(emit)

    assert len(events) == 1
    assert watcher.stopped is True

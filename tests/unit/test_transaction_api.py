from __future__ import annotations

import pytest

from ormdantic import Ormdantic


class RecordingRuntime:
    def __init__(self) -> None:
        self.begin_options = []
        self.commits = 0
        self.rollbacks = 0

    def begin(self, options=None) -> None:
        self.begin_options.append(options)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


async def test_transaction_passes_native_options_to_runtime() -> None:
    db = Ormdantic("sqlite:///:memory:")
    runtime = RecordingRuntime()
    db._runtime = runtime

    async with db.transaction(
        isolation_level="serializable", read_only=True, deferrable=False
    ):
        pass

    assert len(runtime.begin_options) == 1
    assert runtime.begin_options[0] is not None
    assert type(runtime.begin_options[0]).__name__ == "PyTransactionOptions"
    assert runtime.commits == 1
    assert runtime.rollbacks == 0


async def test_session_passes_native_options_to_runtime() -> None:
    db = Ormdantic("sqlite:///:memory:")
    runtime = RecordingRuntime()
    db._runtime = runtime

    async with db.session(isolation_level="read_committed", read_only=True):
        pass

    assert len(runtime.begin_options) == 1
    assert runtime.begin_options[0] is not None
    assert type(runtime.begin_options[0]).__name__ == "PyTransactionOptions"
    assert runtime.commits == 1
    assert runtime.rollbacks == 0


async def test_transaction_rejects_unknown_isolation_level() -> None:
    db = Ormdantic("sqlite:///:memory:")

    with pytest.raises(ValueError, match="unsupported isolation level 'bogus'"):
        async with db.transaction(isolation_level="bogus"):
            pass

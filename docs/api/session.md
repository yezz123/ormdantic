# Session

The session API provides a small async unit-of-work wrapper around an `Ormdantic` database instance.

Use `async with db.session()` for commit-on-success and rollback-on-error flows. The session tracks pending inserts, dirty loaded models, deleted models, relationship additions, and an identity map for repeated loads.

`Session.savepoint(name=None)` opens a nested savepoint. If the block raises, Ormdantic rolls back to the database savepoint and restores the session's pending, dirty, deleted, identity, and snapshot state.

If `flush()` fails, the session restores the pre-flush unit-of-work state and requires `rollback()` before more work is accepted.

::: ormdantic.session.Session

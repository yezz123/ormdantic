# Events

Events let applications register sync or async handlers around ORM lifecycle operations.

Common transaction and unit-of-work events:

| Event | When it fires |
| --- | --- |
| `before_flush`, `after_flush` | Around `Session.flush()`. |
| `before_begin`, `after_begin` | Around transaction begin. |
| `before_commit`, `after_commit` | Around transaction commit. |
| `before_rollback`, `after_rollback` | Around transaction rollback. |
| `before_savepoint`, `after_savepoint` | Around savepoint creation. |
| `before_release_savepoint`, `after_release_savepoint` | Around successful savepoint release. |
| `before_rollback_to_savepoint`, `after_rollback_to_savepoint` | Around savepoint rollback. |

`after_*` transaction events include `duration_ms` and `error`; `error` is `None` when the operation succeeds.

::: ormdantic.events.EventHandler
::: ormdantic.events.EventRegistry

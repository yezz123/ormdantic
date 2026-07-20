---
title: Watch model and database schemas
contentType: Conceptual
---

# Watch model and database schemas

The playground compares two normalized snapshots: registered Python models and the connected live database. File events and database polling create numbered refresh generations, so an older result cannot replace newer state.

## Understand each refresh source

Model inspection imports the configured target in a child Python process. Import output, exceptions, and malformed targets become diagnostics without polluting the TUI process.

Live reflection runs through Ormdantic's migration manager in a worker thread. The event loop remains responsive while a native driver call runs.

## Follow a refresh generation

A refresh follows this sequence:

1. The watcher emits a reason and generation number
2. Model inspection and live reflection start together
3. The migration planner compares available fresh snapshots
4. The controller accepts the result when no newer generation exists
5. Every visible screen receives the immutable state snapshot

The Drift screen shows the accepted generation. The status bar shows `watching`, `paused`, the environment, the safe connection source, and the detected dialect.

## Interpret partial and stale state

The playground retains the last valid snapshot when one source fails. It marks the combined schema state stale and blocks generation or mutation that depends on current review.

Use the diagnostics to distinguish failures:

- **Model unavailable**: fix the import target or import-time exception
- **Database unavailable**: check the URL source, driver, network, and permissions
- **Planning unavailable**: check whether the dialect supports the reported change
- **History unavailable**: check read permission on `ormdantic_migrations`

The Schemas screen still renders the available side during partial refresh.

## Control file watching

Press `p` or select **Pause watcher** in Settings. Pausing stops both file polling and periodic database checks.

Resume with the same action. Resume emits one new generation before regular polling continues.

## Change watched files safely

The watcher coalesces changes during `debounce_milliseconds`. One refresh includes every path changed during that window.

Dirty editor documents are not replaced by an external migration-file event. Save or discard your editor changes, then refresh to load the disk version.

## Refresh manually

Press `r` when you need a new generation before the next poll. Refreshes run serially, so an operation cannot publish completion against an older history snapshot.

## Understand cancellation

Cancelling a model inspection terminates and joins its child process. Cancelling a TUI database worker discards its result, but a native database call already running in a worker thread may finish in the background. The playground never reports that driver-side work was cancelled.

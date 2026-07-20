"""Textual application entry point for the Ormdantic playground."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from textual import events, on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import ContentSwitcher, Footer, Static
from textual.worker import WorkerCancelled

from ormdantic.playground.config import (
    EffectiveConfig,
    discover_config,
    load_config,
    resolve_database_url,
    write_config_source,
)
from ormdantic.playground.controller import (
    ControllerActionOutcome,
    PlaygroundController,
)
from ormdantic.playground.messages import Navigate
from ormdantic.playground.safety import ActionRequest, PreflightContext
from ormdantic.playground.screens.confirmations import ConfirmQuitScreen
from ormdantic.playground.screens.drift import DriftView
from ormdantic.playground.screens.editor import EditorView
from ormdantic.playground.screens.help import HelpView
from ormdantic.playground.screens.history import HistoryView
from ormdantic.playground.screens.migrations import MigrationsView
from ormdantic.playground.screens.overview import OverviewView
from ormdantic.playground.screens.schema import SchemaView
from ormdantic.playground.screens.settings import SettingsView
from ormdantic.playground.screens.setup import SetupScreen
from ormdantic.playground.screens.workflows import (
    GenerateDialog,
    RepairDialog,
    SquashDialog,
)
from ormdantic.playground.state import PlaygroundState
from ormdantic.playground.watcher import SchemaWatcher, WatchEvent, WatchReason
from ormdantic.playground.widgets.action_dialog import ActionDialog
from ormdantic.playground.widgets.navigation import NavigationRail
from ormdantic.playground.widgets.status_bar import StatusBar


class PlaygroundApp(App[None]):
    """Interactive schema watching and migration workspace."""

    CSS_PATH = "styles.tcss"
    TITLE = "Ormdantic Playground"
    BINDINGS = [
        Binding("r", "refresh", "Refresh"),
        Binding("p", "pause_watcher", "Pause watcher"),
        Binding("ctrl+s", "save", "Save"),
        Binding("g", "generate", "Generate"),
        Binding("a", "apply_migration", "Apply"),
        Binding("b", "rollback_migration", "Rollback"),
        Binding("?", "help", "Help"),
        Binding("q", "quit", "Quit"),
    ]

    def __init__(
        self,
        *,
        config: EffectiveConfig | None,
        setup_path: Path | None = None,
        controller: PlaygroundController | None = None,
        auto_watch: bool | None = None,
        watcher_factory: Any = SchemaWatcher,
    ) -> None:
        super().__init__()
        self.config = config
        self.setup_path = (setup_path or Path.cwd() / "ormdantic.toml").resolve()
        self.controller = controller or (
            PlaygroundController(config) if config is not None else None
        )
        self._auto_watch = controller is None if auto_watch is None else auto_watch
        self._watcher_factory = watcher_factory
        self._watcher: SchemaWatcher | None = None
        self._watcher_worker: Any | None = None
        self._unsubscribe: Any | None = None
        self._shutting_down = False

    @classmethod
    def from_cli(
        cls,
        *,
        config_path: Path | None,
        environment: str | None,
        target: str | None,
        migrations_dir: Path | None,
    ) -> PlaygroundApp:
        """Resolve CLI overrides without importing project models."""
        path = config_path.resolve() if config_path is not None else None
        discovered = (
            path if path is not None and path.is_file() else discover_config(Path.cwd())
        )
        if discovered is None:
            return cls(config=None, setup_path=path)
        config = load_config(
            discovered,
            environment=environment,
            target=target,
            migrations_dir=migrations_dir,
        )
        return cls(config=config)

    def compose(self) -> ComposeResult:
        state = self._state
        with Vertical(id="app-frame"):
            with Horizontal(id="app-header"):
                yield Static("ORMDANTIC", id="brand")
                yield Static("PLAYGROUND", id="product")
                yield Static("Schema intelligence for async applications", id="tagline")
                yield Static(state.environment, id="environment-badge")
            with Horizontal(id="app-body"):
                yield NavigationRail(id="navigation")
                yield ContentSwitcher(
                    OverviewView(state),
                    SchemaView(state),
                    DriftView(state),
                    MigrationsView(state),
                    EditorView(state),
                    HistoryView(state),
                    SettingsView(state, self.config),
                    HelpView(),
                    id="content",
                    initial="view-overview",
                )
            yield StatusBar(state, id="global-status")
            yield Footer()

    def on_mount(self) -> None:
        self._update_layout(self.size.width)
        if self.controller is not None:
            self._subscribe_controller(self.controller)
            self._start_watcher()
        if self.config is None:
            self.push_screen(
                SetupScreen(self.setup_path),
                callback=self._setup_complete,
            )

    async def on_unmount(self) -> None:
        self._shutting_down = True
        if self._unsubscribe is not None:
            self._unsubscribe()
        if self._watcher is not None:
            await self._watcher.stop()
        if self._watcher_worker is not None:
            self._watcher_worker.cancel()
            try:
                await self._watcher_worker.wait()
            except WorkerCancelled:
                pass

    def on_resize(self, event: events.Resize) -> None:
        self._update_layout(event.size.width)

    @on(Navigate)
    def navigate(self, message: Navigate) -> None:
        self._show_section(message.section)

    def action_help(self) -> None:
        self._show_section("help")

    async def action_quit(self) -> None:
        dirty = self.controller is not None and any(
            document.dirty for document in self.controller.workspace.documents
        )
        if dirty:
            self.push_screen(ConfirmQuitScreen(), callback=self._quit_complete)
            return
        self.exit()

    def action_refresh(self) -> None:
        if self.controller is None:
            return
        self.run_worker(
            self.controller.refresh(),
            name="schema-refresh",
            group="schema-refresh",
            exclusive=True,
        )

    def action_pause_watcher(self) -> None:
        if self.controller is None:
            return
        paused = not self.controller.state.watcher_paused
        if self._watcher is not None:
            if self._watcher.paused:
                self._watcher.resume()
            else:
                self._watcher.pause()
            paused = self._watcher.paused
        self.controller.state = replace(
            self.controller.state,
            watcher_paused=paused,
        )
        self._state_changed(self.controller.state)

    def action_save(self) -> None:
        if self.controller is None or self.controller.active_document is None:
            self.notify("Select an editable TOML migration first.", severity="warning")
            return
        try:
            self.controller.save_active()
        except Exception as exc:
            self.notify(str(exc), severity="error")

    def action_generate(self) -> None:
        self._show_section("drift")
        self.open_generate_dialog()

    def action_apply_migration(self) -> None:
        self._show_section("migrations")
        self._open_action_dialog("apply")

    def action_rollback_migration(self) -> None:
        self._show_section("history")
        self._open_action_dialog("rollback")

    def switch_environment(self, name: str) -> None:
        """Select another configured environment without retaining old state."""
        if self.config is None:
            raise ValueError("complete playground setup before changing environment")
        selected = load_config(self.config.path, environment=name)
        controller = PlaygroundController(selected)
        self.config = selected
        self.controller = controller
        settings = self.query_one("#view-settings", SettingsView)
        settings.update_config(selected)
        self._subscribe_controller(controller)
        self._restart_watcher()

    def save_config_source(self, source: str) -> None:
        """Validate, atomically save, and reload the embedded project TOML."""
        if self.config is None:
            raise ValueError("complete playground setup before saving configuration")
        environment = self.config.environment.name
        write_config_source(self.config.path, source)
        self.switch_environment(environment)

    def open_generate_dialog(self) -> None:
        """Open generation review for the currently published drift."""
        if self.controller is None:
            return
        state = self.controller.state
        if (
            state.schema.stale
            or state.schema.model_snapshot is None
            or state.schema.live_snapshot is None
            or not state.schema.forward_sql
        ):
            self.notify(
                "Refresh a non-empty schema drift before generating.",
                severity="warning",
            )
            return
        self.push_screen(
            GenerateDialog(self.controller), callback=self._generate_complete
        )

    def open_repair_dialog(self, revision: str) -> None:
        """Open an exact dirty-history repair review."""
        if self.controller is None:
            return
        try:
            request = self.controller.build_repair_request(
                revision,
                database_name=self._database_name(),
            )
            entry = next(
                item
                for item in self.controller.state.migrations.history
                if item.revision == revision
            )
            context = self._workflow_preflight(
                request, revision_state_valid=entry.dirty
            )
        except Exception as exc:
            self.notify(str(exc), severity="error")
            return
        self.push_screen(
            RepairDialog(
                controller=self.controller,
                request=request,
                context=context,
                status=entry.status,
            ),
            callback=self._action_complete,
        )

    def open_squash_dialog(self, paths: tuple[Path, ...]) -> None:
        """Open a checksum-bound review for all selected pending artifacts."""
        if self.controller is None or len(paths) < 2:
            self.notify("Select at least two pending migrations.", severity="warning")
            return
        default_revision = f"squashed_{paths[0].stem}_{paths[-1].stem}"
        try:
            request = self.controller.build_squash_request(
                paths,
                default_revision,
                database_name=self._database_name(),
            )
            context = self._workflow_preflight(
                request,
                revision_state_valid=True,
            )
        except Exception as exc:
            self.notify(str(exc), severity="error")
            return
        self.push_screen(
            SquashDialog(
                controller=self.controller,
                paths=paths,
                database_name=request.database_name,
                context=context,
                default_revision=default_revision,
            ),
            callback=self._action_complete,
        )

    def _show_section(self, section: str) -> None:
        switcher = self.query_one(ContentSwitcher)
        view_id = f"view-{section}"
        if self.query(f"#{view_id}"):
            switcher.current = view_id

    def _setup_complete(self, config: EffectiveConfig | None) -> None:
        if config is None:
            self.exit()
            return
        self.config = config
        self.controller = PlaygroundController(config)
        self.query_one("#view-settings", SettingsView).update_config(config)
        self._subscribe_controller(self.controller)
        self._start_watcher()
        self.query_one(ContentSwitcher).current = "view-overview"

    def _quit_complete(self, confirmed: bool | None) -> None:
        if confirmed:
            self.exit()

    def _open_action_dialog(self, action: str) -> None:
        controller = self.controller
        if controller is None or controller.active_document is None:
            self.notify("Select a migration revision first.", severity="warning")
            return
        try:
            request = controller.build_action_request(
                action,
                database_name=self._database_name(),
            )
            context = self._preflight_context(action)
        except Exception as exc:
            self.notify(str(exc), severity="error")
            return
        self.push_screen(
            ActionDialog(
                controller=controller,
                request=request,
                context=context,
            ),
            callback=self._action_complete,
        )

    def _action_complete(self, outcome: ControllerActionOutcome | None) -> None:
        if outcome is None:
            return
        if outcome.error is not None:
            self.notify(outcome.error.message, severity="error")
        elif outcome.executed:
            self.notify("Migration operation completed after a fresh schema check.")

    def _generate_complete(self, path: Path | None) -> None:
        if path is None:
            return
        self._show_section("editor")
        self.notify(f"Generated {path.name}; review and save before applying.")

    def _database_name(self) -> str:
        if self.config is None:
            return "database"
        resolved = resolve_database_url(self.config.environment)
        parsed = urlsplit(resolved.value)
        name = parsed.path.rstrip("/").rsplit("/", 1)[-1]
        if name:
            return name
        return parsed.hostname or self.config.environment.name

    def _preflight_context(self, action: str) -> PreflightContext:
        controller = self.controller
        if controller is None or controller.active_document is None:
            raise ValueError("select a migration revision first")
        document = controller.active_document
        artifact = document.artifact
        checksum_valid = artifact is not None
        if artifact is not None:
            try:
                artifact.validate_checksum()
            except ValueError:
                checksum_valid = False
        state = controller.state
        artifact_status = next(
            (
                item.status
                for item in state.migrations.artifacts
                if item.path == document.path
            ),
            "invalid",
        )
        revision_state_valid = (
            artifact_status != "applied"
            if action == "apply"
            else artifact_status == "applied"
        )
        operations = ()
        if artifact is not None:
            operations = (
                artifact.operations
                if action == "apply"
                else artifact.rollback_operations
            )
        return PreflightContext(
            connected=state.schema.live_snapshot is not None,
            target_imported=state.schema.model_snapshot is not None,
            dialect=state.dialect,
            artifact_dialect=artifact.dialect if artifact is not None else None,
            history_readable=not any(
                diagnostic.code == "database.history_failed"
                for diagnostic in state.diagnostics
            ),
            history_dirty=state.migrations.dirty,
            artifact_valid=artifact is not None,
            checksum_valid=checksum_valid,
            dependencies_valid=not any(
                diagnostic.code.startswith("artifact.depend")
                for diagnostic in document.diagnostics
            ),
            revision_state_valid=revision_state_valid,
            rollback_available=bool(
                artifact is not None and artifact.rollback_operations
            ),
            snapshot_current=(
                not state.schema.stale
                and state.schema.model_snapshot is not None
                and state.schema.live_snapshot is not None
            ),
            operations_supported=bool(operations)
            and all(operation.sql.strip() for operation in operations),
            operation_running=state.operation.running,
            editor_valid=artifact is not None,
            editor_dirty=document.dirty,
            sql_present=bool(operations),
            destructive_reviewed=(
                artifact is not None
                and not any(operation.destructive for operation in operations)
            ),
            artifact_checksum=artifact.checksum if artifact is not None else None,
            generation=state.generation,
        )

    def _workflow_preflight(
        self,
        request: ActionRequest,
        *,
        revision_state_valid: bool,
    ) -> PreflightContext:
        controller = self.controller
        if controller is None:
            raise ValueError("playground is not configured")
        state = controller.state
        return PreflightContext(
            connected=state.schema.live_snapshot is not None,
            target_imported=state.schema.model_snapshot is not None,
            dialect=state.dialect,
            artifact_dialect=state.dialect,
            history_readable=not any(
                diagnostic.code == "database.history_failed"
                for diagnostic in state.diagnostics
            ),
            history_dirty=state.migrations.dirty,
            artifact_valid=True,
            checksum_valid=True,
            dependencies_valid=True,
            revision_state_valid=revision_state_valid,
            rollback_available=True,
            snapshot_current=(
                not state.schema.stale
                and state.schema.model_snapshot is not None
                and state.schema.live_snapshot is not None
            ),
            operations_supported=True,
            operation_running=state.operation.running,
            editor_valid=True,
            editor_dirty=any(
                document.dirty for document in controller.workspace.documents
            ),
            sql_present=bool(request.sql),
            destructive_reviewed=True,
            artifact_checksum=request.artifact_checksum,
            generation=state.generation,
        )

    def _subscribe_controller(self, controller: PlaygroundController) -> None:
        if self._unsubscribe is not None:
            self._unsubscribe()
        self._unsubscribe = controller.subscribe(self._state_changed)
        self._state_changed(controller.state)

    def _start_watcher(self) -> None:
        if (
            not self._auto_watch
            or self.config is None
            or self.controller is None
            or self._watcher is not None
        ):
            return
        project = self.config.project
        self._watcher = self._watcher_factory(
            root=self.config.root,
            patterns=project.watch,
            database_poll_seconds=project.database_poll_seconds,
            debounce_milliseconds=project.debounce_milliseconds,
        )
        self._watcher_worker = self.run_worker(
            self._watcher.run(self._watch_event),
            name="schema-watcher",
            group="schema-watcher",
            exclusive=True,
        )

    def _restart_watcher(self) -> None:
        if self._watcher_worker is not None:
            self._watcher_worker.cancel()
        if self._watcher is not None:
            self.run_worker(
                self._watcher.stop(),
                name="stop-schema-watcher",
                group="schema-watcher-stop",
                exclusive=True,
            )
        self._watcher = None
        self._watcher_worker = None
        self._start_watcher()

    async def _watch_event(self, event: WatchEvent) -> None:
        controller = self.controller
        if controller is None:
            return
        if WatchReason.FILES in event.reasons and not any(
            document.dirty for document in controller.workspace.documents
        ):
            controller.reload_workspace()
        await controller.refresh(generation=event.generation)

    def _state_changed(self, state: PlaygroundState) -> None:
        if self._shutting_down or not self.query("#global-status"):
            return
        self.query_one("#global-status", StatusBar).update_state(state)
        self.query_one("#environment-badge", Static).update(state.environment)
        for widget in self.query(".state-aware"):
            update_state = getattr(widget, "update_state", None)
            if update_state is not None:
                update_state(state)

    def _update_layout(self, width: int) -> None:
        body = self.query_one("#app-body")
        body.set_class(width < 100, "compact")

    @property
    def _state(self) -> PlaygroundState:
        if self.controller is not None:
            return self.controller.state
        return PlaygroundState(environment="setup required")

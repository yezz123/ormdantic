use ormdantic_core::{EventKind, EventPayload};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::sync::Mutex;

#[pyclass]
#[derive(Default)]
pub(crate) struct PyEventBridge {
    events: Mutex<Vec<EventPayload>>,
}

type PyEventRecord = (String, Option<String>, Option<String>);

#[pymethods]
impl PyEventBridge {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn emit(&self, kind: &str, target: Option<String>, message: Option<String>) -> PyResult<()> {
        let mut payload = EventPayload::new(parse_event_kind(kind)?);
        if let Some(target) = target {
            payload = payload.with_target(target);
        }
        if let Some(message) = message {
            payload = payload.with_message(message);
        }
        self.events
            .lock()
            .map_err(|_| PyValueError::new_err("event bridge lock poisoned"))?
            .push(payload);
        Ok(())
    }

    fn events(&self) -> PyResult<Vec<PyEventRecord>> {
        Ok(self
            .events
            .lock()
            .map_err(|_| PyValueError::new_err("event bridge lock poisoned"))?
            .iter()
            .map(|event| {
                (
                    event_kind_name(event.kind()).to_string(),
                    event.target().map(ToString::to_string),
                    event.message().map(ToString::to_string),
                )
            })
            .collect())
    }
}

fn parse_event_kind(value: &str) -> PyResult<EventKind> {
    match value.to_ascii_lowercase().replace(['-', ' '], "_").as_str() {
        "before_execute" => Ok(EventKind::BeforeExecute),
        "after_execute" => Ok(EventKind::AfterExecute),
        "before_commit" => Ok(EventKind::BeforeCommit),
        "after_commit" => Ok(EventKind::AfterCommit),
        "after_rollback" => Ok(EventKind::AfterRollback),
        "before_flush" => Ok(EventKind::BeforeFlush),
        "after_flush" => Ok(EventKind::AfterFlush),
        "before_migration" => Ok(EventKind::BeforeMigration),
        "after_migration" => Ok(EventKind::AfterMigration),
        "before_reflection" => Ok(EventKind::BeforeReflection),
        "after_reflection" => Ok(EventKind::AfterReflection),
        other => Err(PyValueError::new_err(format!(
            "unsupported event kind '{other}'"
        ))),
    }
}

fn event_kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::BeforeExecute => "before_execute",
        EventKind::AfterExecute => "after_execute",
        EventKind::BeforeCommit => "before_commit",
        EventKind::AfterCommit => "after_commit",
        EventKind::AfterRollback => "after_rollback",
        EventKind::BeforeFlush => "before_flush",
        EventKind::AfterFlush => "after_flush",
        EventKind::BeforeMigration => "before_migration",
        EventKind::AfterMigration => "after_migration",
        EventKind::BeforeReflection => "before_reflection",
        EventKind::AfterReflection => "after_reflection",
    }
}

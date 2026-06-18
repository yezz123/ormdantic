use ormdantic_core::{DeferrableMode, IsolationLevel, TransactionAccessMode, TransactionOptions};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyclass(from_py_object)]
#[derive(Clone, Default)]
pub(crate) struct PyTransactionOptions {
    isolation_level: Option<String>,
    read_only: bool,
    deferrable: Option<bool>,
}

#[pymethods]
impl PyTransactionOptions {
    #[new]
    #[pyo3(signature = (isolation_level=None, read_only=false, deferrable=None))]
    fn new(isolation_level: Option<String>, read_only: bool, deferrable: Option<bool>) -> Self {
        Self {
            isolation_level,
            read_only,
            deferrable,
        }
    }
}

impl PyTransactionOptions {
    pub(crate) fn to_rust_options(&self) -> PyResult<TransactionOptions> {
        let mut options = TransactionOptions::new();
        if let Some(isolation_level) = &self.isolation_level {
            options = options.with_isolation_level(parse_isolation_level(isolation_level)?);
        }
        if self.read_only {
            options = options.with_access_mode(TransactionAccessMode::ReadOnly);
        }
        if let Some(deferrable) = self.deferrable {
            options = options.with_deferrable_mode(if deferrable {
                DeferrableMode::Deferrable
            } else {
                DeferrableMode::NotDeferrable
            });
        }
        Ok(options)
    }
}

fn parse_isolation_level(value: &str) -> PyResult<IsolationLevel> {
    match value.replace(['-', ' '], "_").to_ascii_lowercase().as_str() {
        "read_uncommitted" => Ok(IsolationLevel::ReadUncommitted),
        "read_committed" => Ok(IsolationLevel::ReadCommitted),
        "repeatable_read" => Ok(IsolationLevel::RepeatableRead),
        "serializable" => Ok(IsolationLevel::Serializable),
        "snapshot" => Ok(IsolationLevel::Snapshot),
        other => Err(PyValueError::new_err(format!(
            "unsupported isolation level '{other}'"
        ))),
    }
}

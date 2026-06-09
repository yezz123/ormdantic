use ormdantic_core::IdentityKey;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;

#[pyclass]
#[derive(Default)]
pub(crate) struct PySessionRuntime {
    identities: Mutex<Vec<IdentityKey>>,
    dirty_snapshots: Mutex<HashMap<String, Vec<String>>>,
    flush_order: Mutex<Vec<String>>,
    cascades: Mutex<Vec<String>>,
}

#[pymethods]
impl PySessionRuntime {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn record_identity(&self, model_key: &str, primary_key: Vec<String>) -> PyResult<()> {
        self.identities
            .lock()
            .map_err(|_| PyValueError::new_err("session identity lock poisoned"))?
            .push(IdentityKey::new(model_key, primary_key));
        Ok(())
    }

    fn identity_keys(&self) -> PyResult<Vec<(String, Vec<String>)>> {
        Ok(self
            .identities
            .lock()
            .map_err(|_| PyValueError::new_err("session identity lock poisoned"))?
            .iter()
            .map(|key| (key.model_key().to_string(), key.primary_key().to_vec()))
            .collect())
    }

    fn mark_dirty(&self, identity: &str, fields: Vec<String>) -> PyResult<()> {
        self.dirty_snapshots
            .lock()
            .map_err(|_| PyValueError::new_err("session dirty lock poisoned"))?
            .insert(identity.to_string(), fields);
        Ok(())
    }

    fn dirty_keys(&self) -> PyResult<Vec<String>> {
        Ok(self
            .dirty_snapshots
            .lock()
            .map_err(|_| PyValueError::new_err("session dirty lock poisoned"))?
            .keys()
            .cloned()
            .collect())
    }

    fn set_flush_order(&self, flush_order: Vec<String>) -> PyResult<()> {
        *self
            .flush_order
            .lock()
            .map_err(|_| PyValueError::new_err("session flush lock poisoned"))? = flush_order;
        Ok(())
    }

    fn flush_order(&self) -> PyResult<Vec<String>> {
        Ok(self
            .flush_order
            .lock()
            .map_err(|_| PyValueError::new_err("session flush lock poisoned"))?
            .clone())
    }

    fn record_cascade(&self, cascade_path: &str) -> PyResult<()> {
        self.cascades
            .lock()
            .map_err(|_| PyValueError::new_err("session cascade lock poisoned"))?
            .push(cascade_path.to_string());
        Ok(())
    }

    fn cascade_paths(&self) -> PyResult<Vec<String>> {
        Ok(self
            .cascades
            .lock()
            .map_err(|_| PyValueError::new_err("session cascade lock poisoned"))?
            .clone())
    }
}

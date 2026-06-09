mod bindings;
mod database;
mod ddl;
mod events;
mod hydration;
mod migrations;
mod query;
mod runtime;
mod schema;
mod session;
mod table_handle;
mod transactions;
mod utils;

use pyo3::prelude::*;

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    bindings::register(m)
}

#[pymodule]
fn _ormdantic(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register_module(m)
}

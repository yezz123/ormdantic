use crate::database::PyDatabase;
use crate::events::PyEventBridge;
use crate::session::PySessionRuntime;
use crate::table_handle::PyTableHandle;
use crate::transactions::PyTransactionOptions;
use crate::{ddl, hydration, query, runtime, schema, utils};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<runtime::PyNativeConnection>()?;
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyTableHandle>()?;
    m.add_class::<PyTransactionOptions>()?;
    m.add_class::<PySessionRuntime>()?;
    m.add_class::<PyEventBridge>()?;
    m.add_function(wrap_pyfunction!(hydration::hydrate_flat, m)?)?;
    m.add_function(wrap_pyfunction!(hydration::hydrate_joined, m)?)?;
    m.add_function(wrap_pyfunction!(hydration::plan_result_shape, m)?)?;
    m.add_function(wrap_pyfunction!(schema::validate_schema_tables, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_select_pk, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_find_many, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_joined_find_many, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_count, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_insert, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_update, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_upsert, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_delete_pk, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_expression_query, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_typed_expression_query, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_typed_update_query, m)?)?;
    m.add_function(wrap_pyfunction!(schema::compile_schema_diff, m)?)?;
    m.add_function(wrap_pyfunction!(runtime::reflect_schema, m)?)?;
    m.add_function(wrap_pyfunction!(query::compile_selectin_plan, m)?)?;
    m.add_function(wrap_pyfunction!(hydration::execute_selectin_load, m)?)?;
    m.add_function(wrap_pyfunction!(runtime::execute_native, m)?)?;
    m.add_function(wrap_pyfunction!(query::normalize_filters, m)?)?;
    m.add_function(wrap_pyfunction!(utils::snake_case, m)?)?;
    m.add_function(wrap_pyfunction!(utils::sql_value, m)?)?;
    m.add_function(wrap_pyfunction!(ddl::compile_create_table_sql, m)?)?;
    m.add_function(wrap_pyfunction!(ddl::compile_drop_table_sql, m)?)?;
    m.add_function(wrap_pyfunction!(runtime::runtime_capabilities, m)?)?;
    Ok(())
}

use crate::ddl::{create_table_sql, drop_table_sql};
use crate::migrations::{
    applied_revisions_sql, ensure_revision_table, py_operations_to_db, run_migration,
    MigrationDirection,
};
use crate::runtime::{
    columns_sql, db_value_to_bool, db_value_to_string, foreign_keys_sql, indexes_sql,
    table_names_sql,
};
use crate::schema::RuntimeTableSpec;
use crate::table_handle::{PyTableHandle, RuntimeTable};
use crate::transactions::PyTransactionOptions;
use ormdantic_dialects::AnyDialect;
use ormdantic_engine::{DbValue, NativeConnection};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[pyclass]
pub(crate) struct PyDatabase {
    url: String,
    connection: Arc<Mutex<NativeConnection>>,
    tables: Arc<HashMap<String, RuntimeTable>>,
    table_order: Arc<Vec<String>>,
}

#[pymethods]
impl PyDatabase {
    #[new]
    fn new(url: &str, tables: Vec<RuntimeTableSpec>) -> PyResult<Self> {
        let mut table_order = Vec::new();
        let tables = tables
            .into_iter()
            .map(
                |(
                    model_key,
                    table,
                    primary_key,
                    columns,
                    indexes,
                    unique_constraints,
                    relationships,
                )| {
                    table_order.push(model_key.clone());
                    (
                        model_key.clone(),
                        RuntimeTable {
                            table,
                            primary_key,
                            columns,
                            indexes,
                            unique_constraints,
                            relationships,
                        },
                    )
                },
            )
            .collect::<HashMap<_, _>>();
        Ok(Self {
            url: url.to_string(),
            connection: Arc::new(Mutex::new(
                NativeConnection::open(url)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )),
            tables: Arc::new(tables),
            table_order: Arc::new(table_order),
        })
    }

    fn table(&self, model_key: &str) -> PyResult<PyTableHandle> {
        let table = self
            .tables
            .get(model_key)
            .or_else(|| self.tables.values().find(|table| table.table == model_key))
            .cloned()
            .ok_or_else(|| PyValueError::new_err(format!("unknown table '{model_key}'")))?;
        Ok(PyTableHandle {
            url: self.url.clone(),
            connection: Arc::clone(&self.connection),
            tables: Arc::clone(&self.tables),
            table,
        })
    }

    fn create_all(&self) -> PyResult<()> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        for model_key in self.table_order.iter() {
            let Some(table) = self.tables.get(model_key) else {
                continue;
            };
            for sql in create_table_sql(
                &self.url,
                &table.table,
                table.columns.clone(),
                table.indexes.clone(),
                table.unique_constraints.clone(),
            )? {
                connection
                    .execute(&sql, &[])
                    .map_err(|error| PyValueError::new_err(error.to_string()))?;
            }
        }
        Ok(())
    }

    fn drop_all(&self) -> PyResult<()> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        for model_key in self.table_order.iter().rev() {
            let Some(table) = self.tables.get(model_key) else {
                continue;
            };
            let sql = drop_table_sql(&self.url, &table.table)?;
            connection
                .execute(&sql, &[])
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        }
        Ok(())
    }

    fn table_names(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        let dialect = AnyDialect::parse(connection.dialect())
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let result = connection
            .execute(table_names_sql(&dialect).as_str(), &[])
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let names = PyList::empty(py);
        for row in result.rows() {
            if let Some(value) = row.first().and_then(db_value_to_string) {
                names.append(value)?;
            }
        }
        Ok(names.into_any().unbind())
    }

    fn columns(&self, py: Python<'_>, table: &str) -> PyResult<Py<PyAny>> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        let dialect = AnyDialect::parse(connection.dialect())
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let result = connection
            .execute(
                columns_sql(&dialect).as_str(),
                &[DbValue::Text(table.to_string())],
            )
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let columns = PyList::empty(py);
        for row in result.rows() {
            let column = PyDict::new(py);
            column.set_item("name", row.first().and_then(db_value_to_string))?;
            column.set_item("type", row.get(1).and_then(db_value_to_string))?;
            column.set_item("nullable", row.get(2).map_or(true, db_value_to_bool))?;
            column.set_item("default", row.get(3).and_then(db_value_to_string))?;
            column.set_item("primary_key", row.get(4).is_some_and(db_value_to_bool))?;
            columns.append(column)?;
        }
        Ok(columns.into_any().unbind())
    }

    fn indexes(&self, py: Python<'_>, table: &str) -> PyResult<Py<PyAny>> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        let dialect = AnyDialect::parse(connection.dialect())
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let result = connection
            .execute(
                indexes_sql(&dialect).as_str(),
                &[DbValue::Text(table.to_string())],
            )
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let indexes = PyList::empty(py);
        for row in result.rows() {
            let Some(name) = row.first().and_then(db_value_to_string) else {
                continue;
            };
            if name.starts_with("sqlite_autoindex_") {
                continue;
            }
            let index = PyDict::new(py);
            index.set_item("name", name)?;
            index.set_item("unique", row.get(1).is_some_and(db_value_to_bool))?;
            indexes.append(index)?;
        }
        Ok(indexes.into_any().unbind())
    }

    fn foreign_keys(&self, py: Python<'_>, table: &str) -> PyResult<Py<PyAny>> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        let dialect = AnyDialect::parse(connection.dialect())
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let result = connection
            .execute(
                foreign_keys_sql(&dialect).as_str(),
                &[DbValue::Text(table.to_string())],
            )
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let foreign_keys = PyList::empty(py);
        for row in result.rows() {
            let foreign_key = PyDict::new(py);
            foreign_key.set_item("table", row.first().and_then(db_value_to_string))?;
            foreign_key.set_item("from", row.get(1).and_then(db_value_to_string))?;
            foreign_key.set_item("to", row.get(2).and_then(db_value_to_string))?;
            foreign_keys.append(foreign_key)?;
        }
        Ok(foreign_keys.into_any().unbind())
    }

    fn ensure_revision_table(&self) -> PyResult<()> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        ensure_revision_table(&mut connection)
    }

    fn applied_revisions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        ensure_revision_table(&mut connection)?;
        let dialect = AnyDialect::parse(connection.dialect())
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let result = connection
            .execute(applied_revisions_sql(&dialect).as_str(), &[])
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let revisions = PyList::empty(py);
        for row in result.rows() {
            if let Some(revision) = row.first().and_then(db_value_to_string) {
                revisions.append(revision)?;
            }
        }
        Ok(revisions.into_any().unbind())
    }

    fn apply_migration(
        &self,
        py: Python<'_>,
        revision: &str,
        operations: Vec<(String, Vec<Py<PyAny>>)>,
    ) -> PyResult<()> {
        let operations = py_operations_to_db(py, operations)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        run_migration(
            &mut connection,
            revision,
            operations,
            MigrationDirection::Apply,
        )
    }

    fn rollback_migration(
        &self,
        py: Python<'_>,
        revision: &str,
        operations: Vec<(String, Vec<Py<PyAny>>)>,
    ) -> PyResult<()> {
        let operations = py_operations_to_db(py, operations)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        run_migration(
            &mut connection,
            revision,
            operations,
            MigrationDirection::Rollback,
        )
    }

    #[pyo3(signature = (options=None))]
    fn begin(&self, options: Option<PyTransactionOptions>) -> PyResult<()> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?;
        match options {
            Some(options) => connection
                .begin_with(options.to_rust_options()?)
                .map_err(|error| PyValueError::new_err(error.to_string())),
            None => connection
                .begin()
                .map_err(|error| PyValueError::new_err(error.to_string())),
        }
    }

    fn commit(&self) -> PyResult<()> {
        self.connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .commit()
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn rollback(&self) -> PyResult<()> {
        self.connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .rollback()
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn savepoint(&self, name: &str) -> PyResult<()> {
        self.connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .savepoint(name)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn rollback_to_savepoint(&self, name: &str) -> PyResult<()> {
        self.connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .rollback_to_savepoint(name)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn release_savepoint(&self, name: &str) -> PyResult<()> {
        self.connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .release_savepoint(name)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }
}

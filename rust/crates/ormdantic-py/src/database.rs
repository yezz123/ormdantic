use crate::ddl::{create_enum_type_sql, create_table_sql, drop_enum_type_sql, drop_table_sql};
use crate::migrations::{
    applied_revisions_sql, ensure_revision_table, py_operations_to_db, run_migration,
    MigrationDirection,
};
use crate::runtime::{
    columns_sql, db_value_to_bool, db_value_to_string, foreign_keys_sql, index_columns_sql,
    indexes_sql, table_names_sql,
};
use crate::schema::{runtime_table_specs_from_py, RuntimeEnumType};
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
    enum_types: Arc<Vec<RuntimeEnumType>>,
}

#[pymethods]
impl PyDatabase {
    #[new]
    #[pyo3(signature = (url, tables, enum_types=None))]
    fn new(
        url: &str,
        tables: &Bound<'_, PyAny>,
        enum_types: Option<Vec<RuntimeEnumType>>,
    ) -> PyResult<Self> {
        let mut table_order = Vec::new();
        let tables = runtime_table_specs_from_py(tables)?
            .into_iter()
            .map(
                |(
                    model_key,
                    table,
                    primary_key,
                    columns,
                    indexes,
                    unique_constraints,
                    named_unique_constraints,
                    check_constraints,
                    foreign_key_constraints,
                    exclusion_constraints,
                    table_options,
                    relationships,
                )| {
                    let (
                        comment,
                        tablespace,
                        mysql_engine,
                        mysql_charset,
                        mysql_collation,
                        mysql_row_format,
                        postgres_inherits,
                        postgres_with,
                        postgres_using,
                        postgres_partition_by,
                        postgres_partition_of,
                        postgres_partition_for,
                        postgres_unlogged,
                        sqlite_strict,
                        sqlite_without_rowid,
                        schema,
                        mssql_primary_key_nonclustered,
                        oracle_compress,
                        mysql_key_block_size,
                        mysql_pack_keys,
                        mysql_checksum,
                        mysql_delay_key_write,
                        mysql_stats_persistent,
                        mysql_stats_auto_recalc,
                        mysql_stats_sample_pages,
                        mysql_avg_row_length,
                        mysql_max_rows,
                        mysql_min_rows,
                        mysql_insert_method,
                        mysql_data_directory,
                        mysql_index_directory,
                        mysql_connection,
                        mysql_union,
                        mysql_partition_by,
                        mysql_partitions,
                        mysql_subpartition_by,
                        mysql_subpartitions,
                        mysql_auto_increment,
                    ) = table_options;
                    table_order.push(model_key.clone());
                    (
                        model_key.clone(),
                        RuntimeTable {
                            table,
                            schema,
                            primary_key,
                            columns,
                            indexes,
                            unique_constraints,
                            named_unique_constraints,
                            check_constraints,
                            foreign_key_constraints,
                            exclusion_constraints,
                            comment,
                            tablespace,
                            mysql_engine,
                            mysql_charset,
                            mysql_collation,
                            mysql_row_format,
                            mysql_key_block_size,
                            mysql_pack_keys,
                            mysql_checksum,
                            mysql_delay_key_write,
                            mysql_stats_persistent,
                            mysql_stats_auto_recalc,
                            mysql_stats_sample_pages,
                            mysql_avg_row_length,
                            mysql_max_rows,
                            mysql_min_rows,
                            mysql_insert_method,
                            mysql_data_directory,
                            mysql_index_directory,
                            mysql_connection,
                            mysql_union,
                            mysql_partition_by,
                            mysql_partitions,
                            mysql_subpartition_by,
                            mysql_subpartitions,
                            mysql_auto_increment,
                            postgres_inherits,
                            postgres_with,
                            postgres_using,
                            postgres_partition_by,
                            postgres_partition_of,
                            postgres_partition_for,
                            postgres_unlogged,
                            sqlite_strict,
                            sqlite_without_rowid,
                            mssql_primary_key_nonclustered,
                            oracle_compress,
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
            enum_types: Arc::new(enum_types.unwrap_or_default()),
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
        for enum_type in self.enum_types.iter() {
            if let Some(sql) = create_enum_type_sql(&self.url, enum_type)? {
                connection
                    .execute(&sql, &[])
                    .map_err(|error| PyValueError::new_err(error.to_string()))?;
            }
        }
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
                table.named_unique_constraints.clone(),
                table.check_constraints.clone(),
                table.foreign_key_constraints.clone(),
                table.exclusion_constraints.clone(),
                table.comment.clone(),
                table.tablespace.clone(),
                table.mysql_engine.clone(),
                table.mysql_charset.clone(),
                table.mysql_collation.clone(),
                table.mysql_row_format.clone(),
                table.postgres_inherits.clone(),
                table.postgres_with.clone(),
                table.postgres_using.clone(),
                table.postgres_partition_by.clone(),
                table.postgres_partition_of.clone(),
                table.postgres_partition_for.clone(),
                table.postgres_unlogged,
                table.sqlite_strict,
                table.sqlite_without_rowid,
                table.schema.clone(),
                table.mssql_primary_key_nonclustered,
                table.oracle_compress.clone(),
                table.mysql_key_block_size,
                table.mysql_pack_keys,
                table.mysql_checksum,
                table.mysql_delay_key_write,
                table.mysql_stats_persistent,
                table.mysql_stats_auto_recalc,
                table.mysql_stats_sample_pages,
                table.mysql_avg_row_length,
                table.mysql_max_rows,
                table.mysql_min_rows,
                table.mysql_insert_method.clone(),
                table.mysql_data_directory.clone(),
                table.mysql_index_directory.clone(),
                table.mysql_connection.clone(),
                table.mysql_union.clone(),
                table.mysql_partition_by.clone(),
                table.mysql_partitions,
                table.mysql_subpartition_by.clone(),
                table.mysql_subpartitions,
                table.mysql_auto_increment,
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
            let sql = drop_table_sql(&self.url, &table.qualified_table_name())?;
            connection
                .execute(&sql, &[])
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        }
        for enum_type in self.enum_types.iter().rev() {
            if let Some(sql) = drop_enum_type_sql(&self.url, enum_type)? {
                connection
                    .execute(&sql, &[])
                    .map_err(|error| PyValueError::new_err(error.to_string()))?;
            }
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
            column.set_item("nullable", row.get(2).is_none_or(db_value_to_bool))?;
            column.set_item("default", row.get(3).and_then(db_value_to_string))?;
            column.set_item("primary_key", row.get(4).is_some_and(db_value_to_bool))?;
            columns.append(column)?;
        }
        Ok(columns.into_any().unbind())
    }

    #[pyo3(signature = (table, include_autoindexes=false))]
    fn indexes(
        &self,
        py: Python<'_>,
        table: &str,
        include_autoindexes: bool,
    ) -> PyResult<Py<PyAny>> {
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
            if name.starts_with("sqlite_autoindex_") && !include_autoindexes {
                continue;
            }
            let index = PyDict::new(py);
            if let Some(sql) = index_columns_sql(&dialect) {
                let columns_result = connection
                    .execute(sql.as_str(), &[DbValue::Text(name.clone())])
                    .map_err(|error| PyValueError::new_err(error.to_string()))?;
                let columns = PyList::empty(py);
                for column_row in columns_result.rows() {
                    if let Some(column_name) = column_row.first().and_then(db_value_to_string) {
                        columns.append(column_name)?;
                    }
                }
                index.set_item("columns", columns)?;
            }
            index.set_item("name", name)?;
            index.set_item("unique", row.get(1).is_some_and(db_value_to_bool))?;
            index.set_item("origin", row.get(2).and_then(db_value_to_string))?;
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
            foreign_key.set_item("on_update", row.get(3).and_then(db_value_to_string))?;
            foreign_key.set_item("on_delete", row.get(4).and_then(db_value_to_string))?;
            foreign_key.set_item("name", row.get(5).and_then(db_value_to_string))?;
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

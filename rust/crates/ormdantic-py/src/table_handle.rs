use crate::query::{
    bind_select_columns as select_columns, delete_ast_from_payload, joined_filters,
    joined_order_by, parse_filter_input, parse_sort_direction, select_ast_from_payload,
    update_ast_from_payload, RuntimeJoinedFilter, RuntimeJoinedOrder, RuntimeJoinedQuery,
};
use crate::runtime::{py_to_db_value, query_result_to_python};
use crate::schema::{
    RuntimeColumn, RuntimeExclusionConstraint, RuntimeForeignKeyConstraint, RuntimeIndex,
    RuntimeRelationship, RuntimeTableCheck, RuntimeUniqueConstraint,
};
use ormdantic_dialects::{AnyDialect, Dialect, DialectKind};
use ormdantic_engine::{DbValue, NativeConnection};
use ormdantic_sql::{
    CompiledQuery, DmlAst, Expr, Filter, JoinSpec, JoinedFilter, JoinedOrderBy, JoinedSelectColumn,
    OrderBy, QueryAst, QueryOperation, SortDirection, TableRef, TableSource,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct RuntimeTable {
    pub(crate) table: String,
    pub(crate) schema: Option<String>,
    pub(crate) primary_key: String,
    pub(crate) columns: Vec<RuntimeColumn>,
    pub(crate) indexes: Vec<RuntimeIndex>,
    pub(crate) unique_constraints: Vec<Vec<String>>,
    pub(crate) named_unique_constraints: Vec<RuntimeUniqueConstraint>,
    pub(crate) check_constraints: Vec<RuntimeTableCheck>,
    pub(crate) foreign_key_constraints: Vec<RuntimeForeignKeyConstraint>,
    pub(crate) exclusion_constraints: Vec<RuntimeExclusionConstraint>,
    pub(crate) comment: Option<String>,
    pub(crate) tablespace: Option<String>,
    pub(crate) mysql_engine: Option<String>,
    pub(crate) mysql_charset: Option<String>,
    pub(crate) mysql_collation: Option<String>,
    pub(crate) mysql_row_format: Option<String>,
    pub(crate) mysql_key_block_size: Option<u32>,
    pub(crate) mysql_pack_keys: Option<bool>,
    pub(crate) mysql_checksum: Option<bool>,
    pub(crate) mysql_delay_key_write: Option<bool>,
    pub(crate) mysql_stats_persistent: Option<bool>,
    pub(crate) mysql_stats_auto_recalc: Option<bool>,
    pub(crate) mysql_stats_sample_pages: Option<u32>,
    pub(crate) mysql_avg_row_length: Option<u32>,
    pub(crate) mysql_max_rows: Option<u32>,
    pub(crate) mysql_min_rows: Option<u32>,
    pub(crate) mysql_insert_method: Option<String>,
    pub(crate) mysql_data_directory: Option<String>,
    pub(crate) mysql_index_directory: Option<String>,
    pub(crate) mysql_connection: Option<String>,
    pub(crate) mysql_union: Vec<String>,
    pub(crate) mysql_partition_by: Option<String>,
    pub(crate) mysql_partitions: Option<u32>,
    pub(crate) mysql_subpartition_by: Option<String>,
    pub(crate) mysql_subpartitions: Option<u32>,
    pub(crate) mysql_auto_increment: Option<u32>,
    pub(crate) postgres_inherits: Vec<String>,
    pub(crate) postgres_with: Vec<(String, String)>,
    pub(crate) postgres_using: Option<String>,
    pub(crate) postgres_partition_by: Option<String>,
    pub(crate) postgres_partition_of: Option<String>,
    pub(crate) postgres_partition_for: Option<String>,
    pub(crate) postgres_unlogged: bool,
    pub(crate) sqlite_strict: bool,
    pub(crate) sqlite_without_rowid: bool,
    pub(crate) mssql_primary_key_nonclustered: bool,
    pub(crate) oracle_compress: Option<String>,
    pub(crate) relationships: Vec<RuntimeRelationship>,
}

struct JoinedQueryInput {
    filters: Vec<Filter>,
    order_by: Vec<String>,
    direction: SortDirection,
    limit: Option<usize>,
    offset: Option<usize>,
    depth: usize,
}

impl RuntimeTable {
    fn persisted_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|(name, ..)| name.clone())
            .collect::<Vec<_>>()
    }

    pub(crate) fn qualified_table_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}", self.table),
            None => self.table.clone(),
        }
    }
}

#[pyclass]
pub(crate) struct PyTableHandle {
    pub(crate) url: String,
    pub(crate) connection: Arc<Mutex<NativeConnection>>,
    pub(crate) tables: Arc<HashMap<String, RuntimeTable>>,
    pub(crate) table: RuntimeTable,
    pub(crate) compiled_dml: Mutex<HashMap<(QueryOperation, Vec<String>), CompiledQuery>>,
}

#[pymethods]
impl PyTableHandle {
    fn insert(&self, py: Python<'_>, payload: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        self.execute_write(py, QueryOperation::Insert, payload)
    }

    fn insert_many(&self, py: Python<'_>, payloads: Vec<Py<PyDict>>) -> PyResult<Py<PyAny>> {
        self.execute_many_write(py, QueryOperation::Insert, payloads)
    }

    fn upsert_many(&self, py: Python<'_>, payloads: Vec<Py<PyDict>>) -> PyResult<Py<PyAny>> {
        self.execute_many_write(py, QueryOperation::Upsert, payloads)
    }

    fn update(&self, py: Python<'_>, payload: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        self.execute_write(py, QueryOperation::Update, payload)
    }

    fn upsert(&self, py: Python<'_>, payload: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        self.execute_write(py, QueryOperation::Upsert, payload)
    }

    fn delete(&self, py: Python<'_>, primary_key: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let key = (QueryOperation::Delete, vec![self.table.primary_key.clone()]);
        let compiled = cached_or_compile(&self.compiled_dml, key, || {
            QueryAst::Delete {
                table: TableRef::new(self.table.qualified_table_name()),
                pk: self.table.primary_key.clone(),
            }
            .compile(&self.dialect()?)
            .map_err(|error| PyValueError::new_err(error.to_string()))
        })?;
        self.execute_compiled(py, compiled, vec![py_to_db_value(py, primary_key)?])
    }

    #[pyo3(signature = (primary_key, depth=0))]
    fn find_one(
        &self,
        py: Python<'_>,
        primary_key: Py<PyAny>,
        depth: usize,
    ) -> PyResult<Py<PyAny>> {
        let dialect = self.dialect()?;
        let columns = if depth == 0 {
            self.flat_select_columns()
        } else {
            Vec::new()
        };
        let aliases = if depth == 0 {
            Some(self.flat_aliases())
        } else {
            None
        };
        let query = if depth == 0 {
            QueryAst::Select {
                table: TableRef::new(self.table.qualified_table_name()),
                columns: select_columns(columns, aliases)?,
                filters: sqlite_decimal_filters(
                    vec![Filter::Eq {
                        column: self.table.primary_key.clone(),
                        param: self.table.primary_key.clone(),
                    }],
                    &self.table,
                    &dialect,
                ),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }
        } else {
            self.joined_query(
                JoinedQueryInput {
                    filters: vec![Filter::Eq {
                        column: self.table.primary_key.clone(),
                        param: self.table.primary_key.clone(),
                    }],
                    order_by: Vec::new(),
                    direction: SortDirection::Asc,
                    limit: None,
                    offset: None,
                    depth,
                },
                &dialect,
            )?
        };
        let compiled = query
            .compile(&dialect)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        self.execute_compiled(py, compiled, vec![py_to_db_value(py, primary_key)?])
    }

    fn find_one_with_paths(
        &self,
        py: Python<'_>,
        values: &Bound<'_, PyDict>,
        paths: Vec<String>,
        relationship_filters: Vec<RuntimeJoinedFilter>,
        relationship_order_by: Vec<RuntimeJoinedOrder>,
    ) -> PyResult<Py<PyAny>> {
        let dialect = self.dialect()?;
        let query = self.joined_query_for_paths(
            RuntimeJoinedQuery {
                filters: vec![Filter::Eq {
                    column: self.table.primary_key.clone(),
                    param: self.table.primary_key.clone(),
                }],
                order_by: Vec::new(),
                direction: SortDirection::Asc,
                limit: None,
                offset: None,
                paths,
                relationship_filters,
                relationship_order_by,
            },
            &dialect,
        )?;
        let compiled = query
            .compile(&dialect)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let params = bind_values(py, compiled.params(), values)?;
        self.execute_compiled(py, compiled, params)
    }

    #[pyo3(signature = (filters, values, order_by, order_direction, limit=None, offset=None, depth=0))]
    #[allow(clippy::too_many_arguments)]
    fn find_many(
        &self,
        py: Python<'_>,
        filters: &Bound<'_, PyAny>,
        values: &Bound<'_, PyDict>,
        order_by: Vec<String>,
        order_direction: &str,
        limit: Option<usize>,
        offset: Option<usize>,
        depth: usize,
    ) -> PyResult<Py<PyAny>> {
        let dialect = self.dialect()?;
        let direction = parse_sort_direction(order_direction)?;
        let filter_params = parse_filter_input(filters)?;
        let query = if depth == 0 {
            QueryAst::Select {
                table: TableRef::new(self.table.qualified_table_name()),
                columns: select_columns(self.flat_select_columns(), Some(self.flat_aliases()))?,
                filters: sqlite_decimal_filters(filter_params, &self.table, &dialect),
                order_by: order_by
                    .into_iter()
                    .map(|column| {
                        sqlite_decimal_order_by(column, direction.clone(), &self.table, &dialect)
                    })
                    .collect(),
                limit,
                offset,
            }
        } else {
            self.joined_query(
                JoinedQueryInput {
                    filters: filter_params,
                    order_by,
                    direction,
                    limit,
                    offset,
                    depth,
                },
                &dialect,
            )?
        };
        let compiled = query
            .compile(&dialect)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let params = bind_values(py, compiled.params(), values)?;
        self.execute_compiled(py, compiled, params)
    }

    #[pyo3(signature = (
        filters,
        values,
        order_by,
        order_direction,
        limit=None,
        offset=None,
        paths=Vec::new(),
        relationship_filters=Vec::new(),
        relationship_order_by=Vec::new()
    ))]
    #[allow(clippy::too_many_arguments)]
    fn find_many_with_paths(
        &self,
        py: Python<'_>,
        filters: &Bound<'_, PyAny>,
        values: &Bound<'_, PyDict>,
        order_by: Vec<String>,
        order_direction: &str,
        limit: Option<usize>,
        offset: Option<usize>,
        paths: Vec<String>,
        relationship_filters: Vec<RuntimeJoinedFilter>,
        relationship_order_by: Vec<RuntimeJoinedOrder>,
    ) -> PyResult<Py<PyAny>> {
        let dialect = self.dialect()?;
        let direction = parse_sort_direction(order_direction)?;
        let filter_params = parse_filter_input(filters)?;
        let query = self.joined_query_for_paths(
            RuntimeJoinedQuery {
                filters: filter_params,
                order_by,
                direction,
                limit,
                offset,
                paths,
                relationship_filters,
                relationship_order_by,
            },
            &dialect,
        )?;
        let compiled = query
            .compile(&dialect)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let params = bind_values(py, compiled.params(), values)?;
        self.execute_compiled(py, compiled, params)
    }

    fn count(
        &self,
        py: Python<'_>,
        filters: &Bound<'_, PyAny>,
        values: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyAny>> {
        let dialect = self.dialect()?;
        let compiled = QueryAst::Count {
            table: TableRef::new(self.table.qualified_table_name()),
            filters: sqlite_decimal_filters(parse_filter_input(filters)?, &self.table, &dialect),
        }
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let params = bind_values(py, compiled.params(), values)?;
        self.execute_compiled(py, compiled, params)
    }

    fn select_expression(&self, py: Python<'_>, query: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let query = query.cast::<PyDict>()?;
        let dialect = self.dialect()?;
        let mut ast = select_ast_from_payload(py, query)?;
        if dialect.kind() == DialectKind::Sqlite {
            ast = ast.rewrite_sqlite_decimal_columns(
                &decimal_columns(&self.table),
                &runtime_table_names(&self.table),
            );
        }
        let compiled = ast
            .compile(&dialect)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let empty_values = PyDict::new(py);
        let values = match query.get_item("values")? {
            Some(values) => values.cast::<PyDict>()?.clone(),
            None => empty_values,
        };
        let params = bind_values(py, compiled.params(), &values)?;
        self.execute_compiled(py, compiled, params)
    }

    fn update_expression(&self, py: Python<'_>, query: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let query = query.cast::<PyDict>()?;
        let dialect = self.dialect()?;
        let mut ast = update_ast_from_payload(py, query)?;
        if dialect.kind() == DialectKind::Sqlite {
            ast = ast.rewrite_sqlite_decimal_columns(
                &decimal_columns(&self.table),
                &runtime_table_names(&self.table),
            );
        }
        let compiled = ast
            .compile(&dialect)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let empty_values = PyDict::new(py);
        let values = match query.get_item("values")? {
            Some(values) => values.cast::<PyDict>()?.clone(),
            None => empty_values,
        };
        let params = bind_values(py, compiled.params(), &values)?;
        self.execute_compiled(py, compiled, params)
    }

    fn delete_expression(&self, py: Python<'_>, query: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let query = query.cast::<PyDict>()?;
        let dialect = self.dialect()?;
        let mut ast = delete_ast_from_payload(query)?;
        if dialect.kind() == DialectKind::Sqlite {
            ast = ast.rewrite_sqlite_decimal_columns(
                &decimal_columns(&self.table),
                &runtime_table_names(&self.table),
            );
        }
        let compiled = ast
            .compile(&dialect)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let empty_values = PyDict::new(py);
        let values = match query.get_item("values")? {
            Some(values) => values.cast::<PyDict>()?.clone(),
            None => empty_values,
        };
        let params = bind_values(py, compiled.params(), &values)?;
        self.execute_compiled(py, compiled, params)
    }

    fn max_bind_parameters(&self) -> PyResult<Option<usize>> {
        Ok(self.dialect()?.max_bind_parameters())
    }
}

impl PyTableHandle {
    fn dialect(&self) -> PyResult<AnyDialect> {
        AnyDialect::parse(&self.url).map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn execute_many_write(
        &self,
        py: Python<'_>,
        operation: QueryOperation,
        payloads: Vec<Py<PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let operation_name = match operation {
            QueryOperation::Insert => "insert_many",
            QueryOperation::Upsert => "upsert_many",
            _ => {
                return Err(PyValueError::new_err(
                    "bulk table writes support insert and upsert only",
                ))
            }
        };
        let first = payloads.first().ok_or_else(|| {
            PyValueError::new_err(format!("{operation_name} requires at least one payload"))
        })?;
        let columns = payload_columns(first.bind(py))?;
        let rows = (0..payloads.len())
            .map(|row| {
                columns
                    .iter()
                    .map(|column| Expr::param(format!("row_{row}__{column}")))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let table = TableSource::table(self.table.qualified_table_name());
        let dml = match operation {
            QueryOperation::Insert => DmlAst::Insert {
                table,
                columns: columns.clone(),
                rows,
                returning: Vec::new(),
            },
            QueryOperation::Upsert => DmlAst::Upsert {
                table,
                columns: columns.clone(),
                rows,
                conflict_target: vec![self.table.primary_key.clone()],
                update_assignments: Vec::new(),
                returning: Vec::new(),
            },
            _ => unreachable!("operation validated above"),
        };
        let compiled = dml
            .compile(&self.dialect()?)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let mut values = Vec::with_capacity(payloads.len() * columns.len());
        for payload in payloads {
            let payload = payload.bind(py);
            if payload_columns(payload)? != columns {
                return Err(PyValueError::new_err(format!(
                    "{operation_name} payloads must use the same ordered columns"
                )));
            }
            for column in &columns {
                let value = payload.get_item(column)?.ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "{operation_name} payload is missing column '{column}'"
                    ))
                })?;
                values.push(py_to_db_value(py, value.unbind())?);
            }
        }
        self.execute_compiled(py, compiled, values)
    }

    fn execute_write(
        &self,
        py: Python<'_>,
        operation: QueryOperation,
        payload: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyAny>> {
        let payload_columns = payload_columns(payload)?;
        let query = match operation {
            QueryOperation::Insert => QueryAst::Insert {
                table: TableRef::new(self.table.qualified_table_name()),
                columns: payload_columns.clone(),
            },
            QueryOperation::Update => QueryAst::Update {
                table: TableRef::new(self.table.qualified_table_name()),
                columns: payload_columns
                    .iter()
                    .filter(|column| column.as_str() != self.table.primary_key.as_str())
                    .cloned()
                    .collect(),
                pk: self.table.primary_key.clone(),
            },
            QueryOperation::Upsert => QueryAst::Upsert {
                table: TableRef::new(self.table.qualified_table_name()),
                columns: payload_columns.clone(),
                pk: self.table.primary_key.clone(),
            },
            _ => {
                return Err(PyValueError::new_err(
                    "unsupported write operation for table handle",
                ))
            }
        };
        let key = (operation, payload_columns);
        let compiled = cached_or_compile(&self.compiled_dml, key, || {
            query
                .compile(&self.dialect()?)
                .map_err(|error| PyValueError::new_err(error.to_string()))
        })?;
        let params = bind_values(py, compiled.params(), payload)?;
        self.execute_compiled(py, compiled, params)
    }

    fn execute_compiled(
        &self,
        py: Python<'_>,
        compiled: CompiledQuery,
        values: Vec<DbValue>,
    ) -> PyResult<Py<PyAny>> {
        let result = py
            .detach(|| {
                self.connection
                    .lock()
                    .map_err(|_| "native connection lock poisoned".to_string())?
                    .execute(compiled.sql(), &values)
                    .map_err(|error| error.to_string())
            })
            .map_err(PyValueError::new_err)?;
        query_result_to_python(py, result)
    }

    fn flat_select_columns(&self) -> Vec<String> {
        self.table.persisted_columns()
    }

    fn flat_aliases(&self) -> Vec<String> {
        self.flat_select_columns()
            .into_iter()
            .map(|column| format!("{}\\{column}", self.table.table))
            .collect()
    }

    fn joined_query(&self, input: JoinedQueryInput, dialect: &AnyDialect) -> PyResult<QueryAst> {
        let JoinedQueryInput {
            filters,
            order_by,
            direction,
            limit,
            offset,
            depth,
        } = input;
        Ok(QueryAst::JoinedSelect {
            table: TableRef::new(self.table.qualified_table_name()),
            columns: self.joined_columns(&self.table, depth, None),
            joins: self.join_specs(&self.table, depth, None),
            filters: sqlite_decimal_filters(filters, &self.table, dialect),
            relationship_filters: Vec::new(),
            order_by: order_by
                .into_iter()
                .map(|column| {
                    sqlite_decimal_order_by(column, direction.clone(), &self.table, dialect)
                })
                .collect(),
            relationship_order_by: Vec::new(),
            limit,
            offset,
        })
    }

    fn joined_query_for_paths(
        &self,
        query: RuntimeJoinedQuery,
        dialect: &AnyDialect,
    ) -> PyResult<QueryAst> {
        let RuntimeJoinedQuery {
            filters,
            order_by,
            direction,
            limit,
            offset,
            paths,
            relationship_filters,
            relationship_order_by,
        } = query;
        let included_paths = normalize_loader_paths(paths);
        let joins = self.join_specs_for_paths(&self.table, &included_paths, None, None);
        let relationship_filters = sqlite_decimal_joined_filters(
            joined_filters(relationship_filters)?,
            &joins,
            self.tables.as_ref(),
            dialect,
        );
        let relationship_order_by = sqlite_decimal_joined_order_by(
            joined_order_by(relationship_order_by)?,
            &joins,
            self.tables.as_ref(),
            dialect,
        );
        Ok(QueryAst::JoinedSelect {
            table: TableRef::new(self.table.qualified_table_name()),
            columns: self.joined_columns_for_paths(&self.table, &included_paths, None, None),
            joins,
            filters: sqlite_decimal_filters(filters, &self.table, dialect),
            relationship_filters,
            order_by: order_by
                .into_iter()
                .map(|column| {
                    sqlite_decimal_order_by(column, direction.clone(), &self.table, dialect)
                })
                .collect(),
            relationship_order_by,
            limit,
            offset,
        })
    }

    fn joined_columns(
        &self,
        table: &RuntimeTable,
        depth: usize,
        table_path: Option<String>,
    ) -> Vec<JoinedSelectColumn> {
        let table_path = table_path.unwrap_or_else(|| table.table.clone());
        let relation_fields = table
            .relationships
            .iter()
            .map(|(field, ..)| field)
            .collect::<HashSet<_>>();
        let mut columns = table
            .persisted_columns()
            .into_iter()
            .filter(|column| depth == 0 || !relation_fields.contains(column))
            .map(|column| {
                JoinedSelectColumn::aliased(
                    table_path.clone(),
                    column.clone(),
                    format!("{table_path}\\{column}"),
                )
            })
            .collect::<Vec<_>>();
        if depth == 0 {
            return columns;
        }
        for (field, foreign_table, _, _) in &table.relationships {
            let Some(related) = self
                .tables
                .values()
                .find(|table| &table.table == foreign_table)
            else {
                continue;
            };
            let relation_path = format!("{table_path}/{field}");
            columns.extend(self.joined_columns(related, depth - 1, Some(relation_path)));
        }
        columns
    }

    fn joined_columns_for_paths(
        &self,
        table: &RuntimeTable,
        included_paths: &HashSet<String>,
        table_path: Option<String>,
        relative_path: Option<String>,
    ) -> Vec<JoinedSelectColumn> {
        let table_path = table_path.unwrap_or_else(|| table.table.clone());
        let loaded_relation_fields = table
            .relationships
            .iter()
            .filter_map(|(field, ..)| {
                let relation_path = append_loader_path(relative_path.as_deref(), field);
                loader_path_included(included_paths, &relation_path).then_some(field)
            })
            .collect::<HashSet<_>>();
        let mut columns = table
            .persisted_columns()
            .into_iter()
            .filter(|column| !loaded_relation_fields.contains(column))
            .map(|column| {
                JoinedSelectColumn::aliased(
                    table_path.clone(),
                    column.clone(),
                    format!("{table_path}\\{column}"),
                )
            })
            .collect::<Vec<_>>();

        for (field, foreign_table, _, _) in &table.relationships {
            let relation_relative_path = append_loader_path(relative_path.as_deref(), field);
            if !loader_path_included(included_paths, &relation_relative_path) {
                continue;
            }
            let Some(related) = self
                .tables
                .values()
                .find(|table| &table.table == foreign_table)
            else {
                continue;
            };
            let relation_table_path = format!("{table_path}/{field}");
            columns.extend(self.joined_columns_for_paths(
                related,
                included_paths,
                Some(relation_table_path),
                Some(relation_relative_path),
            ));
        }
        columns
    }

    fn join_specs(
        &self,
        table: &RuntimeTable,
        depth: usize,
        table_path: Option<String>,
    ) -> Vec<JoinSpec> {
        if depth == 0 {
            return Vec::new();
        }
        let table_path = table_path.unwrap_or_else(|| table.table.clone());
        let mut joins = Vec::new();
        for (field, foreign_table, foreign_column, back_reference) in &table.relationships {
            let Some(related) = self
                .tables
                .values()
                .find(|table| &table.table == foreign_table)
            else {
                continue;
            };
            let relation_path = format!("{table_path}/{field}");
            if let Some(back_reference) = back_reference {
                joins.push(JoinSpec::left_join(
                    related.qualified_table_name(),
                    &relation_path,
                    &table_path,
                    &table.primary_key,
                    &relation_path,
                    back_reference,
                ));
            } else {
                joins.push(JoinSpec::left_join(
                    related.qualified_table_name(),
                    &relation_path,
                    &table_path,
                    field,
                    &relation_path,
                    foreign_column,
                ));
            }
            joins.extend(self.join_specs(related, depth - 1, Some(relation_path)));
        }
        joins
    }

    fn join_specs_for_paths(
        &self,
        table: &RuntimeTable,
        included_paths: &HashSet<String>,
        table_path: Option<String>,
        relative_path: Option<String>,
    ) -> Vec<JoinSpec> {
        let table_path = table_path.unwrap_or_else(|| table.table.clone());
        let mut joins = Vec::new();
        for (field, foreign_table, foreign_column, back_reference) in &table.relationships {
            let relation_relative_path = append_loader_path(relative_path.as_deref(), field);
            if !loader_path_included(included_paths, &relation_relative_path) {
                continue;
            }
            let Some(related) = self
                .tables
                .values()
                .find(|table| &table.table == foreign_table)
            else {
                continue;
            };
            let relation_table_path = format!("{table_path}/{field}");
            if let Some(back_reference) = back_reference {
                joins.push(JoinSpec::left_join(
                    related.qualified_table_name(),
                    &relation_table_path,
                    &table_path,
                    &table.primary_key,
                    &relation_table_path,
                    back_reference,
                ));
            } else {
                joins.push(JoinSpec::left_join(
                    related.qualified_table_name(),
                    &relation_table_path,
                    &table_path,
                    field,
                    &relation_table_path,
                    foreign_column,
                ));
            }
            joins.extend(self.join_specs_for_paths(
                related,
                included_paths,
                Some(relation_table_path),
                Some(relation_relative_path),
            ));
        }
        joins
    }
}

fn sqlite_decimal_filters(
    filters: Vec<Filter>,
    table: &RuntimeTable,
    dialect: &AnyDialect,
) -> Vec<Filter> {
    if dialect.kind() != DialectKind::Sqlite {
        return filters;
    }
    let columns = decimal_columns(table);
    filters
        .into_iter()
        .map(|filter| sqlite_decimal_filter(filter, &columns))
        .collect()
}

fn sqlite_decimal_filter(filter: Filter, decimal_columns: &HashSet<String>) -> Filter {
    match filter {
        Filter::Eq { column, param } if decimal_columns.contains(&column) => {
            Filter::DecimalEq { column, param }
        }
        Filter::Ne { column, param } if decimal_columns.contains(&column) => {
            Filter::DecimalNe { column, param }
        }
        Filter::Lt { column, param } if decimal_columns.contains(&column) => {
            Filter::DecimalLt { column, param }
        }
        Filter::Le { column, param } if decimal_columns.contains(&column) => {
            Filter::DecimalLe { column, param }
        }
        Filter::Gt { column, param } if decimal_columns.contains(&column) => {
            Filter::DecimalGt { column, param }
        }
        Filter::Ge { column, param } if decimal_columns.contains(&column) => {
            Filter::DecimalGe { column, param }
        }
        Filter::In { column, params } if decimal_columns.contains(&column) => {
            Filter::DecimalIn { column, params }
        }
        Filter::NotIn { column, params } if decimal_columns.contains(&column) => {
            Filter::DecimalNotIn { column, params }
        }
        Filter::And(filters) => Filter::And(
            filters
                .into_iter()
                .map(|filter| sqlite_decimal_filter(filter, decimal_columns))
                .collect(),
        ),
        Filter::Or(filters) => Filter::Or(
            filters
                .into_iter()
                .map(|filter| sqlite_decimal_filter(filter, decimal_columns))
                .collect(),
        ),
        filter => filter,
    }
}

fn sqlite_decimal_order_by(
    column: String,
    direction: SortDirection,
    table: &RuntimeTable,
    dialect: &AnyDialect,
) -> OrderBy {
    let order = OrderBy::new(column.clone(), direction);
    if dialect.kind() == DialectKind::Sqlite && decimal_columns(table).contains(&column) {
        order.decimal(true)
    } else {
        order
    }
}

fn sqlite_decimal_joined_filters(
    filters: Vec<JoinedFilter>,
    joins: &[JoinSpec],
    tables: &HashMap<String, RuntimeTable>,
    dialect: &AnyDialect,
) -> Vec<JoinedFilter> {
    if dialect.kind() != DialectKind::Sqlite {
        return filters;
    }
    filters
        .into_iter()
        .map(|filter| {
            let Some(table) = joined_filter_table(&filter, joins, tables) else {
                return filter;
            };
            let columns = decimal_columns(table);
            JoinedFilter::new(
                filter.table_alias().to_string(),
                sqlite_decimal_filter(filter.filter().clone(), &columns),
            )
        })
        .collect()
}

fn sqlite_decimal_joined_order_by(
    order_by: Vec<JoinedOrderBy>,
    joins: &[JoinSpec],
    tables: &HashMap<String, RuntimeTable>,
    dialect: &AnyDialect,
) -> Vec<JoinedOrderBy> {
    if dialect.kind() != DialectKind::Sqlite {
        return order_by;
    }
    order_by
        .into_iter()
        .map(|order| {
            let Some(table) = joined_order_table(&order, joins, tables) else {
                return order;
            };
            JoinedOrderBy::new(
                order.table_alias().to_string(),
                sqlite_decimal_order_by(
                    order.order_by().column().to_string(),
                    order.order_by().direction().clone(),
                    table,
                    dialect,
                ),
            )
        })
        .collect()
}

fn joined_filter_table<'a>(
    filter: &JoinedFilter,
    joins: &[JoinSpec],
    tables: &'a HashMap<String, RuntimeTable>,
) -> Option<&'a RuntimeTable> {
    joins
        .iter()
        .find(|join| join.alias() == filter.table_alias())
        .and_then(|join| table_by_sql_name(tables, join.table()))
}

fn joined_order_table<'a>(
    order: &JoinedOrderBy,
    joins: &[JoinSpec],
    tables: &'a HashMap<String, RuntimeTable>,
) -> Option<&'a RuntimeTable> {
    joins
        .iter()
        .find(|join| join.alias() == order.table_alias())
        .and_then(|join| table_by_sql_name(tables, join.table()))
}

fn table_by_sql_name<'a>(
    tables: &'a HashMap<String, RuntimeTable>,
    table_name: &str,
) -> Option<&'a RuntimeTable> {
    tables
        .values()
        .find(|table| table.table == table_name || table.qualified_table_name() == table_name)
}

fn decimal_columns(table: &RuntimeTable) -> HashSet<String> {
    table
        .columns
        .iter()
        .filter(|(_name, kind, ..)| kind == "decimal")
        .map(|(name, ..)| name.clone())
        .collect()
}

fn runtime_table_names(table: &RuntimeTable) -> Vec<String> {
    let mut names = vec![table.table.clone()];
    let qualified = table.qualified_table_name();
    if qualified != table.table {
        names.push(qualified);
    }
    names
}

fn normalize_loader_paths(paths: Vec<String>) -> HashSet<String> {
    paths
        .into_iter()
        .map(|path| path.replace('.', "/"))
        .map(|path| path.trim_matches('/').to_string())
        .filter(|path| !path.is_empty())
        .collect()
}

fn append_loader_path(prefix: Option<&str>, field: &str) -> String {
    match prefix {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}/{field}"),
        _ => field.to_string(),
    }
}

fn loader_path_included(included_paths: &HashSet<String>, path: &str) -> bool {
    included_paths.contains(path)
        || included_paths
            .iter()
            .any(|included| included.starts_with(&format!("{path}/")))
}

fn payload_columns(payload: &Bound<'_, PyDict>) -> PyResult<Vec<String>> {
    payload
        .keys()
        .iter()
        .map(|key| key.extract::<String>())
        .collect::<PyResult<Vec<_>>>()
}

fn bind_values(
    py: Python<'_>,
    param_names: &[String],
    values: &Bound<'_, PyDict>,
) -> PyResult<Vec<DbValue>> {
    param_names
        .iter()
        .map(|param| {
            let value = values
                .get_item(param)?
                .ok_or_else(|| PyValueError::new_err(format!("missing bind value '{param}'")))?;
            py_to_db_value(py, value.unbind())
        })
        .collect()
}

fn cached_or_compile(
    cache: &Mutex<HashMap<(QueryOperation, Vec<String>), CompiledQuery>>,
    key: (QueryOperation, Vec<String>),
    compile: impl FnOnce() -> PyResult<CompiledQuery>,
) -> PyResult<CompiledQuery> {
    let mut cache = cache
        .lock()
        .map_err(|_| PyValueError::new_err("compiled DML cache lock poisoned"))?;
    if let Some(compiled) = cache.get(&key) {
        return Ok(compiled.clone());
    }
    let compiled = compile()?;
    cache.insert(key, compiled.clone());
    Ok(compiled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_loader_paths_accepts_dots_and_slashes() {
        let paths = normalize_loader_paths(vec![
            "author.posts".to_string(),
            "/comments/replies/".to_string(),
            "".to_string(),
        ]);

        assert!(paths.contains("author/posts"));
        assert!(paths.contains("comments/replies"));
        assert!(!paths.contains(""));
    }

    #[test]
    fn loader_path_included_matches_prefixes() {
        let paths = HashSet::from(["author/posts".to_string()]);

        assert!(loader_path_included(&paths, "author"));
        assert!(loader_path_included(&paths, "author/posts"));
        assert!(!loader_path_included(&paths, "comments"));
    }

    #[test]
    fn compiled_dml_cache_reuses_shapes_and_separates_other_shapes() {
        let cache = Mutex::new(HashMap::new());
        let mut compile_count = 0;
        let mut compile = |columns: Vec<String>| {
            cached_or_compile(
                &cache,
                (QueryOperation::Insert, columns),
                || -> PyResult<CompiledQuery> {
                    compile_count += 1;
                    Ok(CompiledQuery::new(
                        "INSERT".to_string(),
                        Vec::new(),
                        QueryOperation::Insert,
                    ))
                },
            )
        };

        compile(vec!["id".to_string(), "name".to_string()]).unwrap();
        compile(vec!["id".to_string(), "name".to_string()]).unwrap();
        compile(vec!["id".to_string()]).unwrap();

        assert_eq!(compile_count, 2);
    }
}

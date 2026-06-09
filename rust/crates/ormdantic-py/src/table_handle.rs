use crate::query::{
    bind_select_columns as select_columns, joined_filters, joined_order_by, parse_filter_input,
    parse_sort_direction, select_ast_from_payload, update_ast_from_payload, RuntimeJoinedFilter,
    RuntimeJoinedOrder, RuntimeJoinedQuery,
};
use crate::runtime::{py_to_db_value, query_result_to_python};
use crate::schema::{RuntimeColumn, RuntimeIndex, RuntimeRelationship};
use ormdantic_dialects::AnyDialect;
use ormdantic_engine::{DbValue, NativeConnection};
use ormdantic_sql::{
    CompiledQuery, Filter, JoinSpec, JoinedSelectColumn, OrderBy, QueryAst, QueryOperation,
    SortDirection, TableRef,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct RuntimeTable {
    pub(crate) table: String,
    pub(crate) primary_key: String,
    pub(crate) columns: Vec<RuntimeColumn>,
    pub(crate) indexes: Vec<RuntimeIndex>,
    pub(crate) unique_constraints: Vec<Vec<String>>,
    pub(crate) relationships: Vec<RuntimeRelationship>,
}

impl RuntimeTable {
    fn persisted_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|(name, ..)| name.clone())
            .collect::<Vec<_>>()
    }
}

#[pyclass]
pub(crate) struct PyTableHandle {
    pub(crate) url: String,
    pub(crate) connection: Arc<Mutex<NativeConnection>>,
    pub(crate) tables: Arc<HashMap<String, RuntimeTable>>,
    pub(crate) table: RuntimeTable,
}

#[pymethods]
impl PyTableHandle {
    fn insert(&self, py: Python<'_>, payload: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        self.execute_write(py, QueryOperation::Insert, payload)
    }

    fn update(&self, py: Python<'_>, payload: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        self.execute_write(py, QueryOperation::Update, payload)
    }

    fn upsert(&self, py: Python<'_>, payload: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        self.execute_write(py, QueryOperation::Upsert, payload)
    }

    fn delete(&self, py: Python<'_>, primary_key: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let compiled = QueryAst::Delete {
            table: TableRef::new(&self.table.table),
            pk: self.table.primary_key.clone(),
        }
        .compile(
            &AnyDialect::parse(&self.url)
                .map_err(|error| PyValueError::new_err(error.to_string()))?,
        )
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
        self.execute_compiled(py, compiled, vec![py_to_db_value(py, primary_key)?])
    }

    #[pyo3(signature = (primary_key, depth=0))]
    fn find_one(
        &self,
        py: Python<'_>,
        primary_key: Py<PyAny>,
        depth: usize,
    ) -> PyResult<Py<PyAny>> {
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
                table: TableRef::new(&self.table.table),
                columns: select_columns(columns, aliases)?,
                filters: vec![Filter::Eq {
                    column: self.table.primary_key.clone(),
                    param: self.table.primary_key.clone(),
                }],
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }
        } else {
            self.joined_query(
                vec![Filter::Eq {
                    column: self.table.primary_key.clone(),
                    param: self.table.primary_key.clone(),
                }],
                Vec::new(),
                SortDirection::Asc,
                None,
                None,
                depth,
            )?
        };
        let compiled = query
            .compile(
                &AnyDialect::parse(&self.url)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )
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
        let query = self.joined_query_for_paths(RuntimeJoinedQuery {
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
        })?;
        let compiled = query
            .compile(
                &AnyDialect::parse(&self.url)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )
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
        let direction = parse_sort_direction(order_direction)?;
        let filter_params = parse_filter_input(filters)?;
        let query = if depth == 0 {
            QueryAst::Select {
                table: TableRef::new(&self.table.table),
                columns: select_columns(self.flat_select_columns(), Some(self.flat_aliases()))?,
                filters: filter_params,
                order_by: order_by
                    .into_iter()
                    .map(|column| OrderBy::new(column, direction.clone()))
                    .collect(),
                limit,
                offset,
            }
        } else {
            self.joined_query(filter_params, order_by, direction, limit, offset, depth)?
        };
        let compiled = query
            .compile(
                &AnyDialect::parse(&self.url)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )
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
        let direction = parse_sort_direction(order_direction)?;
        let filter_params = parse_filter_input(filters)?;
        let query = self.joined_query_for_paths(RuntimeJoinedQuery {
            filters: filter_params,
            order_by,
            direction,
            limit,
            offset,
            paths,
            relationship_filters,
            relationship_order_by,
        })?;
        let compiled = query
            .compile(
                &AnyDialect::parse(&self.url)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )
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
        let compiled = QueryAst::Count {
            table: TableRef::new(&self.table.table),
            filters: parse_filter_input(filters)?,
        }
        .compile(
            &AnyDialect::parse(&self.url)
                .map_err(|error| PyValueError::new_err(error.to_string()))?,
        )
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let params = bind_values(py, compiled.params(), values)?;
        self.execute_compiled(py, compiled, params)
    }

    fn select_expression(&self, py: Python<'_>, query: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let query = query.downcast::<PyDict>()?;
        let compiled = select_ast_from_payload(py, query)?
            .compile(
                &AnyDialect::parse(&self.url)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let empty_values = PyDict::new(py);
        let values = match query.get_item("values")? {
            Some(values) => values.downcast::<PyDict>()?.clone(),
            None => empty_values,
        };
        let params = bind_values(py, compiled.params(), &values)?;
        self.execute_compiled(py, compiled, params)
    }

    fn update_expression(&self, py: Python<'_>, query: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let query = query.downcast::<PyDict>()?;
        let compiled = update_ast_from_payload(py, query)?
            .compile(
                &AnyDialect::parse(&self.url)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let empty_values = PyDict::new(py);
        let values = match query.get_item("values")? {
            Some(values) => values.downcast::<PyDict>()?.clone(),
            None => empty_values,
        };
        let params = bind_values(py, compiled.params(), &values)?;
        self.execute_compiled(py, compiled, params)
    }
}

impl PyTableHandle {
    fn execute_write(
        &self,
        py: Python<'_>,
        operation: QueryOperation,
        payload: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyAny>> {
        let columns = self.table.persisted_columns();
        let query = match operation {
            QueryOperation::Insert => QueryAst::Insert {
                table: TableRef::new(&self.table.table),
                columns: columns.clone(),
            },
            QueryOperation::Update => QueryAst::Update {
                table: TableRef::new(&self.table.table),
                columns: columns.clone(),
                pk: self.table.primary_key.clone(),
            },
            QueryOperation::Upsert => QueryAst::Upsert {
                table: TableRef::new(&self.table.table),
                columns: columns.clone(),
                pk: self.table.primary_key.clone(),
            },
            _ => {
                return Err(PyValueError::new_err(
                    "unsupported write operation for table handle",
                ))
            }
        };
        let compiled = query
            .compile(
                &AnyDialect::parse(&self.url)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let params = bind_values(py, compiled.params(), payload)?;
        self.execute_compiled(py, compiled, params)
    }

    fn execute_compiled(
        &self,
        py: Python<'_>,
        compiled: CompiledQuery,
        values: Vec<DbValue>,
    ) -> PyResult<Py<PyAny>> {
        let result = self
            .connection
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .execute(compiled.sql(), &values)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
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

    fn joined_query(
        &self,
        filters: Vec<Filter>,
        order_by: Vec<String>,
        direction: SortDirection,
        limit: Option<usize>,
        offset: Option<usize>,
        depth: usize,
    ) -> PyResult<QueryAst> {
        Ok(QueryAst::JoinedSelect {
            table: TableRef::new(&self.table.table),
            columns: self.joined_columns(&self.table, depth, None),
            joins: self.join_specs(&self.table, depth, None),
            filters,
            relationship_filters: Vec::new(),
            order_by: order_by
                .into_iter()
                .map(|column| OrderBy::new(column, direction.clone()))
                .collect(),
            relationship_order_by: Vec::new(),
            limit,
            offset,
        })
    }

    fn joined_query_for_paths(&self, query: RuntimeJoinedQuery) -> PyResult<QueryAst> {
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
        Ok(QueryAst::JoinedSelect {
            table: TableRef::new(&self.table.table),
            columns: self.joined_columns_for_paths(&self.table, &included_paths, None, None),
            joins: self.join_specs_for_paths(&self.table, &included_paths, None, None),
            filters,
            relationship_filters: joined_filters(relationship_filters)?,
            order_by: order_by
                .into_iter()
                .map(|column| OrderBy::new(column, direction.clone()))
                .collect(),
            relationship_order_by: joined_order_by(relationship_order_by)?,
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
                    foreign_table,
                    &relation_path,
                    &table_path,
                    &table.primary_key,
                    &relation_path,
                    back_reference,
                ));
            } else {
                joins.push(JoinSpec::left_join(
                    foreign_table,
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
                    foreign_table,
                    &relation_table_path,
                    &table_path,
                    &table.primary_key,
                    &relation_table_path,
                    back_reference,
                ));
            } else {
                joins.push(JoinSpec::left_join(
                    foreign_table,
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
}

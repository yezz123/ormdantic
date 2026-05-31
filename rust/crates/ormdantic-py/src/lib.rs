use ormdantic_dialects::{AnyDialect, Dialect};
use ormdantic_engine::{execute_url, DbValue, NativeConnection};
use ormdantic_hydrate::{FlatHydrationPlan, ResultShape};
use ormdantic_schema::{SchemaRegistry, TableDef};
use ormdantic_sql::{
    CompiledQuery, Filter, JoinSpec, JoinedSelectColumn, OrderBy, QueryAst, QueryOperation,
    SelectColumn, SortDirection, TableRef,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use pyo3::IntoPyObjectExt;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

type FilterSpec = (String, String, Vec<String>);

#[pyclass]
struct PyNativeConnection {
    inner: Mutex<NativeConnection>,
}

#[pymethods]
impl PyNativeConnection {
    #[new]
    fn new(url: &str) -> PyResult<Self> {
        Ok(Self {
            inner: Mutex::new(
                NativeConnection::open(url)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            ),
        })
    }

    fn execute(&self, py: Python<'_>, sql: &str, params: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let values = params
            .into_iter()
            .map(|value| py_to_db_value(py, value))
            .collect::<PyResult<Vec<_>>>()?;
        let result = self
            .inner
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .execute(sql, &values)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        query_result_to_python(py, result)
    }

    fn begin(&self) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .begin()
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn commit(&self) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .commit()
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn rollback(&self) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| PyValueError::new_err("native connection lock poisoned"))?
            .rollback()
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }
}

#[pyfunction]
fn hydrate_flat(
    py: Python<'_>,
    tablename: &str,
    pk: &str,
    columns: Vec<String>,
    rows: Vec<Vec<Py<PyAny>>>,
    is_array: bool,
) -> PyResult<Py<PyAny>> {
    let table = TableDef::new(tablename, pk, Vec::new());
    let plan = FlatHydrationPlan::new(table, &columns)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;

    if rows.is_empty() {
        return Ok(py.None());
    }

    if !is_array {
        return row_to_dict(py, &rows[0], plan.parsed_columns())
            .map(|record| record.into_any().unbind());
    }

    let seen = PyDict::new(py);
    let mut records = Vec::new();
    for row in rows {
        let Some(pk_value) = row.get(plan.primary_key_index()) else {
            continue;
        };
        let bound_pk = pk_value.bind(py);
        if seen.contains(bound_pk)? {
            continue;
        }

        let record = row_to_dict(py, &row, plan.parsed_columns())?;
        seen.set_item(bound_pk, &record)?;
        records.push(record.into_any().unbind());
    }

    Ok(PyList::new(py, records)?.into_any().unbind())
}

#[pyfunction]
fn hydrate_joined(
    py: Python<'_>,
    columns: Vec<String>,
    rows: Vec<Vec<Py<PyAny>>>,
    path_pks: Vec<(String, String)>,
    array_paths: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let parsed_columns = columns
        .iter()
        .map(|alias| split_alias(alias))
        .collect::<PyResult<Vec<_>>>()?;
    let path_pk_map = path_pks.into_iter().collect::<HashMap<_, _>>();
    let array_paths = array_paths.into_iter().collect::<HashSet<_>>();
    let root = PyDict::new(py);

    for row in rows {
        let row_pks = collect_row_pks(py, &row, &parsed_columns, &path_pk_map);
        for (idx, (path, column)) in parsed_columns.iter().enumerate() {
            let mut node = root.clone();
            let mut current_path = String::new();
            let mut should_set = true;
            for (branch_idx, branch) in path.split('/').enumerate() {
                if branch_idx == 0 {
                    current_path.push_str(branch);
                } else {
                    current_path.push('/');
                    current_path.push_str(branch);
                }
                let Some(pk_value) = row_pks.get(&current_path) else {
                    should_set = false;
                    break;
                };
                if pk_value.bind(py).is_none() {
                    should_set = false;
                    break;
                }
                if !node.contains(branch)? {
                    node.set_item(branch, PyDict::new(py))?;
                }
                let branch_node = node
                    .get_item(branch)?
                    .expect("branch exists")
                    .downcast_into::<PyDict>()?;
                if array_paths.contains(&current_path) {
                    if !branch_node.contains(pk_value.bind(py))? {
                        branch_node.set_item(pk_value.bind(py), PyDict::new(py))?;
                    }
                    node = branch_node
                        .get_item(pk_value.bind(py))?
                        .expect("array item exists")
                        .downcast_into::<PyDict>()?;
                } else {
                    node = branch_node;
                }
            }
            if should_set && !column.is_empty() {
                if let Some(value) = row.get(idx) {
                    node.set_item(column, value.bind(py))?;
                }
            }
        }
    }

    Ok(root.into_any().unbind())
}

#[pyfunction]
fn plan_result_shape(
    py: Python<'_>,
    root_table: &str,
    columns: Vec<String>,
    array_paths: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let shape = ResultShape::new(root_table, &columns, array_paths);
    let result = PyDict::new(py);
    let columns = PyList::empty(py);
    for column in shape.columns() {
        let item = PyDict::new(py);
        item.set_item("alias", column.alias())?;
        item.set_item("table_path", column.table_path())?;
        item.set_item("column", column.column())?;
        columns.append(item)?;
    }
    result.set_item("root_table", shape.root_table())?;
    result.set_item("columns", columns)?;
    result.set_item("relationship_paths", shape.relationship_paths())?;
    result.set_item("array_paths", shape.array_paths())?;
    Ok(result.into_any().unbind())
}

fn split_alias(alias: &str) -> PyResult<(String, String)> {
    alias.split_once('\\').map_or_else(
        || {
            Err(PyValueError::new_err(format!(
                "invalid result alias '{alias}'"
            )))
        },
        |(path, column)| Ok((path.to_string(), column.to_string())),
    )
}

fn collect_row_pks(
    py: Python<'_>,
    row: &[Py<PyAny>],
    parsed_columns: &[(String, String)],
    path_pk_map: &HashMap<String, String>,
) -> HashMap<String, Py<PyAny>> {
    let mut row_pks = HashMap::new();
    for (idx, (path, column)) in parsed_columns.iter().enumerate() {
        if path_pk_map.get(path) == Some(column) {
            if let Some(value) = row.get(idx) {
                row_pks.insert(path.clone(), value.clone_ref(py));
            }
        }
    }
    row_pks
}

#[pyfunction]
fn validate_schema_tables(tables: Vec<(String, String, Vec<String>)>) -> PyResult<usize> {
    let mut registry = SchemaRegistry::new();
    for (tablename, primary_key, columns) in tables {
        registry
            .register_table(TableDef::new(tablename, primary_key, columns))
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
    }
    registry
        .validate_relationships()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(registry.tables().len())
}

#[pyfunction(signature = (dialect, table, primary_key, columns, aliases=None))]
fn compile_select_pk(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    primary_key: &str,
    columns: Vec<String>,
    aliases: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let query = QueryAst::Select {
        table: TableRef::new(table),
        columns: select_columns(columns, aliases)?,
        filters: vec![Filter::Eq {
            column: primary_key.to_string(),
            param: primary_key.to_string(),
        }],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };
    compile_to_python(py, dialect, query)
}

#[pyfunction(signature = (
    dialect,
    table,
    columns,
    filter_columns,
    order_columns,
    order_direction,
    limit=None,
    offset=None,
    aliases=None
))]
fn compile_find_many(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    columns: Vec<String>,
    filter_columns: Vec<FilterSpec>,
    order_columns: Vec<String>,
    order_direction: &str,
    limit: Option<usize>,
    offset: Option<usize>,
    aliases: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let direction = parse_sort_direction(order_direction)?;
    let query = QueryAst::Select {
        table: TableRef::new(table),
        columns: select_columns(columns, aliases)?,
        filters: filter_specs(filter_columns)?,
        order_by: order_columns
            .into_iter()
            .map(|column| OrderBy::new(column, direction.clone()))
            .collect(),
        limit,
        offset,
    };
    compile_to_python(py, dialect, query)
}

#[pyfunction(signature = (
    dialect,
    table,
    columns,
    joins,
    filter_columns,
    order_columns,
    order_direction,
    limit=None,
    offset=None
))]
fn compile_joined_find_many(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    columns: Vec<(String, String, String)>,
    joins: Vec<(String, String, String, String, String, String)>,
    filter_columns: Vec<FilterSpec>,
    order_columns: Vec<String>,
    order_direction: &str,
    limit: Option<usize>,
    offset: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let direction = parse_sort_direction(order_direction)?;
    let query = QueryAst::JoinedSelect {
        table: TableRef::new(table),
        columns: columns
            .into_iter()
            .map(|(table_alias, column, alias)| {
                JoinedSelectColumn::aliased(table_alias, column, alias)
            })
            .collect(),
        joins: joins
            .into_iter()
            .map(
                |(table, alias, left_alias, left_column, right_alias, right_column)| {
                    JoinSpec::left_join(
                        table,
                        alias,
                        left_alias,
                        left_column,
                        right_alias,
                        right_column,
                    )
                },
            )
            .collect(),
        filters: filter_specs(filter_columns)?,
        order_by: order_columns
            .into_iter()
            .map(|column| OrderBy::new(column, direction.clone()))
            .collect(),
        limit,
        offset,
    };
    compile_to_python(py, dialect, query)
}

#[pyfunction]
fn compile_count(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    filter_columns: Vec<FilterSpec>,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Count {
            table: TableRef::new(table),
            filters: filter_specs(filter_columns)?,
        },
    )
}

#[pyfunction]
fn compile_insert(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    columns: Vec<String>,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Insert {
            table: TableRef::new(table),
            columns,
        },
    )
}

#[pyfunction]
fn compile_update(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    primary_key: &str,
    columns: Vec<String>,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Update {
            table: TableRef::new(table),
            columns,
            pk: primary_key.to_string(),
        },
    )
}

#[pyfunction]
fn compile_upsert(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    primary_key: &str,
    columns: Vec<String>,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Upsert {
            table: TableRef::new(table),
            columns,
            pk: primary_key.to_string(),
        },
    )
}

#[pyfunction]
fn compile_delete_pk(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    primary_key: &str,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Delete {
            table: TableRef::new(table),
            pk: primary_key.to_string(),
        },
    )
}

#[pyfunction]
fn execute_native(
    py: Python<'_>,
    url: &str,
    sql: &str,
    params: Vec<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let values = params
        .into_iter()
        .map(|value| py_to_db_value(py, value))
        .collect::<PyResult<Vec<_>>>()?;
    let result =
        execute_url(url, sql, &values).map_err(|error| PyValueError::new_err(error.to_string()))?;
    query_result_to_python(py, result)
}

fn query_result_to_python(
    py: Python<'_>,
    result: ormdantic_engine::QueryResult,
) -> PyResult<Py<PyAny>> {
    let output = PyDict::new(py);
    output.set_item("columns", result.columns())?;
    let rows = PyList::empty(py);
    for row in result.rows() {
        let py_row = PyList::empty(py);
        for value in row {
            py_row.append(db_value_to_py(py, value)?)?;
        }
        rows.append(py_row)?;
    }
    output.set_item("rows", rows)?;
    Ok(output.into_any().unbind())
}

#[pyfunction]
fn snake_case(value: &str) -> String {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut previous: Option<char> = None;
    for ch in value.chars() {
        if ch == '_' || ch == '-' || ch.is_whitespace() {
            push_word(&mut words, &mut current);
            previous = None;
            continue;
        }
        if let Some(prev) = previous {
            if (prev.is_lowercase() && ch.is_uppercase())
                || (prev.is_ascii_digit() && ch.is_alphabetic())
            {
                push_word(&mut words, &mut current);
            }
        }
        current.push(ch);
        previous = Some(ch);
    }
    push_word(&mut words, &mut current);
    words
        .into_iter()
        .map(|word| word.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("_")
}

#[pyfunction]
fn sql_value(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    match py_to_db_value(py, value)? {
        DbValue::Null => Ok(py.None()),
        DbValue::Integer(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Real(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Text(value) => Ok(PyString::new(py, &value).into_any().unbind()),
        DbValue::Bool(value) => Ok(value.into_py_any(py)?),
    }
}

type ColumnDdl = (
    String,
    String,
    bool,
    bool,
    Option<String>,
    Option<String>,
    Option<usize>,
    bool,
    Vec<String>,
);

#[pyfunction]
fn compile_create_table_sql(
    dialect: &str,
    table: &str,
    columns: Vec<ColumnDdl>,
    indexes: Vec<(String, Vec<String>, bool)>,
    unique_constraints: Vec<Vec<String>>,
) -> PyResult<Vec<String>> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let column_sql = columns
        .into_iter()
        .map(
            |(
                name,
                kind,
                nullable,
                primary_key,
                foreign_table,
                foreign_column,
                max_length,
                unique,
                checks,
            )| {
                let mut sql = format!(
                    "{} {}",
                    dialect.quote_ident(&name),
                    ddl_type(&kind, max_length)
                );
                if primary_key {
                    sql.push_str(" PRIMARY KEY");
                }
                if !nullable || primary_key {
                    sql.push_str(" NOT NULL");
                }
                if unique {
                    sql.push_str(" UNIQUE");
                }
                if let (Some(foreign_table), Some(foreign_column)) = (foreign_table, foreign_column)
                {
                    sql.push_str(&format!(
                        " REFERENCES {}({})",
                        dialect.quote_ident(&foreign_table),
                        dialect.quote_ident(&foreign_column)
                    ));
                }
                for check in checks {
                    sql.push_str(&format!(" CHECK ({check})"));
                }
                sql
            },
        )
        .collect::<Vec<_>>();
    let mut table_parts = column_sql;
    for columns in unique_constraints {
        let rendered = columns
            .iter()
            .map(|column| dialect.quote_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        table_parts.push(format!("UNIQUE ({rendered})"));
    }
    let mut statements = vec![format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        dialect.quote_ident(table),
        table_parts.join(", ")
    )];
    for (name, columns, unique) in indexes {
        let rendered = columns
            .iter()
            .map(|column| dialect.quote_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let uniqueness = if unique { "UNIQUE " } else { "" };
        statements.push(format!(
            "CREATE {uniqueness}INDEX IF NOT EXISTS {} ON {} ({rendered})",
            dialect.quote_ident(&name),
            dialect.quote_ident(table)
        ));
    }
    Ok(statements)
}

#[pyfunction]
fn compile_drop_table_sql(dialect: &str, table: &str) -> PyResult<String> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(format!(
        "DROP TABLE IF EXISTS {}",
        dialect.quote_ident(table)
    ))
}

fn select_columns(
    columns: Vec<String>,
    aliases: Option<Vec<String>>,
) -> PyResult<Vec<SelectColumn>> {
    let Some(aliases) = aliases else {
        return Ok(columns.into_iter().map(SelectColumn::new).collect());
    };
    if columns.len() != aliases.len() {
        return Err(PyValueError::new_err(
            "select column aliases must match selected columns",
        ));
    }
    Ok(columns
        .into_iter()
        .zip(aliases)
        .map(|(column, alias)| SelectColumn::aliased(column, alias))
        .collect())
}

fn filter_specs(filters: Vec<FilterSpec>) -> PyResult<Vec<Filter>> {
    filters
        .into_iter()
        .map(|(column, operator, params)| {
            let first = params.first().cloned().unwrap_or_else(|| column.clone());
            match operator.as_str() {
                "eq" => Ok(Filter::Eq {
                    column,
                    param: first,
                }),
                "ne" => Ok(Filter::Ne {
                    column,
                    param: first,
                }),
                "lt" => Ok(Filter::Lt {
                    column,
                    param: first,
                }),
                "le" => Ok(Filter::Le {
                    column,
                    param: first,
                }),
                "gt" => Ok(Filter::Gt {
                    column,
                    param: first,
                }),
                "ge" => Ok(Filter::Ge {
                    column,
                    param: first,
                }),
                "like" => Ok(Filter::Like {
                    column,
                    param: first,
                }),
                "ilike" => Ok(Filter::ILike {
                    column,
                    param: first,
                }),
                "in" => Ok(Filter::In { column, params }),
                "not_in" => Ok(Filter::NotIn { column, params }),
                "is_null" => Ok(Filter::IsNull { column }),
                "is_not_null" => Ok(Filter::IsNotNull { column }),
                other => Err(PyValueError::new_err(format!(
                    "unsupported filter operator '{other}'"
                ))),
            }
        })
        .collect()
}

fn parse_sort_direction(direction: &str) -> PyResult<SortDirection> {
    match direction {
        "asc" | "ASC" => Ok(SortDirection::Asc),
        "desc" | "DESC" => Ok(SortDirection::Desc),
        other => Err(PyValueError::new_err(format!(
            "unsupported sort direction '{other}'"
        ))),
    }
}

fn compile_to_python(py: Python<'_>, dialect: &str, query: QueryAst) -> PyResult<Py<PyAny>> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = query
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    compiled_query_to_dict(py, compiled)
}

fn compiled_query_to_dict(py: Python<'_>, query: CompiledQuery) -> PyResult<Py<PyAny>> {
    let result = PyDict::new(py);
    result.set_item("sql", query.sql())?;
    result.set_item("params", query.params())?;
    result.set_item("operation", operation_name(query.operation()))?;
    Ok(result.into_any().unbind())
}

fn operation_name(operation: &QueryOperation) -> &'static str {
    match operation {
        QueryOperation::Select => "select",
        QueryOperation::Insert => "insert",
        QueryOperation::Update => "update",
        QueryOperation::Upsert => "upsert",
        QueryOperation::Delete => "delete",
        QueryOperation::Count => "count",
    }
}

fn py_to_db_value(py: Python<'_>, value: Py<PyAny>) -> PyResult<DbValue> {
    let value = value.bind(py);
    if value.is_none() {
        return Ok(DbValue::Null);
    }
    if let Ok(value) = value.extract::<bool>() {
        return Ok(DbValue::Bool(value));
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(DbValue::Integer(value));
    }
    if let Ok(value) = value.extract::<f64>() {
        return Ok(DbValue::Real(value));
    }
    Ok(DbValue::Text(value.str()?.to_string()))
}

fn push_word(words: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        words.push(std::mem::take(current));
    }
}

fn db_value_to_py(py: Python<'_>, value: &DbValue) -> PyResult<Py<PyAny>> {
    match value {
        DbValue::Null => Ok(py.None()),
        DbValue::Integer(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Real(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Text(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Bool(value) => Ok((*value).into_py_any(py)?),
    }
}

fn ddl_type(kind: &str, max_length: Option<usize>) -> String {
    match kind {
        "uuid" => "TEXT".to_string(),
        "str" => max_length
            .map(|max_length| format!("VARCHAR({max_length})"))
            .unwrap_or_else(|| "TEXT".to_string()),
        "int" => "INTEGER".to_string(),
        "float" => "REAL".to_string(),
        "bool" => "BOOLEAN".to_string(),
        "date" => "DATE".to_string(),
        "datetime" => "DATETIME".to_string(),
        "decimal" => "NUMERIC".to_string(),
        "bytes" => "BLOB".to_string(),
        "enum" => "TEXT".to_string(),
        "json" | "model_json" | "list" | "dict" => "JSON".to_string(),
        _ => "TEXT".to_string(),
    }
}

fn row_to_dict<'py>(
    py: Python<'py>,
    row: &[Py<PyAny>],
    parsed_columns: &[Option<String>],
) -> PyResult<Bound<'py, PyDict>> {
    let record = PyDict::new(py);
    for (idx, column) in parsed_columns.iter().enumerate() {
        let Some(column) = column else {
            continue;
        };
        let Some(value) = row.get(idx) else {
            continue;
        };
        record.set_item(column, value.bind(py))?;
    }
    Ok(record)
}

#[pymodule]
fn _ormdantic(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyNativeConnection>()?;
    m.add_function(wrap_pyfunction!(hydrate_flat, m)?)?;
    m.add_function(wrap_pyfunction!(hydrate_joined, m)?)?;
    m.add_function(wrap_pyfunction!(plan_result_shape, m)?)?;
    m.add_function(wrap_pyfunction!(validate_schema_tables, m)?)?;
    m.add_function(wrap_pyfunction!(compile_select_pk, m)?)?;
    m.add_function(wrap_pyfunction!(compile_find_many, m)?)?;
    m.add_function(wrap_pyfunction!(compile_joined_find_many, m)?)?;
    m.add_function(wrap_pyfunction!(compile_count, m)?)?;
    m.add_function(wrap_pyfunction!(compile_insert, m)?)?;
    m.add_function(wrap_pyfunction!(compile_update, m)?)?;
    m.add_function(wrap_pyfunction!(compile_upsert, m)?)?;
    m.add_function(wrap_pyfunction!(compile_delete_pk, m)?)?;
    m.add_function(wrap_pyfunction!(execute_native, m)?)?;
    m.add_function(wrap_pyfunction!(snake_case, m)?)?;
    m.add_function(wrap_pyfunction!(sql_value, m)?)?;
    m.add_function(wrap_pyfunction!(compile_create_table_sql, m)?)?;
    m.add_function(wrap_pyfunction!(compile_drop_table_sql, m)?)?;
    Ok(())
}

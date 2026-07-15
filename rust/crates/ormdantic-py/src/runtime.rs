use crate::transactions::PyTransactionOptions;
use ormdantic_dialects::{AnyDialect, Dialect, ReflectionScope};
use ormdantic_engine::{
    execute_url, runtime_capabilities as engine_runtime_capabilities, DbValue, NativeConnection,
    QueryResult, Reflector,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::{exceptions::PyValueError, IntoPyObjectExt};
use std::sync::Mutex;

#[pyclass]
pub(crate) struct PyNativeConnection {
    inner: Mutex<NativeConnection>,
}

#[pyfunction]
pub(crate) fn execute_native(
    py: Python<'_>,
    url: &str,
    sql: &str,
    params: Vec<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let values = params
        .into_iter()
        .map(|value| py_to_db_value(py, value))
        .collect::<PyResult<Vec<_>>>()?;
    let result = py
        .detach(|| execute_url(url, sql, &values).map_err(|error| error.to_string()))
        .map_err(PyValueError::new_err)?;
    query_result_to_python(py, result)
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
        let result = py
            .detach(|| {
                self.inner
                    .lock()
                    .map_err(|_| "native connection lock poisoned".to_string())?
                    .execute(sql, &values)
                    .map_err(|error| error.to_string())
            })
            .map_err(PyValueError::new_err)?;
        query_result_to_python(py, result)
    }

    #[pyo3(signature = (options=None))]
    fn begin(&self, options: Option<PyTransactionOptions>) -> PyResult<()> {
        let mut connection = self
            .inner
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

pub(crate) fn query_result_to_python(py: Python<'_>, result: QueryResult) -> PyResult<Py<PyAny>> {
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
    output.set_item("rowcount", result.row_count())?;
    Ok(output.into_any().unbind())
}

#[pyfunction(signature = (url, scope=None))]
pub(crate) fn reflect_schema(
    py: Python<'_>,
    url: &str,
    scope: Option<String>,
) -> PyResult<Py<PyAny>> {
    let reflector =
        Reflector::for_url(url).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let scope = scope
        .map(|schema| ReflectionScope::new().schema(schema))
        .unwrap_or_default();
    let output = PyDict::new(py);
    let queries = PyList::empty(py);
    for query in reflector.reflection_queries(&scope) {
        let item = PyDict::new(py);
        item.set_item("kind", format!("{:?}", query.kind()).to_ascii_lowercase())?;
        item.set_item("sql", query.sql())?;
        queries.append(item)?;
    }
    output.set_item("queries", queries)?;
    output.set_item("tables", PyList::empty(py))?;
    Ok(output.into_any().unbind())
}

pub(crate) fn table_names_sql(dialect: &AnyDialect) -> String {
    match dialect.kind() {
        ormdantic_dialects::DialectKind::Sqlite => {
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name".to_string()
        }
        ormdantic_dialects::DialectKind::Postgres => {
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name".to_string()
        }
        ormdantic_dialects::DialectKind::MySql | ormdantic_dialects::DialectKind::MariaDb => {
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' ORDER BY table_name".to_string()
        }
        ormdantic_dialects::DialectKind::MsSql => {
            "SELECT name FROM sys.tables ORDER BY name".to_string()
        }
        ormdantic_dialects::DialectKind::Oracle => {
            "SELECT table_name FROM user_tables ORDER BY table_name".to_string()
        }
    }
}

pub(crate) fn columns_sql(dialect: &AnyDialect) -> String {
    let table = dialect.placeholder(1);
    match dialect.kind() {
        ormdantic_dialects::DialectKind::Sqlite => {
            format!("SELECT name, type, NOT [notnull], dflt_value, pk FROM pragma_table_xinfo({table}) WHERE hidden <> 1")
        }
        ormdantic_dialects::DialectKind::Postgres => format!(
            "SELECT c.column_name, c.data_type, (c.is_nullable = 'YES'), c.column_default, EXISTS (SELECT 1 FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = c.table_schema AND tc.table_name = c.table_name AND kcu.column_name = c.column_name) FROM information_schema.columns c WHERE c.table_schema = 'public' AND c.table_name = {table} ORDER BY c.ordinal_position"
        ),
        ormdantic_dialects::DialectKind::MySql | ormdantic_dialects::DialectKind::MariaDb => format!(
            "SELECT column_name, data_type, (is_nullable = 'YES'), column_default, (column_key = 'PRI') FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = {table} ORDER BY ordinal_position"
        ),
        ormdantic_dialects::DialectKind::MsSql => format!(
            "SELECT c.name, t.name, CONVERT(bit, c.is_nullable), OBJECT_DEFINITION(c.default_object_id), CONVERT(bit, CASE WHEN ic.column_id IS NULL THEN 0 ELSE 1 END) FROM sys.columns c JOIN sys.types t ON c.user_type_id = t.user_type_id JOIN sys.tables tb ON c.object_id = tb.object_id LEFT JOIN sys.indexes i ON tb.object_id = i.object_id AND i.is_primary_key = 1 LEFT JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND c.column_id = ic.column_id WHERE tb.name = {table} ORDER BY c.column_id"
        ),
        ormdantic_dialects::DialectKind::Oracle => format!(
            "SELECT column_name, data_type, CASE nullable WHEN 'Y' THEN 1 ELSE 0 END, data_default, 0 FROM user_tab_columns WHERE table_name = UPPER({table}) ORDER BY column_id"
        ),
    }
}

pub(crate) fn indexes_sql(dialect: &AnyDialect) -> String {
    let table = dialect.placeholder(1);
    match dialect.kind() {
        ormdantic_dialects::DialectKind::Sqlite => {
            format!("SELECT name, [unique], origin FROM pragma_index_list({table})")
        }
        ormdantic_dialects::DialectKind::Postgres => format!(
            "SELECT i.relname, ix.indisunique FROM pg_class t JOIN pg_index ix ON t.oid = ix.indrelid JOIN pg_class i ON i.oid = ix.indexrelid WHERE t.relname = {table} AND NOT ix.indisprimary ORDER BY i.relname"
        ),
        ormdantic_dialects::DialectKind::MySql | ormdantic_dialects::DialectKind::MariaDb => format!(
            "SELECT index_name, (non_unique = 0) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = {table} AND index_name <> 'PRIMARY' GROUP BY index_name, non_unique ORDER BY index_name"
        ),
        ormdantic_dialects::DialectKind::MsSql => format!(
            "SELECT i.name, CONVERT(bit, i.is_unique) FROM sys.indexes i JOIN sys.tables t ON i.object_id = t.object_id WHERE t.name = {table} AND i.is_primary_key = 0 AND i.name IS NOT NULL ORDER BY i.name"
        ),
        ormdantic_dialects::DialectKind::Oracle => format!(
            "SELECT index_name, CASE uniqueness WHEN 'UNIQUE' THEN 1 ELSE 0 END FROM user_indexes WHERE table_name = UPPER({table}) ORDER BY index_name"
        ),
    }
}

pub(crate) fn index_columns_sql(dialect: &AnyDialect) -> Option<String> {
    let index = dialect.placeholder(1);
    match dialect.kind() {
        ormdantic_dialects::DialectKind::Sqlite => Some(format!(
            "SELECT name FROM pragma_index_info({index}) ORDER BY seqno"
        )),
        _ => None,
    }
}

pub(crate) fn foreign_keys_sql(dialect: &AnyDialect) -> String {
    let table = dialect.placeholder(1);
    match dialect.kind() {
        ormdantic_dialects::DialectKind::Sqlite => {
            format!(
                "SELECT [table], [from], [to], on_update, on_delete, NULL \
                 FROM pragma_foreign_key_list({table})"
            )
        }
        ormdantic_dialects::DialectKind::Postgres => format!(
            "SELECT ccu.table_name, kcu.column_name, ccu.column_name, rc.update_rule, rc.delete_rule, tc.constraint_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema JOIN information_schema.referential_constraints rc ON rc.constraint_name = tc.constraint_name AND rc.constraint_schema = tc.constraint_schema WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public' AND tc.table_name = {table} ORDER BY kcu.ordinal_position"
        ),
        ormdantic_dialects::DialectKind::MySql | ormdantic_dialects::DialectKind::MariaDb => format!(
            "SELECT kcu.referenced_table_name, kcu.column_name, kcu.referenced_column_name, rc.update_rule, rc.delete_rule, kcu.constraint_name FROM information_schema.key_column_usage kcu JOIN information_schema.referential_constraints rc ON rc.constraint_schema = kcu.constraint_schema AND rc.constraint_name = kcu.constraint_name AND rc.table_name = kcu.table_name WHERE kcu.table_schema = DATABASE() AND kcu.table_name = {table} AND kcu.referenced_table_name IS NOT NULL ORDER BY kcu.ordinal_position"
        ),
        ormdantic_dialects::DialectKind::MsSql => format!(
            "SELECT rt.name, pc.name, rc.name, fk.update_referential_action_desc, fk.delete_referential_action_desc, fk.name FROM sys.foreign_key_columns fkc JOIN sys.foreign_keys fk ON fkc.constraint_object_id = fk.object_id JOIN sys.tables pt ON fkc.parent_object_id = pt.object_id JOIN sys.columns pc ON pc.object_id = pt.object_id AND pc.column_id = fkc.parent_column_id JOIN sys.tables rt ON fkc.referenced_object_id = rt.object_id JOIN sys.columns rc ON rc.object_id = rt.object_id AND rc.column_id = fkc.referenced_column_id WHERE pt.name = {table} ORDER BY pc.column_id"
        ),
        ormdantic_dialects::DialectKind::Oracle => format!(
            "SELECT r.table_name, cc.column_name, rcc.column_name, NULL, c.delete_rule, c.constraint_name FROM user_constraints c JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name JOIN user_constraints r ON c.r_constraint_name = r.constraint_name JOIN user_cons_columns rcc ON r.constraint_name = rcc.constraint_name AND cc.position = rcc.position WHERE c.constraint_type = 'R' AND c.table_name = UPPER({table}) ORDER BY cc.position"
        ),
    }
}

pub(crate) fn db_value_to_string(value: &DbValue) -> Option<String> {
    match value {
        DbValue::Null => None,
        DbValue::Integer(value) => Some(value.to_string()),
        DbValue::UnsignedInteger(value) => Some(value.to_string()),
        DbValue::Decimal(value) => Some(value.clone()),
        DbValue::Real(value) => Some(value.to_string()),
        DbValue::Text(value) => Some(value.clone()),
        DbValue::Bool(value) => Some(value.to_string()),
    }
}

pub(crate) fn db_value_to_bool(value: &DbValue) -> bool {
    match value {
        DbValue::Null => false,
        DbValue::Integer(value) => *value != 0,
        DbValue::UnsignedInteger(value) => *value != 0,
        DbValue::Decimal(value) => value != "0" && value != "0.0",
        DbValue::Real(value) => *value != 0.0,
        DbValue::Text(value) => matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "t" | "true" | "y" | "yes"
        ),
        DbValue::Bool(value) => *value,
    }
}

pub(crate) fn py_to_db_value(py: Python<'_>, value: Py<PyAny>) -> PyResult<DbValue> {
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
    if let Ok(value) = value.extract::<u64>() {
        return Ok(DbValue::UnsignedInteger(value));
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(DbValue::Text(value));
    }
    let int_type = py.import("builtins")?.getattr("int")?;
    if value.is_instance(&int_type)? {
        return Ok(DbValue::Decimal(value.str()?.to_string()));
    }
    let decimal_type = py.import("decimal")?.getattr("Decimal")?;
    if value.is_instance(&decimal_type)? {
        let formatted = value
            .call_method1("__format__", ("f",))?
            .extract::<String>()?;
        return Ok(DbValue::Decimal(formatted));
    }
    if let Ok(value) = value.extract::<f64>() {
        return Ok(DbValue::Real(value));
    }
    Ok(DbValue::Text(value.str()?.to_string()))
}

fn db_value_to_py(py: Python<'_>, value: &DbValue) -> PyResult<Py<PyAny>> {
    match value {
        DbValue::Null => Ok(py.None()),
        DbValue::Integer(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::UnsignedInteger(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Decimal(value) => decimal_to_py(py, value),
        DbValue::Real(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Text(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Bool(value) => Ok((*value).into_py_any(py)?),
    }
}

fn decimal_to_py(py: Python<'_>, value: &str) -> PyResult<Py<PyAny>> {
    let decimal = py.import("decimal")?.getattr("Decimal")?;
    decimal.call1((value,)).map(|value| value.unbind())
}

#[pyfunction]
pub(crate) fn runtime_capabilities(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let capabilities = PyDict::new(py);
    for (name, available) in engine_runtime_capabilities() {
        capabilities.set_item(name, available)?;
    }
    Ok(capabilities.into_any().unbind())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reflection_sql_uses_dialect_specific_placeholder_styles() {
        let sqlite = AnyDialect::parse("sqlite").unwrap();
        let postgres = AnyDialect::parse("postgresql").unwrap();
        let mysql = AnyDialect::parse("mysql").unwrap();
        let mssql = AnyDialect::parse("mssql").unwrap();
        let oracle = AnyDialect::parse("oracle").unwrap();

        assert!(table_names_sql(&sqlite).contains("sqlite_master"));
        assert!(columns_sql(&sqlite).contains("pragma_table_xinfo"));
        assert!(columns_sql(&sqlite).contains("hidden <> 1"));
        assert!(columns_sql(&postgres).contains("$1"));
        assert!(indexes_sql(&mysql).contains("?"));
        assert!(foreign_keys_sql(&mssql).contains("@P1"));
        assert!(columns_sql(&oracle).contains(":1"));
    }

    #[test]
    fn db_value_helpers_convert_strings_and_truthiness() {
        assert_eq!(db_value_to_string(&DbValue::Null), None);
        assert_eq!(
            db_value_to_string(&DbValue::Text("flavor".to_string())),
            Some("flavor".to_string())
        );
        assert!(db_value_to_bool(&DbValue::Text("YES".to_string())));
        assert!(db_value_to_bool(&DbValue::Integer(1)));
        assert!(!db_value_to_bool(&DbValue::Real(0.0)));
        assert!(!db_value_to_bool(&DbValue::Null));
    }
}

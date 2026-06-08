use ormdantic_core::{
    DeferrableMode, EventKind, EventPayload, IdentityKey, IsolationLevel, TransactionAccessMode,
    TransactionOptions,
};
use ormdantic_dialects::{AnyDialect, Dialect, ReflectionScope};
use ormdantic_engine::{
    execute_url, runtime_capabilities as engine_runtime_capabilities, DbValue, NativeConnection,
    Reflector,
};
use ormdantic_hydrate::{
    merge_selectin_results, FlatHydrationPlan, HydratedRow, ResultShape, SelectInHydrationPlan,
};
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, FieldKind, ForeignKeyDef, IndexDef, RelationshipCardinality,
    RelationshipDef, SchemaDef, SchemaDiffer, SchemaOperation, SchemaRegistry, SchemaSnapshot,
    TableDef, UniqueConstraintDef,
};
use ormdantic_sql::{
    BinaryOp, CompiledQuery, DdlAst, DmlAst, Expr, Filter, JoinSpec, JoinedFilter, JoinedOrderBy,
    JoinedSelectColumn, OrderBy, OrderExpr, OrderNulls, Projection, QueryAst, QueryOperation,
    SelectAst, SelectColumn, SelectInPlan as SqlSelectInPlan, SortDirection, SqlLiteral, TableRef,
    TableSource, UnaryOp,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use pyo3::IntoPyObjectExt;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

type FilterSpec = (String, String, Vec<String>);
type RuntimeJoinedFilter = (String, Vec<FilterSpec>);
type RuntimeJoinedOrder = (String, String, String);
type RuntimeCheck = (String, String, String);

struct RuntimeJoinedQuery {
    filters: Vec<Filter>,
    order_by: Vec<String>,
    direction: SortDirection,
    limit: Option<usize>,
    offset: Option<usize>,
    paths: Vec<String>,
    relationship_filters: Vec<RuntimeJoinedFilter>,
    relationship_order_by: Vec<RuntimeJoinedOrder>,
}

const FILTER_OPERATORS: &[&str] = &[
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "like",
    "ilike",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
];
type RuntimeColumn = (
    String,
    String,
    bool,
    bool,
    Option<String>,
    Option<String>,
    Option<usize>,
    bool,
    Vec<RuntimeCheck>,
);
type RuntimeIndex = (String, Vec<String>, bool);
type RuntimeRelationship = (String, String, String, Option<String>);
type RuntimeTableSpec = (
    String,
    String,
    String,
    Vec<RuntimeColumn>,
    Vec<RuntimeIndex>,
    Vec<Vec<String>>,
    Vec<RuntimeRelationship>,
);

#[pyclass]
struct PyNativeConnection {
    inner: Mutex<NativeConnection>,
}

#[derive(Clone)]
struct RuntimeTable {
    table: String,
    primary_key: String,
    columns: Vec<RuntimeColumn>,
    indexes: Vec<RuntimeIndex>,
    unique_constraints: Vec<Vec<String>>,
    relationships: Vec<RuntimeRelationship>,
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
struct PyDatabase {
    url: String,
    connection: Arc<Mutex<NativeConnection>>,
    tables: Arc<HashMap<String, RuntimeTable>>,
    table_order: Arc<Vec<String>>,
}

#[pyclass]
struct PyTableHandle {
    url: String,
    connection: Arc<Mutex<NativeConnection>>,
    tables: Arc<HashMap<String, RuntimeTable>>,
    table: RuntimeTable,
}

#[pyclass]
#[derive(Clone, Default)]
struct PyTransactionOptions {
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
    fn to_rust_options(&self) -> PyResult<TransactionOptions> {
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

#[pyclass]
#[derive(Default)]
struct PySessionRuntime {
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

#[pyclass]
#[derive(Default)]
struct PyEventBridge {
    events: Mutex<Vec<EventPayload>>,
}

type PyEventRecord = (String, Option<String>, Option<String>);

#[pymethods]
impl PyEventBridge {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn emit(&self, kind: &str, target: Option<String>, message: Option<String>) -> PyResult<()> {
        let mut payload = EventPayload::new(parse_event_kind(kind)?);
        if let Some(target) = target {
            payload = payload.with_target(target);
        }
        if let Some(message) = message {
            payload = payload.with_message(message);
        }
        self.events
            .lock()
            .map_err(|_| PyValueError::new_err("event bridge lock poisoned"))?
            .push(payload);
        Ok(())
    }

    fn events(&self) -> PyResult<Vec<PyEventRecord>> {
        Ok(self
            .events
            .lock()
            .map_err(|_| PyValueError::new_err("event bridge lock poisoned"))?
            .iter()
            .map(|event| {
                (
                    event_kind_name(event.kind()).to_string(),
                    event.target().map(ToString::to_string),
                    event.message().map(ToString::to_string),
                )
            })
            .collect())
    }
}

fn parse_event_kind(value: &str) -> PyResult<EventKind> {
    match value.to_ascii_lowercase().replace(['-', ' '], "_").as_str() {
        "before_execute" => Ok(EventKind::BeforeExecute),
        "after_execute" => Ok(EventKind::AfterExecute),
        "before_commit" => Ok(EventKind::BeforeCommit),
        "after_commit" => Ok(EventKind::AfterCommit),
        "after_rollback" => Ok(EventKind::AfterRollback),
        "before_flush" => Ok(EventKind::BeforeFlush),
        "after_flush" => Ok(EventKind::AfterFlush),
        "before_migration" => Ok(EventKind::BeforeMigration),
        "after_migration" => Ok(EventKind::AfterMigration),
        "before_reflection" => Ok(EventKind::BeforeReflection),
        "after_reflection" => Ok(EventKind::AfterReflection),
        other => Err(PyValueError::new_err(format!(
            "unsupported event kind '{other}'"
        ))),
    }
}

fn event_kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::BeforeExecute => "before_execute",
        EventKind::AfterExecute => "after_execute",
        EventKind::BeforeCommit => "before_commit",
        EventKind::AfterCommit => "after_commit",
        EventKind::AfterRollback => "after_rollback",
        EventKind::BeforeFlush => "before_flush",
        EventKind::AfterFlush => "after_flush",
        EventKind::BeforeMigration => "before_migration",
        EventKind::AfterMigration => "after_migration",
        EventKind::BeforeReflection => "before_reflection",
        EventKind::AfterReflection => "after_reflection",
    }
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
fn validate_schema_tables(tables: &Bound<'_, PyAny>) -> PyResult<usize> {
    let mut registry = SchemaRegistry::new();
    if let Ok(tables) = tables.extract::<Vec<RuntimeTableSpec>>() {
        for (
            model_key,
            tablename,
            primary_key,
            columns,
            indexes,
            unique_constraints,
            relationships,
        ) in tables
        {
            registry
                .register_table(runtime_table_def(
                    model_key,
                    tablename,
                    primary_key,
                    columns,
                    indexes,
                    unique_constraints,
                    relationships,
                )?)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        }
    } else {
        for (tablename, primary_key, columns) in
            tables.extract::<Vec<(String, String, Vec<String>)>>()?
        {
            registry
                .register_table(TableDef::new(tablename, primary_key, columns))
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        }
    }
    registry
        .validate_relationships()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(registry.tables().len())
}

fn runtime_table_def(
    model_key: String,
    tablename: String,
    primary_key: String,
    columns: Vec<RuntimeColumn>,
    indexes: Vec<RuntimeIndex>,
    unique_constraints: Vec<Vec<String>>,
    relationships: Vec<RuntimeRelationship>,
) -> PyResult<TableDef> {
    let mut foreign_keys = Vec::new();
    let mut check_constraints = Vec::new();
    let mut unique_column_constraints = Vec::new();
    let columns = columns
        .into_iter()
        .map(
            |(
                name,
                kind,
                nullable,
                primary_key,
                foreign_table,
                foreign_column,
                _max_length,
                unique,
                checks,
            )| {
                if unique {
                    unique_column_constraints.push(vec![name.clone()]);
                }
                if let (Some(foreign_table), Some(foreign_column)) =
                    (foreign_table.clone(), foreign_column)
                {
                    foreign_keys.push(ForeignKeyDef::new(
                        vec![name.clone()],
                        foreign_table.clone(),
                        vec![foreign_column],
                    ));
                }
                for check in checks {
                    check_constraints.push(
                        CheckConstraintDef::new(render_check_constraint(&name, &check)?).named(
                            format!(
                                "{tablename}_{name}_{}_check",
                                check_constraint_suffix(&check)?
                            ),
                        ),
                    );
                }
                let kind = foreign_table
                    .map(|target_table| FieldKind::ForeignKey { target_table })
                    .unwrap_or_else(|| field_kind_from_runtime(&kind));
                Ok(ColumnDef::new(name, kind)
                    .nullable(nullable)
                    .primary_key(primary_key))
            },
        )
        .collect::<PyResult<Vec<_>>>()?;
    let indexes = indexes
        .into_iter()
        .map(|(name, columns, unique)| IndexDef::new(name, columns).unique(unique))
        .collect::<Vec<_>>();
    let unique_constraints = unique_constraints
        .into_iter()
        .chain(unique_column_constraints)
        .enumerate()
        .map(|(idx, columns)| {
            UniqueConstraintDef::new(format!("{tablename}_unique_{idx}"), columns)
        })
        .collect::<Vec<_>>();
    let relationships = relationships
        .into_iter()
        .map(|(field, target_table, target_field, back_reference)| {
            let cardinality = if back_reference.is_some() {
                RelationshipCardinality::Many
            } else {
                RelationshipCardinality::One
            };
            let relationship = RelationshipDef::new(field, target_table, target_field, cardinality);
            if let Some(back_reference) = back_reference {
                relationship.with_back_reference(back_reference)
            } else {
                relationship
            }
        })
        .collect::<Vec<_>>();
    Ok(TableDef::from_parts(
        tablename,
        model_key,
        primary_key,
        columns,
        indexes,
        unique_constraints,
        relationships,
    )
    .with_check_constraints(check_constraints)
    .with_foreign_keys(foreign_keys))
}

fn field_kind_from_runtime(kind: &str) -> FieldKind {
    match kind {
        "str" => FieldKind::String,
        "int" => FieldKind::Integer,
        "float" => FieldKind::Float,
        "bool" => FieldKind::Boolean,
        "uuid" => FieldKind::Uuid,
        "date" => FieldKind::Date,
        "datetime" => FieldKind::DateTime,
        "dict" | "list" | "json" => FieldKind::Json,
        "model_json" => FieldKind::ModelJson,
        "enum" => FieldKind::Enum,
        "decimal" => FieldKind::Decimal,
        "bytes" => FieldKind::Binary,
        _ => FieldKind::Unknown,
    }
}

fn schema_def_from_runtime(tables: Vec<RuntimeTableSpec>) -> PyResult<SchemaDef> {
    Ok(SchemaDef::from_tables(
        tables
            .into_iter()
            .map(
                |(
                    model_key,
                    tablename,
                    primary_key,
                    columns,
                    indexes,
                    unique_constraints,
                    relationships,
                )| {
                    runtime_table_def(
                        model_key,
                        tablename,
                        primary_key,
                        columns,
                        indexes,
                        unique_constraints,
                        relationships,
                    )
                },
            )
            .collect::<PyResult<Vec<_>>>()?,
    ))
}

fn compiled_queries_to_list(py: Python<'_>, queries: Vec<CompiledQuery>) -> PyResult<Py<PyAny>> {
    let output = PyList::empty(py);
    for query in queries {
        output.append(compiled_query_to_dict(py, query)?)?;
    }
    Ok(output.into_any().unbind())
}

fn hash_to_hydrated_row(row: HashMap<String, String>) -> HydratedRow {
    row.into_iter().collect::<BTreeMap<_, _>>()
}

fn hydrated_rows_to_python(py: Python<'_>, rows: Vec<HydratedRow>) -> PyResult<Py<PyAny>> {
    let output = PyList::empty(py);
    for row in rows {
        let item = PyDict::new(py);
        for (key, value) in row {
            item.set_item(key, value)?;
        }
        output.append(item)?;
    }
    Ok(output.into_any().unbind())
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
#[allow(clippy::too_many_arguments)]
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
#[allow(clippy::too_many_arguments)]
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
        relationship_filters: Vec::new(),
        order_by: order_columns
            .into_iter()
            .map(|column| OrderBy::new(column, direction.clone()))
            .collect(),
        relationship_order_by: Vec::new(),
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

#[allow(clippy::too_many_arguments)]
#[pyfunction(signature = (dialect, table, projections, where_column=None, where_param=None, limit=None, offset=None))]
fn compile_expression_query(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    projections: Vec<(String, Option<String>)>,
    where_column: Option<String>,
    where_param: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let projections = projections
        .into_iter()
        .map(|(column, alias)| match alias {
            Some(alias) => Projection::aliased(Expr::column(column), alias),
            None => Projection::new(Expr::column(column)),
        })
        .collect::<Vec<_>>();
    let mut query = SelectAst::new(projections).from(TableSource::table(table));
    if let (Some(column), Some(param)) = (where_column, where_param) {
        query = query.where_expr(Expr::eq(Expr::column(column), Expr::param(param)));
    }
    if let Some(limit) = limit {
        query = query.limit(limit);
    }
    if let Some(offset) = offset {
        query = query.offset(offset);
    }
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = query
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    compiled_query_to_dict(py, compiled)
}

#[pyfunction]
fn compile_typed_expression_query(
    py: Python<'_>,
    dialect: &str,
    query: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let query = query.downcast::<PyDict>()?;
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = select_ast_from_payload(py, query)?
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let result = PyDict::new(py);
    result.set_item("sql", compiled.sql())?;
    result.set_item("params", compiled.params())?;
    result.set_item("operation", operation_name(compiled.operation()))?;
    if let Some(values) = query.get_item("values")? {
        result.set_item("values", values)?;
    }
    Ok(result.into_any().unbind())
}

#[pyfunction]
fn compile_typed_update_query(
    py: Python<'_>,
    dialect: &str,
    query: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let query = query.downcast::<PyDict>()?;
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = update_ast_from_payload(py, query)?
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let result = PyDict::new(py);
    result.set_item("sql", compiled.sql())?;
    result.set_item("params", compiled.params())?;
    result.set_item("operation", operation_name(compiled.operation()))?;
    if let Some(values) = query.get_item("values")? {
        result.set_item("values", values)?;
    }
    Ok(result.into_any().unbind())
}

fn select_ast_from_payload(py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<SelectAst> {
    let table: String = required_item(query, "table")?.extract()?;
    let projections = required_item(query, "projections")?.extract::<Vec<Py<PyAny>>>()?;
    let mut select =
        SelectAst::new(parse_projections(py, projections)?).from(TableSource::table(table));

    if let Some(where_expr) = query.get_item("where")? {
        select = select.where_expr(parse_expression(where_expr)?);
    }
    if let Some(group_by) = query.get_item("group_by")? {
        select = select.group_by(parse_expression_list(py, group_by)?);
    }
    if let Some(having) = query.get_item("having")? {
        select = select.having(parse_expression(having)?);
    }
    if let Some(order_by) = query.get_item("order_by")? {
        select = select.order_by(parse_order_expressions(py, order_by)?);
    }
    if let Some(distinct) = query.get_item("distinct")? {
        select = select.distinct(distinct.extract()?);
    }
    if let Some(limit) = query.get_item("limit")? {
        select = select.limit(limit.extract()?);
    }
    if let Some(offset) = query.get_item("offset")? {
        select = select.offset(offset.extract()?);
    }
    Ok(select)
}

fn update_ast_from_payload(py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<DmlAst> {
    let table: String = required_item(query, "table")?.extract()?;
    let assignments = required_item(query, "assignments")?
        .extract::<Vec<Py<PyAny>>>()?
        .into_iter()
        .map(|assignment| {
            let assignment = assignment.bind(py).downcast::<PyDict>()?;
            Ok((
                required_item(assignment, "column")?.extract::<String>()?,
                parse_expression(required_item(assignment, "expr")?)?,
            ))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let where_expr = query.get_item("where")?.map(parse_expression).transpose()?;
    Ok(DmlAst::Update {
        table: TableSource::table(table),
        assignments,
        where_expr,
        returning: Vec::new(),
    })
}

fn parse_projections(py: Python<'_>, projections: Vec<Py<PyAny>>) -> PyResult<Vec<Projection>> {
    projections
        .into_iter()
        .map(|projection| {
            let projection = projection.bind(py).downcast::<PyDict>()?;
            let expr = parse_expression(required_item(projection, "expr")?)?;
            match projection.get_item("alias")? {
                Some(alias) => Ok(Projection::aliased(expr, alias.extract::<String>()?)),
                None => Ok(Projection::new(expr)),
            }
        })
        .collect()
}

fn parse_expression_list(py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<Vec<Expr>> {
    value
        .extract::<Vec<Py<PyAny>>>()?
        .into_iter()
        .map(|expr| parse_expression(expr.bind(py).clone()))
        .collect()
}

fn parse_order_expressions(py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<Vec<OrderExpr>> {
    value
        .extract::<Vec<Py<PyAny>>>()?
        .into_iter()
        .map(|order| {
            let order_payload = order.bind(py).downcast::<PyDict>()?;
            let expr = parse_expression(required_item(order_payload, "expr")?)?;
            let direction: String = required_item(order_payload, "direction")?.extract()?;
            let mut order_expr = OrderExpr::new(expr, parse_sort_direction(&direction)?);
            if let Some(nulls) = order_payload.get_item("nulls")? {
                order_expr = order_expr.nulls(parse_order_nulls(&nulls.extract::<String>()?)?);
            }
            Ok(order_expr)
        })
        .collect()
}

fn parse_expression(expr: Bound<'_, PyAny>) -> PyResult<Expr> {
    let expr = expr.downcast::<PyDict>()?;
    let kind: String = required_item(expr, "kind")?.extract()?;
    match kind.as_str() {
        "column" => {
            let name: String = required_item(expr, "name")?.extract()?;
            match expr.get_item("table")? {
                Some(table) => Ok(Expr::qualified_column(table.extract::<String>()?, name)),
                None => Ok(Expr::column(name)),
            }
        }
        "param" => Ok(Expr::param(
            required_item(expr, "name")?.extract::<String>()?,
        )),
        "literal" => parse_literal_expr(required_item(expr, "value")?),
        "raw_safe" => Ok(Expr::RawSafe(required_item(expr, "sql")?.extract()?)),
        "binary" => Ok(Expr::Binary {
            left: Box::new(parse_expression(required_item(expr, "left")?)?),
            op: parse_binary_op(&required_item(expr, "op")?.extract::<String>()?)?,
            right: Box::new(parse_expression(required_item(expr, "right")?)?),
        }),
        "unary" => Ok(Expr::Unary {
            op: parse_unary_op(&required_item(expr, "op")?.extract::<String>()?)?,
            expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
        }),
        "function" => {
            let args = match expr.get_item("args")? {
                Some(args) => args
                    .extract::<Vec<Py<PyAny>>>()?
                    .into_iter()
                    .map(|arg| parse_expression(arg.bind(expr.py()).clone()))
                    .collect::<PyResult<Vec<_>>>()?,
                None => Vec::new(),
            };
            Ok(Expr::Function {
                name: required_item(expr, "name")?.extract()?,
                args,
                over: None,
            })
        }
        "between" => Ok(Expr::Between {
            expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
            low: Box::new(parse_expression(required_item(expr, "low")?)?),
            high: Box::new(parse_expression(required_item(expr, "high")?)?),
        }),
        "in_list" => {
            let values = required_item(expr, "values")?
                .extract::<Vec<Py<PyAny>>>()?
                .into_iter()
                .map(|value| parse_expression(value.bind(expr.py()).clone()))
                .collect::<PyResult<Vec<_>>>()?;
            let negated = expr
                .get_item("negated")?
                .map(|value| value.extract::<bool>())
                .transpose()?
                .unwrap_or(false);
            Ok(Expr::InList {
                expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
                values,
                negated,
            })
        }
        "in_subquery" => {
            let negated = expr
                .get_item("negated")?
                .map(|value| value.extract::<bool>())
                .transpose()?
                .unwrap_or(false);
            let subquery_payload = required_item(expr, "subquery")?;
            let subquery = subquery_payload.downcast::<PyDict>()?;
            Ok(Expr::InSubquery {
                expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
                subquery: Box::new(select_ast_from_payload(expr.py(), subquery)?),
                negated,
            })
        }
        "exists" => {
            let subquery_payload = required_item(expr, "subquery")?;
            let subquery = subquery_payload.downcast::<PyDict>()?;
            Ok(Expr::Exists(Box::new(select_ast_from_payload(
                expr.py(),
                subquery,
            )?)))
        }
        "case" => {
            let whens = required_item(expr, "whens")?
                .extract::<Vec<Py<PyAny>>>()?
                .into_iter()
                .map(|when| {
                    let when = when.bind(expr.py()).downcast::<PyDict>()?;
                    Ok((
                        parse_expression(required_item(when, "when")?)?,
                        parse_expression(required_item(when, "then")?)?,
                    ))
                })
                .collect::<PyResult<Vec<_>>>()?;
            let else_expr = match expr.get_item("else")? {
                Some(value) if value.is_none() => None,
                Some(value) => Some(Box::new(parse_expression(value)?)),
                None => None,
            };
            Ok(Expr::Case { whens, else_expr })
        }
        "cast" => Ok(Expr::Cast {
            expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
            type_name: required_item(expr, "type")?.extract()?,
        }),
        "tuple" => {
            let values = required_item(expr, "values")?
                .extract::<Vec<Py<PyAny>>>()?
                .into_iter()
                .map(|value| parse_expression(value.bind(expr.py()).clone()))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(Expr::Tuple(values))
        }
        other => Err(PyValueError::new_err(format!(
            "unsupported expression kind '{other}'"
        ))),
    }
}

fn parse_literal_expr(value: Bound<'_, PyAny>) -> PyResult<Expr> {
    if value.is_none() {
        return Ok(Expr::Literal(SqlLiteral::Null));
    }
    if let Ok(value) = value.extract::<bool>() {
        return Ok(Expr::Literal(SqlLiteral::Boolean(value)));
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(Expr::Literal(SqlLiteral::Integer(value)));
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(Expr::Literal(SqlLiteral::String(value)));
    }
    Err(PyValueError::new_err(
        "literal expressions support None, bool, int, and str values",
    ))
}

fn parse_binary_op(op: &str) -> PyResult<BinaryOp> {
    match op {
        "eq" => Ok(BinaryOp::Eq),
        "ne" => Ok(BinaryOp::Ne),
        "lt" => Ok(BinaryOp::Lt),
        "le" => Ok(BinaryOp::Le),
        "gt" => Ok(BinaryOp::Gt),
        "ge" => Ok(BinaryOp::Ge),
        "add" => Ok(BinaryOp::Add),
        "sub" => Ok(BinaryOp::Sub),
        "mul" => Ok(BinaryOp::Mul),
        "div" => Ok(BinaryOp::Div),
        "and" => Ok(BinaryOp::And),
        "or" => Ok(BinaryOp::Or),
        "like" => Ok(BinaryOp::Like),
        "ilike" => Ok(BinaryOp::ILike),
        other => Err(PyValueError::new_err(format!(
            "unsupported binary operator '{other}'"
        ))),
    }
}

fn parse_unary_op(op: &str) -> PyResult<UnaryOp> {
    match op {
        "not" => Ok(UnaryOp::Not),
        "neg" => Ok(UnaryOp::Neg),
        "is_null" => Ok(UnaryOp::IsNull),
        "is_not_null" => Ok(UnaryOp::IsNotNull),
        other => Err(PyValueError::new_err(format!(
            "unsupported unary operator '{other}'"
        ))),
    }
}

fn parse_order_nulls(nulls: &str) -> PyResult<OrderNulls> {
    match nulls {
        "first" | "FIRST" => Ok(OrderNulls::First),
        "last" | "LAST" => Ok(OrderNulls::Last),
        other => Err(PyValueError::new_err(format!(
            "unsupported null ordering '{other}'"
        ))),
    }
}

fn required_item<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Bound<'py, PyAny>> {
    dict.get_item(key)?
        .ok_or_else(|| PyValueError::new_err(format!("expression payload missing '{key}'")))
}

#[pyfunction]
fn compile_schema_diff(
    py: Python<'_>,
    dialect: &str,
    from_schema: Vec<RuntimeTableSpec>,
    to_schema: Vec<RuntimeTableSpec>,
) -> PyResult<Py<PyAny>> {
    let from = SchemaSnapshot::new(schema_def_from_runtime(from_schema)?);
    let to = SchemaSnapshot::new(schema_def_from_runtime(to_schema)?);
    let diff =
        SchemaDiffer::diff(&from, &to).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = DdlAst::from_diff(diff)
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    compiled_queries_to_list(py, compiled)
}

#[pyfunction(signature = (url, scope=None))]
fn reflect_schema(py: Python<'_>, url: &str, scope: Option<String>) -> PyResult<Py<PyAny>> {
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

#[pyfunction]
fn compile_selectin_plan(
    py: Python<'_>,
    dialect: &str,
    parent_table: &str,
    child_table: &str,
    parent_key_columns: Vec<String>,
    child_key_columns: Vec<String>,
    param_names: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let plan = SqlSelectInPlan::new(
        parent_table,
        child_table,
        parent_key_columns,
        child_key_columns,
    );
    let compiled = plan
        .query_for_batch(param_names)
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    compiled_query_to_dict(py, compiled)
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
fn execute_selectin_load(
    py: Python<'_>,
    parent_rows: Vec<HashMap<String, String>>,
    child_rows: Vec<HashMap<String, String>>,
    parent_key_columns: Vec<String>,
    child_key_columns: Vec<String>,
    relationship_field: &str,
    target_table: &str,
    target_field: &str,
    uselist: bool,
) -> PyResult<Py<PyAny>> {
    let relationship = RelationshipDef::new(
        relationship_field,
        target_table,
        target_field,
        if uselist {
            RelationshipCardinality::Many
        } else {
            RelationshipCardinality::One
        },
    )
    .uselist(uselist);
    let plan = SelectInHydrationPlan::new(parent_key_columns, child_key_columns, relationship);
    let merged = merge_selectin_results(
        parent_rows.into_iter().map(hash_to_hydrated_row).collect(),
        child_rows.into_iter().map(hash_to_hydrated_row).collect(),
        &plan,
    );
    hydrated_rows_to_python(py, merged)
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

fn table_names_sql(dialect: &AnyDialect) -> String {
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

fn columns_sql(dialect: &AnyDialect) -> String {
    let table = dialect.placeholder(1);
    match dialect.kind() {
        ormdantic_dialects::DialectKind::Sqlite => {
            format!("SELECT name, type, NOT [notnull], dflt_value, pk FROM pragma_table_info({table})")
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

fn indexes_sql(dialect: &AnyDialect) -> String {
    let table = dialect.placeholder(1);
    match dialect.kind() {
        ormdantic_dialects::DialectKind::Sqlite => {
            format!("SELECT name, [unique] FROM pragma_index_list({table})")
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

fn foreign_keys_sql(dialect: &AnyDialect) -> String {
    let table = dialect.placeholder(1);
    match dialect.kind() {
        ormdantic_dialects::DialectKind::Sqlite => {
            format!("SELECT [table], [from], [to] FROM pragma_foreign_key_list({table})")
        }
        ormdantic_dialects::DialectKind::Postgres => format!(
            "SELECT ccu.table_name, kcu.column_name, ccu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public' AND tc.table_name = {table} ORDER BY kcu.ordinal_position"
        ),
        ormdantic_dialects::DialectKind::MySql | ormdantic_dialects::DialectKind::MariaDb => format!(
            "SELECT referenced_table_name, column_name, referenced_column_name FROM information_schema.key_column_usage WHERE table_schema = DATABASE() AND table_name = {table} AND referenced_table_name IS NOT NULL ORDER BY ordinal_position"
        ),
        ormdantic_dialects::DialectKind::MsSql => format!(
            "SELECT rt.name, pc.name, rc.name FROM sys.foreign_key_columns fkc JOIN sys.tables pt ON fkc.parent_object_id = pt.object_id JOIN sys.columns pc ON pc.object_id = pt.object_id AND pc.column_id = fkc.parent_column_id JOIN sys.tables rt ON fkc.referenced_object_id = rt.object_id JOIN sys.columns rc ON rc.object_id = rt.object_id AND rc.column_id = fkc.referenced_column_id WHERE pt.name = {table} ORDER BY pc.column_id"
        ),
        ormdantic_dialects::DialectKind::Oracle => format!(
            "SELECT r.table_name, cc.column_name, rcc.column_name FROM user_constraints c JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name JOIN user_constraints r ON c.r_constraint_name = r.constraint_name JOIN user_cons_columns rcc ON r.constraint_name = rcc.constraint_name AND cc.position = rcc.position WHERE c.constraint_type = 'R' AND c.table_name = UPPER({table}) ORDER BY cc.position"
        ),
    }
}

fn ensure_revision_table(connection: &mut NativeConnection) -> PyResult<()> {
    let dialect = AnyDialect::parse(connection.dialect())
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    connection
        .execute(ensure_revision_table_sql(&dialect).as_str(), &[])
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(())
}

fn ensure_revision_table_sql(dialect: &AnyDialect) -> String {
    let table = dialect.quote_ident("ormdantic_migrations");
    let revision = dialect.quote_ident("revision");
    let revision_type = match dialect.kind() {
        ormdantic_dialects::DialectKind::MsSql => "NVARCHAR(255)",
        ormdantic_dialects::DialectKind::Oracle => "VARCHAR2(255)",
        _ => "TEXT",
    };
    format!("CREATE TABLE IF NOT EXISTS {table} ({revision} {revision_type} PRIMARY KEY)")
}

fn applied_revisions_sql(dialect: &AnyDialect) -> String {
    format!(
        "SELECT {} FROM {} ORDER BY {}",
        dialect.quote_ident("revision"),
        dialect.quote_ident("ormdantic_migrations"),
        dialect.quote_ident("revision")
    )
}

fn insert_revision_sql(dialect: &AnyDialect) -> String {
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        dialect.quote_ident("ormdantic_migrations"),
        dialect.quote_ident("revision"),
        dialect.placeholder(1)
    )
}

fn delete_revision_sql(dialect: &AnyDialect) -> String {
    format!(
        "DELETE FROM {} WHERE {} = {}",
        dialect.quote_ident("ormdantic_migrations"),
        dialect.quote_ident("revision"),
        dialect.placeholder(1)
    )
}

enum MigrationDirection {
    Apply,
    Rollback,
}

fn py_operations_to_db(
    py: Python<'_>,
    operations: Vec<(String, Vec<Py<PyAny>>)>,
) -> PyResult<Vec<(String, Vec<DbValue>)>> {
    operations
        .into_iter()
        .map(|(sql, params)| {
            params
                .into_iter()
                .map(|param| py_to_db_value(py, param))
                .collect::<PyResult<Vec<_>>>()
                .map(|params| (sql, params))
        })
        .collect()
}

fn run_migration(
    connection: &mut NativeConnection,
    revision: &str,
    operations: Vec<(String, Vec<DbValue>)>,
    direction: MigrationDirection,
) -> PyResult<()> {
    ensure_revision_table(connection)?;
    let dialect = AnyDialect::parse(connection.dialect())
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let revision_sql = match direction {
        MigrationDirection::Apply => insert_revision_sql(&dialect),
        MigrationDirection::Rollback => delete_revision_sql(&dialect),
    };
    let result: Result<(), ormdantic_core::OrmdanticError> = (|| {
        connection.begin()?;
        for (sql, params) in operations {
            connection.execute(&sql, &params)?;
        }
        connection.execute(&revision_sql, &[DbValue::Text(revision.to_string())])?;
        connection.commit()?;
        Ok(())
    })();
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = connection.rollback();
            Err(PyValueError::new_err(error.to_string()))
        }
    }
}

fn db_value_to_string(value: &DbValue) -> Option<String> {
    match value {
        DbValue::Null => None,
        DbValue::Integer(value) => Some(value.to_string()),
        DbValue::Real(value) => Some(value.to_string()),
        DbValue::Text(value) => Some(value.clone()),
        DbValue::Bool(value) => Some(value.to_string()),
    }
}

fn db_value_to_bool(value: &DbValue) -> bool {
    match value {
        DbValue::Null => false,
        DbValue::Integer(value) => *value != 0,
        DbValue::Real(value) => *value != 0.0,
        DbValue::Text(value) => matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "t" | "true" | "y" | "yes"
        ),
        DbValue::Bool(value) => *value,
    }
}

#[pyfunction]
fn normalize_filters(py: Python<'_>, filters: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let values = PyDict::new(py);
    let normalized_filters = normalize_filter_input_for_python(py, filters, &values, None)?;
    let output = PyDict::new(py);
    output.set_item("filters", normalized_filters)?;
    output.set_item("values", values)?;
    Ok(output.into_any().unbind())
}

fn normalize_filter_input_for_python(
    py: Python<'_>,
    filters: &Bound<'_, PyAny>,
    values: &Bound<'_, PyDict>,
    prefix: Option<&str>,
) -> PyResult<Py<PyAny>> {
    if filters.is_none() {
        return Ok(PyList::empty(py).into_any().unbind());
    }
    if filters.extract::<Vec<FilterSpec>>().is_ok() {
        return Ok(filters.clone().unbind());
    }
    let dict = filters.downcast::<PyDict>()?;
    if dict.contains("connector")? {
        return normalize_filter_tree_for_python(py, dict, values, prefix.unwrap_or("expr"));
    }
    normalize_filter_dict_for_python(py, dict, values, prefix)
}

fn normalize_filter_tree_for_python(
    py: Python<'_>,
    tree: &Bound<'_, PyDict>,
    values: &Bound<'_, PyDict>,
    prefix: &str,
) -> PyResult<Py<PyAny>> {
    let connector: String = tree
        .get_item("connector")?
        .ok_or_else(|| PyValueError::new_err("filter tree missing connector"))?
        .extract()?;
    let output = PyDict::new(py);
    output.set_item("connector", connector.as_str())?;
    match connector.as_str() {
        "leaf" => {
            let filters = tree
                .get_item("filters")?
                .ok_or_else(|| PyValueError::new_err("leaf filter tree missing filters"))?;
            let normalized = normalize_filter_input_for_python(py, &filters, values, Some(prefix))?;
            output.set_item("filters", normalized)?;
        }
        "and" | "or" => {
            let children = tree
                .get_item("children")?
                .ok_or_else(|| PyValueError::new_err("group filter tree missing children"))?;
            let children = children.extract::<Vec<Py<PyAny>>>()?;
            let normalized_children = PyList::empty(py);
            for (idx, child) in children.into_iter().enumerate() {
                let child_prefix = format!("{prefix}_{idx}");
                normalized_children.append(normalize_filter_input_for_python(
                    py,
                    child.bind(py),
                    values,
                    Some(&child_prefix),
                )?)?;
            }
            output.set_item("children", normalized_children)?;
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "unsupported filter connector '{other}'"
            )))
        }
    }
    Ok(output.into_any().unbind())
}

fn normalize_filter_dict_for_python(
    py: Python<'_>,
    filters: &Bound<'_, PyDict>,
    values: &Bound<'_, PyDict>,
    prefix: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let specs = PyList::empty(py);
    for (key, value) in filters.iter() {
        let key: String = key.extract()?;
        let (column, operator) = split_filter_key(&key);
        match operator.as_str() {
            "is_null" | "is_not_null" => {
                specs.append((column, operator, Vec::<String>::new()).into_pyobject(py)?)?;
            }
            "in" | "not_in" => {
                let items = value.extract::<Vec<Py<PyAny>>>()?;
                let mut params = Vec::with_capacity(items.len());
                for (idx, item) in items.into_iter().enumerate() {
                    let param = prefixed_param(prefix, &format!("{column}__{operator}_{idx}"));
                    values.set_item(&param, item.bind(py))?;
                    params.push(param);
                }
                specs.append((column, operator, params).into_pyobject(py)?)?;
            }
            _ => {
                let param = prefixed_param(prefix, &key);
                values.set_item(&param, value)?;
                specs.append((column, operator, vec![param]).into_pyobject(py)?)?;
            }
        }
    }
    Ok(specs.into_any().unbind())
}

fn split_filter_key(key: &str) -> (String, String) {
    let Some((column, operator)) = key.rsplit_once("__") else {
        return (key.to_string(), "eq".to_string());
    };
    if FILTER_OPERATORS.contains(&operator) {
        (column.to_string(), operator.to_string())
    } else {
        (key.to_string(), "eq".to_string())
    }
}

fn prefixed_param(prefix: Option<&str>, param: &str) -> String {
    prefix.map_or_else(|| param.to_string(), |prefix| format!("{prefix}__{param}"))
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
    Vec<RuntimeCheck>,
);

#[pyfunction]
fn compile_create_table_sql(
    dialect: &str,
    table: &str,
    columns: Vec<ColumnDdl>,
    indexes: Vec<(String, Vec<String>, bool)>,
    unique_constraints: Vec<Vec<String>>,
) -> PyResult<Vec<String>> {
    create_table_sql(dialect, table, columns, indexes, unique_constraints)
}

fn create_table_sql(
    dialect: &str,
    table: &str,
    columns: Vec<ColumnDdl>,
    indexes: Vec<(String, Vec<String>, bool)>,
    unique_constraints: Vec<Vec<String>>,
) -> PyResult<Vec<String>> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let mut foreign_keys = Vec::new();
    let mut check_constraints = Vec::new();
    let mut unique_column_constraints = Vec::new();
    let columns = columns
        .into_iter()
        .map(
            |(
                name,
                kind,
                nullable,
                primary_key,
                foreign_table,
                foreign_column,
                _max_length,
                unique,
                checks,
            )| {
                if unique {
                    unique_column_constraints.push(vec![name.clone()]);
                }
                if let (Some(foreign_table), Some(foreign_column)) = (foreign_table, foreign_column)
                {
                    foreign_keys.push(ForeignKeyDef::new(
                        vec![name.clone()],
                        foreign_table,
                        vec![foreign_column],
                    ));
                }
                for check in checks {
                    check_constraints.push(
                        CheckConstraintDef::new(render_check_constraint(&name, &check)?).named(
                            format!("{table}_{name}_{}_check", check_constraint_suffix(&check)?),
                        ),
                    );
                }
                Ok(ColumnDef::new(name, field_kind_from_runtime(&kind))
                    .nullable(nullable)
                    .primary_key(primary_key))
            },
        )
        .collect::<PyResult<Vec<_>>>()?;
    let indexes = indexes
        .into_iter()
        .map(|(name, columns, unique)| IndexDef::new(name, columns).unique(unique))
        .collect::<Vec<_>>();
    let unique_constraints = unique_constraints
        .into_iter()
        .chain(unique_column_constraints)
        .enumerate()
        .map(|(idx, columns)| UniqueConstraintDef::new(format!("{table}_unique_{idx}"), columns))
        .collect::<Vec<_>>();
    let table = TableDef::from_parts(
        table,
        table,
        columns
            .iter()
            .find(|column| column.is_primary_key())
            .map(|column| column.name().to_string())
            .unwrap_or_else(|| "id".to_string()),
        columns,
        indexes,
        unique_constraints,
        Vec::new(),
    )
    .with_check_constraints(check_constraints)
    .with_foreign_keys(foreign_keys);
    let ddl = DdlAst::new(vec![SchemaOperation::CreateTable(table)]);
    Ok(ddl
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?
        .into_iter()
        .map(|query| query.sql().to_string())
        .collect())
}

#[pyfunction]
fn compile_drop_table_sql(dialect: &str, table: &str) -> PyResult<String> {
    drop_table_sql(dialect, table)
}

fn drop_table_sql(dialect: &str, table: &str) -> PyResult<String> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let ddl = DdlAst::new(vec![SchemaOperation::DropTable {
        name: table.to_string(),
    }]);
    ddl.compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?
        .into_iter()
        .next()
        .map(|query| query.sql().to_string())
        .ok_or_else(|| PyValueError::new_err("drop table did not compile"))
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

fn joined_filters(filters: Vec<RuntimeJoinedFilter>) -> PyResult<Vec<JoinedFilter>> {
    let mut output = Vec::new();
    for (table_alias, specs) in filters {
        for filter in filter_specs(specs)? {
            output.push(JoinedFilter::new(table_alias.clone(), filter));
        }
    }
    Ok(output)
}

fn joined_order_by(order_by: Vec<RuntimeJoinedOrder>) -> PyResult<Vec<JoinedOrderBy>> {
    order_by
        .into_iter()
        .map(|(table_alias, column, direction)| {
            Ok(JoinedOrderBy::new(
                table_alias,
                OrderBy::new(column, parse_sort_direction(&direction)?),
            ))
        })
        .collect()
}

fn parse_filter_input(filters: &Bound<'_, PyAny>) -> PyResult<Vec<Filter>> {
    if let Ok(specs) = filters.extract::<Vec<FilterSpec>>() {
        return filter_specs(specs);
    }

    let dict = filters.downcast::<PyDict>()?;
    let connector: String = dict
        .get_item("connector")?
        .ok_or_else(|| PyValueError::new_err("filter tree missing connector"))?
        .extract()?;

    match connector.as_str() {
        "leaf" => {
            let filters = dict
                .get_item("filters")?
                .ok_or_else(|| PyValueError::new_err("leaf filter tree missing filters"))?;
            let specs = filters.extract::<Vec<FilterSpec>>()?;
            filter_specs(specs)
        }
        "and" | "or" => {
            let children = dict
                .get_item("children")?
                .ok_or_else(|| PyValueError::new_err("group filter tree missing children"))?;
            let children = children.extract::<Vec<Py<PyAny>>>()?;
            let mut parsed = Vec::new();
            for child in children {
                parsed.extend(parse_filter_input(child.bind(filters.py()))?);
            }
            if connector == "and" {
                Ok(vec![Filter::And(parsed)])
            } else {
                Ok(vec![Filter::Or(parsed)])
            }
        }
        other => Err(PyValueError::new_err(format!(
            "unsupported filter connector '{other}'"
        ))),
    }
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
        QueryOperation::Ddl => "ddl",
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

fn render_check_constraint(field: &str, check: &RuntimeCheck) -> PyResult<String> {
    let (kind, operator, value) = check;
    match kind.as_str() {
        "comparison" => Ok(format!("{field} {operator} {value}")),
        "length" => Ok(format!("LENGTH({field}) {operator} {value}")),
        other => Err(PyValueError::new_err(format!(
            "unsupported check constraint kind '{other}'"
        ))),
    }
}

fn check_constraint_suffix(check: &RuntimeCheck) -> PyResult<&'static str> {
    let (kind, operator, _) = check;
    match (kind.as_str(), operator.as_str()) {
        ("comparison", ">=") => Ok("ge"),
        ("comparison", ">") => Ok("gt"),
        ("comparison", "<=") => Ok("le"),
        ("comparison", "<") => Ok("lt"),
        ("length", ">=") => Ok("min_length"),
        ("length", "<=") => Ok("max_length"),
        _ => Err(PyValueError::new_err(format!(
            "unsupported check constraint operator '{operator}' for kind '{kind}'"
        ))),
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

#[pyfunction]
fn runtime_capabilities(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let capabilities = PyDict::new(py);
    for (name, available) in engine_runtime_capabilities() {
        capabilities.set_item(name, available)?;
    }
    Ok(capabilities.into_any().unbind())
}

#[pymodule]
fn _ormdantic(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyNativeConnection>()?;
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyTableHandle>()?;
    m.add_class::<PyTransactionOptions>()?;
    m.add_class::<PySessionRuntime>()?;
    m.add_class::<PyEventBridge>()?;
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
    m.add_function(wrap_pyfunction!(compile_expression_query, m)?)?;
    m.add_function(wrap_pyfunction!(compile_typed_expression_query, m)?)?;
    m.add_function(wrap_pyfunction!(compile_typed_update_query, m)?)?;
    m.add_function(wrap_pyfunction!(compile_schema_diff, m)?)?;
    m.add_function(wrap_pyfunction!(reflect_schema, m)?)?;
    m.add_function(wrap_pyfunction!(compile_selectin_plan, m)?)?;
    m.add_function(wrap_pyfunction!(execute_selectin_load, m)?)?;
    m.add_function(wrap_pyfunction!(execute_native, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_filters, m)?)?;
    m.add_function(wrap_pyfunction!(snake_case, m)?)?;
    m.add_function(wrap_pyfunction!(sql_value, m)?)?;
    m.add_function(wrap_pyfunction!(compile_create_table_sql, m)?)?;
    m.add_function(wrap_pyfunction!(compile_drop_table_sql, m)?)?;
    m.add_function(wrap_pyfunction!(runtime_capabilities, m)?)?;
    Ok(())
}

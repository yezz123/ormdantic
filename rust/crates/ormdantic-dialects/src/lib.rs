use ormdantic_core::{
    BackendFeature, DeferrableMode, FeatureSet, IsolationLevel, OrmdanticError, OrmdanticResult,
    SavepointName, TransactionAccessMode, TransactionOptions,
};
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, ConstraintDef, FieldKind, ForeignKeyAction, ForeignKeyDef,
    IndexDef, SchemaOperation, TableDef, UniqueConstraintDef,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectKind {
    Sqlite,
    Postgres,
    MySql,
    MariaDb,
    MsSql,
    Oracle,
}

impl DialectKind {
    pub fn parse(name: &str) -> OrmdanticResult<Self> {
        let normalized = normalize_dialect_name(name);
        match normalized.as_str() {
            "sqlite" | "sqlite3" | "aiosqlite" => Ok(Self::Sqlite),
            "postgres" | "postgresql" | "asyncpg" | "psycopg" | "psycopg2" | "pg8000" => {
                Ok(Self::Postgres)
            }
            "mysql" | "pymysql" | "mysqlconnector" | "aiomysql" | "asyncmy" => Ok(Self::MySql),
            "mariadb" | "mariadbconnector" => Ok(Self::MariaDb),
            "mssql" | "pyodbc" | "pymssql" | "aioodbc" => Ok(Self::MsSql),
            "oracle" | "oracledb" | "cx_oracle" => Ok(Self::Oracle),
            other => Err(OrmdanticError::UnsupportedDialect {
                dialect: other.to_string(),
            }),
        }
    }
}

pub trait Dialect {
    fn kind(&self) -> DialectKind;
    fn name(&self) -> &'static str;
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, index: usize) -> String;
    fn supports_returning(&self) -> bool;
    fn supports_native_uuid(&self) -> bool;
    fn supports_json(&self) -> bool;

    fn feature_set(&self) -> FeatureSet {
        let mut features = FeatureSet::new([
            BackendFeature::Ctes,
            BackendFeature::Savepoints,
            BackendFeature::Windows,
        ]);
        if self.supports_returning() {
            features.insert(BackendFeature::Returning);
        }
        if self.supports_json() {
            features.insert(BackendFeature::NativeJson);
        }
        if self.supports_native_uuid() {
            features.insert(BackendFeature::NativeUuid);
        }
        features
    }

    fn supports_feature(&self, feature: BackendFeature) -> bool {
        self.feature_set().contains(feature)
    }

    fn render_column_type(&self, column: &ColumnDef) -> String {
        match column.kind() {
            FieldKind::String | FieldKind::Enum => "TEXT".to_string(),
            FieldKind::Integer => "INTEGER".to_string(),
            FieldKind::Float => "REAL".to_string(),
            FieldKind::Boolean => "BOOLEAN".to_string(),
            FieldKind::Uuid if self.supports_native_uuid() => "UUID".to_string(),
            FieldKind::Uuid => "TEXT".to_string(),
            FieldKind::Date => "DATE".to_string(),
            FieldKind::DateTime => "TIMESTAMP".to_string(),
            FieldKind::Json | FieldKind::ModelJson if self.supports_json() => "JSON".to_string(),
            FieldKind::Json | FieldKind::ModelJson => "TEXT".to_string(),
            FieldKind::Decimal => match (column.precision(), column.scale()) {
                (Some(precision), Some(scale)) => format!("NUMERIC({precision}, {scale})"),
                _ => "NUMERIC".to_string(),
            },
            FieldKind::Binary => "BLOB".to_string(),
            FieldKind::ForeignKey { .. } | FieldKind::Unknown => "TEXT".to_string(),
        }
    }

    fn compile_schema_operation(
        &self,
        operation: &SchemaOperation,
    ) -> OrmdanticResult<Vec<String>> {
        Ok(match operation {
            SchemaOperation::CreateNamespace(namespace) => {
                vec![format!(
                    "CREATE SCHEMA IF NOT EXISTS {}",
                    self.quote_ident(namespace.name())
                )]
            }
            SchemaOperation::DropNamespace { name } => {
                vec![format!("DROP SCHEMA IF EXISTS {}", self.quote_ident(name))]
            }
            SchemaOperation::CreateTable(table) => compile_create_table(self, table),
            SchemaOperation::DropTable { name } => {
                vec![format!("DROP TABLE IF EXISTS {}", self.quote_ident(name))]
            }
            SchemaOperation::AddColumn { table, column } => vec![format!(
                "ALTER TABLE {} ADD COLUMN {}",
                self.quote_ident(table),
                render_column_def(self, column)
            )],
            SchemaOperation::DropColumn { table, column } => vec![format!(
                "ALTER TABLE {} DROP COLUMN {}",
                self.quote_ident(table),
                self.quote_ident(column)
            )],
            SchemaOperation::AlterColumn { table, column } => vec![format!(
                "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                self.quote_ident(table),
                self.quote_ident(column.name()),
                self.render_column_type(column)
            )],
            SchemaOperation::CreateIndex { table, index } => {
                vec![compile_create_index(self, table, index)]
            }
            SchemaOperation::DropIndex { name, .. } => {
                vec![format!("DROP INDEX IF EXISTS {}", self.quote_ident(name))]
            }
            SchemaOperation::AddConstraint { table, constraint } => vec![format!(
                "ALTER TABLE {} ADD {}",
                self.quote_ident(table),
                render_constraint(self, constraint)
            )],
            SchemaOperation::DropConstraint { table, name } => vec![format!(
                "ALTER TABLE {} DROP CONSTRAINT {}",
                self.quote_ident(table),
                self.quote_ident(name)
            )],
        })
    }

    fn begin_transaction_sql(&self, options: &TransactionOptions) -> Vec<String> {
        let mut statements = Vec::new();
        if let Some(isolation_level) = options.isolation_level() {
            statements.push(self.set_isolation_sql(isolation_level));
        }
        let mut begin = "BEGIN".to_string();
        if options.access_mode() == TransactionAccessMode::ReadOnly {
            begin.push_str(" READ ONLY");
        }
        match options.deferrable_mode() {
            Some(DeferrableMode::Deferrable) => begin.push_str(" DEFERRABLE"),
            Some(DeferrableMode::NotDeferrable) => begin.push_str(" NOT DEFERRABLE"),
            None => {}
        }
        statements.push(begin);
        statements
    }

    fn set_isolation_sql(&self, isolation_level: IsolationLevel) -> String {
        format!(
            "SET TRANSACTION ISOLATION LEVEL {}",
            render_isolation_level(isolation_level)
        )
    }

    fn savepoint_sql(&self, name: &SavepointName) -> String {
        format!("SAVEPOINT {}", self.quote_ident(name.as_str()))
    }

    fn rollback_to_savepoint_sql(&self, name: &SavepointName) -> String {
        format!("ROLLBACK TO SAVEPOINT {}", self.quote_ident(name.as_str()))
    }

    fn release_savepoint_sql(&self, name: &SavepointName) -> String {
        format!("RELEASE SAVEPOINT {}", self.quote_ident(name.as_str()))
    }

    fn reflection_queries(&self, scope: &ReflectionScope) -> Vec<ReflectionQuery> {
        vec![
            ReflectionQuery::new(
                ReflectionQueryKind::Tables,
                format!("SELECT table_name FROM information_schema.tables{}", scope_predicate(scope)),
            ),
            ReflectionQuery::new(
                ReflectionQueryKind::Columns,
                format!("SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns{}", scope_predicate(scope)),
            ),
            ReflectionQuery::new(
                ReflectionQueryKind::Constraints,
                format!("SELECT table_name, constraint_name, constraint_type FROM information_schema.table_constraints{}", scope_predicate(scope)),
            ),
        ]
    }

    fn upsert_conflict_clause(&self, conflict_column: &str, update_columns: &[String]) -> String {
        let target = self.quote_ident(conflict_column);
        if update_columns.is_empty() {
            return format!("ON CONFLICT ({target}) DO NOTHING");
        }

        let assignments = update_columns
            .iter()
            .map(|column| {
                format!(
                    "{} = excluded.{}",
                    self.quote_ident(column),
                    self.quote_ident(column)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("ON CONFLICT ({target}) DO UPDATE SET {assignments}")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReflectionScope {
    schema: Option<String>,
    tables: Vec<String>,
}

impl ReflectionScope {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    pub fn schema_name(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn table_names(&self) -> &[String] {
        &self.tables
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReflectionQueryKind {
    Tables,
    Columns,
    Constraints,
    Indexes,
    ForeignKeys,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReflectionQuery {
    kind: ReflectionQueryKind,
    sql: String,
}

impl ReflectionQuery {
    pub fn new(kind: ReflectionQueryKind, sql: impl Into<String>) -> Self {
        Self {
            kind,
            sql: sql.into(),
        }
    }

    pub fn kind(&self) -> ReflectionQueryKind {
        self.kind
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Sqlite
    }

    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_double(ident)
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        false
    }

    fn supports_json(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Postgres
    }

    fn name(&self) -> &'static str {
        "postgresql"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_double(ident)
    }

    fn placeholder(&self, index: usize) -> String {
        format!("${index}")
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        true
    }

    fn supports_json(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MySqlDialect;

impl Dialect for MySqlDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::MySql
    }

    fn name(&self) -> &'static str {
        "mysql"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_backtick(ident)
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn supports_returning(&self) -> bool {
        false
    }

    fn supports_native_uuid(&self) -> bool {
        false
    }

    fn supports_json(&self) -> bool {
        true
    }

    fn release_savepoint_sql(&self, name: &SavepointName) -> String {
        format!("RELEASE SAVEPOINT {}", self.quote_ident(name.as_str()))
    }

    fn upsert_conflict_clause(&self, _conflict_column: &str, update_columns: &[String]) -> String {
        if update_columns.is_empty() {
            return "ON DUPLICATE KEY UPDATE 1 = 1".to_string();
        }
        let assignments = update_columns
            .iter()
            .map(|column| {
                format!(
                    "{} = VALUES({})",
                    self.quote_ident(column),
                    self.quote_ident(column)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("ON DUPLICATE KEY UPDATE {assignments}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MariaDbDialect;

impl Dialect for MariaDbDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::MariaDb
    }

    fn name(&self) -> &'static str {
        "mariadb"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_backtick(ident)
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        false
    }

    fn supports_json(&self) -> bool {
        true
    }

    fn upsert_conflict_clause(&self, conflict_column: &str, update_columns: &[String]) -> String {
        MySqlDialect.upsert_conflict_clause(conflict_column, update_columns)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsSqlDialect;

impl Dialect for MsSqlDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::MsSql
    }

    fn name(&self) -> &'static str {
        "mssql"
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("[{}]", ident.replace(']', "]]"))
    }

    fn placeholder(&self, index: usize) -> String {
        format!("@P{index}")
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        true
    }

    fn supports_json(&self) -> bool {
        false
    }

    fn savepoint_sql(&self, name: &SavepointName) -> String {
        format!("SAVE TRANSACTION {}", self.quote_ident(name.as_str()))
    }

    fn rollback_to_savepoint_sql(&self, name: &SavepointName) -> String {
        format!("ROLLBACK TRANSACTION {}", self.quote_ident(name.as_str()))
    }

    fn release_savepoint_sql(&self, _name: &SavepointName) -> String {
        String::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleDialect;

impl Dialect for OracleDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Oracle
    }

    fn name(&self) -> &'static str {
        "oracle"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_double(ident)
    }

    fn placeholder(&self, index: usize) -> String {
        format!(":{index}")
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        false
    }

    fn supports_json(&self) -> bool {
        true
    }

    fn rollback_to_savepoint_sql(&self, name: &SavepointName) -> String {
        format!("ROLLBACK TO SAVEPOINT {}", self.quote_ident(name.as_str()))
    }

    fn release_savepoint_sql(&self, _name: &SavepointName) -> String {
        String::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AnyDialect {
    Sqlite(SqliteDialect),
    Postgres(PostgresDialect),
    MySql(MySqlDialect),
    MariaDb(MariaDbDialect),
    MsSql(MsSqlDialect),
    Oracle(OracleDialect),
}

impl AnyDialect {
    pub fn parse(name: &str) -> OrmdanticResult<Self> {
        Ok(match DialectKind::parse(name)? {
            DialectKind::Sqlite => Self::Sqlite(SqliteDialect),
            DialectKind::Postgres => Self::Postgres(PostgresDialect),
            DialectKind::MySql => Self::MySql(MySqlDialect),
            DialectKind::MariaDb => Self::MariaDb(MariaDbDialect),
            DialectKind::MsSql => Self::MsSql(MsSqlDialect),
            DialectKind::Oracle => Self::Oracle(OracleDialect),
        })
    }
}

impl Dialect for AnyDialect {
    fn kind(&self) -> DialectKind {
        match self {
            Self::Sqlite(dialect) => dialect.kind(),
            Self::Postgres(dialect) => dialect.kind(),
            Self::MySql(dialect) => dialect.kind(),
            Self::MariaDb(dialect) => dialect.kind(),
            Self::MsSql(dialect) => dialect.kind(),
            Self::Oracle(dialect) => dialect.kind(),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Sqlite(dialect) => dialect.name(),
            Self::Postgres(dialect) => dialect.name(),
            Self::MySql(dialect) => dialect.name(),
            Self::MariaDb(dialect) => dialect.name(),
            Self::MsSql(dialect) => dialect.name(),
            Self::Oracle(dialect) => dialect.name(),
        }
    }

    fn quote_ident(&self, ident: &str) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.quote_ident(ident),
            Self::Postgres(dialect) => dialect.quote_ident(ident),
            Self::MySql(dialect) => dialect.quote_ident(ident),
            Self::MariaDb(dialect) => dialect.quote_ident(ident),
            Self::MsSql(dialect) => dialect.quote_ident(ident),
            Self::Oracle(dialect) => dialect.quote_ident(ident),
        }
    }

    fn placeholder(&self, index: usize) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.placeholder(index),
            Self::Postgres(dialect) => dialect.placeholder(index),
            Self::MySql(dialect) => dialect.placeholder(index),
            Self::MariaDb(dialect) => dialect.placeholder(index),
            Self::MsSql(dialect) => dialect.placeholder(index),
            Self::Oracle(dialect) => dialect.placeholder(index),
        }
    }

    fn supports_returning(&self) -> bool {
        match self {
            Self::Sqlite(dialect) => dialect.supports_returning(),
            Self::Postgres(dialect) => dialect.supports_returning(),
            Self::MySql(dialect) => dialect.supports_returning(),
            Self::MariaDb(dialect) => dialect.supports_returning(),
            Self::MsSql(dialect) => dialect.supports_returning(),
            Self::Oracle(dialect) => dialect.supports_returning(),
        }
    }

    fn supports_native_uuid(&self) -> bool {
        match self {
            Self::Sqlite(dialect) => dialect.supports_native_uuid(),
            Self::Postgres(dialect) => dialect.supports_native_uuid(),
            Self::MySql(dialect) => dialect.supports_native_uuid(),
            Self::MariaDb(dialect) => dialect.supports_native_uuid(),
            Self::MsSql(dialect) => dialect.supports_native_uuid(),
            Self::Oracle(dialect) => dialect.supports_native_uuid(),
        }
    }

    fn supports_json(&self) -> bool {
        match self {
            Self::Sqlite(dialect) => dialect.supports_json(),
            Self::Postgres(dialect) => dialect.supports_json(),
            Self::MySql(dialect) => dialect.supports_json(),
            Self::MariaDb(dialect) => dialect.supports_json(),
            Self::MsSql(dialect) => dialect.supports_json(),
            Self::Oracle(dialect) => dialect.supports_json(),
        }
    }

    fn feature_set(&self) -> FeatureSet {
        match self {
            Self::Sqlite(dialect) => dialect.feature_set(),
            Self::Postgres(dialect) => dialect.feature_set(),
            Self::MySql(dialect) => dialect.feature_set(),
            Self::MariaDb(dialect) => dialect.feature_set(),
            Self::MsSql(dialect) => dialect.feature_set(),
            Self::Oracle(dialect) => dialect.feature_set(),
        }
    }

    fn render_column_type(&self, column: &ColumnDef) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.render_column_type(column),
            Self::Postgres(dialect) => dialect.render_column_type(column),
            Self::MySql(dialect) => dialect.render_column_type(column),
            Self::MariaDb(dialect) => dialect.render_column_type(column),
            Self::MsSql(dialect) => dialect.render_column_type(column),
            Self::Oracle(dialect) => dialect.render_column_type(column),
        }
    }

    fn compile_schema_operation(
        &self,
        operation: &SchemaOperation,
    ) -> OrmdanticResult<Vec<String>> {
        match self {
            Self::Sqlite(dialect) => dialect.compile_schema_operation(operation),
            Self::Postgres(dialect) => dialect.compile_schema_operation(operation),
            Self::MySql(dialect) => dialect.compile_schema_operation(operation),
            Self::MariaDb(dialect) => dialect.compile_schema_operation(operation),
            Self::MsSql(dialect) => dialect.compile_schema_operation(operation),
            Self::Oracle(dialect) => dialect.compile_schema_operation(operation),
        }
    }

    fn begin_transaction_sql(&self, options: &TransactionOptions) -> Vec<String> {
        match self {
            Self::Sqlite(dialect) => dialect.begin_transaction_sql(options),
            Self::Postgres(dialect) => dialect.begin_transaction_sql(options),
            Self::MySql(dialect) => dialect.begin_transaction_sql(options),
            Self::MariaDb(dialect) => dialect.begin_transaction_sql(options),
            Self::MsSql(dialect) => dialect.begin_transaction_sql(options),
            Self::Oracle(dialect) => dialect.begin_transaction_sql(options),
        }
    }

    fn set_isolation_sql(&self, isolation_level: IsolationLevel) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.set_isolation_sql(isolation_level),
            Self::Postgres(dialect) => dialect.set_isolation_sql(isolation_level),
            Self::MySql(dialect) => dialect.set_isolation_sql(isolation_level),
            Self::MariaDb(dialect) => dialect.set_isolation_sql(isolation_level),
            Self::MsSql(dialect) => dialect.set_isolation_sql(isolation_level),
            Self::Oracle(dialect) => dialect.set_isolation_sql(isolation_level),
        }
    }

    fn savepoint_sql(&self, name: &SavepointName) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.savepoint_sql(name),
            Self::Postgres(dialect) => dialect.savepoint_sql(name),
            Self::MySql(dialect) => dialect.savepoint_sql(name),
            Self::MariaDb(dialect) => dialect.savepoint_sql(name),
            Self::MsSql(dialect) => dialect.savepoint_sql(name),
            Self::Oracle(dialect) => dialect.savepoint_sql(name),
        }
    }

    fn rollback_to_savepoint_sql(&self, name: &SavepointName) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.rollback_to_savepoint_sql(name),
            Self::Postgres(dialect) => dialect.rollback_to_savepoint_sql(name),
            Self::MySql(dialect) => dialect.rollback_to_savepoint_sql(name),
            Self::MariaDb(dialect) => dialect.rollback_to_savepoint_sql(name),
            Self::MsSql(dialect) => dialect.rollback_to_savepoint_sql(name),
            Self::Oracle(dialect) => dialect.rollback_to_savepoint_sql(name),
        }
    }

    fn release_savepoint_sql(&self, name: &SavepointName) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.release_savepoint_sql(name),
            Self::Postgres(dialect) => dialect.release_savepoint_sql(name),
            Self::MySql(dialect) => dialect.release_savepoint_sql(name),
            Self::MariaDb(dialect) => dialect.release_savepoint_sql(name),
            Self::MsSql(dialect) => dialect.release_savepoint_sql(name),
            Self::Oracle(dialect) => dialect.release_savepoint_sql(name),
        }
    }

    fn reflection_queries(&self, scope: &ReflectionScope) -> Vec<ReflectionQuery> {
        match self {
            Self::Sqlite(dialect) => dialect.reflection_queries(scope),
            Self::Postgres(dialect) => dialect.reflection_queries(scope),
            Self::MySql(dialect) => dialect.reflection_queries(scope),
            Self::MariaDb(dialect) => dialect.reflection_queries(scope),
            Self::MsSql(dialect) => dialect.reflection_queries(scope),
            Self::Oracle(dialect) => dialect.reflection_queries(scope),
        }
    }

    fn upsert_conflict_clause(&self, conflict_column: &str, update_columns: &[String]) -> String {
        match self {
            Self::Sqlite(dialect) => {
                dialect.upsert_conflict_clause(conflict_column, update_columns)
            }
            Self::Postgres(dialect) => {
                dialect.upsert_conflict_clause(conflict_column, update_columns)
            }
            Self::MySql(dialect) => dialect.upsert_conflict_clause(conflict_column, update_columns),
            Self::MariaDb(dialect) => {
                dialect.upsert_conflict_clause(conflict_column, update_columns)
            }
            Self::MsSql(dialect) => dialect.upsert_conflict_clause(conflict_column, update_columns),
            Self::Oracle(dialect) => {
                dialect.upsert_conflict_clause(conflict_column, update_columns)
            }
        }
    }
}

pub fn normalize_dialect_name(name_or_url: &str) -> String {
    let lower = name_or_url.trim().to_ascii_lowercase();
    let before_url = lower
        .split_once("://")
        .map_or(lower.as_str(), |(scheme, _)| scheme);
    before_url
        .split('+')
        .next()
        .unwrap_or(before_url)
        .replace(['-', '_'], "")
}

fn quote_double(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn quote_backtick(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

fn compile_create_table(dialect: &(impl Dialect + ?Sized), table: &TableDef) -> Vec<String> {
    let mut parts = table
        .columns()
        .iter()
        .map(|column| render_column_def(dialect, column))
        .collect::<Vec<_>>();

    for constraint in table.unique_constraints() {
        parts.push(render_unique_constraint(dialect, constraint));
    }
    for constraint in table.check_constraints() {
        parts.push(render_check_constraint(dialect, constraint));
    }
    for foreign_key in table.foreign_keys() {
        parts.push(render_foreign_key(dialect, foreign_key));
    }

    let mut statements = vec![format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        dialect.quote_ident(table.name()),
        parts.join(", ")
    )];
    for index in table.indexes() {
        statements.push(compile_create_index(dialect, table.name(), index));
    }
    statements
}

fn render_column_def(dialect: &(impl Dialect + ?Sized), column: &ColumnDef) -> String {
    let mut sql = format!(
        "{} {}",
        dialect.quote_ident(column.name()),
        dialect.render_column_type(column)
    );
    if column.is_primary_key() {
        sql.push_str(" PRIMARY KEY");
    }
    if !column.is_nullable() || column.is_primary_key() {
        sql.push_str(" NOT NULL");
    }
    if column.is_autoincrement() {
        sql.push_str(" AUTOINCREMENT");
    }
    if let Some(default) = column.server_default() {
        sql.push_str(" DEFAULT ");
        sql.push_str(default);
    }
    if let Some(collation) = column.collation() {
        sql.push_str(" COLLATE ");
        sql.push_str(collation);
    }
    if let Some(computed) = column.computed() {
        sql.push_str(" GENERATED ALWAYS AS (");
        sql.push_str(computed.expression());
        sql.push(')');
        if computed.is_persisted() {
            sql.push_str(" STORED");
        }
    }
    sql
}

fn compile_create_index(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    index: &IndexDef,
) -> String {
    let uniqueness = if index.is_unique() { "UNIQUE " } else { "" };
    let method = index
        .method_name()
        .map(|method| format!(" USING {method}"))
        .unwrap_or_default();
    let columns = index
        .columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let include = if index.include_columns_ref().is_empty() {
        String::new()
    } else {
        format!(
            " INCLUDE ({})",
            index
                .include_columns_ref()
                .iter()
                .map(|column| dialect.quote_ident(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let predicate = index
        .predicate()
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    format!(
        "CREATE {uniqueness}INDEX IF NOT EXISTS {} ON {}{method} ({columns}){include}{predicate}",
        dialect.quote_ident(index.name()),
        dialect.quote_ident(table)
    )
}

fn render_constraint(dialect: &(impl Dialect + ?Sized), constraint: &ConstraintDef) -> String {
    match constraint {
        ConstraintDef::Unique(constraint) => render_unique_constraint(dialect, constraint),
        ConstraintDef::Check(constraint) => render_check_constraint(dialect, constraint),
        ConstraintDef::ForeignKey(constraint) => render_foreign_key(dialect, constraint),
    }
}

fn render_unique_constraint(
    dialect: &(impl Dialect + ?Sized),
    constraint: &UniqueConstraintDef,
) -> String {
    let columns = constraint
        .columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CONSTRAINT {} UNIQUE ({columns})",
        dialect.quote_ident(constraint.name())
    )
}

fn render_check_constraint(
    dialect: &(impl Dialect + ?Sized),
    constraint: &CheckConstraintDef,
) -> String {
    match constraint.name() {
        Some(name) => format!(
            "CONSTRAINT {} CHECK ({})",
            dialect.quote_ident(name),
            constraint.expression()
        ),
        None => format!("CHECK ({})", constraint.expression()),
    }
}

fn render_foreign_key(dialect: &(impl Dialect + ?Sized), foreign_key: &ForeignKeyDef) -> String {
    let local_columns = foreign_key
        .local_columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let remote_columns = foreign_key
        .remote_columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = String::new();
    if let Some(name) = foreign_key.name() {
        sql.push_str("CONSTRAINT ");
        sql.push_str(&dialect.quote_ident(name));
        sql.push(' ');
    }
    sql.push_str(&format!(
        "FOREIGN KEY ({local_columns}) REFERENCES {} ({remote_columns})",
        dialect.quote_ident(foreign_key.remote_table())
    ));
    if let Some(action) = foreign_key.on_delete_action() {
        sql.push_str(" ON DELETE ");
        sql.push_str(render_foreign_key_action(action));
    }
    if let Some(action) = foreign_key.on_update_action() {
        sql.push_str(" ON UPDATE ");
        sql.push_str(render_foreign_key_action(action));
    }
    sql
}

fn render_foreign_key_action(action: &ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::Cascade => "CASCADE",
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::SetNull => "SET NULL",
        ForeignKeyAction::SetDefault => "SET DEFAULT",
        ForeignKeyAction::NoAction => "NO ACTION",
    }
}

fn render_isolation_level(isolation_level: IsolationLevel) -> &'static str {
    match isolation_level {
        IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
        IsolationLevel::ReadCommitted => "READ COMMITTED",
        IsolationLevel::RepeatableRead => "REPEATABLE READ",
        IsolationLevel::Serializable => "SERIALIZABLE",
        IsolationLevel::Snapshot => "SNAPSHOT",
    }
}

fn scope_predicate(scope: &ReflectionScope) -> String {
    match scope.schema_name() {
        Some(schema) => format!(" WHERE table_schema = '{}'", schema.replace('\'', "''")),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_dialect_name, AnyDialect, Dialect, MariaDbDialect, MsSqlDialect, MySqlDialect,
        OracleDialect, PostgresDialect, SqliteDialect,
    };

    #[test]
    fn quotes_identifiers() {
        assert_eq!(SqliteDialect.quote_ident("user"), "\"user\"");
        assert_eq!(
            PostgresDialect.quote_ident("weird\"name"),
            "\"weird\"\"name\""
        );
    }

    #[test]
    fn renders_placeholders() {
        assert_eq!(SqliteDialect.placeholder(1), "?");
        assert_eq!(PostgresDialect.placeholder(2), "$2");
    }

    #[test]
    fn parses_supported_dialects() {
        assert_eq!(AnyDialect::parse("sqlite").unwrap().name(), "sqlite");
        assert_eq!(
            AnyDialect::parse("postgresql+asyncpg").unwrap().name(),
            "postgresql"
        );
        assert_eq!(
            AnyDialect::parse("postgresql+asyncpg://user:pass@host/db")
                .unwrap()
                .name(),
            "postgresql"
        );
        assert_eq!(
            AnyDialect::parse("mysql+pymysql://host/db").unwrap().name(),
            "mysql"
        );
        assert_eq!(
            AnyDialect::parse("mariadb+mariadbconnector://host/db")
                .unwrap()
                .name(),
            "mariadb"
        );
        assert_eq!(
            AnyDialect::parse("mssql+pyodbc://host/db").unwrap().name(),
            "mssql"
        );
        assert_eq!(
            AnyDialect::parse("oracle+oracledb://host/db")
                .unwrap()
                .name(),
            "oracle"
        );
    }

    #[test]
    fn rejects_unknown_dialects() {
        let error = AnyDialect::parse("db2").expect_err("dialect should fail");

        assert_eq!(error.to_string(), "dialect 'db2' is not supported");
    }

    #[test]
    fn renders_upsert_conflict_clauses() {
        assert_eq!(
            SqliteDialect.upsert_conflict_clause("id", &["name".to_string()]),
            "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = excluded.\"name\""
        );
        assert_eq!(
            PostgresDialect.upsert_conflict_clause("id", &[]),
            "ON CONFLICT (\"id\") DO NOTHING"
        );
        assert_eq!(
            MySqlDialect.upsert_conflict_clause("id", &["name".to_string()]),
            "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );
        assert_eq!(
            MariaDbDialect.upsert_conflict_clause("id", &["name".to_string()]),
            "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );
    }

    #[test]
    fn normalizes_sqlalchemy_connection_strings() {
        assert_eq!(
            normalize_dialect_name("postgresql+asyncpg://user:pass@localhost/db"),
            "postgresql"
        );
        assert_eq!(
            normalize_dialect_name("mysql-connector://localhost/db"),
            "mysqlconnector"
        );
    }

    #[test]
    fn renders_additional_dialect_placeholders() {
        assert_eq!(MySqlDialect.placeholder(1), "?");
        assert_eq!(MariaDbDialect.placeholder(1), "?");
        assert_eq!(MsSqlDialect.placeholder(1), "@P1");
        assert_eq!(OracleDialect.placeholder(3), ":3");
    }
}

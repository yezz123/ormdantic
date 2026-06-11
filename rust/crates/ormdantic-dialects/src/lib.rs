//! SQL dialect support for Ormdantic query compilation.
//!
//! ```
//! use ormdantic_dialects::{AnyDialect, Dialect, DialectKind};
//!
//! assert_eq!(
//!     DialectKind::parse("postgresql+asyncpg://user:pass@localhost/db")?,
//!     DialectKind::Postgres
//! );
//!
//! let dialect = AnyDialect::parse("postgresql")?;
//! assert_eq!(dialect.name(), "postgresql");
//! assert_eq!(dialect.quote_ident("coffee"), "\"coffee\"");
//! assert_eq!(dialect.placeholder(2), "$2");
//!
//! # Ok::<(), ormdantic_core::OrmdanticError>(())
//! ```

mod ddl;
mod identifiers;
mod kind;
mod reflection;
mod transactions;

pub use kind::{normalize_dialect_name, DialectKind};
pub use reflection::{ReflectionQuery, ReflectionQueryKind, ReflectionScope};

use ddl::{
    compile_add_column, compile_add_constraint, compile_alter_column, compile_column_comment,
    compile_create_index, compile_create_table, compile_drop_column, compile_drop_index,
    compile_table_comment, compile_table_mysql_options, compile_table_postgres_inherits,
    compile_table_postgres_using, compile_table_postgres_with, compile_table_tablespace,
    MysqlTableOptionsRef,
};
use identifiers::{quote_backtick, quote_double};
use ormdantic_core::{
    BackendFeature, DeferrableMode, FeatureSet, IsolationLevel, OrmdanticError, OrmdanticResult,
    SavepointName, TransactionAccessMode, TransactionOptions,
};
use ormdantic_schema::{ColumnDef, FieldKind, NamespaceDef, SchemaOperation};
use reflection::scope_predicate;
use transactions::render_isolation_level;

pub trait Dialect {
    fn kind(&self) -> DialectKind;
    fn name(&self) -> &'static str;
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, index: usize) -> String;
    fn max_bind_parameters(&self) -> Option<usize> {
        Some(match self.kind() {
            DialectKind::Sqlite => 32_766,
            DialectKind::Postgres
            | DialectKind::MySql
            | DialectKind::MariaDb
            | DialectKind::Oracle => 65_535,
            DialectKind::MsSql => 2_100,
        })
    }
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
            FieldKind::String => render_string_type(self.kind(), column.max_length()),
            FieldKind::Enum { name, schema } => match (self.kind(), name) {
                (DialectKind::Postgres, Some(name)) => match schema {
                    Some(schema) => {
                        format!("{}.{}", self.quote_ident(schema), self.quote_ident(name))
                    }
                    None => self.quote_ident(name),
                },
                _ => "TEXT".to_string(),
            },
            FieldKind::Integer => "INTEGER".to_string(),
            FieldKind::Float => "REAL".to_string(),
            FieldKind::Boolean => "BOOLEAN".to_string(),
            FieldKind::Uuid if self.supports_native_uuid() => "UUID".to_string(),
            FieldKind::Uuid => "TEXT".to_string(),
            FieldKind::Date => "DATE".to_string(),
            FieldKind::DateTime => "TIMESTAMP".to_string(),
            FieldKind::Json | FieldKind::ModelJson if self.supports_json() => "JSON".to_string(),
            FieldKind::Json | FieldKind::ModelJson => "TEXT".to_string(),
            FieldKind::Decimal if self.kind() == DialectKind::Sqlite => {
                match (column.precision(), column.scale()) {
                    (Some(precision), Some(scale)) => {
                        format!("DECIMAL_TEXT({precision}, {scale})")
                    }
                    _ => "DECIMAL_TEXT".to_string(),
                }
            }
            FieldKind::Decimal => match (column.precision(), column.scale()) {
                (Some(precision), Some(scale)) => format!("NUMERIC({precision}, {scale})"),
                _ => "NUMERIC".to_string(),
            },
            FieldKind::Binary => "BLOB".to_string(),
            FieldKind::ForeignKey { .. } => render_string_type(self.kind(), column.max_length()),
            FieldKind::Unknown => "TEXT".to_string(),
        }
    }

    fn compile_schema_operation(
        &self,
        operation: &SchemaOperation,
    ) -> OrmdanticResult<Vec<String>> {
        Ok(match operation {
            SchemaOperation::CreateNamespace(namespace) => {
                compile_create_namespace(self, namespace)?
            }
            SchemaOperation::DropNamespace { name } => compile_drop_namespace(self, name)?,
            SchemaOperation::SetNamespaceComment { name, comment } => {
                compile_namespace_comment(self, name, comment.as_deref())?
            }
            SchemaOperation::CreateTable(table) => compile_create_table(self, table)?,
            SchemaOperation::DropTable { name } => vec![drop_table_sql(self, name)],
            SchemaOperation::RecreateTable(table) => {
                let mut statements =
                    vec![drop_table_sql(self, &table.qualified_name().to_string())];
                statements.extend(compile_create_table(self, table)?);
                statements
            }
            SchemaOperation::AddColumn { table, column } => {
                compile_add_column(self, table, column)?
            }
            SchemaOperation::DropColumn { table, column } => {
                vec![compile_drop_column(self, table, column)]
            }
            SchemaOperation::AlterColumn { table, column } => {
                vec![compile_alter_column(self, table, column)?]
            }
            SchemaOperation::SetColumnComment { table, column } => {
                compile_column_comment(self, table, column)?
                    .into_iter()
                    .collect()
            }
            SchemaOperation::CreateIndex { table, index } => {
                vec![compile_create_index(self, table, index)?]
            }
            SchemaOperation::DropIndex { table, name } => {
                vec![compile_drop_index(self, table, name)]
            }
            SchemaOperation::AddConstraint { table, constraint } => {
                vec![compile_add_constraint(self, table, constraint)?]
            }
            SchemaOperation::DropConstraint { table, name } => vec![format!(
                "ALTER TABLE {} DROP CONSTRAINT {}",
                quote_qualified_name(self, table),
                self.quote_ident(name)
            )],
            SchemaOperation::SetTableComment { table, comment } => {
                compile_table_comment(self, table, comment.as_deref())?
                    .into_iter()
                    .collect()
            }
            SchemaOperation::SetTableTablespace { table, tablespace } => {
                compile_table_tablespace(self, table, tablespace.as_deref())?
                    .into_iter()
                    .collect()
            }
            SchemaOperation::SetTableMysqlOptions {
                table,
                engine,
                charset,
                collation,
                row_format,
                key_block_size,
                pack_keys,
                checksum,
                delay_key_write,
                stats_persistent,
                stats_auto_recalc,
                stats_sample_pages,
                avg_row_length,
                max_rows,
                min_rows,
                insert_method,
                data_directory,
                index_directory,
                connection,
                union,
                partition_by,
                partitions,
                subpartition_by,
                subpartitions,
                auto_increment,
            } => compile_table_mysql_options(
                self,
                table,
                MysqlTableOptionsRef {
                    engine: engine.as_deref(),
                    charset: charset.as_deref(),
                    collation: collation.as_deref(),
                    row_format: row_format.as_deref(),
                    key_block_size: *key_block_size,
                    pack_keys: *pack_keys,
                    checksum: *checksum,
                    delay_key_write: *delay_key_write,
                    stats_persistent: *stats_persistent,
                    stats_auto_recalc: *stats_auto_recalc,
                    stats_sample_pages: *stats_sample_pages,
                    avg_row_length: *avg_row_length,
                    max_rows: *max_rows,
                    min_rows: *min_rows,
                    insert_method: insert_method.as_deref(),
                    data_directory: data_directory.as_deref(),
                    index_directory: index_directory.as_deref(),
                    connection: connection.as_deref(),
                    union,
                    partition_by: partition_by.as_deref(),
                    partitions: *partitions,
                    subpartition_by: subpartition_by.as_deref(),
                    subpartitions: *subpartitions,
                    auto_increment: *auto_increment,
                },
            )?
            .into_iter()
            .collect(),
            SchemaOperation::SetTablePostgresInherits { table, add, drop } => {
                compile_table_postgres_inherits(self, table, add, drop)?
            }
            SchemaOperation::SetTablePostgresWith { table, set, reset } => {
                compile_table_postgres_with(self, table, set, reset)?
            }
            SchemaOperation::SetTablePostgresUsing { table, using } => {
                compile_table_postgres_using(self, table, using.as_deref())?
                    .into_iter()
                    .collect()
            }
            SchemaOperation::SetTablePostgresUnlogged { table, unlogged } => {
                if self.kind() != DialectKind::Postgres {
                    return Err(ormdantic_core::OrmdanticError::UnsupportedFeature {
                        feature: "PostgreSQL unlogged tables".to_string(),
                        dialect: self.name().to_string(),
                    });
                }
                vec![format!(
                    "ALTER TABLE {} SET {}",
                    quote_qualified_name(self, table),
                    if *unlogged { "UNLOGGED" } else { "LOGGED" }
                )]
            }
            SchemaOperation::AttachPostgresPartition {
                table,
                parent,
                bound,
            } => {
                if self.kind() != DialectKind::Postgres {
                    return Err(ormdantic_core::OrmdanticError::UnsupportedFeature {
                        feature: "PostgreSQL table partitions".to_string(),
                        dialect: self.name().to_string(),
                    });
                }
                vec![format!(
                    "ALTER TABLE {} ATTACH PARTITION {} {}",
                    quote_qualified_name(self, parent),
                    quote_qualified_name(self, table),
                    bound
                )]
            }
            SchemaOperation::DetachPostgresPartition { table, parent } => {
                if self.kind() != DialectKind::Postgres {
                    return Err(ormdantic_core::OrmdanticError::UnsupportedFeature {
                        feature: "PostgreSQL table partitions".to_string(),
                        dialect: self.name().to_string(),
                    });
                }
                vec![format!(
                    "ALTER TABLE {} DETACH PARTITION {}",
                    quote_qualified_name(self, parent),
                    quote_qualified_name(self, table)
                )]
            }
        })
    }

    fn begin_transaction_sql(&self, options: &TransactionOptions) -> Vec<String> {
        match self.kind() {
            DialectKind::Postgres => postgres_begin_transaction_sql(self, options),
            DialectKind::MySql | DialectKind::MariaDb => mysql_begin_transaction_sql(self, options),
            DialectKind::MsSql => mssql_begin_transaction_sql(self, options),
            DialectKind::Oracle => oracle_begin_transaction_sql(self, options),
            DialectKind::Sqlite => vec!["BEGIN".to_string()],
        }
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

fn render_string_type(kind: DialectKind, max_length: Option<u32>) -> String {
    let Some(max_length) = max_length.filter(|length| *length > 0) else {
        return "TEXT".to_string();
    };
    match kind {
        DialectKind::MsSql => format!("NVARCHAR({max_length})"),
        DialectKind::Oracle => format!("VARCHAR2({max_length})"),
        _ => format!("VARCHAR({max_length})"),
    }
}

fn postgres_begin_transaction_sql(
    dialect: &(impl Dialect + ?Sized),
    options: &TransactionOptions,
) -> Vec<String> {
    let mut statements = Vec::new();
    if let Some(isolation_level) = options.isolation_level() {
        statements.push(dialect.set_isolation_sql(isolation_level));
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

fn mysql_begin_transaction_sql(
    dialect: &(impl Dialect + ?Sized),
    options: &TransactionOptions,
) -> Vec<String> {
    let mut statements = Vec::new();
    if let Some(isolation_level) = options.isolation_level() {
        statements.push(dialect.set_isolation_sql(isolation_level));
    }
    let mut begin = "START TRANSACTION".to_string();
    if options.access_mode() == TransactionAccessMode::ReadOnly {
        begin.push_str(" READ ONLY");
    }
    statements.push(begin);
    statements
}

fn mssql_begin_transaction_sql(
    dialect: &(impl Dialect + ?Sized),
    options: &TransactionOptions,
) -> Vec<String> {
    let mut statements = Vec::new();
    if let Some(isolation_level) = options.isolation_level() {
        statements.push(dialect.set_isolation_sql(isolation_level));
    }
    statements.push("BEGIN TRANSACTION".to_string());
    statements
}

fn oracle_begin_transaction_sql(
    dialect: &(impl Dialect + ?Sized),
    options: &TransactionOptions,
) -> Vec<String> {
    if options.access_mode() == TransactionAccessMode::ReadOnly {
        return vec!["SET TRANSACTION READ ONLY".to_string()];
    }
    if let Some(isolation_level) = options.isolation_level() {
        return vec![dialect.set_isolation_sql(isolation_level)];
    }
    Vec::new()
}

fn quote_qualified_name(dialect: &(impl Dialect + ?Sized), name: &str) -> String {
    name.split('.')
        .map(|part| dialect.quote_ident(part))
        .collect::<Vec<_>>()
        .join(".")
}

fn drop_table_sql(dialect: &(impl Dialect + ?Sized), name: &str) -> String {
    let qualified = quote_qualified_name(dialect, name);
    if dialect.kind() == DialectKind::Oracle {
        return format!("DROP TABLE {qualified}");
    }
    format!("DROP TABLE IF EXISTS {qualified}")
}

fn compile_create_namespace(
    dialect: &(impl Dialect + ?Sized),
    namespace: &NamespaceDef,
) -> OrmdanticResult<Vec<String>> {
    let name = namespace.name();
    let mut statements = match dialect.kind() {
        DialectKind::Postgres | DialectKind::MySql | DialectKind::MariaDb => vec![format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            dialect.quote_ident(name)
        )],
        DialectKind::MsSql => {
            let create_sql = format!("CREATE SCHEMA {}", dialect.quote_ident(name));
            vec![format!(
                "IF SCHEMA_ID({}) IS NULL EXEC({})",
                mssql_unicode_literal(name),
                mssql_unicode_literal(&create_sql)
            )]
        }
        DialectKind::Sqlite | DialectKind::Oracle => {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "namespaces/schemas".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
    };
    if let Some(comment) = namespace.comment() {
        statements.extend(compile_namespace_comment(dialect, name, Some(comment))?);
    }
    Ok(statements)
}

fn compile_drop_namespace(
    dialect: &(impl Dialect + ?Sized),
    name: &str,
) -> OrmdanticResult<Vec<String>> {
    Ok(match dialect.kind() {
        DialectKind::Postgres | DialectKind::MySql | DialectKind::MariaDb | DialectKind::MsSql => {
            vec![format!(
                "DROP SCHEMA IF EXISTS {}",
                dialect.quote_ident(name)
            )]
        }
        DialectKind::Sqlite | DialectKind::Oracle => {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "namespaces/schemas".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
    })
}

fn compile_namespace_comment(
    dialect: &(impl Dialect + ?Sized),
    name: &str,
    comment: Option<&str>,
) -> OrmdanticResult<Vec<String>> {
    Ok(match dialect.kind() {
        DialectKind::Postgres => vec![format!(
            "COMMENT ON SCHEMA {} IS {}",
            dialect.quote_ident(name),
            comment
                .map(sql_string_literal)
                .unwrap_or_else(|| "NULL".to_string())
        )],
        DialectKind::MsSql => vec![compile_mssql_namespace_comment(name, comment)],
        _ => {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "namespace comments".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
    })
}

fn compile_mssql_namespace_comment(name: &str, comment: Option<&str>) -> String {
    let schema_literal = mssql_unicode_literal(name);
    let exists_predicate = format!(
        "EXISTS (SELECT 1 FROM sys.extended_properties ep \
         JOIN sys.schemas s ON ep.major_id = s.schema_id \
         WHERE ep.class = 3 AND ep.minor_id = 0 \
         AND ep.name = N'MS_Description' \
         AND s.name = {schema_literal})"
    );
    let level_args = format!("@level0type = N'SCHEMA', @level0name = {schema_literal}");
    match comment {
        Some(comment) => {
            let comment_literal = mssql_unicode_literal(comment);
            format!(
                "IF {exists_predicate} \
                 EXEC sys.sp_updateextendedproperty @name = N'MS_Description', \
                 @value = {comment_literal}, {level_args}; \
                 ELSE EXEC sys.sp_addextendedproperty @name = N'MS_Description', \
                 @value = {comment_literal}, {level_args}"
            )
        }
        None => format!(
            "IF {exists_predicate} \
             EXEC sys.sp_dropextendedproperty @name = N'MS_Description', {level_args}"
        ),
    }
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn mssql_unicode_literal(value: &str) -> String {
    format!("N'{}'", value.replace('\'', "''"))
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

    fn reflection_queries(&self, scope: &ReflectionScope) -> Vec<ReflectionQuery> {
        let table_view = if scope.schema_name().is_some() {
            "all_tables"
        } else {
            "user_tables"
        };
        let columns_view = if scope.schema_name().is_some() {
            "all_tab_columns"
        } else {
            "user_tab_columns"
        };
        let constraints_view = if scope.schema_name().is_some() {
            "all_constraints"
        } else {
            "user_constraints"
        };
        let owner_filter = scope
            .schema_name()
            .map(|schema| {
                format!(
                    " WHERE owner = '{}'",
                    schema.replace('\'', "''").to_uppercase()
                )
            })
            .unwrap_or_default();
        vec![
            ReflectionQuery::new(
                ReflectionQueryKind::Tables,
                format!("SELECT table_name FROM {table_view}{owner_filter}"),
            ),
            ReflectionQuery::new(
                ReflectionQueryKind::Columns,
                format!("SELECT table_name, column_name, data_type, nullable FROM {columns_view}{owner_filter}"),
            ),
            ReflectionQuery::new(
                ReflectionQueryKind::Constraints,
                format!("SELECT table_name, constraint_name, constraint_type FROM {constraints_view}{owner_filter}"),
            ),
        ]
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

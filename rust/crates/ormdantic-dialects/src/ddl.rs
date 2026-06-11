use ormdantic_core::{OrmdanticError, OrmdanticResult};
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, ConstraintDef, ConstraintTiming, ExclusionConstraintDef,
    FieldKind, ForeignKeyAction, ForeignKeyDef, ForeignKeyMatch, IdentityDef, IndexDef,
    OracleIndexCompression, OracleTableCompression, TableDef, UniqueConstraintDef,
};

use crate::{Dialect, DialectKind};

pub(crate) fn compile_create_table(
    dialect: &(impl Dialect + ?Sized),
    table: &TableDef,
) -> OrmdanticResult<Vec<String>> {
    validate_postgres_unlogged(dialect, table.is_postgres_unlogged())?;
    if table.postgres_partition_for().is_some() && table.postgres_partition_of().is_none() {
        return Err(OrmdanticError::MigrationError {
            message: format!(
                "PostgreSQL partition table '{}' is missing a partition parent",
                table.name()
            ),
        });
    }
    if table.postgres_partition_of().is_some() {
        if dialect.kind() != DialectKind::Postgres {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "PostgreSQL table partitions".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
        if table.postgres_partition_for().is_none() {
            return Err(OrmdanticError::MigrationError {
                message: format!(
                    "PostgreSQL partition table '{}' is missing a partition bound",
                    table.name()
                ),
            });
        }
        if !table.postgres_inherits().is_empty() {
            return Err(OrmdanticError::MigrationError {
                message: format!(
                    "PostgreSQL partition table '{}' cannot also use INHERITS",
                    table.name()
                ),
            });
        }
    }
    let mut parts = if table.postgres_partition_of().is_some() {
        Vec::new()
    } else {
        table
            .columns()
            .iter()
            .map(|column| render_create_table_column_def(dialect, table, column))
            .collect::<OrmdanticResult<Vec<_>>>()?
    };

    for constraint in table.unique_constraints() {
        parts.push(render_unique_constraint(dialect, constraint)?);
    }
    for constraint in table.check_constraints() {
        if should_inline_validated_constraint(dialect, constraint.is_validated()) {
            parts.push(render_check_constraint(
                dialect,
                constraint,
                should_render_inline_constraint_validation(dialect, constraint.is_validated()),
                Some(table.columns()),
            )?);
        } else {
            validate_check_constraint_validation(dialect, constraint.is_validated())?;
        }
    }
    for foreign_key in table.foreign_keys() {
        if should_inline_validated_constraint(dialect, foreign_key.is_validated()) {
            parts.push(render_foreign_key(
                dialect,
                foreign_key,
                should_render_inline_constraint_validation(dialect, foreign_key.is_validated()),
            )?);
        } else {
            validate_foreign_key_constraint_validation(dialect, foreign_key.is_validated())?;
        }
    }
    for constraint in table.exclusion_constraints() {
        parts.push(render_exclusion_constraint(dialect, constraint)?);
    }

    let storage = compile_create_table_storage(dialect, table)?;
    let persistence = if table.is_postgres_unlogged() {
        "UNLOGGED "
    } else {
        ""
    };
    let create_table = if let Some(parent) = table.postgres_partition_of() {
        let Some(bound) = table.postgres_partition_for() else {
            unreachable!("partition child tables are validated before storage rendering")
        };
        let constraints = if parts.is_empty() {
            String::new()
        } else {
            format!(" ({})", parts.join(", "))
        };
        format!(
            "CREATE {persistence}TABLE IF NOT EXISTS {} PARTITION OF {}{} {}{}",
            quote_table_def(dialect, table),
            quote_qualified_name(dialect, parent),
            constraints,
            bound,
            storage,
        )
    } else if dialect.kind() == DialectKind::MsSql {
        format!(
            "IF OBJECT_ID(N'{}', N'U') IS NULL CREATE TABLE {} ({}){}",
            mssql_object_name(table),
            quote_table_def(dialect, table),
            parts.join(", "),
            storage,
        )
    } else if dialect.kind() == DialectKind::Oracle {
        format!(
            "CREATE {persistence}TABLE {} ({}){}",
            quote_table_def(dialect, table),
            parts.join(", "),
            storage,
        )
    } else {
        format!(
            "CREATE {persistence}TABLE IF NOT EXISTS {} ({}){}",
            quote_table_def(dialect, table),
            parts.join(", "),
            storage,
        )
    };
    let mut statements = vec![create_table];
    for constraint in table.check_constraints() {
        if !constraint.is_validated()
            && !should_inline_validated_constraint(dialect, constraint.is_validated())
        {
            statements.push(compile_add_constraint(
                dialect,
                &table.qualified_name().to_string(),
                &ConstraintDef::Check(constraint.clone()),
            )?);
        }
    }
    for foreign_key in table.foreign_keys() {
        if !foreign_key.is_validated()
            && !should_inline_validated_constraint(dialect, foreign_key.is_validated())
        {
            statements.push(compile_add_constraint(
                dialect,
                &table.qualified_name().to_string(),
                &ConstraintDef::ForeignKey(foreign_key.clone()),
            )?);
        }
    }
    let qualified_table_name = table.qualified_name().to_string();
    if let Some(comment) = table.comment() {
        if let Some(comment_sql) =
            compile_table_comment(dialect, &qualified_table_name, Some(comment))?
        {
            statements.push(comment_sql);
        }
    }
    for column in table.columns() {
        if column.comment().is_some() {
            if let Some(comment_sql) =
                compile_create_column_comment(dialect, &qualified_table_name, column)?
            {
                statements.push(comment_sql);
            }
        }
    }
    for index in table.indexes() {
        statements.push(compile_create_index(dialect, &qualified_table_name, index)?);
    }
    Ok(statements)
}

fn mssql_object_name(table: &ormdantic_schema::TableDef) -> String {
    fn escape(value: &str) -> String {
        value.replace('\'', "''")
    }
    match table.schema() {
        Some(schema) => format!("{}.{}", escape(schema), escape(table.name())),
        None => escape(table.name()),
    }
}

fn quote_table_def(
    dialect: &(impl Dialect + ?Sized),
    table: &ormdantic_schema::TableDef,
) -> String {
    match table.schema() {
        Some(schema) => format!(
            "{}.{}",
            dialect.quote_ident(schema),
            dialect.quote_ident(table.name())
        ),
        None => dialect.quote_ident(table.name()),
    }
}

fn quote_qualified_name(dialect: &(impl Dialect + ?Sized), name: &str) -> String {
    name.split('.')
        .map(|part| dialect.quote_ident(part))
        .collect::<Vec<_>>()
        .join(".")
}

fn compile_create_table_storage(
    dialect: &(impl Dialect + ?Sized),
    table: &ormdantic_schema::TableDef,
) -> OrmdanticResult<String> {
    let mut storage = String::new();
    if table.postgres_partition_of().is_some() && !table.postgres_inherits().is_empty() {
        return Err(OrmdanticError::MigrationError {
            message: format!(
                "PostgreSQL partition table '{}' cannot also use INHERITS",
                table.name()
            ),
        });
    }
    if !table.postgres_inherits().is_empty() {
        if dialect.kind() != DialectKind::Postgres {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "PostgreSQL table inheritance".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
        let parents = table
            .postgres_inherits()
            .iter()
            .map(|parent| quote_qualified_name(dialect, parent))
            .collect::<Vec<_>>()
            .join(", ");
        storage.push_str(&format!(" INHERITS ({parents})"));
    }
    if let Some(partition_by) = table.postgres_partition_by() {
        if dialect.kind() != DialectKind::Postgres {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "PostgreSQL table partitioning".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
        storage.push_str(&format!(" PARTITION BY {partition_by}"));
    }
    if let Some(access_method) = table.postgres_using() {
        if dialect.kind() != DialectKind::Postgres {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "PostgreSQL table access methods".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
        storage.push_str(&format!(" USING {}", dialect.quote_ident(access_method)));
    }
    if let Some(options) =
        render_postgres_table_storage_parameters(dialect, table.postgres_with(), true)?
    {
        storage.push(' ');
        storage.push_str(&options);
    }
    if let Some(compress) = table.oracle_compress() {
        if dialect.kind() != DialectKind::Oracle {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "Oracle table compression".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
        storage.push_str(&render_oracle_table_compression(compress));
    }
    if let Some(tablespace) = table.tablespace() {
        match dialect.kind() {
            DialectKind::Sqlite => {}
            DialectKind::Postgres | DialectKind::Oracle => {
                storage.push_str(&format!(" TABLESPACE {}", dialect.quote_ident(tablespace)));
            }
            DialectKind::MySql | DialectKind::MariaDb => {
                storage.push_str(&format!(" TABLESPACE {}", dialect.quote_ident(tablespace)));
            }
            DialectKind::MsSql => {
                storage.push_str(&format!(" ON {}", dialect.quote_ident(tablespace)));
            }
        }
    }
    if let Some(mysql_options) =
        render_mysql_table_options(dialect, MysqlTableOptionsRef::from_table(table), false)?
    {
        storage.push(' ');
        storage.push_str(&mysql_options);
    }
    if table.is_sqlite_strict() || table.is_sqlite_without_rowid() {
        if dialect.kind() != DialectKind::Sqlite {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "SQLite table options".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
        let mut options = Vec::new();
        if table.is_sqlite_strict() {
            options.push("STRICT");
        }
        if table.is_sqlite_without_rowid() {
            options.push("WITHOUT ROWID");
        }
        storage.push(' ');
        storage.push_str(&options.join(", "));
    }
    Ok(storage)
}

fn validate_postgres_unlogged(
    dialect: &(impl Dialect + ?Sized),
    unlogged: bool,
) -> OrmdanticResult<()> {
    if !unlogged || dialect.kind() == DialectKind::Postgres {
        return Ok(());
    }
    Err(OrmdanticError::UnsupportedFeature {
        feature: "PostgreSQL unlogged tables".to_string(),
        dialect: dialect.name().to_string(),
    })
}

pub(crate) fn compile_table_comment(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    comment: Option<&str>,
) -> OrmdanticResult<Option<String>> {
    match dialect.kind() {
        DialectKind::Sqlite => Ok(None),
        DialectKind::Postgres => Ok(Some(format!(
            "COMMENT ON TABLE {} IS {}",
            quote_qualified_name(dialect, table),
            comment
                .map(sql_string_literal)
                .unwrap_or_else(|| "NULL".to_string())
        ))),
        DialectKind::Oracle => Ok(Some(format!(
            "COMMENT ON TABLE {} IS {}",
            quote_qualified_name(dialect, table),
            comment
                .map(sql_string_literal)
                .unwrap_or_else(|| "''".to_string())
        ))),
        DialectKind::MySql | DialectKind::MariaDb => Ok(Some(format!(
            "ALTER TABLE {} COMMENT = {}",
            quote_qualified_name(dialect, table),
            comment
                .map(sql_string_literal)
                .unwrap_or_else(|| "''".to_string())
        ))),
        DialectKind::MsSql => Ok(Some(compile_mssql_table_comment(table, comment))),
    }
}

pub(crate) fn compile_column_comment(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    column: &ColumnDef,
) -> OrmdanticResult<Option<String>> {
    match dialect.kind() {
        DialectKind::Sqlite => Ok(None),
        DialectKind::Postgres => Ok(Some(format!(
            "COMMENT ON COLUMN {}.{} IS {}",
            quote_qualified_name(dialect, table),
            dialect.quote_ident(column.name()),
            column
                .comment()
                .map(sql_string_literal)
                .unwrap_or_else(|| "NULL".to_string())
        ))),
        DialectKind::Oracle => Ok(Some(format!(
            "COMMENT ON COLUMN {}.{} IS {}",
            quote_qualified_name(dialect, table),
            dialect.quote_ident(column.name()),
            column
                .comment()
                .map(sql_string_literal)
                .unwrap_or_else(|| "''".to_string())
        ))),
        DialectKind::MsSql => Ok(Some(compile_mssql_column_comment(
            table,
            column.name(),
            column.comment(),
        ))),
        DialectKind::MySql | DialectKind::MariaDb => Ok(Some(format!(
            "ALTER TABLE {} MODIFY COLUMN {}",
            quote_qualified_name(dialect, table),
            render_mysql_column_modify_def(dialect, column)?
        ))),
    }
}

fn compile_create_column_comment(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    column: &ColumnDef,
) -> OrmdanticResult<Option<String>> {
    match dialect.kind() {
        DialectKind::MySql | DialectKind::MariaDb => Ok(None),
        _ => compile_column_comment(dialect, table, column),
    }
}

pub(crate) fn compile_table_tablespace(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    tablespace: Option<&str>,
) -> OrmdanticResult<Option<String>> {
    match dialect.kind() {
        DialectKind::Sqlite => Ok(None),
        DialectKind::Postgres => Ok(Some(format!(
            "ALTER TABLE {} SET TABLESPACE {}",
            quote_qualified_name(dialect, table),
            dialect.quote_ident(tablespace.unwrap_or("pg_default"))
        ))),
        DialectKind::Oracle => Ok(Some(match tablespace {
            Some(tablespace) => format!(
                "ALTER TABLE {} MOVE TABLESPACE {}",
                quote_qualified_name(dialect, table),
                dialect.quote_ident(tablespace)
            ),
            None => format!("ALTER TABLE {} MOVE", quote_qualified_name(dialect, table)),
        })),
        DialectKind::MySql | DialectKind::MariaDb => Ok(Some(format!(
            "ALTER TABLE {} TABLESPACE {}",
            quote_qualified_name(dialect, table),
            dialect.quote_ident(tablespace.unwrap_or("innodb_file_per_table"))
        ))),
        DialectKind::MsSql => Err(OrmdanticError::UnsupportedFeature {
            feature: "SQL Server table filegroup changes".to_string(),
            dialect: dialect.name().to_string(),
        }),
    }
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_unicode_literal(value: &str) -> String {
    format!("N'{}'", value.replace('\'', "''"))
}

#[derive(Clone, Copy)]
pub(crate) struct MysqlTableOptionsRef<'a> {
    pub engine: Option<&'a str>,
    pub charset: Option<&'a str>,
    pub collation: Option<&'a str>,
    pub row_format: Option<&'a str>,
    pub key_block_size: Option<u32>,
    pub pack_keys: Option<bool>,
    pub checksum: Option<bool>,
    pub delay_key_write: Option<bool>,
    pub stats_persistent: Option<bool>,
    pub stats_auto_recalc: Option<bool>,
    pub stats_sample_pages: Option<u32>,
    pub avg_row_length: Option<u32>,
    pub max_rows: Option<u32>,
    pub min_rows: Option<u32>,
    pub insert_method: Option<&'a str>,
    pub data_directory: Option<&'a str>,
    pub index_directory: Option<&'a str>,
    pub connection: Option<&'a str>,
    pub union: &'a [String],
    pub partition_by: Option<&'a str>,
    pub partitions: Option<u32>,
    pub subpartition_by: Option<&'a str>,
    pub subpartitions: Option<u32>,
    pub auto_increment: Option<u32>,
}

impl<'a> MysqlTableOptionsRef<'a> {
    fn from_table(table: &'a TableDef) -> Self {
        Self {
            engine: table.mysql_engine(),
            charset: table.mysql_charset(),
            collation: table.mysql_collation(),
            row_format: table.mysql_row_format(),
            key_block_size: table.mysql_key_block_size(),
            pack_keys: table.mysql_pack_keys(),
            checksum: table.mysql_checksum(),
            delay_key_write: table.mysql_delay_key_write(),
            stats_persistent: table.mysql_stats_persistent(),
            stats_auto_recalc: table.mysql_stats_auto_recalc(),
            stats_sample_pages: table.mysql_stats_sample_pages(),
            avg_row_length: table.mysql_avg_row_length(),
            max_rows: table.mysql_max_rows(),
            min_rows: table.mysql_min_rows(),
            insert_method: table.mysql_insert_method(),
            data_directory: table.mysql_data_directory(),
            index_directory: table.mysql_index_directory(),
            connection: table.mysql_connection(),
            union: table.mysql_union(),
            partition_by: table.mysql_partition_by(),
            partitions: table.mysql_partitions(),
            subpartition_by: table.mysql_subpartition_by(),
            subpartitions: table.mysql_subpartitions(),
            auto_increment: table.mysql_auto_increment(),
        }
    }

    fn is_empty(&self) -> bool {
        self.engine.is_none()
            && self.charset.is_none()
            && self.collation.is_none()
            && self.row_format.is_none()
            && self.key_block_size.is_none()
            && self.pack_keys.is_none()
            && self.checksum.is_none()
            && self.delay_key_write.is_none()
            && self.stats_persistent.is_none()
            && self.stats_auto_recalc.is_none()
            && self.stats_sample_pages.is_none()
            && self.avg_row_length.is_none()
            && self.max_rows.is_none()
            && self.min_rows.is_none()
            && self.insert_method.is_none()
            && self.data_directory.is_none()
            && self.index_directory.is_none()
            && self.connection.is_none()
            && self.union.is_empty()
            && self.partition_by.is_none()
            && self.partitions.is_none()
            && self.subpartition_by.is_none()
            && self.subpartitions.is_none()
            && self.auto_increment.is_none()
    }
}

pub(crate) fn compile_table_mysql_options(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    options: MysqlTableOptionsRef<'_>,
) -> OrmdanticResult<Option<String>> {
    Ok(
        render_mysql_table_options(dialect, options, true)?.map(|options| {
            format!(
                "ALTER TABLE {} {options}",
                quote_qualified_name(dialect, table)
            )
        }),
    )
}

pub(crate) fn compile_table_postgres_inherits(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    add: &[String],
    drop: &[String],
) -> OrmdanticResult<Vec<String>> {
    if add.is_empty() && drop.is_empty() {
        return Ok(Vec::new());
    }
    if dialect.kind() != DialectKind::Postgres {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "PostgreSQL table inheritance".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    let mut statements = Vec::new();
    for parent in drop {
        statements.push(format!(
            "ALTER TABLE {} NO INHERIT {}",
            quote_qualified_name(dialect, table),
            quote_qualified_name(dialect, parent)
        ));
    }
    for parent in add {
        statements.push(format!(
            "ALTER TABLE {} INHERIT {}",
            quote_qualified_name(dialect, table),
            quote_qualified_name(dialect, parent)
        ));
    }
    Ok(statements)
}

pub(crate) fn compile_table_postgres_with(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    set: &[(String, String)],
    reset: &[String],
) -> OrmdanticResult<Vec<String>> {
    if set.is_empty() && reset.is_empty() {
        return Ok(Vec::new());
    }
    if dialect.kind() != DialectKind::Postgres {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "PostgreSQL table storage parameters".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    let mut statements = Vec::new();
    if !set.is_empty() {
        let options = set
            .iter()
            .map(|(name, value)| render_postgres_storage_parameter(name, value))
            .collect::<OrmdanticResult<Vec<_>>>()?
            .join(", ");
        statements.push(format!(
            "ALTER TABLE {} SET ({options})",
            quote_qualified_name(dialect, table)
        ));
    }
    if !reset.is_empty() {
        let options = reset
            .iter()
            .map(|name| validate_postgres_storage_parameter_name(name).map(|_| name.clone()))
            .collect::<OrmdanticResult<Vec<_>>>()?
            .join(", ");
        statements.push(format!(
            "ALTER TABLE {} RESET ({options})",
            quote_qualified_name(dialect, table)
        ));
    }
    Ok(statements)
}

pub(crate) fn compile_table_postgres_using(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    using: Option<&str>,
) -> OrmdanticResult<Option<String>> {
    if dialect.kind() != DialectKind::Postgres {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "PostgreSQL table access methods".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    Ok(Some(format!(
        "ALTER TABLE {} SET ACCESS METHOD {}",
        quote_qualified_name(dialect, table),
        dialect.quote_ident(using.unwrap_or("heap"))
    )))
}

fn render_postgres_table_storage_parameters(
    dialect: &(impl Dialect + ?Sized),
    parameters: &[(String, String)],
    require_postgres: bool,
) -> OrmdanticResult<Option<String>> {
    render_postgres_storage_parameters(
        dialect,
        parameters,
        require_postgres,
        "PostgreSQL table storage parameters",
    )
}

fn render_postgres_index_storage_parameters(
    dialect: &(impl Dialect + ?Sized),
    parameters: &[(String, String)],
    require_postgres: bool,
) -> OrmdanticResult<Option<String>> {
    render_postgres_storage_parameters(
        dialect,
        parameters,
        require_postgres,
        "PostgreSQL index storage parameters",
    )
}

fn render_postgres_storage_parameters(
    dialect: &(impl Dialect + ?Sized),
    parameters: &[(String, String)],
    require_postgres: bool,
    feature: &str,
) -> OrmdanticResult<Option<String>> {
    if parameters.is_empty() {
        return Ok(None);
    }
    if dialect.kind() != DialectKind::Postgres {
        if require_postgres {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: feature.to_string(),
                dialect: dialect.name().to_string(),
            });
        }
        return Ok(None);
    }
    let rendered = parameters
        .iter()
        .map(|(name, value)| render_postgres_storage_parameter(name, value))
        .collect::<OrmdanticResult<Vec<_>>>()?
        .join(", ");
    Ok(Some(format!("WITH ({rendered})")))
}

fn render_postgres_storage_parameter(name: &str, value: &str) -> OrmdanticResult<String> {
    validate_postgres_storage_parameter_name(name)?;
    validate_postgres_storage_parameter_value(name, value)?;
    Ok(format!("{name} = {value}"))
}

fn validate_postgres_storage_parameter_name(name: &str) -> OrmdanticResult<()> {
    if name.is_empty()
        || !name.split('.').all(|part| {
            let mut chars = part.chars();
            matches!(chars.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
                && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        })
    {
        return Err(OrmdanticError::SchemaDiffError {
            message: format!("invalid PostgreSQL table storage parameter name '{name}'"),
        });
    }
    Ok(())
}

fn validate_postgres_storage_parameter_value(name: &str, value: &str) -> OrmdanticResult<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '$' | '+' | '-'))
    {
        return Err(OrmdanticError::SchemaDiffError {
            message: format!("invalid PostgreSQL table storage parameter value for '{name}'"),
        });
    }
    Ok(())
}

fn render_mysql_table_options(
    dialect: &(impl Dialect + ?Sized),
    mysql: MysqlTableOptionsRef<'_>,
    require_mysql: bool,
) -> OrmdanticResult<Option<String>> {
    if mysql.is_empty() {
        return Ok(None);
    }
    if !matches!(dialect.kind(), DialectKind::MySql | DialectKind::MariaDb) {
        if require_mysql {
            return Err(OrmdanticError::UnsupportedFeature {
                feature: "mysql table options".to_string(),
                dialect: dialect.name().to_string(),
            });
        }
        return Ok(None);
    }

    let MysqlTableOptionsRef {
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
    } = mysql;

    let mut options = Vec::new();
    if let Some(engine) = engine {
        options.push(format!(
            "ENGINE = {}",
            mysql_storage_token("mysql engine", engine)?
        ));
    }
    if let Some(charset) = charset {
        options.push(format!(
            "DEFAULT CHARACTER SET = {}",
            mysql_storage_token("mysql charset", charset)?
        ));
    }
    if let Some(collation) = collation {
        options.push(format!(
            "COLLATE = {}",
            mysql_storage_token("mysql collation", collation)?
        ));
    }
    if let Some(row_format) = row_format {
        options.push(format!(
            "ROW_FORMAT = {}",
            mysql_storage_token("mysql row format", row_format)?
        ));
    }
    if let Some(key_block_size) = key_block_size {
        options.push(format!("KEY_BLOCK_SIZE = {key_block_size}"));
    }
    if let Some(pack_keys) = pack_keys {
        let value = if pack_keys { 1 } else { 0 };
        options.push(format!("PACK_KEYS = {value}"));
    }
    if let Some(checksum) = checksum {
        let value = if checksum { 1 } else { 0 };
        options.push(format!("CHECKSUM = {value}"));
    }
    if let Some(delay_key_write) = delay_key_write {
        let value = if delay_key_write { 1 } else { 0 };
        options.push(format!("DELAY_KEY_WRITE = {value}"));
    }
    if let Some(stats_persistent) = stats_persistent {
        let value = if stats_persistent { 1 } else { 0 };
        options.push(format!("STATS_PERSISTENT = {value}"));
    }
    if let Some(stats_auto_recalc) = stats_auto_recalc {
        let value = if stats_auto_recalc { 1 } else { 0 };
        options.push(format!("STATS_AUTO_RECALC = {value}"));
    }
    if let Some(stats_sample_pages) = stats_sample_pages {
        options.push(format!("STATS_SAMPLE_PAGES = {stats_sample_pages}"));
    }
    if let Some(avg_row_length) = avg_row_length {
        options.push(format!("AVG_ROW_LENGTH = {avg_row_length}"));
    }
    if let Some(max_rows) = max_rows {
        options.push(format!("MAX_ROWS = {max_rows}"));
    }
    if let Some(min_rows) = min_rows {
        options.push(format!("MIN_ROWS = {min_rows}"));
    }
    if let Some(insert_method) = insert_method {
        options.push(format!(
            "INSERT_METHOD = {}",
            mysql_storage_token("mysql insert method", insert_method)?
        ));
    }
    if let Some(data_directory) = data_directory {
        options.push(format!(
            "DATA DIRECTORY = {}",
            sql_string_literal(data_directory)
        ));
    }
    if let Some(index_directory) = index_directory {
        options.push(format!(
            "INDEX DIRECTORY = {}",
            sql_string_literal(index_directory)
        ));
    }
    if let Some(connection) = connection {
        options.push(format!("CONNECTION = {}", sql_string_literal(connection)));
    }
    if !union.is_empty() {
        let tables = union
            .iter()
            .map(|table| quote_qualified_name(dialect, table))
            .collect::<Vec<_>>()
            .join(", ");
        options.push(format!("UNION = ({tables})"));
    }
    if let Some(partition_by) = partition_by {
        options.push(format!(
            "PARTITION BY {}",
            mysql_partition_clause("mysql partition by", partition_by)?
        ));
    }
    if let Some(partitions) = partitions {
        options.push(format!("PARTITIONS {partitions}"));
    }
    if let Some(subpartition_by) = subpartition_by {
        options.push(format!(
            "SUBPARTITION BY {}",
            mysql_partition_clause("mysql subpartition by", subpartition_by)?
        ));
    }
    if let Some(subpartitions) = subpartitions {
        options.push(format!("SUBPARTITIONS {subpartitions}"));
    }
    if let Some(auto_increment) = auto_increment {
        options.push(format!("AUTO_INCREMENT = {auto_increment}"));
    }
    Ok(Some(options.join(" ")))
}

fn mysql_partition_clause(feature: &str, value: &str) -> OrmdanticResult<String> {
    let clause = value.trim();
    if clause.is_empty()
        || clause.contains(';')
        || clause.contains("--")
        || clause.contains("/*")
        || clause.contains("*/")
    {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: format!("{feature} value '{value}'"),
            dialect: "mysql".to_string(),
        });
    }
    Ok(clause.to_string())
}

fn mysql_storage_token(feature: &str, value: &str) -> OrmdanticResult<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$')
    {
        return Ok(value.to_string());
    }
    Err(OrmdanticError::UnsupportedFeature {
        feature: format!("{feature} value '{value}'"),
        dialect: "mysql".to_string(),
    })
}

fn compile_mssql_table_comment(table: &str, comment: Option<&str>) -> String {
    let (schema, table_name) = split_qualified_name(table);
    let schema_value = schema
        .map(sql_unicode_literal)
        .unwrap_or_else(|| "SCHEMA_NAME()".to_string());
    let table_literal = sql_unicode_literal(table_name);
    let exists_predicate = format!(
        "EXISTS (SELECT 1 FROM sys.extended_properties ep \
         JOIN sys.tables t ON ep.major_id = t.object_id \
         JOIN sys.schemas s ON t.schema_id = s.schema_id \
         WHERE ep.class = 1 AND ep.minor_id = 0 \
         AND ep.name = N'MS_Description' \
         AND s.name = @schema AND t.name = {table_literal})"
    );
    let level_args = format!(
        "@level0type = N'SCHEMA', @level0name = @schema, \
         @level1type = N'TABLE', @level1name = {table_literal}"
    );
    match comment {
        Some(comment) => {
            let comment_literal = sql_unicode_literal(comment);
            format!(
                "DECLARE @schema sysname = {schema_value}; \
                 IF {exists_predicate} \
                 EXEC sys.sp_updateextendedproperty @name = N'MS_Description', \
                 @value = {comment_literal}, {level_args}; \
                 ELSE EXEC sys.sp_addextendedproperty @name = N'MS_Description', \
                 @value = {comment_literal}, {level_args}"
            )
        }
        None => format!(
            "DECLARE @schema sysname = {schema_value}; \
             IF {exists_predicate} \
             EXEC sys.sp_dropextendedproperty @name = N'MS_Description', {level_args}"
        ),
    }
}

fn compile_mssql_column_comment(table: &str, column: &str, comment: Option<&str>) -> String {
    let (schema, table_name) = split_qualified_name(table);
    let schema_value = schema
        .map(sql_unicode_literal)
        .unwrap_or_else(|| "SCHEMA_NAME()".to_string());
    let table_literal = sql_unicode_literal(table_name);
    let column_literal = sql_unicode_literal(column);
    let exists_predicate = format!(
        "EXISTS (SELECT 1 FROM sys.extended_properties ep \
         JOIN sys.tables t ON ep.major_id = t.object_id \
         JOIN sys.schemas s ON t.schema_id = s.schema_id \
         JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ep.minor_id \
         WHERE ep.class = 1 AND ep.minor_id > 0 \
         AND ep.name = N'MS_Description' \
         AND s.name = @schema AND t.name = {table_literal} AND c.name = {column_literal})"
    );
    let level_args = format!(
        "@level0type = N'SCHEMA', @level0name = @schema, \
         @level1type = N'TABLE', @level1name = {table_literal}, \
         @level2type = N'COLUMN', @level2name = {column_literal}"
    );
    match comment {
        Some(comment) => {
            let comment_literal = sql_unicode_literal(comment);
            format!(
                "DECLARE @schema sysname = {schema_value}; \
                 IF {exists_predicate} \
                 EXEC sys.sp_updateextendedproperty @name = N'MS_Description', \
                 @value = {comment_literal}, {level_args}; \
                 ELSE EXEC sys.sp_addextendedproperty @name = N'MS_Description', \
                 @value = {comment_literal}, {level_args}"
            )
        }
        None => format!(
            "DECLARE @schema sysname = {schema_value}; \
             IF {exists_predicate} \
             EXEC sys.sp_dropextendedproperty @name = N'MS_Description', {level_args}"
        ),
    }
}

fn split_qualified_name(name: &str) -> (Option<&str>, &str) {
    match name.rsplit_once('.') {
        Some((schema, table)) => (Some(schema), table),
        None => (None, name),
    }
}

pub(crate) fn render_column_def(
    dialect: &(impl Dialect + ?Sized),
    column: &ColumnDef,
) -> OrmdanticResult<String> {
    render_column_def_with_options(dialect, column, false)
}

fn render_create_table_column_def(
    dialect: &(impl Dialect + ?Sized),
    table: &ormdantic_schema::TableDef,
    column: &ColumnDef,
) -> OrmdanticResult<String> {
    render_column_def_with_options(
        dialect,
        column,
        dialect.kind() == DialectKind::MsSql && table.is_mssql_primary_key_nonclustered(),
    )
}

fn render_column_def_with_options(
    dialect: &(impl Dialect + ?Sized),
    column: &ColumnDef,
    mssql_primary_key_nonclustered: bool,
) -> OrmdanticResult<String> {
    validate_sqlite_conflict_clause(
        dialect,
        column.sqlite_on_conflict_primary_key(),
        "SQLite primary-key conflict clauses",
    )?;
    validate_sqlite_conflict_clause(
        dialect,
        column.sqlite_on_conflict_not_null(),
        "SQLite not-null conflict clauses",
    )?;
    validate_sqlite_conflict_clause(
        dialect,
        column.sqlite_on_conflict_unique(),
        "SQLite unique conflict clauses",
    )?;
    let mut sql = format!(
        "{} {}",
        dialect.quote_ident(column.name()),
        dialect.render_column_type(column)
    );
    let generated = render_generated_value(dialect, column)?;
    match dialect.kind() {
        DialectKind::Sqlite => {
            if column.is_primary_key() {
                sql.push_str(" PRIMARY KEY");
                sql.push_str(&render_sqlite_conflict_clause(
                    column.sqlite_on_conflict_primary_key(),
                )?);
            }
            if let Some(generated) = &generated {
                sql.push_str(generated);
            }
            if !column.is_nullable() || column.is_primary_key() {
                sql.push_str(" NOT NULL");
                sql.push_str(&render_sqlite_conflict_clause(
                    column.sqlite_on_conflict_not_null(),
                )?);
            }
        }
        DialectKind::MySql | DialectKind::MariaDb => {
            if !column.is_nullable() || column.is_primary_key() {
                sql.push_str(" NOT NULL");
            }
            if let Some(generated) = &generated {
                sql.push_str(generated);
            }
            if column.is_primary_key() {
                sql.push_str(" PRIMARY KEY");
            }
        }
        DialectKind::MsSql | DialectKind::Postgres | DialectKind::Oracle => {
            if let Some(generated) = &generated {
                sql.push_str(generated);
            }
            if dialect.kind() == DialectKind::Oracle {
                if let Some(default) = column.server_default() {
                    sql.push_str(" DEFAULT ");
                    sql.push_str(default);
                }
            }
            if column.is_primary_key() {
                if dialect.kind() == DialectKind::MsSql && mssql_primary_key_nonclustered {
                    sql.push_str(" PRIMARY KEY NONCLUSTERED");
                } else {
                    sql.push_str(" PRIMARY KEY");
                }
            }
            if !column.is_nullable() || column.is_primary_key() {
                sql.push_str(" NOT NULL");
            }
        }
    }
    if dialect.kind() != DialectKind::Oracle {
        if let Some(default) = column.server_default() {
            sql.push_str(" DEFAULT ");
            sql.push_str(default);
        }
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
    if matches!(dialect.kind(), DialectKind::MySql | DialectKind::MariaDb) {
        if let Some(comment) = column.comment() {
            sql.push_str(" COMMENT ");
            sql.push_str(&sql_string_literal(comment));
        }
    }
    Ok(sql)
}

fn render_generated_value(
    dialect: &(impl Dialect + ?Sized),
    column: &ColumnDef,
) -> OrmdanticResult<Option<String>> {
    if let Some(identity) = column.identity() {
        Ok(Some(render_identity(dialect, identity)?))
    } else if column.is_autoincrement() {
        Ok(Some(render_identity(dialect, &IdentityDef::new())?))
    } else {
        Ok(None)
    }
}

fn render_identity(
    dialect: &(impl Dialect + ?Sized),
    identity: &IdentityDef,
) -> OrmdanticResult<String> {
    validate_identity_options(dialect, identity)?;
    match dialect.kind() {
        DialectKind::Sqlite => Ok(" AUTOINCREMENT".to_string()),
        DialectKind::MySql | DialectKind::MariaDb => Ok(" AUTO_INCREMENT".to_string()),
        DialectKind::MsSql => Ok(format!(
            " IDENTITY({}, {})",
            identity.start_value().unwrap_or(1),
            identity.increment_value().unwrap_or(1)
        )),
        DialectKind::Postgres | DialectKind::Oracle => {
            let mode = match (
                identity.is_always(),
                identity.is_on_null(),
                dialect.kind() == DialectKind::Oracle,
            ) {
                (true, _, _) => "ALWAYS",
                (false, true, true) => "BY DEFAULT ON NULL",
                _ => "BY DEFAULT",
            };
            let mut options = Vec::new();
            if let Some(start) = identity.start_value() {
                options.push(format!("START WITH {start}"));
            }
            if let Some(increment) = identity.increment_value() {
                options.push(format!("INCREMENT BY {increment}"));
            }
            if identity.is_no_min_value() {
                options.push(identity_no_min_value_clause(dialect.kind()).to_string());
            } else if let Some(min_value) = identity.minimum_value() {
                options.push(format!("MINVALUE {min_value}"));
            }
            if identity.is_no_max_value() {
                options.push(identity_no_max_value_clause(dialect.kind()).to_string());
            } else if let Some(max_value) = identity.maximum_value() {
                options.push(format!("MAXVALUE {max_value}"));
            }
            if identity.is_cycle() {
                options.push("CYCLE".to_string());
            }
            if let Some(cache) = identity.cache_value() {
                options.push(format!("CACHE {cache}"));
            }
            if identity.is_ordered() {
                options.push("ORDER".to_string());
            }
            if options.is_empty() {
                Ok(format!(" GENERATED {mode} AS IDENTITY"))
            } else {
                Ok(format!(
                    " GENERATED {mode} AS IDENTITY ({})",
                    options.join(" ")
                ))
            }
        }
    }
}

fn identity_no_min_value_clause(kind: DialectKind) -> &'static str {
    match kind {
        DialectKind::Oracle => "NOMINVALUE",
        _ => "NO MINVALUE",
    }
}

fn identity_no_max_value_clause(kind: DialectKind) -> &'static str {
    match kind {
        DialectKind::Oracle => "NOMAXVALUE",
        _ => "NO MAXVALUE",
    }
}

fn validate_identity_options(
    dialect: &(impl Dialect + ?Sized),
    identity: &IdentityDef,
) -> OrmdanticResult<()> {
    let has_sequence_options = identity.minimum_value().is_some()
        || identity.maximum_value().is_some()
        || identity.is_no_min_value()
        || identity.is_no_max_value()
        || identity.is_cycle()
        || identity.cache_value().is_some()
        || identity.is_ordered();
    if identity.is_no_min_value() && identity.minimum_value().is_some() {
        return Err(OrmdanticError::MigrationError {
            message: "identity no_min_value cannot be combined with min_value".to_string(),
        });
    }
    if identity.is_no_max_value() && identity.maximum_value().is_some() {
        return Err(OrmdanticError::MigrationError {
            message: "identity no_max_value cannot be combined with max_value".to_string(),
        });
    }
    if has_sequence_options
        && !matches!(dialect.kind(), DialectKind::Postgres | DialectKind::Oracle)
    {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "identity sequence options".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    if identity.is_ordered() && dialect.kind() != DialectKind::Oracle {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "Oracle identity ordering".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    if identity.is_on_null() && identity.is_always() {
        return Err(OrmdanticError::MigrationError {
            message: "Oracle identity ON NULL requires BY DEFAULT identity".to_string(),
        });
    }
    if identity.is_on_null() && dialect.kind() != DialectKind::Oracle {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "Oracle identity ON NULL".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    Ok(())
}

pub(crate) fn compile_create_index(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    index: &IndexDef,
) -> OrmdanticResult<String> {
    validate_advanced_index_metadata(dialect, index)?;
    let uniqueness = if index.is_unique() { "UNIQUE " } else { "" };
    let method = render_index_method(dialect, index.method_name())?;
    let mut key_parts = index
        .columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>();
    key_parts.extend(index.expressions_ref().iter().cloned());
    let columns = key_parts.join(", ");
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
    let storage =
        render_postgres_index_storage_parameters(dialect, index.postgres_with_ref(), true)?
            .map(|options| format!(" {options}"))
            .unwrap_or_default();
    let predicate = index
        .predicate()
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    if columns.is_empty() {
        return Err(OrmdanticError::SchemaDiffError {
            message: format!(
                "index '{}' must reference at least one column or SQL expression",
                index.name()
            ),
        });
    }
    let existence = match dialect.kind() {
        DialectKind::Postgres | DialectKind::Sqlite => " IF NOT EXISTS",
        _ => "",
    };
    Ok(format!(
        "CREATE {uniqueness}INDEX{existence} {} ON {}{method} ({columns}){include}{storage}{predicate}",
        dialect.quote_ident(index.name()),
        quote_qualified_name(dialect, table)
    ))
}

fn validate_advanced_index_metadata(
    dialect: &(impl Dialect + ?Sized),
    index: &IndexDef,
) -> OrmdanticResult<()> {
    validate_index_feature(
        dialect,
        !index.postgres_with_ref().is_empty(),
        matches!(dialect.kind(), DialectKind::Postgres),
        "PostgreSQL index storage parameters",
    )?;
    validate_index_feature(
        dialect,
        index.method_name().is_some(),
        matches!(dialect.kind(), DialectKind::Postgres),
        "index methods",
    )?;
    validate_index_feature(
        dialect,
        !index.expressions_ref().is_empty(),
        matches!(dialect.kind(), DialectKind::Postgres | DialectKind::Sqlite),
        "expression indexes",
    )?;
    validate_index_feature(
        dialect,
        !index.include_columns_ref().is_empty(),
        matches!(dialect.kind(), DialectKind::Postgres | DialectKind::MsSql),
        "index INCLUDE columns",
    )?;
    validate_index_feature(
        dialect,
        index.predicate().is_some(),
        matches!(
            dialect.kind(),
            DialectKind::Postgres | DialectKind::Sqlite | DialectKind::MsSql
        ),
        "filtered or partial indexes",
    )
}

fn validate_index_feature(
    dialect: &(impl Dialect + ?Sized),
    enabled: bool,
    supported: bool,
    feature: &str,
) -> OrmdanticResult<()> {
    if enabled && !supported {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: feature.to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    Ok(())
}

fn render_index_method(
    dialect: &(impl Dialect + ?Sized),
    method: Option<&str>,
) -> OrmdanticResult<String> {
    match method {
        Some(method) => {
            validate_index_method_name(method)?;
            if dialect.kind() != DialectKind::Postgres {
                return Err(OrmdanticError::UnsupportedFeature {
                    feature: "index methods".to_string(),
                    dialect: dialect.name().to_string(),
                });
            }
            Ok(format!(" USING {method}"))
        }
        None => Ok(String::new()),
    }
}

fn validate_index_method_name(method: &str) -> OrmdanticResult<()> {
    if method.is_empty()
        || !method
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$')
    {
        return Err(OrmdanticError::SchemaDiffError {
            message: format!("invalid index method '{method}'"),
        });
    }
    Ok(())
}

pub(crate) fn compile_add_column(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    column: &ColumnDef,
) -> OrmdanticResult<Vec<String>> {
    let mut statements = vec![match dialect.kind() {
        DialectKind::MsSql => format!(
            "ALTER TABLE {} ADD {}",
            quote_qualified_name(dialect, table),
            render_column_def(dialect, column)?
        ),
        DialectKind::Oracle => format!(
            "ALTER TABLE {} ADD ({})",
            quote_qualified_name(dialect, table),
            render_column_def(dialect, column)?
        ),
        _ => format!(
            "ALTER TABLE {} ADD COLUMN {}",
            quote_qualified_name(dialect, table),
            render_column_def(dialect, column)?
        ),
    }];
    if column.comment().is_some() {
        if let Some(comment_sql) = compile_create_column_comment(dialect, table, column)? {
            statements.push(comment_sql);
        }
    }
    Ok(statements)
}

fn render_mysql_column_modify_def(
    dialect: &(impl Dialect + ?Sized),
    column: &ColumnDef,
) -> OrmdanticResult<String> {
    let mut sql = format!(
        "{} {}",
        dialect.quote_ident(column.name()),
        dialect.render_column_type(column)
    );
    if !column.is_nullable() || column.is_primary_key() {
        sql.push_str(" NOT NULL");
    }
    if let Some(generated) = render_generated_value(dialect, column)? {
        sql.push_str(&generated);
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
    if let Some(comment) = column.comment() {
        sql.push_str(" COMMENT ");
        sql.push_str(&sql_string_literal(comment));
    } else {
        sql.push_str(" COMMENT ''");
    }
    Ok(sql)
}

pub(crate) fn compile_drop_column(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    column: &str,
) -> String {
    format!(
        "ALTER TABLE {} DROP COLUMN {}",
        quote_qualified_name(dialect, table),
        dialect.quote_ident(column)
    )
}

pub(crate) fn compile_alter_column(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    column: &ColumnDef,
) -> OrmdanticResult<String> {
    Ok(match dialect.kind() {
        DialectKind::MySql | DialectKind::MariaDb => format!(
            "ALTER TABLE {} MODIFY COLUMN {}",
            quote_qualified_name(dialect, table),
            render_mysql_column_modify_def(dialect, column)?
        ),
        DialectKind::MsSql => format!(
            "ALTER TABLE {} ALTER COLUMN {} {}{}",
            quote_qualified_name(dialect, table),
            dialect.quote_ident(column.name()),
            dialect.render_column_type(column),
            if column.is_nullable() && !column.is_primary_key() {
                " NULL"
            } else {
                " NOT NULL"
            }
        ),
        DialectKind::Oracle => format!(
            "ALTER TABLE {} MODIFY ({} {})",
            quote_qualified_name(dialect, table),
            dialect.quote_ident(column.name()),
            dialect.render_column_type(column)
        ),
        _ => format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
            quote_qualified_name(dialect, table),
            dialect.quote_ident(column.name()),
            dialect.render_column_type(column)
        ),
    })
}

pub(crate) fn compile_drop_index(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    name: &str,
) -> String {
    match dialect.kind() {
        DialectKind::MySql | DialectKind::MariaDb | DialectKind::MsSql => format!(
            "DROP INDEX {} ON {}",
            dialect.quote_ident(name),
            quote_qualified_name(dialect, table)
        ),
        DialectKind::Oracle => format!("DROP INDEX {}", dialect.quote_ident(name)),
        _ => format!("DROP INDEX IF EXISTS {}", dialect.quote_ident(name)),
    }
}

pub(crate) fn render_constraint(
    dialect: &(impl Dialect + ?Sized),
    constraint: &ConstraintDef,
) -> OrmdanticResult<String> {
    match constraint {
        ConstraintDef::Unique(constraint) => render_unique_constraint(dialect, constraint),
        ConstraintDef::Check(constraint) => {
            render_check_constraint(dialect, constraint, true, None)
        }
        ConstraintDef::ForeignKey(constraint) => render_foreign_key(dialect, constraint, true),
        ConstraintDef::Exclusion(constraint) => render_exclusion_constraint(dialect, constraint),
    }
}

pub(crate) fn compile_add_constraint(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    constraint: &ConstraintDef,
) -> OrmdanticResult<String> {
    let validation_prefix = render_add_constraint_validation_prefix(dialect, constraint);
    Ok(format!(
        "ALTER TABLE {}{validation_prefix} ADD {}",
        quote_qualified_name(dialect, table),
        render_constraint(dialect, constraint)?
    ))
}

fn should_inline_validated_constraint(dialect: &(impl Dialect + ?Sized), validated: bool) -> bool {
    validated || !matches!(dialect.kind(), DialectKind::Postgres | DialectKind::MsSql)
}

fn should_render_inline_constraint_validation(
    dialect: &(impl Dialect + ?Sized),
    validated: bool,
) -> bool {
    !validated && matches!(dialect.kind(), DialectKind::Oracle | DialectKind::MySql)
}

fn render_unique_constraint(
    dialect: &(impl Dialect + ?Sized),
    constraint: &UniqueConstraintDef,
) -> OrmdanticResult<String> {
    validate_unique_nulls_not_distinct(dialect, constraint.is_nulls_not_distinct())?;
    validate_constraint_timing(
        dialect,
        constraint.timing(),
        "deferrable unique constraints",
        matches!(dialect.kind(), DialectKind::Postgres | DialectKind::Oracle),
    )?;
    if constraint.mssql_clustered().is_some() && dialect.kind() != DialectKind::MsSql {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "SQL Server unique constraint clustering".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    if constraint.mssql_filegroup().is_some() && dialect.kind() != DialectKind::MsSql {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "SQL Server unique constraint filegroups".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    if constraint.oracle_tablespace().is_some() && dialect.kind() != DialectKind::Oracle {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "Oracle unique constraint tablespaces".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    if constraint.oracle_compress().is_some() && dialect.kind() != DialectKind::Oracle {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "Oracle unique constraint compression".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    let columns = constraint
        .columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let nulls = if constraint.is_nulls_not_distinct() {
        " NULLS NOT DISTINCT"
    } else {
        ""
    };
    let mssql_clustered = match constraint.mssql_clustered() {
        Some(true) => " CLUSTERED",
        Some(false) => " NONCLUSTERED",
        None => "",
    };
    let mut sql = format!(
        "CONSTRAINT {} UNIQUE{nulls}{mssql_clustered} ({columns})",
        dialect.quote_ident(constraint.name())
    );
    validate_sqlite_conflict_clause(
        dialect,
        constraint.sqlite_on_conflict(),
        "SQLite unique conflict clauses",
    )?;
    sql.push_str(&render_sqlite_conflict_clause(
        constraint.sqlite_on_conflict(),
    )?);
    if constraint.oracle_compress().is_some() || constraint.oracle_tablespace().is_some() {
        sql.push_str(" USING INDEX");
        if let Some(compress) = constraint.oracle_compress() {
            sql.push_str(&render_oracle_index_compression(compress));
        }
        if let Some(tablespace) = constraint.oracle_tablespace() {
            sql.push_str(" TABLESPACE ");
            sql.push_str(&dialect.quote_ident(tablespace));
        }
    }
    sql.push_str(&render_constraint_timing(constraint.timing()));
    if let Some(filegroup) = constraint.mssql_filegroup() {
        sql.push_str(" ON ");
        sql.push_str(&dialect.quote_ident(filegroup));
    }
    Ok(sql)
}

fn render_oracle_index_compression(compress: &OracleIndexCompression) -> String {
    match compress {
        OracleIndexCompression::Enabled => " COMPRESS".to_string(),
        OracleIndexCompression::Prefix(prefix_length) => format!(" COMPRESS {prefix_length}"),
    }
}

fn render_oracle_table_compression(compress: &OracleTableCompression) -> String {
    match compress {
        OracleTableCompression::Enabled => " COMPRESS".to_string(),
        OracleTableCompression::Level(level) => format!(" COMPRESS FOR {level}"),
    }
}

fn render_sqlite_conflict_clause(policy: Option<&str>) -> OrmdanticResult<String> {
    match policy {
        Some(policy) => Ok(format!(
            " ON CONFLICT {}",
            normalized_sqlite_conflict_policy(policy)?
        )),
        None => Ok(String::new()),
    }
}

fn validate_sqlite_conflict_clause(
    dialect: &(impl Dialect + ?Sized),
    policy: Option<&str>,
    feature: &str,
) -> OrmdanticResult<()> {
    let Some(policy) = policy else {
        return Ok(());
    };
    if dialect.kind() != DialectKind::Sqlite {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: feature.to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    normalized_sqlite_conflict_policy(policy).map(|_| ())
}

fn normalized_sqlite_conflict_policy(policy: &str) -> OrmdanticResult<String> {
    let normalized = policy.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "ROLLBACK" | "ABORT" | "FAIL" | "IGNORE" | "REPLACE" => Ok(normalized),
        _ => Err(OrmdanticError::SchemaDiffError {
            message: format!("invalid SQLite conflict policy '{policy}'"),
        }),
    }
}

fn validate_unique_nulls_not_distinct(
    dialect: &(impl Dialect + ?Sized),
    nulls_not_distinct: bool,
) -> OrmdanticResult<()> {
    if !nulls_not_distinct || dialect.kind() == DialectKind::Postgres {
        return Ok(());
    }
    Err(OrmdanticError::UnsupportedFeature {
        feature: "PostgreSQL unique NULLS NOT DISTINCT".to_string(),
        dialect: dialect.name().to_string(),
    })
}

fn render_check_constraint(
    dialect: &(impl Dialect + ?Sized),
    constraint: &CheckConstraintDef,
    include_validation: bool,
    columns: Option<&[ColumnDef]>,
) -> OrmdanticResult<String> {
    validate_check_constraint_validation(dialect, constraint.is_validated())?;
    validate_check_no_inherit(dialect, constraint.is_no_inherit())?;
    let expression = render_check_expression(dialect, constraint.expression(), columns)?;
    let mut sql = match constraint.name() {
        Some(name) => format!(
            "CONSTRAINT {} CHECK ({})",
            dialect.quote_ident(name),
            expression
        ),
        None => format!("CHECK ({expression})"),
    };
    if constraint.is_no_inherit() {
        sql.push_str(" NO INHERIT");
    }
    if include_validation {
        sql.push_str(&render_check_constraint_validation(
            dialect,
            constraint.is_validated(),
        )?);
    }
    Ok(sql)
}

fn render_check_expression(
    dialect: &(impl Dialect + ?Sized),
    expression: &str,
    columns: Option<&[ColumnDef]>,
) -> OrmdanticResult<String> {
    let expression = sqlite_decimal_check_expression(dialect, expression, columns);
    let expression = regex_check_expression(dialect, &expression)?;
    multiple_of_check_expression(dialect, &expression)
}

fn sqlite_decimal_check_expression(
    dialect: &(impl Dialect + ?Sized),
    expression: &str,
    columns: Option<&[ColumnDef]>,
) -> String {
    if dialect.kind() != DialectKind::Sqlite {
        return expression.to_string();
    }
    let Some(columns) = columns else {
        return expression.to_string();
    };
    for column in columns {
        if column.kind() != &FieldKind::Decimal {
            continue;
        }
        for operator in [">=", "<=", ">", "<"] {
            let prefix = format!("{} {operator} ", column.name());
            if let Some(value) = expression.strip_prefix(&prefix) {
                return format!(
                    "ormdantic_decimal_cmp({}, {}) {operator} 0",
                    column.name(),
                    sqlite_string_literal(value.trim())
                );
            }
        }
    }
    expression.to_string()
}

fn regex_check_expression(
    dialect: &(impl Dialect + ?Sized),
    expression: &str,
) -> OrmdanticResult<String> {
    let Some((field, pattern)) = regex_check_parts(expression) else {
        return Ok(expression.to_string());
    };
    match dialect.kind() {
        DialectKind::Sqlite => Ok(format!("ormdantic_regex_match({field}, {pattern}) = 1")),
        DialectKind::Postgres => Ok(format!("{field} ~ {pattern}")),
        DialectKind::MySql | DialectKind::MariaDb => Ok(format!("{field} REGEXP {pattern}")),
        DialectKind::Oracle => Ok(format!("REGEXP_LIKE({field}, {pattern})")),
        DialectKind::MsSql => Err(OrmdanticError::UnsupportedFeature {
            feature: "regular expression CHECK constraints".to_string(),
            dialect: dialect.name().to_string(),
        }),
    }
}

fn regex_check_parts(expression: &str) -> Option<(&str, &str)> {
    let inner = expression
        .strip_prefix("ormdantic_regex_match(")?
        .strip_suffix(')')?;
    let (field, pattern) = inner.split_once(',')?;
    let field = field.trim();
    let pattern = pattern.trim();
    if field.is_empty() || pattern.is_empty() {
        return None;
    }
    Some((field, pattern))
}

fn multiple_of_check_expression(
    dialect: &(impl Dialect + ?Sized),
    expression: &str,
) -> OrmdanticResult<String> {
    let Some((field, value)) = multiple_of_check_parts(expression) else {
        return Ok(expression.to_string());
    };
    match dialect.kind() {
        DialectKind::Sqlite => Ok(format!(
            "ormdantic_decimal_multiple_of({field}, {value}) = 1"
        )),
        DialectKind::MsSql => Ok(format!("{field} % {value} = 0")),
        _ => Ok(format!("MOD({field}, {value}) = 0")),
    }
}

fn multiple_of_check_parts(expression: &str) -> Option<(&str, &str)> {
    let inner = expression
        .strip_prefix("ormdantic_multiple_of(")?
        .strip_suffix(')')?;
    let (field, value) = inner.split_once(',')?;
    let field = field.trim();
    let value = value.trim();
    if field.is_empty() || value.is_empty() {
        return None;
    }
    Some((field, value))
}

fn sqlite_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn validate_check_no_inherit(
    dialect: &(impl Dialect + ?Sized),
    no_inherit: bool,
) -> OrmdanticResult<()> {
    if !no_inherit || dialect.kind() == DialectKind::Postgres {
        return Ok(());
    }
    Err(OrmdanticError::UnsupportedFeature {
        feature: "check constraint NO INHERIT".to_string(),
        dialect: dialect.name().to_string(),
    })
}

fn render_foreign_key(
    dialect: &(impl Dialect + ?Sized),
    foreign_key: &ForeignKeyDef,
    include_validation: bool,
) -> OrmdanticResult<String> {
    validate_foreign_key_constraint_validation(dialect, foreign_key.is_validated())?;
    validate_constraint_timing(
        dialect,
        foreign_key.timing(),
        "deferrable foreign keys",
        matches!(
            dialect.kind(),
            DialectKind::Postgres | DialectKind::Oracle | DialectKind::Sqlite
        ),
    )?;
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
        quote_qualified_name(dialect, foreign_key.remote_table())
    ));
    if let Some(match_type) = foreign_key.match_type() {
        sql.push_str(render_foreign_key_match(dialect, match_type)?);
    }
    if let Some(action) = foreign_key.on_delete_action() {
        sql.push_str(" ON DELETE ");
        sql.push_str(render_foreign_key_action(action));
    }
    if let Some(action) = foreign_key.on_update_action() {
        sql.push_str(" ON UPDATE ");
        sql.push_str(render_foreign_key_action(action));
    }
    sql.push_str(&render_constraint_timing(foreign_key.timing()));
    if include_validation {
        sql.push_str(&render_foreign_key_constraint_validation(
            dialect,
            foreign_key.is_validated(),
        )?);
    }
    Ok(sql)
}

fn render_exclusion_constraint(
    dialect: &(impl Dialect + ?Sized),
    constraint: &ExclusionConstraintDef,
) -> OrmdanticResult<String> {
    if dialect.kind() != DialectKind::Postgres {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "exclusion constraints".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    validate_constraint_timing(
        dialect,
        constraint.timing(),
        "deferrable exclusion constraints",
        true,
    )?;
    let elements = constraint
        .elements()
        .iter()
        .map(|element| {
            let expression = if element.is_quoted() {
                dialect.quote_ident(element.value())
            } else {
                element.value().to_string()
            };
            let expression = if let Some(opclass) = element.operator_class() {
                format!("{expression} {opclass}")
            } else {
                expression
            };
            format!("{expression} WITH {}", element.operator())
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "CONSTRAINT {} EXCLUDE USING {} ({elements})",
        dialect.quote_ident(constraint.name()),
        constraint.method_name()
    );
    if let Some(predicate) = constraint.predicate() {
        sql.push_str(" WHERE (");
        sql.push_str(predicate);
        sql.push(')');
    }
    sql.push_str(&render_constraint_timing(constraint.timing()));
    Ok(sql)
}

fn render_constraint_timing(timing: &ConstraintTiming) -> String {
    let mut sql = String::new();
    match timing.deferrable() {
        Some(true) => sql.push_str(" DEFERRABLE"),
        Some(false) => sql.push_str(" NOT DEFERRABLE"),
        None => {}
    }
    if timing.initially_deferred() {
        sql.push_str(" INITIALLY DEFERRED");
    }
    sql
}

fn validate_constraint_timing(
    dialect: &(impl Dialect + ?Sized),
    timing: &ConstraintTiming,
    feature: &str,
    supported: bool,
) -> OrmdanticResult<()> {
    if supported || (timing.deferrable().is_none() && !timing.initially_deferred()) {
        return Ok(());
    }
    Err(OrmdanticError::UnsupportedFeature {
        feature: feature.to_string(),
        dialect: dialect.name().to_string(),
    })
}

fn validate_check_constraint_validation(
    dialect: &(impl Dialect + ?Sized),
    validated: bool,
) -> OrmdanticResult<()> {
    if validated
        || matches!(
            dialect.kind(),
            DialectKind::Postgres | DialectKind::Oracle | DialectKind::MsSql | DialectKind::MySql
        )
    {
        return Ok(());
    }
    Err(OrmdanticError::UnsupportedFeature {
        feature: "constraint validation toggles".to_string(),
        dialect: dialect.name().to_string(),
    })
}

fn validate_foreign_key_constraint_validation(
    dialect: &(impl Dialect + ?Sized),
    validated: bool,
) -> OrmdanticResult<()> {
    if validated
        || matches!(
            dialect.kind(),
            DialectKind::Postgres | DialectKind::Oracle | DialectKind::MsSql
        )
    {
        return Ok(());
    }
    Err(OrmdanticError::UnsupportedFeature {
        feature: "constraint validation toggles".to_string(),
        dialect: dialect.name().to_string(),
    })
}

fn render_check_constraint_validation(
    dialect: &(impl Dialect + ?Sized),
    validated: bool,
) -> OrmdanticResult<String> {
    validate_check_constraint_validation(dialect, validated)?;
    if validated {
        Ok(String::new())
    } else if dialect.kind() == DialectKind::Oracle {
        Ok(" ENABLE NOVALIDATE".to_string())
    } else if dialect.kind() == DialectKind::MySql {
        Ok(" NOT ENFORCED".to_string())
    } else if dialect.kind() == DialectKind::MsSql {
        Ok(String::new())
    } else {
        Ok(" NOT VALID".to_string())
    }
}

fn render_foreign_key_constraint_validation(
    dialect: &(impl Dialect + ?Sized),
    validated: bool,
) -> OrmdanticResult<String> {
    validate_foreign_key_constraint_validation(dialect, validated)?;
    if validated {
        Ok(String::new())
    } else if dialect.kind() == DialectKind::Oracle {
        Ok(" ENABLE NOVALIDATE".to_string())
    } else if dialect.kind() == DialectKind::MsSql {
        Ok(String::new())
    } else {
        Ok(" NOT VALID".to_string())
    }
}

fn render_add_constraint_validation_prefix(
    dialect: &(impl Dialect + ?Sized),
    constraint: &ConstraintDef,
) -> &'static str {
    if dialect.kind() == DialectKind::MsSql && !constraint_is_validated(constraint) {
        " WITH NOCHECK"
    } else {
        ""
    }
}

fn constraint_is_validated(constraint: &ConstraintDef) -> bool {
    match constraint {
        ConstraintDef::Check(constraint) => constraint.is_validated(),
        ConstraintDef::ForeignKey(constraint) => constraint.is_validated(),
        ConstraintDef::Unique(_) | ConstraintDef::Exclusion(_) => true,
    }
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

fn render_foreign_key_match(
    dialect: &(impl Dialect + ?Sized),
    match_type: &ForeignKeyMatch,
) -> OrmdanticResult<&'static str> {
    if !matches!(dialect.kind(), DialectKind::Postgres | DialectKind::Sqlite) {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "foreign key match types".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    Ok(match match_type {
        ForeignKeyMatch::Simple => " MATCH SIMPLE",
        ForeignKeyMatch::Full => " MATCH FULL",
    })
}

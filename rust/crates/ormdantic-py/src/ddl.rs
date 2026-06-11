use crate::schema::{
    column_def_from_runtime, exclusion_constraint_def_from_runtime, foreign_key_def_from_runtime,
    index_def_from_runtime, unique_constraint_def_from_runtime, RuntimeColumn, RuntimeEnumType,
    RuntimeExclusionConstraint, RuntimeForeignKeyConstraint, RuntimeIndex, RuntimeTableCheck,
    RuntimeUniqueConstraint,
};
use ormdantic_dialects::{AnyDialect, Dialect, DialectKind};
use ormdantic_schema::{
    CheckConstraintDef, MysqlTableOptions, SchemaOperation, TableDef, UniqueConstraintDef,
};
use ormdantic_sql::DdlAst;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

// Mirrors the Python extension boundary; grouping these fields would change the PyO3 API.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
pub(crate) fn compile_create_table_sql(
    dialect: &str,
    table: &str,
    columns: Vec<RuntimeColumn>,
    indexes: Vec<RuntimeIndex>,
    unique_constraints: Vec<Vec<String>>,
    named_unique_constraints: Vec<RuntimeUniqueConstraint>,
    table_checks: Vec<RuntimeTableCheck>,
    foreign_key_constraints: Vec<RuntimeForeignKeyConstraint>,
    exclusion_constraints: Vec<RuntimeExclusionConstraint>,
    comment: Option<String>,
    tablespace: Option<String>,
    mysql_engine: Option<String>,
    mysql_charset: Option<String>,
    mysql_collation: Option<String>,
    mysql_row_format: Option<String>,
    postgres_inherits: Vec<String>,
    postgres_with: Vec<(String, String)>,
    postgres_using: Option<String>,
    postgres_partition_by: Option<String>,
    postgres_partition_of: Option<String>,
    postgres_partition_for: Option<String>,
    postgres_unlogged: bool,
    sqlite_strict: bool,
    sqlite_without_rowid: bool,
    schema: Option<String>,
    mssql_primary_key_nonclustered: bool,
    oracle_compress: Option<String>,
    mysql_key_block_size: Option<u32>,
    mysql_pack_keys: Option<bool>,
    mysql_checksum: Option<bool>,
    mysql_delay_key_write: Option<bool>,
    mysql_stats_persistent: Option<bool>,
    mysql_stats_auto_recalc: Option<bool>,
    mysql_stats_sample_pages: Option<u32>,
    mysql_avg_row_length: Option<u32>,
    mysql_max_rows: Option<u32>,
    mysql_min_rows: Option<u32>,
    mysql_insert_method: Option<String>,
    mysql_data_directory: Option<String>,
    mysql_index_directory: Option<String>,
    mysql_connection: Option<String>,
    mysql_union: Vec<String>,
    mysql_partition_by: Option<String>,
    mysql_partitions: Option<u32>,
    mysql_subpartition_by: Option<String>,
    mysql_subpartitions: Option<u32>,
    mysql_auto_increment: Option<u32>,
) -> PyResult<Vec<String>> {
    create_table_sql(
        dialect,
        table,
        columns,
        indexes,
        unique_constraints,
        named_unique_constraints,
        table_checks,
        foreign_key_constraints,
        exclusion_constraints,
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
    )
}

// Internal companion to the PyO3 boundary above; keeps call sites aligned with Python inputs.
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_table_sql(
    dialect: &str,
    table: &str,
    columns: Vec<RuntimeColumn>,
    indexes: Vec<RuntimeIndex>,
    unique_constraints: Vec<Vec<String>>,
    named_unique_constraints: Vec<RuntimeUniqueConstraint>,
    table_checks: Vec<RuntimeTableCheck>,
    foreign_key_constraints: Vec<RuntimeForeignKeyConstraint>,
    exclusion_constraints: Vec<RuntimeExclusionConstraint>,
    comment: Option<String>,
    tablespace: Option<String>,
    mysql_engine: Option<String>,
    mysql_charset: Option<String>,
    mysql_collation: Option<String>,
    mysql_row_format: Option<String>,
    postgres_inherits: Vec<String>,
    postgres_with: Vec<(String, String)>,
    postgres_using: Option<String>,
    postgres_partition_by: Option<String>,
    postgres_partition_of: Option<String>,
    postgres_partition_for: Option<String>,
    postgres_unlogged: bool,
    sqlite_strict: bool,
    sqlite_without_rowid: bool,
    schema: Option<String>,
    mssql_primary_key_nonclustered: bool,
    oracle_compress: Option<String>,
    mysql_key_block_size: Option<u32>,
    mysql_pack_keys: Option<bool>,
    mysql_checksum: Option<bool>,
    mysql_delay_key_write: Option<bool>,
    mysql_stats_persistent: Option<bool>,
    mysql_stats_auto_recalc: Option<bool>,
    mysql_stats_sample_pages: Option<u32>,
    mysql_avg_row_length: Option<u32>,
    mysql_max_rows: Option<u32>,
    mysql_min_rows: Option<u32>,
    mysql_insert_method: Option<String>,
    mysql_data_directory: Option<String>,
    mysql_index_directory: Option<String>,
    mysql_connection: Option<String>,
    mysql_union: Vec<String>,
    mysql_partition_by: Option<String>,
    mysql_partitions: Option<u32>,
    mysql_subpartition_by: Option<String>,
    mysql_subpartitions: Option<u32>,
    mysql_auto_increment: Option<u32>,
) -> PyResult<Vec<String>> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let mut foreign_keys = Vec::new();
    let mut check_constraints = Vec::new();
    let mut unique_column_constraints = Vec::new();
    let columns = columns
        .into_iter()
        .map(|column| {
            column_def_from_runtime(
                column,
                table,
                &mut unique_column_constraints,
                &mut foreign_keys,
                &mut check_constraints,
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    let indexes = indexes
        .into_iter()
        .map(index_def_from_runtime)
        .collect::<Vec<_>>();
    let named_unique_constraints = named_unique_constraints
        .into_iter()
        .map(unique_constraint_def_from_runtime)
        .collect::<PyResult<Vec<_>>>()?;
    let anonymous_unique_constraints = unique_constraints
        .into_iter()
        .map(|columns| (columns, None))
        .chain(unique_column_constraints)
        .enumerate()
        .map(|(idx, (columns, sqlite_on_conflict))| {
            UniqueConstraintDef::new(format!("{table}_unique_{idx}"), columns)
                .with_sqlite_on_conflict_option(sqlite_on_conflict)
        });
    let unique_constraints = named_unique_constraints
        .into_iter()
        .chain(anonymous_unique_constraints)
        .collect::<Vec<_>>();
    check_constraints.extend(table_checks.into_iter().map(
        |(name, expression, validated, no_inherit)| {
            let check = CheckConstraintDef::new(expression)
                .named(name)
                .validated(validated);
            if no_inherit {
                check.no_inherit()
            } else {
                check
            }
        },
    ));
    foreign_keys.extend(
        foreign_key_constraints
            .into_iter()
            .map(foreign_key_def_from_runtime)
            .collect::<PyResult<Vec<_>>>()?,
    );
    let exclusion_constraints = exclusion_constraints
        .into_iter()
        .map(exclusion_constraint_def_from_runtime)
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
    .with_foreign_keys(foreign_keys)
    .with_exclusion_constraints(exclusion_constraints);
    let table = if let Some(comment) = comment {
        if comment.is_empty() {
            table
        } else {
            table.with_comment(comment)
        }
    } else {
        table
    };
    let table = if let Some(tablespace) = tablespace {
        if tablespace.is_empty() {
            table
        } else {
            table.with_tablespace(tablespace)
        }
    } else {
        table
    };
    let mut table = table
        .with_mysql_options(MysqlTableOptions {
            engine: mysql_engine,
            charset: mysql_charset,
            collation: mysql_collation,
            row_format: mysql_row_format,
            key_block_size: mysql_key_block_size,
            pack_keys: mysql_pack_keys,
            checksum: mysql_checksum,
            delay_key_write: mysql_delay_key_write,
            stats_persistent: mysql_stats_persistent,
            stats_auto_recalc: mysql_stats_auto_recalc,
            stats_sample_pages: mysql_stats_sample_pages,
            avg_row_length: mysql_avg_row_length,
            max_rows: mysql_max_rows,
            min_rows: mysql_min_rows,
            insert_method: mysql_insert_method,
            data_directory: mysql_data_directory,
            index_directory: mysql_index_directory,
            connection: mysql_connection,
            union: mysql_union,
            partition_by: mysql_partition_by,
            partitions: mysql_partitions,
            subpartition_by: mysql_subpartition_by,
            subpartitions: mysql_subpartitions,
            auto_increment: mysql_auto_increment,
        })
        .with_postgres_inherits(postgres_inherits)
        .with_postgres_with(postgres_with)
        .with_postgres_using_option(postgres_using)
        .with_postgres_partition_by_option(postgres_partition_by)
        .with_postgres_partition_of_option(postgres_partition_of)
        .with_postgres_partition_for_option(postgres_partition_for)
        .with_postgres_unlogged(postgres_unlogged)
        .with_sqlite_strict(sqlite_strict)
        .with_sqlite_without_rowid(sqlite_without_rowid)
        .with_mssql_primary_key_nonclustered(mssql_primary_key_nonclustered)
        .with_oracle_compress_option(crate::schema::oracle_table_compression_from_runtime(
            oracle_compress,
        )?);
    if let Some(schema) = schema {
        if !schema.is_empty() {
            table = table.with_schema(schema);
        }
    }
    let ddl = DdlAst::new(vec![SchemaOperation::CreateTable(table)]);
    Ok(ddl
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?
        .into_iter()
        .map(|query| query.sql().to_string())
        .collect())
}

pub(crate) fn create_enum_type_sql(
    dialect: &str,
    enum_type: &RuntimeEnumType,
) -> PyResult<Option<String>> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    if dialect.kind() != DialectKind::Postgres {
        return Ok(None);
    }
    let (name, values, schema) = enum_type;
    let qualified_name = qualified_enum_type_name(&dialect, name, schema.as_deref());
    let values = values
        .iter()
        .map(|value| sql_literal(value))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(Some(format!(
        "DO $$ BEGIN CREATE TYPE {qualified_name} AS ENUM ({values}); EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    )))
}

pub(crate) fn drop_enum_type_sql(
    dialect: &str,
    enum_type: &RuntimeEnumType,
) -> PyResult<Option<String>> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    if dialect.kind() != DialectKind::Postgres {
        return Ok(None);
    }
    let (name, _values, schema) = enum_type;
    Ok(Some(format!(
        "DROP TYPE IF EXISTS {}",
        qualified_enum_type_name(&dialect, name, schema.as_deref())
    )))
}

fn qualified_enum_type_name(dialect: &AnyDialect, name: &str, schema: Option<&str>) -> String {
    match schema {
        Some(schema) => format!(
            "{}.{}",
            dialect.quote_ident(schema),
            dialect.quote_ident(name)
        ),
        None => dialect.quote_ident(name),
    }
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[pyfunction]
pub(crate) fn compile_drop_table_sql(dialect: &str, table: &str) -> PyResult<String> {
    drop_table_sql(dialect, table)
}

pub(crate) fn drop_table_sql(dialect: &str, table: &str) -> PyResult<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, kind: &str, primary_key: bool) -> RuntimeColumn {
        (
            name.to_string(),
            kind.to_string(),
            false,
            primary_key,
            None,
            None,
            None,
            false,
            Vec::new(),
            (
                None, None, false, false, None, None, None, None, None, None, None, None,
            ),
        )
    }

    #[test]
    fn create_table_sql_renders_table_indexes_and_constraints() {
        let mut code = column("code", "str", false);
        code.7 = true;
        code.8 = vec![("length".to_string(), ">=".to_string(), "2".to_string())];

        let sql = create_table_sql(
            "sqlite",
            "flavor",
            vec![
                column("id", "str", true),
                column("supplier_id", "str", false),
                code,
            ],
            vec![(
                "flavor_code_idx".to_string(),
                vec!["code".to_string()],
                false,
                None,
                Vec::new(),
                None,
                Vec::new(),
                Vec::new(),
            )],
            vec![vec!["id".to_string(), "code".to_string()]],
            vec![(
                "flavor_code_named_unique".to_string(),
                vec!["code".to_string()],
                None,
                false,
                false,
                None,
                None,
                None,
                None,
                None,
            )],
            vec![(
                "flavor_code_not_empty_check".to_string(),
                "LENGTH(code) > 0".to_string(),
                true,
                false,
            )],
            vec![(
                "flavor_supplier_pair_fk".to_string(),
                vec!["supplier_id".to_string(), "code".to_string()],
                "supplier".to_string(),
                vec!["id".to_string(), "code".to_string()],
                Some("cascade".to_string()),
                None,
                None,
                false,
                true,
                None,
            )],
            Vec::new(),
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("create table should compile");

        assert!(sql
            .iter()
            .any(|statement| statement.contains("CREATE TABLE")));
        assert!(sql
            .iter()
            .any(|statement| statement.contains("flavor_code_idx")));
        assert!(sql
            .iter()
            .any(|statement| statement.contains("CHECK (LENGTH(code) >= 2)")));
        assert!(sql.iter().any(|statement| statement
            .contains("CONSTRAINT \"flavor_code_named_unique\" UNIQUE (\"code\")")));
        assert!(sql.iter().any(|statement| statement
            .contains("CONSTRAINT \"flavor_code_not_empty_check\" CHECK (LENGTH(code) > 0)")));
        assert!(sql.iter().any(|statement| statement.contains(
            "CONSTRAINT \"flavor_supplier_pair_fk\" FOREIGN KEY (\"supplier_id\", \"code\")"
        )));
    }

    #[test]
    fn create_table_sql_rejects_unsupported_checks() {
        let mut code = column("code", "str", false);
        code.8 = vec![("regex".to_string(), "~".to_string(), "a".to_string())];

        assert!(create_table_sql(
            "sqlite",
            "flavor",
            vec![code],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
            None,
            None,
            None,
            None,
        )
        .is_err());
    }

    #[test]
    fn create_table_sql_can_render_mssql_nonclustered_primary_key() {
        let sql = create_table_sql(
            "mssql",
            "flavor",
            vec![column("id", "str", true)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("create table should compile");

        assert_eq!(
            sql[0],
            "CREATE TABLE IF NOT EXISTS [flavor] ([id] TEXT PRIMARY KEY NONCLUSTERED NOT NULL)"
        );
    }

    #[test]
    fn enum_type_sql_renders_idempotent_postgres_ddl() {
        let enum_type = (
            "flavor_kind".to_string(),
            vec!["mocha".to_string(), "chef's".to_string()],
            Some("public".to_string()),
        );

        assert_eq!(
            create_enum_type_sql("postgresql", &enum_type)
                .expect("enum type should compile")
                .as_deref(),
            Some(
                "DO $$ BEGIN CREATE TYPE \"public\".\"flavor_kind\" AS ENUM ('mocha', 'chef''s'); EXCEPTION WHEN duplicate_object THEN NULL; END $$"
            )
        );
        assert_eq!(
            drop_enum_type_sql("postgresql", &enum_type)
                .expect("enum type should compile")
                .as_deref(),
            Some("DROP TYPE IF EXISTS \"public\".\"flavor_kind\"")
        );
        assert!(create_enum_type_sql("sqlite", &enum_type)
            .expect("sqlite should ignore native enum types")
            .is_none());
    }

    #[test]
    fn drop_table_sql_renders_for_dialect() {
        assert_eq!(
            drop_table_sql("sqlite", "flavor").expect("drop table should compile"),
            "DROP TABLE IF EXISTS \"flavor\""
        );
    }
}

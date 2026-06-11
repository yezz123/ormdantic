use crate::query::compiled_queries_to_list;
use ormdantic_dialects::AnyDialect;
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, ComputedDef, ConstraintTiming, ExclusionConstraintDef,
    ExclusionElementDef, FieldKind, ForeignKeyAction, ForeignKeyDef, ForeignKeyMatch, IdentityDef,
    IndexDef, MysqlTableOptions, OracleIndexCompression, OracleTableCompression,
    RelationshipCardinality, RelationshipDef, SchemaDef, SchemaDiffer, SchemaRegistry,
    SchemaSnapshot, TableDef, UniqueConstraintDef,
};
use ormdantic_sql::DdlAst;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use std::collections::HashMap;

pub(crate) type RuntimeCheck = (String, String, String);
pub(crate) type RuntimeIdentityOptions = (
    bool,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    bool,
    Option<i64>,
    bool,
    bool,
    bool,
    bool,
);
pub(crate) type RuntimeConstraintTiming = (Option<bool>, bool);
pub(crate) type RuntimeSqliteColumnConflict = (Option<String>, Option<String>, Option<String>);
pub(crate) type RuntimeColumnTail = (
    Option<RuntimeConstraintTiming>,
    Option<String>,
    Option<RuntimeSqliteColumnConflict>,
);
pub(crate) type RuntimeColumnOptions = (
    Option<String>,
    Option<String>,
    bool,
    bool,
    Option<String>,
    Option<u32>,
    Option<u32>,
    Option<RuntimeIdentityOptions>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<RuntimeColumnTail>,
);
pub(crate) type RuntimeColumn = (
    String,
    String,
    bool,
    bool,
    Option<String>,
    Option<String>,
    Option<usize>,
    bool,
    Vec<RuntimeCheck>,
    RuntimeColumnOptions,
);
pub(crate) type RuntimeIndex = (
    String,
    Vec<String>,
    bool,
    Option<String>,
    Vec<String>,
    Option<String>,
    Vec<String>,
    Vec<(String, String)>,
);
pub(crate) type RuntimeTableCheck = (String, String, bool, bool);
pub(crate) type RuntimeUniqueConstraint = (
    String,
    Vec<String>,
    Option<bool>,
    bool,
    bool,
    Option<String>,
    Option<String>,
    Option<bool>,
    Option<String>,
    Option<String>,
);
pub(crate) type RuntimeExclusionElement = (String, String);
pub(crate) type RuntimeExclusionConstraint = (
    String,
    Vec<RuntimeExclusionElement>,
    Vec<RuntimeExclusionElement>,
    String,
    Option<String>,
    Option<bool>,
    bool,
    HashMap<String, String>,
);
pub(crate) type RuntimeForeignKeyConstraint = (
    String,
    Vec<String>,
    String,
    Vec<String>,
    Option<String>,
    Option<String>,
    Option<bool>,
    bool,
    bool,
    Option<String>,
);
pub(crate) type RuntimeEnumType = (String, Vec<String>, Option<String>);
pub(crate) type RuntimeRelationship = (String, String, String, Option<String>);
pub(crate) type RuntimeTableOptions = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Vec<String>,
    Vec<(String, String)>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    bool,
    bool,
    bool,
    Option<String>,
    bool,
    Option<String>,
    Option<u32>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Vec<String>,
    Option<String>,
    Option<u32>,
    Option<String>,
    Option<u32>,
    Option<u32>,
);
pub(crate) type RuntimeTableSpec = (
    String,
    String,
    String,
    Vec<RuntimeColumn>,
    Vec<RuntimeIndex>,
    Vec<Vec<String>>,
    Vec<RuntimeUniqueConstraint>,
    Vec<RuntimeTableCheck>,
    Vec<RuntimeForeignKeyConstraint>,
    Vec<RuntimeExclusionConstraint>,
    RuntimeTableOptions,
    Vec<RuntimeRelationship>,
);

// Mirrors the Python runtime table tuple ABI; keeping the flat signature avoids reshaping FFI data.
#[allow(clippy::too_many_arguments)]
pub(crate) fn runtime_table_def(
    model_key: String,
    tablename: String,
    primary_key: String,
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
    relationships: Vec<RuntimeRelationship>,
) -> PyResult<TableDef> {
    let mut foreign_keys = Vec::new();
    let mut check_constraints = Vec::new();
    let mut unique_column_constraints = Vec::new();
    let columns = columns
        .into_iter()
        .map(|column| {
            column_def_from_runtime(
                column,
                &tablename,
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
            UniqueConstraintDef::new(format!("{tablename}_unique_{idx}"), columns)
                .with_sqlite_on_conflict_option(sqlite_on_conflict)
        });
    let unique_constraints = named_unique_constraints
        .into_iter()
        .chain(anonymous_unique_constraints)
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
    let mut table = TableDef::from_parts(
        tablename,
        model_key,
        primary_key,
        columns,
        indexes,
        unique_constraints,
        relationships,
    )
    .with_check_constraints(check_constraints)
    .with_foreign_keys(foreign_keys)
    .with_exclusion_constraints(exclusion_constraints);
    if let Some(comment) = comment {
        if !comment.is_empty() {
            table = table.with_comment(comment);
        }
    }
    if let Some(tablespace) = tablespace {
        if !tablespace.is_empty() {
            table = table.with_tablespace(tablespace);
        }
    }
    table = table.with_mysql_options(MysqlTableOptions {
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
    });
    table = table.with_postgres_inherits(postgres_inherits);
    table = table.with_postgres_with(postgres_with);
    table = table.with_postgres_using_option(postgres_using);
    table = table.with_postgres_partition_by_option(postgres_partition_by);
    table = table.with_postgres_partition_of_option(postgres_partition_of);
    table = table.with_postgres_partition_for_option(postgres_partition_for);
    table = table.with_postgres_unlogged(postgres_unlogged);
    table = table.with_sqlite_strict(sqlite_strict);
    table = table.with_sqlite_without_rowid(sqlite_without_rowid);
    table = table.with_mssql_primary_key_nonclustered(mssql_primary_key_nonclustered);
    table =
        table.with_oracle_compress_option(oracle_table_compression_from_runtime(oracle_compress)?);
    if let Some(schema) = schema {
        if !schema.is_empty() {
            table = table.with_schema(schema);
        }
    }
    Ok(table)
}

pub(crate) fn unique_constraint_def_from_runtime(
    constraint: RuntimeUniqueConstraint,
) -> PyResult<UniqueConstraintDef> {
    let (
        name,
        columns,
        deferrable,
        initially_deferred,
        nulls_not_distinct,
        sqlite_on_conflict,
        mssql_filegroup,
        mssql_clustered,
        oracle_tablespace,
        oracle_compress,
    ) = constraint;
    Ok(UniqueConstraintDef::new(name, columns)
        .with_timing(ConstraintTiming::new(deferrable, initially_deferred))
        .with_nulls_not_distinct(nulls_not_distinct)
        .with_sqlite_on_conflict_option(sqlite_on_conflict)
        .with_mssql_filegroup_option(mssql_filegroup)
        .with_mssql_clustered_option(mssql_clustered)
        .with_oracle_tablespace_option(oracle_tablespace)
        .with_oracle_compress_option(oracle_index_compression_from_runtime(oracle_compress)?))
}

fn oracle_index_compression_from_runtime(
    value: Option<String>,
) -> PyResult<Option<OracleIndexCompression>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() || matches!(normalized.as_str(), "false" | "none" | "no" | "disabled")
    {
        return Ok(None);
    }
    if matches!(normalized.as_str(), "true" | "compress" | "enabled") {
        return Ok(Some(OracleIndexCompression::Enabled));
    }
    let prefix_length = normalized.parse::<u32>().map_err(|_| {
        PyValueError::new_err("Oracle index compression must be true or a positive integer")
    })?;
    if prefix_length == 0 {
        return Err(PyValueError::new_err(
            "Oracle index compression must be positive",
        ));
    }
    Ok(Some(OracleIndexCompression::Prefix(prefix_length)))
}

pub(crate) fn oracle_table_compression_from_runtime(
    value: Option<String>,
) -> PyResult<Option<OracleTableCompression>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() || matches!(normalized.as_str(), "false" | "none" | "no" | "disabled")
    {
        return Ok(None);
    }
    if matches!(normalized.as_str(), "true" | "compress" | "enabled") {
        return Ok(Some(OracleTableCompression::Enabled));
    }
    let level = normalized.parse::<u32>().map_err(|_| {
        PyValueError::new_err("Oracle table compression must be true or a positive integer")
    })?;
    if level == 0 {
        return Err(PyValueError::new_err(
            "Oracle table compression must be positive",
        ));
    }
    Ok(Some(OracleTableCompression::Level(level)))
}

pub(crate) fn column_def_from_runtime(
    column: RuntimeColumn,
    tablename: &str,
    unique_column_constraints: &mut Vec<(Vec<String>, Option<String>)>,
    foreign_keys: &mut Vec<ForeignKeyDef>,
    check_constraints: &mut Vec<CheckConstraintDef>,
) -> PyResult<ColumnDef> {
    let (
        name,
        kind,
        nullable,
        primary_key,
        foreign_table,
        foreign_column,
        max_length,
        unique,
        checks,
        options,
    ) = column;
    let (
        server_default,
        computed,
        computed_persisted,
        autoincrement,
        collation,
        numeric_precision,
        numeric_scale,
        identity,
        foreign_key_name,
        on_delete,
        on_update,
        tail,
    ) = options;
    let (timing, comment, sqlite_conflicts) = tail.unwrap_or((None, None, None));
    let (sqlite_on_conflict_primary_key, sqlite_on_conflict_not_null, sqlite_on_conflict_unique) =
        sqlite_conflicts.unwrap_or((None, None, None));
    if unique {
        unique_column_constraints.push((vec![name.clone()], sqlite_on_conflict_unique.clone()));
    }
    let has_foreign_key_options = foreign_key_name.is_some()
        || on_delete.is_some()
        || on_update.is_some()
        || timing.is_some();
    if let (Some(foreign_table), Some(foreign_column)) =
        (foreign_table.clone(), foreign_column.clone())
    {
        let mut foreign_key = ForeignKeyDef::new(
            vec![name.clone()],
            foreign_table.clone(),
            vec![foreign_column],
        )
        .named(foreign_key_name.unwrap_or_else(|| format!("{tablename}_{name}_foreign_key")));
        if let Some(action) = on_delete {
            foreign_key = foreign_key.on_delete(foreign_key_action_from_runtime(&action)?);
        }
        if let Some(action) = on_update {
            foreign_key = foreign_key.on_update(foreign_key_action_from_runtime(&action)?);
        }
        if let Some((deferrable, initially_deferred)) = timing {
            foreign_key =
                foreign_key.with_timing(ConstraintTiming::new(deferrable, initially_deferred));
        }
        foreign_keys.push(foreign_key);
    } else if has_foreign_key_options {
        return Err(PyValueError::new_err(format!(
            "foreign key options for column '{name}' require a foreign key relationship"
        )));
    }
    for check in checks {
        check_constraints.push(
            CheckConstraintDef::new(render_check_constraint(&name, &check)?).named(format!(
                "{tablename}_{name}_{}_check",
                check_constraint_suffix(&check)?
            )),
        );
    }
    let kind = foreign_table
        .map(|target_table| FieldKind::ForeignKey { target_table })
        .unwrap_or_else(|| field_kind_from_runtime(&kind));
    let mut column = ColumnDef::new(name, kind)
        .nullable(nullable)
        .primary_key(primary_key)
        .autoincrement(autoincrement);
    if let Some(max_length) = max_length {
        let max_length = u32::try_from(max_length)
            .map_err(|_| PyValueError::new_err("column max_length is too large for schema DDL"))?;
        column = column.with_max_length(max_length);
    }
    if let Some(server_default) = server_default {
        column = column.with_server_default(server_default);
    }
    if let Some(computed) = computed {
        column = column.with_computed(ComputedDef::new(computed).persisted(computed_persisted));
    }
    if let Some(collation) = collation {
        column = column.with_collation(collation);
    }
    if let (Some(precision), Some(scale)) = (numeric_precision, numeric_scale) {
        column = column.numeric(precision, scale);
    }
    if let Some(identity) = identity {
        column = column.with_identity(identity_def_from_runtime(identity));
    }
    if let Some(comment) = comment {
        if !comment.is_empty() {
            column = column.with_comment(comment);
        }
    }
    column = column
        .with_sqlite_on_conflict_primary_key_option(sqlite_on_conflict_primary_key)
        .with_sqlite_on_conflict_not_null_option(sqlite_on_conflict_not_null)
        .with_sqlite_on_conflict_unique_option(sqlite_on_conflict_unique);
    Ok(column)
}

pub(crate) fn foreign_key_def_from_runtime(
    constraint: RuntimeForeignKeyConstraint,
) -> PyResult<ForeignKeyDef> {
    let (
        name,
        columns,
        foreign_table,
        foreign_columns,
        on_delete,
        on_update,
        deferrable,
        initially_deferred,
        validated,
        match_type,
    ) = constraint;
    let mut foreign_key = ForeignKeyDef::new(columns, foreign_table, foreign_columns).named(name);
    if let Some(action) = on_delete {
        foreign_key = foreign_key.on_delete(foreign_key_action_from_runtime(&action)?);
    }
    if let Some(action) = on_update {
        foreign_key = foreign_key.on_update(foreign_key_action_from_runtime(&action)?);
    }
    if deferrable.is_some() || initially_deferred {
        foreign_key =
            foreign_key.with_timing(ConstraintTiming::new(deferrable, initially_deferred));
    }
    foreign_key = foreign_key.validated(validated);
    if let Some(match_type) = match_type {
        foreign_key = foreign_key.with_match(foreign_key_match_from_runtime(&match_type)?);
    }
    Ok(foreign_key)
}

pub(crate) fn exclusion_constraint_def_from_runtime(
    constraint: RuntimeExclusionConstraint,
) -> ExclusionConstraintDef {
    let (name, columns, expressions, method, where_expr, deferrable, initially_deferred, ops) =
        constraint;
    let mut elements = columns
        .into_iter()
        .map(|(column, operator)| {
            let element = ExclusionElementDef::column(column.clone(), operator);
            if let Some(opclass) = ops.get(&column) {
                element.opclass(opclass.clone())
            } else {
                element
            }
        })
        .collect::<Vec<_>>();
    elements.extend(expressions.into_iter().map(|(expression, operator)| {
        let element = ExclusionElementDef::expression(expression.clone(), operator);
        if let Some(opclass) = ops.get(&expression) {
            element.opclass(opclass.clone())
        } else {
            element
        }
    }));
    let mut exclusion = ExclusionConstraintDef::new(name, elements)
        .method(method)
        .with_timing(ConstraintTiming::new(deferrable, initially_deferred));
    if let Some(where_expr) = where_expr {
        exclusion = exclusion.where_expr(where_expr);
    }
    exclusion
}

pub(crate) fn identity_def_from_runtime(identity: RuntimeIdentityOptions) -> IdentityDef {
    let (
        always,
        start,
        increment,
        min_value,
        max_value,
        cycle,
        cache,
        order,
        on_null,
        no_min_value,
        no_max_value,
    ) = identity;
    let mut identity = IdentityDef::new().always(always);
    if let Some(start) = start {
        identity = identity.start(start);
    }
    if let Some(increment) = increment {
        identity = identity.increment(increment);
    }
    if let Some(min_value) = min_value {
        identity = identity.min_value(min_value);
    }
    if let Some(max_value) = max_value {
        identity = identity.max_value(max_value);
    }
    if no_min_value {
        identity = identity.no_min_value(true);
    }
    if no_max_value {
        identity = identity.no_max_value(true);
    }
    if cycle {
        identity = identity.cycle(true);
    }
    if let Some(cache) = cache {
        identity = identity.cache(cache);
    }
    if order {
        identity = identity.order(true);
    }
    if on_null {
        identity = identity.on_null(true);
    }
    identity
}

pub(crate) fn foreign_key_action_from_runtime(action: &str) -> PyResult<ForeignKeyAction> {
    match action {
        "cascade" => Ok(ForeignKeyAction::Cascade),
        "restrict" => Ok(ForeignKeyAction::Restrict),
        "set_null" => Ok(ForeignKeyAction::SetNull),
        "set_default" => Ok(ForeignKeyAction::SetDefault),
        "no_action" => Ok(ForeignKeyAction::NoAction),
        other => Err(PyValueError::new_err(format!(
            "unsupported foreign key action '{other}'"
        ))),
    }
}

pub(crate) fn foreign_key_match_from_runtime(match_type: &str) -> PyResult<ForeignKeyMatch> {
    match match_type {
        "simple" => Ok(ForeignKeyMatch::Simple),
        "full" => Ok(ForeignKeyMatch::Full),
        other => Err(PyValueError::new_err(format!(
            "unsupported foreign key match type '{other}'"
        ))),
    }
}

pub(crate) fn index_def_from_runtime(index: RuntimeIndex) -> IndexDef {
    let (name, columns, unique, where_expr, include_columns, method, expressions, postgres_with) =
        index;
    let mut index = IndexDef::new(name, columns)
        .unique(unique)
        .include_columns(include_columns)
        .expressions(expressions)
        .postgres_with(postgres_with);
    if let Some(where_expr) = where_expr {
        index = index.where_expr(where_expr);
    }
    if let Some(method) = method {
        index = index.method(method);
    }
    index
}

pub(crate) fn field_kind_from_runtime(kind: &str) -> FieldKind {
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
        "enum" => FieldKind::Enum {
            name: None,
            schema: None,
        },
        kind if kind.starts_with("enum:") => {
            let name = kind.trim_start_matches("enum:");
            match name.split_once('.') {
                Some((schema, name)) => FieldKind::Enum {
                    name: Some(name.to_string()),
                    schema: Some(schema.to_string()),
                },
                None => FieldKind::Enum {
                    name: Some(name.to_string()),
                    schema: None,
                },
            }
        }
        "decimal" => FieldKind::Decimal,
        "bytes" => FieldKind::Binary,
        _ => FieldKind::Unknown,
    }
}

pub(crate) fn schema_def_from_runtime(tables: Vec<RuntimeTableSpec>) -> PyResult<SchemaDef> {
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
                    named_unique_constraints,
                    table_checks,
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
                    runtime_table_def(
                        model_key,
                        tablename,
                        primary_key,
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
                        relationships,
                    )
                },
            )
            .collect::<PyResult<Vec<_>>>()?,
    ))
}

pub(crate) fn runtime_table_specs_from_py(
    tables: &Bound<'_, PyAny>,
) -> PyResult<Vec<RuntimeTableSpec>> {
    let py = tables.py();
    tables
        .extract::<Vec<Py<PyAny>>>()?
        .into_iter()
        .map(|table| runtime_table_spec_from_py(table.bind(py)))
        .collect()
}

fn runtime_table_spec_from_py(table: &Bound<'_, PyAny>) -> PyResult<RuntimeTableSpec> {
    let tuple = table.downcast::<PyTuple>()?;
    match tuple.len() {
        12 => {
            let model_key = tuple.get_item(0)?.extract::<String>()?;
            let tablename = tuple.get_item(1)?.extract::<String>()?;
            let primary_key = tuple.get_item(2)?.extract::<String>()?;
            let columns = tuple.get_item(3)?.extract::<Vec<RuntimeColumn>>()?;
            let indexes = tuple.get_item(4)?.extract::<Vec<RuntimeIndex>>()?;
            let unique_constraints = tuple.get_item(5)?.extract::<Vec<Vec<String>>>()?;
            let named_unique_constraints = tuple
                .get_item(6)?
                .extract::<Vec<RuntimeUniqueConstraint>>()?;
            let table_checks = tuple.get_item(7)?.extract::<Vec<RuntimeTableCheck>>()?;
            let foreign_key_constraints = tuple
                .get_item(8)?
                .extract::<Vec<RuntimeForeignKeyConstraint>>()?;
            let exclusion_constraints = tuple
                .get_item(9)?
                .extract::<Vec<RuntimeExclusionConstraint>>()?;
            let table_options = tuple.get_item(10)?;
            let relationships = tuple.get_item(11)?.extract::<Vec<RuntimeRelationship>>()?;
            if let Ok(table_options) = table_options.downcast::<PyTuple>() {
                Ok((
                    model_key,
                    tablename,
                    primary_key,
                    columns,
                    indexes,
                    unique_constraints,
                    named_unique_constraints,
                    table_checks,
                    foreign_key_constraints,
                    exclusion_constraints,
                    runtime_table_options_from_py(table_options)?,
                    relationships,
                ))
            } else {
                Ok((
                    model_key,
                    tablename,
                    primary_key,
                    columns,
                    indexes,
                    unique_constraints,
                    named_unique_constraints,
                    table_checks,
                    foreign_key_constraints,
                    exclusion_constraints,
                    runtime_table_options_with_defaults(table_options.extract::<Option<String>>()?),
                    relationships,
                ))
            }
        }
        11 => Ok((
            tuple.get_item(0)?.extract::<String>()?,
            tuple.get_item(1)?.extract::<String>()?,
            tuple.get_item(2)?.extract::<String>()?,
            tuple.get_item(3)?.extract::<Vec<RuntimeColumn>>()?,
            tuple.get_item(4)?.extract::<Vec<RuntimeIndex>>()?,
            tuple.get_item(5)?.extract::<Vec<Vec<String>>>()?,
            tuple
                .get_item(6)?
                .extract::<Vec<RuntimeUniqueConstraint>>()?,
            tuple.get_item(7)?.extract::<Vec<RuntimeTableCheck>>()?,
            tuple
                .get_item(8)?
                .extract::<Vec<RuntimeForeignKeyConstraint>>()?,
            tuple
                .get_item(9)?
                .extract::<Vec<RuntimeExclusionConstraint>>()?,
            runtime_table_options_with_defaults(None),
            tuple.get_item(10)?.extract::<Vec<RuntimeRelationship>>()?,
        )),
        10 => Ok((
            tuple.get_item(0)?.extract::<String>()?,
            tuple.get_item(1)?.extract::<String>()?,
            tuple.get_item(2)?.extract::<String>()?,
            tuple.get_item(3)?.extract::<Vec<RuntimeColumn>>()?,
            tuple.get_item(4)?.extract::<Vec<RuntimeIndex>>()?,
            tuple.get_item(5)?.extract::<Vec<Vec<String>>>()?,
            tuple
                .get_item(6)?
                .extract::<Vec<RuntimeUniqueConstraint>>()?,
            tuple.get_item(7)?.extract::<Vec<RuntimeTableCheck>>()?,
            tuple
                .get_item(8)?
                .extract::<Vec<RuntimeForeignKeyConstraint>>()?,
            Vec::new(),
            runtime_table_options_with_defaults(None),
            tuple.get_item(9)?.extract::<Vec<RuntimeRelationship>>()?,
        )),
        9 => Ok((
            tuple.get_item(0)?.extract::<String>()?,
            tuple.get_item(1)?.extract::<String>()?,
            tuple.get_item(2)?.extract::<String>()?,
            tuple.get_item(3)?.extract::<Vec<RuntimeColumn>>()?,
            tuple.get_item(4)?.extract::<Vec<RuntimeIndex>>()?,
            tuple.get_item(5)?.extract::<Vec<Vec<String>>>()?,
            tuple
                .get_item(6)?
                .extract::<Vec<RuntimeUniqueConstraint>>()?,
            tuple.get_item(7)?.extract::<Vec<RuntimeTableCheck>>()?,
            Vec::new(),
            Vec::new(),
            runtime_table_options_with_defaults(None),
            tuple.get_item(8)?.extract::<Vec<RuntimeRelationship>>()?,
        )),
        len => Err(PyValueError::new_err(format!(
            "runtime table spec must have 9, 10, 11, or 12 items, got {len}"
        ))),
    }
}

fn runtime_table_options_with_defaults(comment: Option<String>) -> RuntimeTableOptions {
    (
        comment,
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
}

fn runtime_table_options_from_py(options: &Bound<'_, PyTuple>) -> PyResult<RuntimeTableOptions> {
    let len = options.len();
    if len == 11 {
        return Ok((
            options.get_item(0)?.extract::<Option<String>>()?,
            options.get_item(1)?.extract::<Option<String>>()?,
            options.get_item(2)?.extract::<Option<String>>()?,
            options.get_item(3)?.extract::<Option<String>>()?,
            options.get_item(4)?.extract::<Option<String>>()?,
            None,
            options.get_item(5)?.extract::<Vec<String>>()?,
            options.get_item(6)?.extract::<Vec<(String, String)>>()?,
            options.get_item(7)?.extract::<Option<String>>()?,
            options.get_item(8)?.extract::<Option<String>>()?,
            options.get_item(9)?.extract::<Option<String>>()?,
            options.get_item(10)?.extract::<Option<String>>()?,
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
        ));
    }
    if !(12..=38).contains(&len) {
        return Err(PyValueError::new_err(format!(
            "runtime table options must have 11 through 38 items, got {len}"
        )));
    }
    Ok((
        optional_string_item(options, 0)?,
        optional_string_item(options, 1)?,
        optional_string_item(options, 2)?,
        optional_string_item(options, 3)?,
        optional_string_item(options, 4)?,
        optional_string_item(options, 5)?,
        vec_string_item(options, 6)?,
        vec_storage_item(options, 7)?,
        optional_string_item(options, 8)?,
        optional_string_item(options, 9)?,
        optional_string_item(options, 10)?,
        optional_string_item(options, 11)?,
        bool_item_or(options, 12, false)?,
        bool_item_or(options, 13, false)?,
        bool_item_or(options, 14, false)?,
        optional_string_item(options, 15)?,
        bool_item_or(options, 16, false)?,
        optional_string_item(options, 17)?,
        optional_u32_item(options, 18)?,
        optional_bool_item(options, 19)?,
        optional_bool_item(options, 20)?,
        optional_bool_item(options, 21)?,
        optional_bool_item(options, 22)?,
        optional_bool_item(options, 23)?,
        optional_u32_item(options, 24)?,
        optional_u32_item(options, 25)?,
        optional_u32_item(options, 26)?,
        optional_u32_item(options, 27)?,
        optional_string_item(options, 28)?,
        optional_string_item(options, 29)?,
        optional_string_item(options, 30)?,
        optional_string_item(options, 31)?,
        vec_string_item(options, 32)?,
        optional_string_item(options, 33)?,
        optional_u32_item(options, 34)?,
        optional_string_item(options, 35)?,
        optional_u32_item(options, 36)?,
        optional_u32_item(options, 37)?,
    ))
}

fn optional_string_item(options: &Bound<'_, PyTuple>, index: usize) -> PyResult<Option<String>> {
    if options.len() > index {
        options.get_item(index)?.extract::<Option<String>>()
    } else {
        Ok(None)
    }
}

fn optional_bool_item(options: &Bound<'_, PyTuple>, index: usize) -> PyResult<Option<bool>> {
    if options.len() > index {
        options.get_item(index)?.extract::<Option<bool>>()
    } else {
        Ok(None)
    }
}

fn optional_u32_item(options: &Bound<'_, PyTuple>, index: usize) -> PyResult<Option<u32>> {
    if options.len() > index {
        options.get_item(index)?.extract::<Option<u32>>()
    } else {
        Ok(None)
    }
}

fn bool_item_or(options: &Bound<'_, PyTuple>, index: usize, default: bool) -> PyResult<bool> {
    if options.len() > index {
        options.get_item(index)?.extract::<bool>()
    } else {
        Ok(default)
    }
}

fn vec_string_item(options: &Bound<'_, PyTuple>, index: usize) -> PyResult<Vec<String>> {
    if options.len() > index {
        options.get_item(index)?.extract::<Vec<String>>()
    } else {
        Ok(Vec::new())
    }
}

fn vec_storage_item(options: &Bound<'_, PyTuple>, index: usize) -> PyResult<Vec<(String, String)>> {
    if options.len() > index {
        options.get_item(index)?.extract::<Vec<(String, String)>>()
    } else {
        Ok(Vec::new())
    }
}

pub(crate) fn render_check_constraint(field: &str, check: &RuntimeCheck) -> PyResult<String> {
    let (kind, operator, value) = check;
    match kind.as_str() {
        "comparison" => Ok(format!("{field} {operator} {value}")),
        "length" => Ok(format!("LENGTH({field}) {operator} {value}")),
        "enum" if operator == "in" => Ok(format!("{field} IN ({value})")),
        "pattern" if operator == "matches" => {
            Ok(format!("ormdantic_regex_match({field}, {value})"))
        }
        "multiple_of" if operator == "=" => Ok(format!("ormdantic_multiple_of({field}, {value})")),
        other => Err(PyValueError::new_err(format!(
            "unsupported check constraint kind '{other}'"
        ))),
    }
}

pub(crate) fn check_constraint_suffix(check: &RuntimeCheck) -> PyResult<&'static str> {
    let (kind, operator, _) = check;
    match (kind.as_str(), operator.as_str()) {
        ("comparison", ">=") => Ok("ge"),
        ("comparison", ">") => Ok("gt"),
        ("comparison", "<=") => Ok("le"),
        ("comparison", "<") => Ok("lt"),
        ("length", ">=") => Ok("min_length"),
        ("length", "<=") => Ok("max_length"),
        ("enum", "in") => Ok("enum_values"),
        ("pattern", "matches") => Ok("pattern"),
        ("multiple_of", "=") => Ok("multiple_of"),
        _ => Err(PyValueError::new_err(format!(
            "unsupported check constraint operator '{operator}' for kind '{kind}'"
        ))),
    }
}

#[pyfunction]
pub(crate) fn validate_schema_tables(tables: &Bound<'_, PyAny>) -> PyResult<usize> {
    let mut registry = SchemaRegistry::new();
    if let Ok(tables) = runtime_table_specs_from_py(tables) {
        for (
            model_key,
            tablename,
            primary_key,
            columns,
            indexes,
            unique_constraints,
            named_unique_constraints,
            table_checks,
            foreign_key_constraints,
            exclusion_constraints,
            table_options,
            relationships,
        ) in tables
        {
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
            registry
                .register_table(runtime_table_def(
                    model_key,
                    tablename,
                    primary_key,
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

#[pyfunction]
pub(crate) fn compile_schema_diff(
    py: Python<'_>,
    dialect: &str,
    from_schema: &Bound<'_, PyAny>,
    to_schema: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let from = SchemaSnapshot::new(schema_def_from_runtime(runtime_table_specs_from_py(
        from_schema,
    )?)?);
    let to = SchemaSnapshot::new(schema_def_from_runtime(runtime_table_specs_from_py(
        to_schema,
    )?)?);
    let diff =
        SchemaDiffer::diff(&from, &to).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = DdlAst::from_diff(diff)
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    compiled_queries_to_list(py, compiled)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_column(name: &str, kind: &str, nullable: bool, primary_key: bool) -> RuntimeColumn {
        (
            name.to_string(),
            kind.to_string(),
            nullable,
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
    fn runtime_table_def_maps_runtime_metadata() {
        let table = runtime_table_def(
            "Flavor".to_string(),
            "flavor".to_string(),
            "id".to_string(),
            vec![
                runtime_column("id", "int", false, true),
                (
                    "supplier_id".to_string(),
                    "int".to_string(),
                    true,
                    false,
                    Some("supplier".to_string()),
                    Some("id".to_string()),
                    None,
                    false,
                    vec![("comparison".to_string(), ">=".to_string(), "0".to_string())],
                    (
                        None,
                        None,
                        false,
                        false,
                        None,
                        None,
                        None,
                        None,
                        Some("flavor_supplier_fk".to_string()),
                        Some("set_null".to_string()),
                        Some("cascade".to_string()),
                        Some((Some((Some(true), true)), None, None)),
                    ),
                ),
                (
                    "code".to_string(),
                    "str".to_string(),
                    false,
                    false,
                    None,
                    None,
                    None,
                    true,
                    Vec::new(),
                    (
                        Some("'new'".to_string()),
                        Some("LOWER(code)".to_string()),
                        true,
                        false,
                        Some("NOCASE".to_string()),
                        None,
                        None,
                        Some((
                            true,
                            Some(10),
                            Some(5),
                            None,
                            None,
                            false,
                            None,
                            false,
                            false,
                            false,
                            false,
                        )),
                        None,
                        None,
                        None,
                        None,
                    ),
                ),
            ],
            vec![(
                "flavor_code_idx".to_string(),
                vec!["code".to_string()],
                true,
                Some("code IS NOT NULL".to_string()),
                vec!["supplier_id".to_string()],
                Some("btree".to_string()),
                vec!["LOWER(code)".to_string()],
                vec![("fillfactor".to_string(), "70".to_string())],
            )],
            vec![vec!["code".to_string(), "supplier_id".to_string()]],
            vec![(
                "flavor_code_named_unique".to_string(),
                vec!["code".to_string()],
                Some(true),
                true,
                true,
                None,
                Some("constraintspace".to_string()),
                Some(false),
                Some("oraclespace".to_string()),
                Some("2".to_string()),
            )],
            vec![(
                "flavor_rating_positive_check".to_string(),
                "supplier_id >= 0".to_string(),
                true,
                true,
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
                Some("full".to_string()),
            )],
            vec![(
                "flavor_code_exclusion".to_string(),
                vec![("code".to_string(), "=".to_string())],
                vec![("LOWER(code)".to_string(), "=".to_string())],
                "gist".to_string(),
                Some("code IS NOT NULL".to_string()),
                Some(true),
                false,
                HashMap::from([("code".to_string(), "gist_text_ops".to_string())]),
            )],
            Some("Flavor table".to_string()),
            Some("fastspace".to_string()),
            Some("InnoDB".to_string()),
            Some("utf8mb4".to_string()),
            Some("utf8mb4_unicode_ci".to_string()),
            Some("DYNAMIC".to_string()),
            vec!["base_flavor".to_string()],
            vec![("fillfactor".to_string(), "70".to_string())],
            Some("heap".to_string()),
            Some("RANGE (id)".to_string()),
            Some("base_partitioned_flavor".to_string()),
            Some("FOR VALUES FROM (0) TO (100)".to_string()),
            true,
            true,
            true,
            Some("inventory".to_string()),
            true,
            Some("6".to_string()),
            Some(8),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(32),
            Some(64),
            Some(1000),
            Some(10),
            Some("LAST".to_string()),
            Some("/var/lib/mysql/data".to_string()),
            Some("/var/lib/mysql/index".to_string()),
            Some("mysql://remote.example/db/flavor".to_string()),
            vec!["flavor_hot".to_string(), "flavor_cold".to_string()],
            Some("HASH (id)".to_string()),
            Some(4),
            Some("KEY (id)".to_string()),
            Some(2),
            Some(101),
            vec![(
                "supplier".to_string(),
                "supplier".to_string(),
                "id".to_string(),
                None,
            )],
        )
        .expect("runtime table should convert");

        assert_eq!(table.name(), "flavor");
        assert_eq!(table.model_key(), "Flavor");
        assert_eq!(table.primary_key(), "id");
        assert_eq!(table.comment(), Some("Flavor table"));
        assert_eq!(table.tablespace(), Some("fastspace"));
        assert_eq!(table.mysql_engine(), Some("InnoDB"));
        assert_eq!(table.mysql_charset(), Some("utf8mb4"));
        assert_eq!(table.mysql_collation(), Some("utf8mb4_unicode_ci"));
        assert_eq!(table.mysql_row_format(), Some("DYNAMIC"));
        assert_eq!(table.mysql_key_block_size(), Some(8));
        assert_eq!(table.mysql_pack_keys(), Some(true));
        assert_eq!(table.mysql_checksum(), Some(true));
        assert_eq!(table.mysql_delay_key_write(), Some(true));
        assert_eq!(table.mysql_stats_persistent(), Some(true));
        assert_eq!(table.mysql_stats_auto_recalc(), Some(false));
        assert_eq!(table.mysql_stats_sample_pages(), Some(32));
        assert_eq!(table.mysql_avg_row_length(), Some(64));
        assert_eq!(table.mysql_max_rows(), Some(1000));
        assert_eq!(table.mysql_min_rows(), Some(10));
        assert_eq!(table.mysql_insert_method(), Some("LAST"));
        assert_eq!(table.mysql_data_directory(), Some("/var/lib/mysql/data"));
        assert_eq!(table.mysql_index_directory(), Some("/var/lib/mysql/index"));
        assert_eq!(
            table.mysql_connection(),
            Some("mysql://remote.example/db/flavor")
        );
        assert_eq!(
            table.mysql_union(),
            &["flavor_hot".to_string(), "flavor_cold".to_string()]
        );
        assert_eq!(table.mysql_partition_by(), Some("HASH (id)"));
        assert_eq!(table.mysql_partitions(), Some(4));
        assert_eq!(table.mysql_subpartition_by(), Some("KEY (id)"));
        assert_eq!(table.mysql_subpartitions(), Some(2));
        assert_eq!(table.mysql_auto_increment(), Some(101));
        assert_eq!(table.postgres_inherits(), &["base_flavor".to_string()]);
        assert_eq!(
            table.postgres_with(),
            &[("fillfactor".to_string(), "70".to_string())]
        );
        assert_eq!(table.postgres_using(), Some("heap"));
        assert_eq!(table.postgres_partition_by(), Some("RANGE (id)"));
        assert_eq!(
            table.postgres_partition_of(),
            Some("base_partitioned_flavor")
        );
        assert_eq!(
            table.postgres_partition_for(),
            Some("FOR VALUES FROM (0) TO (100)")
        );
        assert!(table.is_postgres_unlogged());
        assert!(table.is_sqlite_strict());
        assert!(table.is_sqlite_without_rowid());
        assert!(table.is_mssql_primary_key_nonclustered());
        assert_eq!(
            table.oracle_compress(),
            Some(&OracleTableCompression::Level(6))
        );
        assert_eq!(table.schema(), Some("inventory"));
        assert_eq!(table.columns().len(), 3);
        assert_eq!(table.indexes().len(), 1);
        assert_eq!(table.indexes()[0].predicate(), Some("code IS NOT NULL"));
        assert_eq!(
            table.indexes()[0].include_columns_ref(),
            &["supplier_id".to_string()]
        );
        assert_eq!(table.indexes()[0].method_name(), Some("btree"));
        assert_eq!(
            table.indexes()[0].postgres_with_ref(),
            &[("fillfactor".to_string(), "70".to_string())]
        );
        assert_eq!(
            table.indexes()[0].expressions_ref(),
            &["LOWER(code)".to_string()]
        );
        assert_eq!(table.columns()[2].server_default(), Some("'new'"));
        assert_eq!(
            table.columns()[2]
                .computed()
                .map(|computed| (computed.expression(), computed.is_persisted())),
            Some(("LOWER(code)", true))
        );
        assert_eq!(table.columns()[2].collation(), Some("NOCASE"));
        assert_eq!(
            table.columns()[2].identity().map(|identity| {
                (
                    identity.is_always(),
                    identity.start_value(),
                    identity.increment_value(),
                )
            }),
            Some((true, Some(10), Some(5)))
        );
        assert_eq!(table.unique_constraints().len(), 3);
        assert!(table.unique_constraints().iter().any(|constraint| {
            constraint.name() == "flavor_code_named_unique"
                && constraint.columns() == ["code".to_string()]
                && constraint.timing().deferrable() == Some(true)
                && constraint.timing().initially_deferred()
                && constraint.mssql_filegroup() == Some("constraintspace")
                && constraint.mssql_clustered() == Some(false)
                && constraint.oracle_tablespace() == Some("oraclespace")
                && constraint.oracle_compress() == Some(&OracleIndexCompression::Prefix(2))
        }));
        assert_eq!(table.check_constraints().len(), 2);
        assert!(table.check_constraints().iter().any(|constraint| {
            constraint.name() == Some("flavor_rating_positive_check")
                && constraint.expression() == "supplier_id >= 0"
                && constraint.is_no_inherit()
        }));
        assert_eq!(table.foreign_keys().len(), 2);
        assert_eq!(table.foreign_keys()[0].name(), Some("flavor_supplier_fk"));
        assert_eq!(
            table.foreign_keys()[1].local_columns(),
            &["supplier_id".to_string(), "code".to_string()]
        );
        assert!(matches!(
            table.foreign_keys()[0].on_delete_action(),
            Some(ForeignKeyAction::SetNull)
        ));
        assert!(matches!(
            table.foreign_keys()[0].on_update_action(),
            Some(ForeignKeyAction::Cascade)
        ));
        assert_eq!(table.foreign_keys()[0].timing().deferrable(), Some(true));
        assert!(table.foreign_keys()[0].timing().initially_deferred());
        assert!(matches!(
            table.foreign_keys()[1].match_type(),
            Some(ForeignKeyMatch::Full)
        ));
        assert_eq!(table.exclusion_constraints().len(), 1);
        assert_eq!(
            table.exclusion_constraints()[0].name(),
            "flavor_code_exclusion"
        );
        assert_eq!(
            table.exclusion_constraints()[0].predicate(),
            Some("code IS NOT NULL")
        );
        assert_eq!(
            table.exclusion_constraints()[0].elements()[0].operator_class(),
            Some("gist_text_ops")
        );
        assert_eq!(table.relationships().len(), 1);
    }

    #[test]
    fn check_constraint_helpers_validate_supported_shapes() {
        let length_check = ("length".to_string(), ">=".to_string(), "2".to_string());
        assert_eq!(
            render_check_constraint("name", &length_check).unwrap(),
            "LENGTH(name) >= 2"
        );
        assert_eq!(
            check_constraint_suffix(&length_check).unwrap(),
            "min_length"
        );

        let pattern_check = (
            "pattern".to_string(),
            "matches".to_string(),
            "'^[A-Z]+$'".to_string(),
        );
        assert_eq!(
            render_check_constraint("code", &pattern_check).unwrap(),
            "ormdantic_regex_match(code, '^[A-Z]+$')"
        );
        assert_eq!(check_constraint_suffix(&pattern_check).unwrap(), "pattern");

        let multiple_of_check = ("multiple_of".to_string(), "=".to_string(), "5".to_string());
        assert_eq!(
            render_check_constraint("quantity", &multiple_of_check).unwrap(),
            "ormdantic_multiple_of(quantity, 5)"
        );
        assert_eq!(
            check_constraint_suffix(&multiple_of_check).unwrap(),
            "multiple_of"
        );

        let unsupported = ("jsonpath".to_string(), "@@".to_string(), "$.a".to_string());
        assert!(render_check_constraint("name", &unsupported).is_err());
        assert!(check_constraint_suffix(&unsupported).is_err());
    }

    #[test]
    fn field_kind_from_runtime_preserves_native_enum_type_names() {
        assert_eq!(
            field_kind_from_runtime("enum:ddl_flavor"),
            FieldKind::Enum {
                name: Some("ddl_flavor".to_string()),
                schema: None
            }
        );
        assert_eq!(
            field_kind_from_runtime("enum:inventory.ddl_flavor"),
            FieldKind::Enum {
                name: Some("ddl_flavor".to_string()),
                schema: Some("inventory".to_string())
            }
        );
        assert_eq!(
            field_kind_from_runtime("enum"),
            FieldKind::Enum {
                name: None,
                schema: None
            }
        );
    }
}

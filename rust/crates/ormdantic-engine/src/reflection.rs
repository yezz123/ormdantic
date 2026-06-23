use ormdantic_core::OrmdanticResult;
use ormdantic_dialects::{
    AnyDialect, Dialect, ReflectionQuery, ReflectionQueryKind, ReflectionScope,
};
use ormdantic_schema::{ColumnDef, FieldKind, ReflectedSchema, ReflectedTable, SchemaDef};
use std::collections::BTreeMap;

use crate::{DbValue, NativeConnection, QueryResult};

#[derive(Debug, Clone)]
pub struct Reflector {
    dialect: AnyDialect,
}

impl Reflector {
    pub fn new(dialect: AnyDialect) -> Self {
        Self { dialect }
    }

    pub fn for_url(url: &str) -> OrmdanticResult<Self> {
        Ok(Self {
            dialect: AnyDialect::parse(url)?,
        })
    }

    pub fn reflection_queries(&self, scope: &ReflectionScope) -> Vec<ReflectionQuery> {
        self.dialect.reflection_queries(scope)
    }

    pub fn empty_schema(&self) -> SchemaDef {
        ReflectedSchema::new().into_schema_def()
    }
}

pub struct Inspector<'a> {
    connection: &'a mut NativeConnection,
}

impl<'a> Inspector<'a> {
    pub fn new(connection: &'a mut NativeConnection) -> Self {
        Self { connection }
    }

    pub fn reflection_queries(
        &self,
        scope: &ReflectionScope,
    ) -> OrmdanticResult<Vec<ReflectionQuery>> {
        Ok(AnyDialect::parse(self.connection.dialect())?.reflection_queries(scope))
    }

    pub fn inspect(&mut self, scope: &ReflectionScope) -> OrmdanticResult<ReflectedSchema> {
        let mut builder = ReflectedSchemaBuilder::default();
        for query in self.reflection_queries(scope)? {
            let result = self.connection.execute(query.sql(), &[])?;
            builder.apply(query.kind(), &result);
        }
        Ok(builder.into_schema())
    }
}

#[derive(Debug, Default)]
struct ReflectedSchemaBuilder {
    tables: BTreeMap<(Option<String>, String), ReflectedTableBuilder>,
}

impl ReflectedSchemaBuilder {
    fn apply(&mut self, kind: ReflectionQueryKind, result: &QueryResult) {
        match kind {
            ReflectionQueryKind::Tables => self.apply_tables(result),
            ReflectionQueryKind::Columns => self.apply_columns(result),
            ReflectionQueryKind::Constraints
            | ReflectionQueryKind::Indexes
            | ReflectionQueryKind::ForeignKeys => {}
        }
    }

    fn apply_tables(&mut self, result: &QueryResult) {
        for row in result.rows() {
            let Some(table_name) =
                row_string(result, row, "table_name").or_else(|| first_string(row))
            else {
                continue;
            };
            let schema = row_string(result, row, "table_schema");
            self.table_builder(schema, table_name);
        }
    }

    fn apply_columns(&mut self, result: &QueryResult) {
        for row in result.rows() {
            let Some(table_name) = row_string(result, row, "table_name") else {
                continue;
            };
            let Some(column_name) = row_string(result, row, "column_name") else {
                continue;
            };
            let schema = row_string(result, row, "table_schema");
            let kind = row_string(result, row, "data_type")
                .map(|value| reflected_field_kind(&value))
                .unwrap_or(FieldKind::Unknown);
            let nullable = row_bool(result, row, "is_nullable").unwrap_or(true);
            let primary_key = row_bool(result, row, "primary_key").unwrap_or(false);
            let mut column = ColumnDef::new(column_name.clone(), kind)
                .nullable(nullable)
                .primary_key(primary_key);
            if let Some(default) = row_string(result, row, "column_default") {
                column = column.with_server_default(default);
            }
            let table = self.table_builder(schema, table_name);
            if primary_key && table.primary_key.is_none() {
                table.primary_key = Some(column_name);
            }
            table.columns.push(column);
        }
    }

    fn table_builder(
        &mut self,
        schema: Option<String>,
        table_name: String,
    ) -> &mut ReflectedTableBuilder {
        let key = (schema.clone(), table_name.clone());
        self.tables
            .entry(key)
            .or_insert_with(|| ReflectedTableBuilder {
                schema,
                name: table_name,
                primary_key: None,
                columns: Vec::new(),
            })
    }

    fn into_schema(self) -> ReflectedSchema {
        ReflectedSchema::new().with_tables(
            self.tables
                .into_values()
                .map(ReflectedTableBuilder::into_table)
                .collect(),
        )
    }
}

#[derive(Debug)]
struct ReflectedTableBuilder {
    schema: Option<String>,
    name: String,
    primary_key: Option<String>,
    columns: Vec<ColumnDef>,
}

impl ReflectedTableBuilder {
    fn into_table(self) -> ReflectedTable {
        let primary_key = self
            .primary_key
            .or_else(|| {
                self.columns
                    .iter()
                    .find(|column| column.is_primary_key())
                    .map(|column| column.name().to_string())
            })
            .or_else(|| self.columns.first().map(|column| column.name().to_string()))
            .unwrap_or_else(|| "id".to_string());
        let table = ReflectedTable::new(self.name, primary_key, self.columns);
        if let Some(schema) = self.schema {
            table.with_schema(schema)
        } else {
            table
        }
    }
}

fn row_string(result: &QueryResult, row: &[DbValue], column: &str) -> Option<String> {
    column_index(result, column).and_then(|index| row.get(index).and_then(value_string))
}

fn row_bool(result: &QueryResult, row: &[DbValue], column: &str) -> Option<bool> {
    column_index(result, column).and_then(|index| row.get(index).and_then(value_bool))
}

fn column_index(result: &QueryResult, column: &str) -> Option<usize> {
    result
        .columns()
        .iter()
        .position(|candidate| candidate.eq_ignore_ascii_case(column))
}

fn first_string(row: &[DbValue]) -> Option<String> {
    row.first().and_then(value_string)
}

fn value_string(value: &DbValue) -> Option<String> {
    match value {
        DbValue::Null => None,
        DbValue::Integer(value) => Some(value.to_string()),
        DbValue::UnsignedInteger(value) => Some(value.to_string()),
        DbValue::Decimal(value) | DbValue::Text(value) => Some(value.clone()),
        DbValue::Real(value) => Some(value.to_string()),
        DbValue::Bool(value) => Some(value.to_string()),
    }
}

fn value_bool(value: &DbValue) -> Option<bool> {
    match value {
        DbValue::Null => None,
        DbValue::Integer(value) => Some(*value != 0),
        DbValue::UnsignedInteger(value) => Some(*value != 0),
        DbValue::Decimal(value) | DbValue::Text(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            Some(matches!(
                normalized.as_str(),
                "1" | "t" | "true" | "y" | "yes"
            ))
        }
        DbValue::Real(value) => Some(*value != 0.0),
        DbValue::Bool(value) => Some(*value),
    }
}

fn reflected_field_kind(data_type: &str) -> FieldKind {
    let normalized = data_type.trim().to_ascii_uppercase();
    if normalized.contains("UUID") || normalized == "UNIQUEIDENTIFIER" {
        return FieldKind::Uuid;
    }
    if normalized.contains("JSON") {
        return FieldKind::Json;
    }
    if normalized.contains("BLOB")
        || normalized.contains("BINARY")
        || normalized.contains("BYTEA")
        || normalized.contains("RAW")
    {
        return FieldKind::Binary;
    }
    if normalized.contains("BOOL") || normalized == "BIT" {
        return FieldKind::Boolean;
    }
    if normalized.contains("DOUBLE") || normalized.contains("FLOAT") || normalized.contains("REAL")
    {
        return FieldKind::Float;
    }
    if normalized.contains("INT") || normalized.contains("SERIAL") {
        return FieldKind::Integer;
    }
    if normalized == "DATE" {
        return FieldKind::Date;
    }
    if normalized.contains("TIMESTAMP") || normalized.contains("DATETIME") {
        return FieldKind::DateTime;
    }
    if normalized.contains("DECIMAL") || normalized.contains("NUMERIC") || normalized == "NUMBER" {
        return FieldKind::Decimal;
    }
    if normalized.contains("CHAR")
        || normalized.contains("CLOB")
        || normalized.contains("TEXT")
        || normalized.contains("STRING")
        || normalized.contains("ENUM")
    {
        return FieldKind::String;
    }
    FieldKind::Unknown
}

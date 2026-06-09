use crate::query::compiled_queries_to_list;
use ormdantic_dialects::AnyDialect;
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, FieldKind, ForeignKeyDef, IndexDef, RelationshipCardinality,
    RelationshipDef, SchemaDef, SchemaDiffer, SchemaRegistry, SchemaSnapshot, TableDef,
    UniqueConstraintDef,
};
use ormdantic_sql::DdlAst;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub(crate) type RuntimeCheck = (String, String, String);
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
);
pub(crate) type RuntimeIndex = (String, Vec<String>, bool);
pub(crate) type RuntimeRelationship = (String, String, String, Option<String>);
pub(crate) type RuntimeTableSpec = (
    String,
    String,
    String,
    Vec<RuntimeColumn>,
    Vec<RuntimeIndex>,
    Vec<Vec<String>>,
    Vec<RuntimeRelationship>,
);

pub(crate) fn runtime_table_def(
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
        "enum" => FieldKind::Enum,
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

pub(crate) fn render_check_constraint(field: &str, check: &RuntimeCheck) -> PyResult<String> {
    let (kind, operator, value) = check;
    match kind.as_str() {
        "comparison" => Ok(format!("{field} {operator} {value}")),
        "length" => Ok(format!("LENGTH({field}) {operator} {value}")),
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
        _ => Err(PyValueError::new_err(format!(
            "unsupported check constraint operator '{operator}' for kind '{kind}'"
        ))),
    }
}

#[pyfunction]
pub(crate) fn validate_schema_tables(tables: &Bound<'_, PyAny>) -> PyResult<usize> {
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

#[pyfunction]
pub(crate) fn compile_schema_diff(
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
                ),
            ],
            vec![(
                "flavor_code_idx".to_string(),
                vec!["code".to_string()],
                true,
            )],
            vec![vec!["code".to_string(), "supplier_id".to_string()]],
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
        assert_eq!(table.columns().len(), 3);
        assert_eq!(table.indexes().len(), 1);
        assert_eq!(table.unique_constraints().len(), 2);
        assert_eq!(table.check_constraints().len(), 1);
        assert_eq!(table.foreign_keys().len(), 1);
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

        let unsupported = ("regex".to_string(), "~".to_string(), "a".to_string());
        assert!(render_check_constraint("name", &unsupported).is_err());
        assert!(check_constraint_suffix(&unsupported).is_err());
    }
}

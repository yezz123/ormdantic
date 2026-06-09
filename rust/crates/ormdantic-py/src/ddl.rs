use crate::schema::{
    check_constraint_suffix, field_kind_from_runtime, render_check_constraint, RuntimeCheck,
};
use ormdantic_dialects::AnyDialect;
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, ForeignKeyDef, IndexDef, SchemaOperation, TableDef,
    UniqueConstraintDef,
};
use ormdantic_sql::DdlAst;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub(crate) type ColumnDdl = (
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
pub(crate) fn compile_create_table_sql(
    dialect: &str,
    table: &str,
    columns: Vec<ColumnDdl>,
    indexes: Vec<(String, Vec<String>, bool)>,
    unique_constraints: Vec<Vec<String>>,
) -> PyResult<Vec<String>> {
    create_table_sql(dialect, table, columns, indexes, unique_constraints)
}

pub(crate) fn create_table_sql(
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

    fn column(name: &str, kind: &str, primary_key: bool) -> ColumnDdl {
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
            vec![column("id", "str", true), code],
            vec![(
                "flavor_code_idx".to_string(),
                vec!["code".to_string()],
                false,
            )],
            vec![vec!["id".to_string(), "code".to_string()]],
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
    }

    #[test]
    fn create_table_sql_rejects_unsupported_checks() {
        let mut code = column("code", "str", false);
        code.8 = vec![("regex".to_string(), "~".to_string(), "a".to_string())];

        assert!(create_table_sql("sqlite", "flavor", vec![code], Vec::new(), Vec::new()).is_err());
    }

    #[test]
    fn drop_table_sql_renders_for_dialect() {
        assert_eq!(
            drop_table_sql("sqlite", "flavor").expect("drop table should compile"),
            "DROP TABLE IF EXISTS \"flavor\""
        );
    }
}

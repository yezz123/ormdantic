use crate::runtime::py_to_db_value;
use ormdantic_dialects::{AnyDialect, Dialect};
use ormdantic_engine::{DbValue, NativeConnection};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub(crate) fn ensure_revision_table(connection: &mut NativeConnection) -> PyResult<()> {
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

pub(crate) fn applied_revisions_sql(dialect: &AnyDialect) -> String {
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

pub(crate) enum MigrationDirection {
    Apply,
    Rollback,
}

pub(crate) fn py_operations_to_db(
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

pub(crate) fn run_migration(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_table_sql_uses_backend_identifier_and_text_types() {
        let sqlite = AnyDialect::parse("sqlite").unwrap();
        let mysql = AnyDialect::parse("mysql").unwrap();
        let mssql = AnyDialect::parse("mssql").unwrap();
        let oracle = AnyDialect::parse("oracle").unwrap();

        assert_eq!(
            ensure_revision_table_sql(&sqlite),
            "CREATE TABLE IF NOT EXISTS \"ormdantic_migrations\" (\"revision\" TEXT PRIMARY KEY)"
        );
        assert_eq!(
            applied_revisions_sql(&mysql),
            "SELECT `revision` FROM `ormdantic_migrations` ORDER BY `revision`"
        );
        assert!(ensure_revision_table_sql(&mssql).contains("NVARCHAR(255)"));
        assert!(ensure_revision_table_sql(&oracle).contains("VARCHAR2(255)"));
    }

    #[test]
    fn revision_mutation_sql_uses_dialect_placeholders() {
        let postgres = AnyDialect::parse("postgresql").unwrap();
        let mssql = AnyDialect::parse("mssql").unwrap();
        let oracle = AnyDialect::parse("oracle").unwrap();

        assert!(insert_revision_sql(&postgres).contains("$1"));
        assert!(delete_revision_sql(&mssql).contains("@P1"));
        assert!(insert_revision_sql(&oracle).contains(":1"));
    }
}

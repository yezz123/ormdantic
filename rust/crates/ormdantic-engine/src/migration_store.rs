use ormdantic_core::{OrmdanticResult, RevisionId};
use ormdantic_dialects::{AnyDialect, Dialect};

use crate::{DbValue, NativeConnection};

pub struct MigrationStore<'a> {
    connection: &'a mut NativeConnection,
    table_name: String,
}

impl<'a> MigrationStore<'a> {
    pub fn new(connection: &'a mut NativeConnection) -> Self {
        Self {
            connection,
            table_name: "ormdantic_migrations".to_string(),
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    pub fn ensure(&mut self) -> OrmdanticResult<()> {
        let dialect = AnyDialect::parse(self.connection.dialect())?;
        self.connection.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {} (revision TEXT PRIMARY KEY, applied_at TEXT NOT NULL)",
                dialect.quote_ident(&self.table_name)
            ),
            &[],
        )?;
        Ok(())
    }

    pub fn record_revision(&mut self, revision: &RevisionId) -> OrmdanticResult<()> {
        self.ensure()?;
        let dialect = AnyDialect::parse(self.connection.dialect())?;
        self.connection.execute(
            &format!(
                "INSERT INTO {} (revision, applied_at) VALUES ({}, CURRENT_TIMESTAMP)",
                dialect.quote_ident(&self.table_name),
                dialect.placeholder(1)
            ),
            &[DbValue::Text(revision.as_str().to_string())],
        )?;
        Ok(())
    }

    pub fn revisions(&mut self) -> OrmdanticResult<Vec<RevisionId>> {
        self.ensure()?;
        let dialect = AnyDialect::parse(self.connection.dialect())?;
        let result = self.connection.execute(
            &format!(
                "SELECT revision FROM {} ORDER BY applied_at",
                dialect.quote_ident(&self.table_name)
            ),
            &[],
        )?;
        result
            .rows()
            .iter()
            .filter_map(|row| match row.first() {
                Some(DbValue::Text(value)) => Some(RevisionId::new(value.clone())),
                _ => None,
            })
            .collect()
    }
}

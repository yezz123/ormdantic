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

pub(crate) fn scope_predicate(scope: &ReflectionScope) -> String {
    match scope.schema_name() {
        Some(schema) => format!(" WHERE table_schema = '{}'", schema.replace('\'', "''")),
        None => String::new(),
    }
}

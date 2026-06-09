use crate::{ColumnDef, NamespaceDef, SchemaDef, TableDef};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReflectedSchema {
    namespaces: Vec<NamespaceDef>,
    tables: Vec<ReflectedTable>,
}

impl ReflectedSchema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_namespaces(mut self, namespaces: Vec<NamespaceDef>) -> Self {
        self.namespaces = namespaces;
        self
    }

    pub fn with_tables(mut self, tables: Vec<ReflectedTable>) -> Self {
        self.tables = tables;
        self
    }

    pub fn into_schema_def(self) -> SchemaDef {
        SchemaDef::from_tables(
            self.tables
                .into_iter()
                .map(ReflectedTable::into_table_def)
                .collect(),
        )
        .with_namespaces(self.namespaces)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReflectedTable {
    name: String,
    primary_key: String,
    columns: Vec<ColumnDef>,
    schema: Option<String>,
}

impl ReflectedTable {
    pub fn new(
        name: impl Into<String>,
        primary_key: impl Into<String>,
        columns: Vec<ColumnDef>,
    ) -> Self {
        Self {
            name: name.into(),
            primary_key: primary_key.into(),
            columns,
            schema: None,
        }
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    fn into_table_def(self) -> TableDef {
        let table = TableDef::from_parts(
            self.name.clone(),
            self.name,
            self.primary_key,
            self.columns,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        if let Some(schema) = self.schema {
            table.with_schema(schema)
        } else {
            table
        }
    }
}

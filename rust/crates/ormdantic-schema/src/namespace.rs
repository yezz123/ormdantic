use crate::TableDef;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaDef {
    namespaces: Vec<NamespaceDef>,
    tables: Vec<TableDef>,
}

impl SchemaDef {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_tables(tables: Vec<TableDef>) -> Self {
        Self {
            namespaces: Vec::new(),
            tables,
        }
    }

    pub fn with_namespaces(mut self, namespaces: Vec<NamespaceDef>) -> Self {
        self.namespaces = namespaces;
        self
    }

    pub fn namespaces(&self) -> &[NamespaceDef] {
        &self.namespaces
    }

    pub fn tables(&self) -> &[TableDef] {
        &self.tables
    }

    pub fn table(&self, name: &str) -> Option<&TableDef> {
        self.tables.iter().find(|table| table.name() == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceDef {
    name: String,
    comment: Option<String>,
}

impl NamespaceDef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            comment: None,
        }
    }

    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

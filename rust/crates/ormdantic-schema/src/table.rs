use ormdantic_core::{QualifiedName, TableId};

use crate::{
    CheckConstraintDef, ColumnDef, FieldKind, ForeignKeyDef, IndexDef, RelationshipDef,
    UniqueConstraintDef,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    id: Option<TableId>,
    model_key: String,
    name: String,
    primary_key: String,
    columns: Vec<ColumnDef>,
    indexes: Vec<IndexDef>,
    unique_constraints: Vec<UniqueConstraintDef>,
    relationships: Vec<RelationshipDef>,
    check_constraints: Vec<CheckConstraintDef>,
    foreign_keys: Vec<ForeignKeyDef>,
    schema: Option<String>,
    comment: Option<String>,
}

impl TableDef {
    pub fn new(
        name: impl Into<String>,
        primary_key: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        let name = name.into();
        Self {
            id: None,
            model_key: name.clone(),
            name,
            primary_key: primary_key.into(),
            columns: columns
                .into_iter()
                .map(|column| ColumnDef::new(column, FieldKind::Unknown))
                .collect(),
            indexes: Vec::new(),
            unique_constraints: Vec::new(),
            relationships: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            schema: None,
            comment: None,
        }
    }

    pub fn from_parts(
        name: impl Into<String>,
        model_key: impl Into<String>,
        primary_key: impl Into<String>,
        columns: Vec<ColumnDef>,
        indexes: Vec<IndexDef>,
        unique_constraints: Vec<UniqueConstraintDef>,
        relationships: Vec<RelationshipDef>,
    ) -> Self {
        Self {
            id: None,
            model_key: model_key.into(),
            name: name.into(),
            primary_key: primary_key.into(),
            columns,
            indexes,
            unique_constraints,
            relationships,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            schema: None,
            comment: None,
        }
    }

    pub fn with_relationships(mut self, relationships: Vec<RelationshipDef>) -> Self {
        self.relationships = relationships;
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    pub fn with_check_constraints(mut self, check_constraints: Vec<CheckConstraintDef>) -> Self {
        self.check_constraints = check_constraints;
        self
    }

    pub fn with_foreign_keys(mut self, foreign_keys: Vec<ForeignKeyDef>) -> Self {
        self.foreign_keys = foreign_keys;
        self
    }

    pub fn set_id(&mut self, id: TableId) {
        self.id = Some(id);
    }

    pub fn id(&self) -> Option<TableId> {
        self.id
    }

    pub fn model_key(&self) -> &str {
        &self.model_key
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }

    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|column| column.name())
    }

    pub fn relationships(&self) -> &[RelationshipDef] {
        &self.relationships
    }

    pub fn indexes(&self) -> &[IndexDef] {
        &self.indexes
    }

    pub fn unique_constraints(&self) -> &[UniqueConstraintDef] {
        &self.unique_constraints
    }

    pub fn check_constraints(&self) -> &[CheckConstraintDef] {
        &self.check_constraints
    }

    pub fn foreign_keys(&self) -> &[ForeignKeyDef] {
        &self.foreign_keys
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn qualified_name(&self) -> QualifiedName {
        QualifiedName::unchecked(self.schema.clone(), self.name.clone())
    }
}

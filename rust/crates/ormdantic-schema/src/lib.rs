use std::collections::{HashMap, HashSet};

use ormdantic_core::{OrmdanticError, OrmdanticResult, TableId};

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
        }
    }

    pub fn with_relationships(mut self, relationships: Vec<RelationshipDef>) -> Self {
        self.relationships = relationships;
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    name: String,
    kind: FieldKind,
    nullable: bool,
    primary_key: bool,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, kind: FieldKind) -> Self {
        Self {
            name: name.into(),
            kind,
            nullable: false,
            primary_key: false,
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn primary_key(mut self, primary_key: bool) -> Self {
        self.primary_key = primary_key;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> &FieldKind {
        &self.kind
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn is_primary_key(&self) -> bool {
        self.primary_key
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldKind {
    String,
    Integer,
    Float,
    Boolean,
    Uuid,
    Date,
    DateTime,
    Json,
    ModelJson,
    Enum,
    Decimal,
    Binary,
    ForeignKey { target_table: String },
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    name: String,
    columns: Vec<String>,
}

impl IndexDef {
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueConstraintDef {
    name: String,
    columns: Vec<String>,
}

impl UniqueConstraintDef {
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDef {
    field: String,
    target_table: String,
    target_field: String,
    cardinality: RelationshipCardinality,
    back_reference: Option<String>,
}

impl RelationshipDef {
    pub fn new(
        field: impl Into<String>,
        target_table: impl Into<String>,
        target_field: impl Into<String>,
        cardinality: RelationshipCardinality,
    ) -> Self {
        Self {
            field: field.into(),
            target_table: target_table.into(),
            target_field: target_field.into(),
            cardinality,
            back_reference: None,
        }
    }

    pub fn with_back_reference(mut self, back_reference: impl Into<String>) -> Self {
        self.back_reference = Some(back_reference.into());
        self
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn target_table(&self) -> &str {
        &self.target_table
    }

    pub fn target_field(&self) -> &str {
        &self.target_field
    }

    pub fn cardinality(&self) -> &RelationshipCardinality {
        &self.cardinality
    }

    pub fn back_reference(&self) -> Option<&str> {
        self.back_reference.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipCardinality {
    One,
    Many,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaRegistry {
    tables: Vec<TableDef>,
    table_ids: HashMap<String, TableId>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_table(&mut self, mut table: TableDef) -> OrmdanticResult<TableId> {
        if self.table_ids.contains_key(table.name()) {
            return Err(OrmdanticError::DuplicateTable {
                tablename: table.name().to_string(),
            });
        }

        validate_columns(&table)?;
        validate_primary_key(&table)?;

        let table_id = TableId(self.tables.len());
        table.set_id(table_id);
        self.table_ids.insert(table.name().to_string(), table_id);
        self.tables.push(table);
        Ok(table_id)
    }

    pub fn validate_relationships(&self) -> OrmdanticResult<()> {
        for table in &self.tables {
            for relationship in table.relationships() {
                let Some(target_table) = self.get_table(relationship.target_table()) else {
                    return Err(OrmdanticError::InvalidRelationship {
                        table: table.name().to_string(),
                        field: relationship.field().to_string(),
                        target_table: relationship.target_table().to_string(),
                    });
                };
                if !target_table
                    .column_names()
                    .any(|column| column == relationship.target_field())
                {
                    return Err(OrmdanticError::InvalidRelationship {
                        table: table.name().to_string(),
                        field: relationship.field().to_string(),
                        target_table: relationship.target_table().to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn get_table(&self, tablename: &str) -> Option<&TableDef> {
        self.table_ids
            .get(tablename)
            .and_then(|table_id| self.tables.get(table_id.0))
    }

    pub fn tables(&self) -> &[TableDef] {
        &self.tables
    }
}

fn validate_columns(table: &TableDef) -> OrmdanticResult<()> {
    let mut seen = HashSet::new();
    for column in table.columns() {
        if !seen.insert(column.name()) {
            return Err(OrmdanticError::DuplicateColumn {
                tablename: table.name().to_string(),
                column: column.name().to_string(),
            });
        }
    }
    Ok(())
}

fn validate_primary_key(table: &TableDef) -> OrmdanticResult<()> {
    if table
        .columns()
        .iter()
        .any(|column| column.name() == table.primary_key())
    {
        return Ok(());
    }

    Err(OrmdanticError::MissingPrimaryKey {
        tablename: table.name().to_string(),
        primary_key: table.primary_key().to_string(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnAlias {
    table_path: String,
    column: String,
}

impl ColumnAlias {
    pub fn parse(alias: &str) -> Option<Self> {
        let (table_path, column) = alias.split_once('\\')?;
        Some(Self {
            table_path: table_path.to_string(),
            column: column.to_string(),
        })
    }

    pub fn column_for_table(&self, tablename: &str) -> Option<&str> {
        if self.table_path == tablename {
            Some(&self.column)
        } else {
            None
        }
    }

    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    pub fn column(&self) -> &str {
        &self.column
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ColumnAlias, ColumnDef, FieldKind, RelationshipCardinality, RelationshipDef,
        SchemaRegistry, TableDef,
    };

    #[test]
    fn parses_column_aliases() {
        let alias = ColumnAlias::parse("flavors\\id").expect("alias should parse");

        assert_eq!(alias.column_for_table("flavors"), Some("id"));
        assert_eq!(alias.column_for_table("coffee"), None);
    }

    #[test]
    fn registers_tables() {
        let mut registry = SchemaRegistry::new();

        let table_id = registry
            .register_table(TableDef::new(
                "flavors",
                "id",
                vec!["id".to_string(), "name".to_string()],
            ))
            .expect("table should register");

        assert_eq!(table_id.0, 0);
        assert_eq!(registry.get_table("flavors").unwrap().name(), "flavors");
    }

    #[test]
    fn rejects_duplicate_tables() {
        let mut registry = SchemaRegistry::new();

        registry
            .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
            .expect("first table should register");
        let error = registry
            .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
            .expect_err("duplicate table should fail");

        assert_eq!(error.to_string(), "table 'flavors' is already registered");
    }

    #[test]
    fn rejects_duplicate_columns() {
        let mut registry = SchemaRegistry::new();

        let error = registry
            .register_table(TableDef::new(
                "flavors",
                "id",
                vec!["id".to_string(), "id".to_string()],
            ))
            .expect_err("duplicate column should fail");

        assert_eq!(
            error.to_string(),
            "column 'flavors.id' is already registered"
        );
    }

    #[test]
    fn rejects_missing_primary_key() {
        let mut registry = SchemaRegistry::new();

        let error = registry
            .register_table(TableDef::new("flavors", "id", vec!["name".to_string()]))
            .expect_err("missing pk should fail");

        assert_eq!(
            error.to_string(),
            "table 'flavors' does not define primary key column 'id'"
        );
    }

    #[test]
    fn validates_relationship_targets() {
        let mut registry = SchemaRegistry::new();
        registry
            .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
            .expect("target should register");
        registry
            .register_table(TableDef::from_parts(
                "coffee",
                "Coffee",
                "id",
                vec![
                    ColumnDef::new("id", FieldKind::Uuid),
                    ColumnDef::new(
                        "flavor",
                        FieldKind::ForeignKey {
                            target_table: "flavors".to_string(),
                        },
                    ),
                ],
                Vec::new(),
                Vec::new(),
                vec![RelationshipDef::new(
                    "flavor",
                    "flavors",
                    "id",
                    RelationshipCardinality::One,
                )],
            ))
            .expect("source should register");

        registry
            .validate_relationships()
            .expect("relationships should be valid");
    }

    #[test]
    fn rejects_unknown_relationship_target() {
        let mut registry = SchemaRegistry::new();
        registry
            .register_table(TableDef::from_parts(
                "coffee",
                "Coffee",
                "id",
                vec![ColumnDef::new("id", FieldKind::Uuid)],
                Vec::new(),
                Vec::new(),
                vec![RelationshipDef::new(
                    "flavor",
                    "flavors",
                    "id",
                    RelationshipCardinality::One,
                )],
            ))
            .expect("source should register");

        let error = registry
            .validate_relationships()
            .expect_err("relationship target should fail");

        assert_eq!(
            error.to_string(),
            "relationship 'coffee.flavor' targets unknown table 'flavors'"
        );
    }
}

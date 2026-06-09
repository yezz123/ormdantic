use std::collections::HashSet;

use ormdantic_schema::{RelationshipDef, TableDef};

use crate::{HydratedRow, HydrationKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipNode {
    path: String,
    relationship: RelationshipDef,
    children: Vec<RelationshipNode>,
}

impl RelationshipNode {
    pub fn new(path: impl Into<String>, relationship: RelationshipDef) -> Self {
        Self {
            path: path.into(),
            relationship,
            children: Vec::new(),
        }
    }

    pub fn with_children(mut self, children: Vec<RelationshipNode>) -> Self {
        self.children = children;
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn relationship(&self) -> &RelationshipDef {
        &self.relationship
    }

    pub fn children(&self) -> &[RelationshipNode] {
        &self.children
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydrationGraph {
    root_table: TableDef,
    primary_key_columns: Vec<String>,
    relationships: Vec<RelationshipNode>,
}

impl HydrationGraph {
    pub fn new(root_table: TableDef) -> Self {
        let primary_key_columns = vec![root_table.primary_key().to_string()];
        Self {
            root_table,
            primary_key_columns,
            relationships: Vec::new(),
        }
    }

    pub fn composite_key(mut self, primary_key_columns: Vec<String>) -> Self {
        self.primary_key_columns = primary_key_columns;
        self
    }

    pub fn with_relationships(mut self, relationships: Vec<RelationshipNode>) -> Self {
        self.relationships = relationships;
        self
    }

    pub fn root_table(&self) -> &TableDef {
        &self.root_table
    }

    pub fn primary_key_columns(&self) -> &[String] {
        &self.primary_key_columns
    }

    pub fn relationships(&self) -> &[RelationshipNode] {
        &self.relationships
    }

    pub fn deduplicate_rows(&self, rows: Vec<HydratedRow>) -> Vec<HydratedRow> {
        let mut seen = HashSet::new();
        let mut output = Vec::new();
        for row in rows {
            let Some(key) =
                HydrationKey::from_row(self.root_table.name(), &self.primary_key_columns, &row)
            else {
                continue;
            };
            if seen.insert(key) {
                output.push(row);
            }
        }
        output
    }
}

//! Row hydration planning helpers for Ormdantic result sets.
//!
//! ```
//! use ormdantic_hydrate::{
//!     FlatHydrationPlan, HydratedRow, HydrationKey, ResultColumn, ResultShape,
//! };
//! use ormdantic_schema::TableDef;
//!
//! let column = ResultColumn::parse("coffee/flavor\\name").unwrap();
//! assert_eq!(column.table_path(), "coffee/flavor");
//! assert_eq!(column.column(), "name");
//!
//! let mut row = HydratedRow::new();
//! row.insert("id".to_string(), "42".to_string());
//! let key = HydrationKey::from_row("coffee", &["id".to_string()], &row).unwrap();
//! assert_eq!(key.identity_key().model_key(), "coffee");
//!
//! let shape = ResultShape::new(
//!     "coffee",
//!     &["coffee\\id".to_string(), "coffee/flavor\\name".to_string()],
//!     Vec::new(),
//! );
//! assert_eq!(shape.relationship_paths(), &["coffee/flavor".to_string()]);
//!
//! let table = TableDef::new("coffee", "id", vec!["id".to_string()]);
//! let plan = FlatHydrationPlan::new(table, &["coffee\\id".to_string()])?;
//! assert_eq!(plan.primary_key_index(), 0);
//!
//! # Ok::<(), ormdantic_core::OrmdanticError>(())
//! ```

use std::collections::{BTreeMap, BTreeSet, HashSet};

use ormdantic_core::{IdentityKey, OrmdanticError, OrmdanticResult};
use ormdantic_schema::{ColumnAlias, RelationshipDef, TableDef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlatHydrationPlan {
    table: TableDef,
    parsed_columns: Vec<Option<String>>,
    primary_key_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumn {
    alias: String,
    table_path: String,
    column: String,
}

impl ResultColumn {
    pub fn parse(alias: impl Into<String>) -> Option<Self> {
        let alias = alias.into();
        let parsed = ColumnAlias::parse(&alias)?;
        Some(Self {
            alias,
            table_path: parsed.table_path().to_string(),
            column: parsed.column().to_string(),
        })
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    pub fn column(&self) -> &str {
        &self.column
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultShape {
    root_table: String,
    columns: Vec<ResultColumn>,
    relationship_paths: Vec<String>,
    array_paths: Vec<String>,
}

pub type HydratedRow = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HydrationKey {
    table_path: String,
    values: Vec<String>,
}

impl HydrationKey {
    pub fn new(table_path: impl Into<String>, values: Vec<String>) -> Self {
        Self {
            table_path: table_path.into(),
            values,
        }
    }

    pub fn from_row(
        table_path: impl Into<String>,
        key_columns: &[String],
        row: &HydratedRow,
    ) -> Option<Self> {
        let values = key_columns
            .iter()
            .map(|column| row.get(column).cloned())
            .collect::<Option<Vec<_>>>()?;
        if values.iter().any(|value| value.is_empty()) {
            return None;
        }
        Some(Self::new(table_path, values))
    }

    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }

    pub fn identity_key(&self) -> IdentityKey {
        IdentityKey::new(self.table_path.clone(), self.values.clone())
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectInHydrationPlan {
    parent_key_columns: Vec<String>,
    child_key_columns: Vec<String>,
    relationship: RelationshipDef,
    child_collection_key: String,
}

impl SelectInHydrationPlan {
    pub fn new(
        parent_key_columns: Vec<String>,
        child_key_columns: Vec<String>,
        relationship: RelationshipDef,
    ) -> Self {
        let child_collection_key = relationship.field().to_string();
        Self {
            parent_key_columns,
            child_key_columns,
            relationship,
            child_collection_key,
        }
    }

    pub fn parent_key_columns(&self) -> &[String] {
        &self.parent_key_columns
    }

    pub fn child_key_columns(&self) -> &[String] {
        &self.child_key_columns
    }

    pub fn relationship(&self) -> &RelationshipDef {
        &self.relationship
    }

    pub fn child_collection_key(&self) -> &str {
        &self.child_collection_key
    }

    pub fn parent_keys(&self, parent_rows: &[HydratedRow]) -> Vec<HydrationKey> {
        let mut seen = HashSet::new();
        let mut keys = Vec::new();
        for row in parent_rows {
            if let Some(key) = HydrationKey::from_row("parent", &self.parent_key_columns, row) {
                if seen.insert(key.clone()) {
                    keys.push(key);
                }
            }
        }
        keys
    }
}

pub fn merge_selectin_results(
    parent_rows: Vec<HydratedRow>,
    child_rows: Vec<HydratedRow>,
    relationship: &SelectInHydrationPlan,
) -> Vec<HydratedRow> {
    let mut children_by_parent = BTreeMap::<Vec<String>, Vec<HydratedRow>>::new();
    let mut child_seen = HashSet::<HydrationKey>::new();
    for child in child_rows {
        let Some(parent_key) = key_values(&relationship.child_key_columns, &child) else {
            continue;
        };
        let child_identity = HydrationKey::from_row(
            relationship.relationship().target_table(),
            &relationship.child_key_columns,
            &child,
        );
        if let Some(identity) = child_identity {
            if !child_seen.insert(identity) {
                continue;
            }
        }
        children_by_parent
            .entry(parent_key)
            .or_default()
            .push(child);
    }

    parent_rows
        .into_iter()
        .map(|mut parent| {
            if let Some(parent_key) = key_values(&relationship.parent_key_columns, &parent) {
                let children = children_by_parent.remove(&parent_key).unwrap_or_default();
                if relationship.relationship().is_uselist() {
                    parent.insert(
                        relationship.child_collection_key().to_string(),
                        format_collection(children),
                    );
                } else if let Some(child) = children.into_iter().next() {
                    parent.insert(
                        relationship.child_collection_key().to_string(),
                        format_row(child),
                    );
                }
            }
            parent
        })
        .collect()
}

fn key_values(columns: &[String], row: &HydratedRow) -> Option<Vec<String>> {
    columns
        .iter()
        .map(|column| row.get(column).cloned())
        .collect::<Option<Vec<_>>>()
}

fn format_collection(rows: Vec<HydratedRow>) -> String {
    rows.into_iter()
        .map(format_row)
        .collect::<Vec<_>>()
        .join(";")
}

fn format_row(row: HydratedRow) -> String {
    row.into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

impl ResultShape {
    pub fn new(
        root_table: impl Into<String>,
        aliases: &[String],
        array_paths: Vec<String>,
    ) -> Self {
        let root_table = root_table.into();
        let columns: Vec<ResultColumn> = aliases
            .iter()
            .filter_map(|alias| ResultColumn::parse(alias.clone()))
            .collect();
        let mut relationship_paths = BTreeSet::new();
        for column in &columns {
            if column.table_path() == root_table {
                continue;
            }
            let mut current = String::new();
            for (idx, branch) in column.table_path().split('/').enumerate() {
                if idx == 0 {
                    current.push_str(branch);
                } else {
                    current.push('/');
                    current.push_str(branch);
                    relationship_paths.insert(current.clone());
                }
            }
        }

        Self {
            root_table,
            columns,
            relationship_paths: relationship_paths.into_iter().collect(),
            array_paths,
        }
    }

    pub fn root_table(&self) -> &str {
        &self.root_table
    }

    pub fn columns(&self) -> &[ResultColumn] {
        &self.columns
    }

    pub fn relationship_paths(&self) -> &[String] {
        &self.relationship_paths
    }

    pub fn array_paths(&self) -> &[String] {
        &self.array_paths
    }
}

impl FlatHydrationPlan {
    pub fn new(table: TableDef, aliases: &[String]) -> OrmdanticResult<Self> {
        let parsed_columns: Vec<Option<String>> = aliases
            .iter()
            .map(|alias| {
                ColumnAlias::parse(alias).and_then(|column_alias| {
                    column_alias
                        .column_for_table(table.name())
                        .map(str::to_string)
                })
            })
            .collect();
        let primary_key_index = parsed_columns
            .iter()
            .position(|column| column.as_deref() == Some(table.primary_key()))
            .ok_or_else(|| OrmdanticError::MissingPrimaryKeyAlias {
                tablename: table.name().to_string(),
                primary_key: table.primary_key().to_string(),
            })?;

        Ok(Self {
            table,
            parsed_columns,
            primary_key_index,
        })
    }

    pub fn table(&self) -> &TableDef {
        &self.table
    }

    pub fn parsed_columns(&self) -> &[Option<String>] {
        &self.parsed_columns
    }

    pub fn primary_key_index(&self) -> usize {
        self.primary_key_index
    }
}

#[cfg(test)]
mod tests {
    use super::{FlatHydrationPlan, ResultColumn, ResultShape};
    use ormdantic_schema::TableDef;

    #[test]
    fn builds_flat_hydration_plan() {
        let table = TableDef::new("flavors", "id", vec!["id".to_string()]);

        let plan = FlatHydrationPlan::new(
            table,
            &["flavors\\id".to_string(), "flavors\\name".to_string()],
        )
        .expect("plan should build");

        assert_eq!(plan.primary_key_index(), 0);
        assert_eq!(
            plan.parsed_columns(),
            &[Some("id".to_string()), Some("name".to_string())]
        );
    }

    #[test]
    fn errors_when_primary_key_alias_is_missing() {
        let table = TableDef::new("flavors", "id", vec!["id".to_string()]);

        let error = FlatHydrationPlan::new(table, &["flavors\\name".to_string()])
            .expect_err("missing primary key should error");

        assert_eq!(
            error.to_string(),
            "primary key column 'flavors\\id' was not found"
        );
    }

    #[test]
    fn parses_result_columns() {
        let column = ResultColumn::parse("coffee/flavor\\name").expect("column should parse");

        assert_eq!(column.alias(), "coffee/flavor\\name");
        assert_eq!(column.table_path(), "coffee/flavor");
        assert_eq!(column.column(), "name");
    }

    #[test]
    fn builds_relationship_result_shape() {
        let shape = ResultShape::new(
            "coffee",
            &[
                "coffee\\id".to_string(),
                "coffee/flavor\\id".to_string(),
                "coffee/flavor\\name".to_string(),
                "coffee/flavor/roast\\id".to_string(),
            ],
            vec!["coffee/flavor/roast".to_string()],
        );

        assert_eq!(shape.root_table(), "coffee");
        assert_eq!(
            shape.relationship_paths(),
            &[
                "coffee/flavor".to_string(),
                "coffee/flavor/roast".to_string()
            ]
        );
        assert_eq!(shape.array_paths(), &["coffee/flavor/roast".to_string()]);
    }
}

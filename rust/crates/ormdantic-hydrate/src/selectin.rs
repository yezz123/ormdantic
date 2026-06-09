use std::collections::{BTreeMap, HashSet};

use ormdantic_schema::RelationshipDef;

use crate::row::{format_collection, format_row, key_values, row_fingerprint};
use crate::{HydratedRow, HydrationKey};

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
    let mut child_seen = HashSet::<Vec<(String, String)>>::new();
    for child in child_rows {
        let Some(parent_key) = key_values(&relationship.child_key_columns, &child) else {
            continue;
        };
        if !child_seen.insert(row_fingerprint(&child)) {
            continue;
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

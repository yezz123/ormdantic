use ormdantic_core::IdentityKey;

use crate::HydratedRow;

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

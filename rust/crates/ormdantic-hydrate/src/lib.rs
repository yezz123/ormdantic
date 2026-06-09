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

mod columns;
mod flat;
mod graph;
mod keys;
mod row;
mod selectin;

pub use columns::{ResultColumn, ResultShape};
pub use flat::FlatHydrationPlan;
pub use graph::{HydrationGraph, RelationshipNode};
pub use keys::HydrationKey;
pub use row::HydratedRow;
pub use selectin::{merge_selectin_results, SelectInHydrationPlan};

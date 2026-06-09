//! Schema metadata structures used by Ormdantic's Rust runtime.
//!
//! ```
//! use ormdantic_schema::{
//!     ColumnAlias, ColumnDef, FieldKind, SchemaRegistry, TableDef,
//! };
//!
//! let table = TableDef::from_parts(
//!     "coffee",
//!     "Coffee",
//!     "id",
//!     vec![
//!         ColumnDef::new("id", FieldKind::Integer).primary_key(true),
//!         ColumnDef::new("name", FieldKind::String),
//!     ],
//!     Vec::new(),
//!     Vec::new(),
//!     Vec::new(),
//! );
//!
//! let mut registry = SchemaRegistry::new();
//! let table_id = registry.register_table(table)?;
//! assert_eq!(table_id.0, 0);
//! assert_eq!(registry.get_table("coffee").unwrap().primary_key(), "id");
//!
//! let alias = ColumnAlias::parse("coffee\\name").unwrap();
//! assert_eq!(alias.table_path(), "coffee");
//! assert_eq!(alias.column(), "name");
//!
//! # Ok::<(), ormdantic_core::OrmdanticError>(())
//! ```

mod alias;
mod column;
mod constraints;
mod diff;
mod index;
mod namespace;
mod reflection;
mod registry;
mod relationships;
mod table;

pub use alias::ColumnAlias;
pub use column::{ColumnDef, ColumnDefault, ComputedDef, FieldKind, IdentityDef};
pub use constraints::{
    CheckConstraintDef, ConstraintDef, ForeignKeyAction, ForeignKeyDef, UniqueConstraintDef,
};
pub use diff::{SchemaDiff, SchemaDiffer, SchemaOperation, SchemaSnapshot};
pub use index::IndexDef;
pub use namespace::{NamespaceDef, SchemaDef};
pub use reflection::{ReflectedSchema, ReflectedTable};
pub use registry::SchemaRegistry;
pub use relationships::{
    CascadeAction, LoaderStrategy, RelationshipCardinality, RelationshipDef, RelationshipDirection,
};
pub use table::TableDef;

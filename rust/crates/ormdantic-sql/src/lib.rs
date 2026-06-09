//! SQL AST and query compiler for Ormdantic.
//!
//! ```
//! use ormdantic_dialects::PostgresDialect;
//! use ormdantic_sql::{Filter, QueryAst, SelectColumn, TableRef};
//!
//! let compiled = QueryAst::Select {
//!     table: TableRef::new("flavors"),
//!     columns: vec![
//!         SelectColumn::aliased("id", "flavors\\id"),
//!         SelectColumn::aliased("name", "flavors\\name"),
//!     ],
//!     filters: vec![Filter::Eq {
//!         column: "id".to_string(),
//!         param: "id".to_string(),
//!     }],
//!     order_by: Vec::new(),
//!     limit: None,
//!     offset: None,
//! }
//! .compile(&PostgresDialect)?;
//!
//! assert_eq!(
//!     compiled.sql(),
//!     "SELECT \"flavors\".\"id\" AS \"flavors\\id\", \"flavors\".\"name\" AS \"flavors\\name\" FROM \"flavors\" WHERE \"id\" = $1"
//! );
//! assert_eq!(compiled.params(), &["id".to_string()]);
//!
//! # Ok::<(), ormdantic_core::OrmdanticError>(())
//! ```

mod ast;
mod compiler;
mod filters;

pub use ast::{
    BinaryOp, CompiledQuery, DdlAst, DmlAst, Expr, JoinAst, JoinKind, JoinSpec, JoinedFilter,
    JoinedOrderBy, JoinedSelectColumn, OrderBy, OrderExpr, OrderNulls, Projection, QueryAst,
    QueryOperation, SelectAst, SelectColumn, SelectInPlan, SelectInQuery, SortDirection,
    SqlLiteral, TableRef, TableSource, UnaryOp,
};
pub use compiler::DdlCompiler;
pub use filters::Filter;

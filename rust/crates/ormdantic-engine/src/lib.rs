//! Native database execution primitives for Ormdantic.
//!
//! ```
//! use ormdantic_engine::{runtime_capabilities, DbValue, QueryResult, StatementResult};
//!
//! let result = QueryResult::new(
//!     vec!["id".to_string()],
//!     vec![vec![DbValue::Integer(1)]],
//! );
//! let statement = StatementResult::from_query_result(result);
//!
//! assert_eq!(statement.row_count(), 1);
//! assert_eq!(statement.columns(), &["id".to_string()]);
//! assert!(runtime_capabilities().iter().any(|(name, _)| *name == "sqlite"));
//! ```

mod connection;
mod drivers;
mod migration_store;
mod reflection;
mod result;
mod runtime;
mod statement;
mod url;
mod value;

pub use connection::{Connection, NativeConnection, TransactionState};
pub use migration_store::MigrationStore;
pub use reflection::{Inspector, Reflector};
pub use result::QueryResult;
pub use runtime::{execute_url, returns_rows, runtime_capabilities, sql_error};
pub use statement::StatementResult;
pub use value::DbValue;

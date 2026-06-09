use ormdantic_core::OrmdanticResult;
use ormdantic_dialects::Dialect;
use ormdantic_schema::{SchemaDiff, SchemaOperation};

use crate::compiler::{
    compile_dml_ast, compile_query_ast, compile_select_ast, compile_select_in_query, DdlCompiler,
};
use crate::Filter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOperation {
    Select,
    Insert,
    Update,
    Upsert,
    Delete,
    Count,
    Ddl,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledQuery {
    sql: String,
    params: Vec<String>,
    operation: QueryOperation,
}

impl CompiledQuery {
    pub fn new(sql: String, params: Vec<String>, operation: QueryOperation) -> Self {
        Self {
            sql,
            params,
            operation,
        }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn params(&self) -> &[String] {
        &self.params
    }

    pub fn operation(&self) -> &QueryOperation {
        &self.operation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    name: String,
}

impl TableRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectColumn {
    name: String,
    alias: Option<String>,
}

impl SelectColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            alias: None,
        }
    }

    pub fn aliased(name: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            alias: Some(alias.into()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    column: String,
    direction: SortDirection,
}

impl OrderBy {
    pub fn new(column: impl Into<String>, direction: SortDirection) -> Self {
        Self {
            column: column.into(),
            direction,
        }
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn direction(&self) -> &SortDirection {
        &self.direction
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlLiteral {
    Null,
    Integer(i64),
    String(String),
    Boolean(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    And,
    Or,
    Like,
    ILike,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderExpr {
    pub(crate) expr: Expr,
    pub(crate) direction: SortDirection,
    pub(crate) nulls: Option<OrderNulls>,
}

impl OrderExpr {
    pub fn new(expr: Expr, direction: SortDirection) -> Self {
        Self {
            expr,
            direction,
            nulls: None,
        }
    }

    pub fn nulls(mut self, nulls: OrderNulls) -> Self {
        self.nulls = Some(nulls);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderNulls {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column {
        table: Option<String>,
        name: String,
    },
    Param(String),
    Literal(SqlLiteral),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    InList {
        expr: Box<Expr>,
        values: Vec<Expr>,
        negated: bool,
    },
    Case {
        whens: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        type_name: String,
    },
    Tuple(Vec<Expr>),
    RawSafe(String),
}

impl Expr {
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column {
            table: None,
            name: name.into(),
        }
    }

    pub fn qualified_column(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Column {
            table: Some(table.into()),
            name: name.into(),
        }
    }

    pub fn param(name: impl Into<String>) -> Self {
        Self::Param(name.into())
    }

    pub fn eq(left: Expr, right: Expr) -> Self {
        Self::Binary {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    pub(crate) expr: Expr,
    pub(crate) alias: Option<String>,
}

impl Projection {
    pub fn new(expr: Expr) -> Self {
        Self { expr, alias: None }
    }

    pub fn aliased(expr: Expr, alias: impl Into<String>) -> Self {
        Self {
            expr,
            alias: Some(alias.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableSource {
    Table { name: String, alias: Option<String> },
    RawSafe(String),
}

impl TableSource {
    pub fn table(name: impl Into<String>) -> Self {
        Self::Table {
            name: name.into(),
            alias: None,
        }
    }

    pub fn aliased_table(name: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::Table {
            name: name.into(),
            alias: Some(alias.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinAst {
    pub(crate) kind: JoinKind,
    pub(crate) source: TableSource,
    pub(crate) on: Option<Expr>,
}

impl JoinAst {
    pub fn new(kind: JoinKind, source: TableSource, on: Option<Expr>) -> Self {
        Self { kind, source, on }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectAst {
    pub(crate) projections: Vec<Projection>,
    pub(crate) from: Option<TableSource>,
    pub(crate) joins: Vec<JoinAst>,
    pub(crate) where_expr: Option<Expr>,
    pub(crate) group_by: Vec<Expr>,
    pub(crate) having: Option<Expr>,
    pub(crate) order_by: Vec<OrderExpr>,
    pub(crate) distinct: bool,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

impl SelectAst {
    pub fn new(projections: Vec<Projection>) -> Self {
        Self {
            projections,
            from: None,
            joins: Vec::new(),
            where_expr: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            limit: None,
            offset: None,
        }
    }

    pub fn from(mut self, source: TableSource) -> Self {
        self.from = Some(source);
        self
    }

    pub fn join(mut self, join: JoinAst) -> Self {
        self.joins.push(join);
        self
    }

    pub fn where_expr(mut self, expr: Expr) -> Self {
        self.where_expr = Some(expr);
        self
    }

    pub fn group_by(mut self, group_by: Vec<Expr>) -> Self {
        self.group_by = group_by;
        self
    }

    pub fn having(mut self, having: Expr) -> Self {
        self.having = Some(having);
        self
    }

    pub fn order_by(mut self, order_by: Vec<OrderExpr>) -> Self {
        self.order_by = order_by;
        self
    }

    pub fn distinct(mut self, distinct: bool) -> Self {
        self.distinct = distinct;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn compile(&self, dialect: &impl Dialect) -> OrmdanticResult<CompiledQuery> {
        compile_select_ast(dialect, self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DmlAst {
    Insert {
        table: TableSource,
        columns: Vec<String>,
        rows: Vec<Vec<Expr>>,
        returning: Vec<Expr>,
    },
    Update {
        table: TableSource,
        assignments: Vec<(String, Expr)>,
        where_expr: Option<Expr>,
        returning: Vec<Expr>,
    },
    Delete {
        table: TableSource,
        where_expr: Option<Expr>,
        returning: Vec<Expr>,
    },
    Upsert {
        table: TableSource,
        columns: Vec<String>,
        rows: Vec<Vec<Expr>>,
        conflict_target: Vec<String>,
        update_assignments: Vec<(String, Expr)>,
        returning: Vec<Expr>,
    },
}

impl DmlAst {
    pub fn compile(&self, dialect: &impl Dialect) -> OrmdanticResult<CompiledQuery> {
        compile_dml_ast(dialect, self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlAst {
    operations: Vec<SchemaOperation>,
}

impl DdlAst {
    pub fn new(operations: Vec<SchemaOperation>) -> Self {
        Self { operations }
    }

    pub fn from_diff(diff: SchemaDiff) -> Self {
        Self {
            operations: diff.operations().to_vec(),
        }
    }

    pub fn compile(&self, dialect: &impl Dialect) -> OrmdanticResult<Vec<CompiledQuery>> {
        DdlCompiler::compile(dialect, &self.operations)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectInPlan {
    pub(crate) parent_table: String,
    pub(crate) child_table: String,
    pub(crate) parent_key_columns: Vec<String>,
    pub(crate) child_key_columns: Vec<String>,
    pub(crate) batch_size: usize,
}

impl SelectInPlan {
    pub fn new(
        parent_table: impl Into<String>,
        child_table: impl Into<String>,
        parent_key_columns: Vec<String>,
        child_key_columns: Vec<String>,
    ) -> Self {
        Self {
            parent_table: parent_table.into(),
            child_table: child_table.into(),
            parent_key_columns,
            child_key_columns,
            batch_size: 500,
        }
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    pub fn query_for_batch(&self, param_names: Vec<String>) -> SelectInQuery {
        SelectInQuery {
            plan: self.clone(),
            param_names,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectInQuery {
    pub(crate) plan: SelectInPlan,
    pub(crate) param_names: Vec<String>,
}

impl SelectInQuery {
    pub fn compile(&self, dialect: &impl Dialect) -> OrmdanticResult<CompiledQuery> {
        compile_select_in_query(dialect, self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSpec {
    table: String,
    alias: String,
    left_alias: String,
    left_column: String,
    right_alias: String,
    right_column: String,
}

impl JoinSpec {
    pub fn left_join(
        table: impl Into<String>,
        alias: impl Into<String>,
        left_alias: impl Into<String>,
        left_column: impl Into<String>,
        right_alias: impl Into<String>,
        right_column: impl Into<String>,
    ) -> Self {
        Self {
            table: table.into(),
            alias: alias.into(),
            left_alias: left_alias.into(),
            left_column: left_column.into(),
            right_alias: right_alias.into(),
            right_column: right_column.into(),
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn left_alias(&self) -> &str {
        &self.left_alias
    }

    pub fn left_column(&self) -> &str {
        &self.left_column
    }

    pub fn right_alias(&self) -> &str {
        &self.right_alias
    }

    pub fn right_column(&self) -> &str {
        &self.right_column
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedSelectColumn {
    table_alias: String,
    column: String,
    alias: String,
}

impl JoinedSelectColumn {
    pub fn aliased(
        table_alias: impl Into<String>,
        column: impl Into<String>,
        alias: impl Into<String>,
    ) -> Self {
        Self {
            table_alias: table_alias.into(),
            column: column.into(),
            alias: alias.into(),
        }
    }

    pub fn table_alias(&self) -> &str {
        &self.table_alias
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedFilter {
    table_alias: String,
    filter: Filter,
}

impl JoinedFilter {
    pub fn new(table_alias: impl Into<String>, filter: Filter) -> Self {
        Self {
            table_alias: table_alias.into(),
            filter,
        }
    }

    pub fn table_alias(&self) -> &str {
        &self.table_alias
    }

    pub fn filter(&self) -> &Filter {
        &self.filter
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedOrderBy {
    table_alias: String,
    order_by: OrderBy,
}

impl JoinedOrderBy {
    pub fn new(table_alias: impl Into<String>, order_by: OrderBy) -> Self {
        Self {
            table_alias: table_alias.into(),
            order_by,
        }
    }

    pub fn table_alias(&self) -> &str {
        &self.table_alias
    }

    pub fn order_by(&self) -> &OrderBy {
        &self.order_by
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryAst {
    Select {
        table: TableRef,
        columns: Vec<SelectColumn>,
        filters: Vec<Filter>,
        order_by: Vec<OrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    JoinedSelect {
        table: TableRef,
        columns: Vec<JoinedSelectColumn>,
        joins: Vec<JoinSpec>,
        filters: Vec<Filter>,
        relationship_filters: Vec<JoinedFilter>,
        order_by: Vec<OrderBy>,
        relationship_order_by: Vec<JoinedOrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    Count {
        table: TableRef,
        filters: Vec<Filter>,
    },
    Insert {
        table: TableRef,
        columns: Vec<String>,
    },
    Update {
        table: TableRef,
        columns: Vec<String>,
        pk: String,
    },
    Upsert {
        table: TableRef,
        columns: Vec<String>,
        pk: String,
    },
    Delete {
        table: TableRef,
        pk: String,
    },
}

impl QueryAst {
    pub fn compile(&self, dialect: &impl Dialect) -> OrmdanticResult<CompiledQuery> {
        compile_query_ast(dialect, self)
    }
}

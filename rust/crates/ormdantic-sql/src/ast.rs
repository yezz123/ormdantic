use std::collections::HashSet;

use ormdantic_core::OrmdanticResult;
use ormdantic_dialects::Dialect;
use ormdantic_schema::{SchemaDiff, SchemaOperation};

use crate::compiler::{
    compile_dml_ast, compile_query_ast, compile_select_ast, compile_select_in_query, DdlCompiler,
};
use crate::Filter;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
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
    decimal: bool,
}

impl OrderBy {
    pub fn new(column: impl Into<String>, direction: SortDirection) -> Self {
        Self {
            column: column.into(),
            direction,
            decimal: false,
        }
    }

    pub fn decimal(mut self, decimal: bool) -> Self {
        self.decimal = decimal;
        self
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn direction(&self) -> &SortDirection {
        &self.direction
    }

    pub fn is_decimal(&self) -> bool {
        self.decimal
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
    Window {
        expr: Box<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderExpr>,
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
    Subquery(Box<SelectAst>),
    Exists {
        select: Box<SelectAst>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        select: Box<SelectAst>,
        negated: bool,
    },
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
pub struct CommonTableExpr {
    pub(crate) name: String,
    pub(crate) query: SelectAst,
    pub(crate) columns: Vec<String>,
    pub(crate) recursive: bool,
}

impl CommonTableExpr {
    pub fn new(name: impl Into<String>, query: SelectAst) -> Self {
        Self {
            name: name.into(),
            query,
            columns: Vec::new(),
            recursive: false,
        }
    }

    pub fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    pub fn recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectAst {
    pub(crate) ctes: Vec<CommonTableExpr>,
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
            ctes: Vec::new(),
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

    pub fn with_cte(mut self, cte: CommonTableExpr) -> Self {
        self.ctes.push(cte);
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

    pub fn rewrite_sqlite_decimal_columns(
        mut self,
        decimal_columns: &HashSet<String>,
        table_names: &[String],
    ) -> Self {
        if decimal_columns.is_empty() {
            return self;
        }
        self.projections = self
            .projections
            .into_iter()
            .map(|projection| Projection {
                expr: rewrite_sqlite_decimal_expr(projection.expr, decimal_columns, table_names),
                alias: projection.alias,
            })
            .collect();
        self.joins = self
            .joins
            .into_iter()
            .map(|join| JoinAst {
                kind: join.kind,
                source: join.source,
                on: join
                    .on
                    .map(|expr| rewrite_sqlite_decimal_expr(expr, decimal_columns, table_names)),
            })
            .collect();
        self.where_expr = self
            .where_expr
            .map(|expr| rewrite_sqlite_decimal_expr(expr, decimal_columns, table_names));
        self.group_by = self
            .group_by
            .into_iter()
            .map(|expr| rewrite_sqlite_decimal_expr(expr, decimal_columns, table_names))
            .collect();
        self.having = self
            .having
            .map(|expr| rewrite_sqlite_decimal_expr(expr, decimal_columns, table_names));
        self.order_by = self
            .order_by
            .into_iter()
            .map(|order| rewrite_sqlite_decimal_order(order, decimal_columns, table_names))
            .collect();
        self
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

    pub fn rewrite_sqlite_decimal_columns(
        self,
        decimal_columns: &HashSet<String>,
        table_names: &[String],
    ) -> Self {
        if decimal_columns.is_empty() {
            return self;
        }
        match self {
            Self::Insert {
                table,
                columns,
                rows,
                returning,
            } => Self::Insert {
                table,
                columns,
                rows,
                returning,
            },
            Self::Update {
                table,
                assignments,
                where_expr,
                returning,
            } => Self::Update {
                table,
                assignments,
                where_expr: where_expr
                    .map(|expr| rewrite_sqlite_decimal_expr(expr, decimal_columns, table_names)),
                returning,
            },
            Self::Delete {
                table,
                where_expr,
                returning,
            } => Self::Delete {
                table,
                where_expr: where_expr
                    .map(|expr| rewrite_sqlite_decimal_expr(expr, decimal_columns, table_names)),
                returning,
            },
            Self::Upsert {
                table,
                columns,
                rows,
                conflict_target,
                update_assignments,
                returning,
            } => Self::Upsert {
                table,
                columns,
                rows,
                conflict_target,
                update_assignments,
                returning,
            },
        }
    }
}

fn rewrite_sqlite_decimal_order(
    order: OrderExpr,
    decimal_columns: &HashSet<String>,
    table_names: &[String],
) -> OrderExpr {
    let expr = if is_sqlite_decimal_column(&order.expr, decimal_columns, table_names) {
        Expr::Function {
            name: "ormdantic_decimal_sort_key".to_string(),
            args: vec![order.expr],
        }
    } else {
        rewrite_sqlite_decimal_expr(order.expr, decimal_columns, table_names)
    };
    OrderExpr {
        expr,
        direction: order.direction,
        nulls: order.nulls,
    }
}

fn rewrite_sqlite_decimal_expr(
    expr: Expr,
    decimal_columns: &HashSet<String>,
    table_names: &[String],
) -> Expr {
    match expr {
        Expr::Binary { left, op, right } => {
            let left = rewrite_sqlite_decimal_expr(*left, decimal_columns, table_names);
            let right = rewrite_sqlite_decimal_expr(*right, decimal_columns, table_names);
            if is_decimal_comparison_op(&op)
                && (is_sqlite_decimal_column(&left, decimal_columns, table_names)
                    || is_sqlite_decimal_column(&right, decimal_columns, table_names))
            {
                return sqlite_decimal_comparison(left, op, right);
            }
            Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(rewrite_sqlite_decimal_expr(
                *expr,
                decimal_columns,
                table_names,
            )),
        },
        Expr::Function { name, args } => Expr::Function {
            name,
            args: args
                .into_iter()
                .map(|arg| rewrite_sqlite_decimal_expr(arg, decimal_columns, table_names))
                .collect(),
        },
        Expr::Window {
            expr,
            partition_by,
            order_by,
        } => Expr::Window {
            expr: Box::new(rewrite_sqlite_decimal_expr(
                *expr,
                decimal_columns,
                table_names,
            )),
            partition_by: partition_by
                .into_iter()
                .map(|expr| rewrite_sqlite_decimal_expr(expr, decimal_columns, table_names))
                .collect(),
            order_by: order_by
                .into_iter()
                .map(|order| rewrite_sqlite_decimal_order(order, decimal_columns, table_names))
                .collect(),
        },
        Expr::Between { expr, low, high } => {
            let expr = rewrite_sqlite_decimal_expr(*expr, decimal_columns, table_names);
            let low = rewrite_sqlite_decimal_expr(*low, decimal_columns, table_names);
            let high = rewrite_sqlite_decimal_expr(*high, decimal_columns, table_names);
            if is_sqlite_decimal_column(&expr, decimal_columns, table_names) {
                return Expr::Binary {
                    left: Box::new(sqlite_decimal_comparison(expr.clone(), BinaryOp::Ge, low)),
                    op: BinaryOp::And,
                    right: Box::new(sqlite_decimal_comparison(expr, BinaryOp::Le, high)),
                };
            }
            Expr::Between {
                expr: Box::new(expr),
                low: Box::new(low),
                high: Box::new(high),
            }
        }
        Expr::InList {
            expr,
            values,
            negated,
        } => {
            let expr = rewrite_sqlite_decimal_expr(*expr, decimal_columns, table_names);
            let values = values
                .into_iter()
                .map(|value| rewrite_sqlite_decimal_expr(value, decimal_columns, table_names))
                .collect::<Vec<_>>();
            if !values.is_empty() && is_sqlite_decimal_column(&expr, decimal_columns, table_names) {
                let comparisons = values
                    .into_iter()
                    .map(|value| sqlite_decimal_comparison(expr.clone(), BinaryOp::Eq, value))
                    .collect::<Vec<_>>();
                let comparison = combine_sqlite_decimal_comparisons(comparisons);
                return if negated {
                    Expr::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(comparison),
                    }
                } else {
                    comparison
                };
            }
            Expr::InList {
                expr: Box::new(expr),
                values,
                negated,
            }
        }
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(when, then)| {
                    (
                        rewrite_sqlite_decimal_expr(when, decimal_columns, table_names),
                        rewrite_sqlite_decimal_expr(then, decimal_columns, table_names),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|expr| {
                Box::new(rewrite_sqlite_decimal_expr(
                    *expr,
                    decimal_columns,
                    table_names,
                ))
            }),
        },
        Expr::Cast { expr, type_name } => Expr::Cast {
            expr: Box::new(rewrite_sqlite_decimal_expr(
                *expr,
                decimal_columns,
                table_names,
            )),
            type_name,
        },
        Expr::Tuple(values) => Expr::Tuple(
            values
                .into_iter()
                .map(|value| rewrite_sqlite_decimal_expr(value, decimal_columns, table_names))
                .collect(),
        ),
        Expr::Subquery(select) => Expr::Subquery(select),
        Expr::Exists { select, negated } => Expr::Exists { select, negated },
        Expr::InSubquery {
            expr,
            select,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(rewrite_sqlite_decimal_expr(
                *expr,
                decimal_columns,
                table_names,
            )),
            select,
            negated,
        },
        expr => expr,
    }
}

fn is_sqlite_decimal_column(
    expr: &Expr,
    decimal_columns: &HashSet<String>,
    table_names: &[String],
) -> bool {
    match expr {
        Expr::Column { table, name } if decimal_columns.contains(name) => table
            .as_ref()
            .is_none_or(|table| table_names.iter().any(|known| known == table)),
        _ => false,
    }
}

fn is_decimal_comparison_op(op: &BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    )
}

fn sqlite_decimal_comparison(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(Expr::Function {
            name: "ormdantic_decimal_cmp".to_string(),
            args: vec![left, right],
        }),
        op,
        right: Box::new(Expr::Literal(SqlLiteral::Integer(0))),
    }
}

fn combine_sqlite_decimal_comparisons(comparisons: Vec<Expr>) -> Expr {
    let mut comparisons = comparisons.into_iter();
    let first = comparisons
        .next()
        .expect("non-empty decimal IN comparisons are validated before combining");
    comparisons.fold(first, |left, right| Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::Or,
        right: Box::new(right),
    })
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

use ormdantic_core::{OrmdanticError, OrmdanticResult};
use ormdantic_dialects::Dialect;
use ormdantic_schema::{SchemaDiff, SchemaOperation};

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
pub enum Filter {
    Eq { column: String, param: String },
    Ne { column: String, param: String },
    Lt { column: String, param: String },
    Le { column: String, param: String },
    Gt { column: String, param: String },
    Ge { column: String, param: String },
    Like { column: String, param: String },
    ILike { column: String, param: String },
    In { column: String, params: Vec<String> },
    NotIn { column: String, params: Vec<String> },
    IsNull { column: String },
    IsNotNull { column: String },
    And(Vec<Filter>),
    Or(Vec<Filter>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnRef {
    name: String,
}

impl ColumnRef {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BindParam {
    name: String,
}

impl BindParam {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    ILike,
}

impl ComparisonOp {
    fn sql_operator(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::Like | Self::ILike => "LIKE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BoolOp {
    And,
    Or,
}

impl BoolOp {
    fn sql_operator(&self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::Or => "OR",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PredicateExpr {
    Compare {
        left: ColumnRef,
        op: ComparisonOp,
        right: BindParam,
    },
    InList {
        left: ColumnRef,
        params: Vec<BindParam>,
        negated: bool,
    },
    NullCheck {
        expr: ColumnRef,
        negated: bool,
    },
    Bool {
        op: BoolOp,
        exprs: Vec<PredicateExpr>,
    },
}

impl From<&Filter> for PredicateExpr {
    fn from(filter: &Filter) -> Self {
        match filter {
            Filter::Eq { column, param } => comparison_expr(column, ComparisonOp::Eq, param),
            Filter::Ne { column, param } => comparison_expr(column, ComparisonOp::Ne, param),
            Filter::Lt { column, param } => comparison_expr(column, ComparisonOp::Lt, param),
            Filter::Le { column, param } => comparison_expr(column, ComparisonOp::Le, param),
            Filter::Gt { column, param } => comparison_expr(column, ComparisonOp::Gt, param),
            Filter::Ge { column, param } => comparison_expr(column, ComparisonOp::Ge, param),
            Filter::Like { column, param } => comparison_expr(column, ComparisonOp::Like, param),
            Filter::ILike { column, param } => comparison_expr(column, ComparisonOp::ILike, param),
            Filter::In { column, params } => PredicateExpr::InList {
                left: ColumnRef::new(column.clone()),
                params: params.iter().cloned().map(BindParam::new).collect(),
                negated: false,
            },
            Filter::NotIn { column, params } => PredicateExpr::InList {
                left: ColumnRef::new(column.clone()),
                params: params.iter().cloned().map(BindParam::new).collect(),
                negated: true,
            },
            Filter::IsNull { column } => PredicateExpr::NullCheck {
                expr: ColumnRef::new(column.clone()),
                negated: false,
            },
            Filter::IsNotNull { column } => PredicateExpr::NullCheck {
                expr: ColumnRef::new(column.clone()),
                negated: true,
            },
            Filter::And(filters) => PredicateExpr::Bool {
                op: BoolOp::And,
                exprs: filters.iter().map(PredicateExpr::from).collect(),
            },
            Filter::Or(filters) => PredicateExpr::Bool {
                op: BoolOp::Or,
                exprs: filters.iter().map(PredicateExpr::from).collect(),
            },
        }
    }
}

fn comparison_expr(column: &str, op: ComparisonOp, param: &str) -> PredicateExpr {
    PredicateExpr::Compare {
        left: ColumnRef::new(column),
        op,
        right: BindParam::new(param),
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
    expr: Expr,
    direction: SortDirection,
    nulls: Option<OrderNulls>,
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
pub struct WindowSpec {
    partition_by: Vec<Expr>,
    order_by: Vec<OrderExpr>,
}

impl WindowSpec {
    pub fn new(partition_by: Vec<Expr>, order_by: Vec<OrderExpr>) -> Self {
        Self {
            partition_by,
            order_by,
        }
    }
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
        over: Option<WindowSpec>,
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
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectAst>,
        negated: bool,
    },
    Exists(Box<SelectAst>),
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
    expr: Expr,
    alias: Option<String>,
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
    Table {
        name: String,
        alias: Option<String>,
    },
    Subquery {
        subquery: Box<SelectAst>,
        alias: String,
    },
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

pub type Subquery = SelectAst;

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
    kind: JoinKind,
    source: TableSource,
    on: Option<Expr>,
}

impl JoinAst {
    pub fn new(kind: JoinKind, source: TableSource, on: Option<Expr>) -> Self {
        Self { kind, source, on }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CteAst {
    name: String,
    query: Box<SelectAst>,
    recursive: bool,
}

impl CteAst {
    pub fn new(name: impl Into<String>, query: SelectAst) -> Self {
        Self {
            name: name.into(),
            query: Box::new(query),
            recursive: false,
        }
    }

    pub fn recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectAst {
    projections: Vec<Projection>,
    from: Option<TableSource>,
    joins: Vec<JoinAst>,
    where_expr: Option<Expr>,
    group_by: Vec<Expr>,
    having: Option<Expr>,
    order_by: Vec<OrderExpr>,
    distinct: bool,
    limit: Option<usize>,
    offset: Option<usize>,
    ctes: Vec<CteAst>,
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
            ctes: Vec::new(),
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

    pub fn with_cte(mut self, cte: CteAst) -> Self {
        self.ctes.push(cte);
        self
    }

    pub fn compile(&self, dialect: &impl Dialect) -> OrmdanticResult<CompiledQuery> {
        let mut params = Vec::new();
        let mut bind_index = 1;
        let sql = render_select_ast(dialect, self, &mut params, &mut bind_index)?;
        Ok(CompiledQuery::new(sql, params, QueryOperation::Select))
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
        let mut params = Vec::new();
        let mut bind_index = 1;
        let (sql, operation) = render_dml_ast(dialect, self, &mut params, &mut bind_index)?;
        Ok(CompiledQuery::new(sql, params, operation))
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

pub struct DdlCompiler;

impl DdlCompiler {
    pub fn compile(
        dialect: &impl Dialect,
        operations: &[SchemaOperation],
    ) -> OrmdanticResult<Vec<CompiledQuery>> {
        let mut compiled = Vec::new();
        for operation in operations {
            for sql in dialect.compile_schema_operation(operation)? {
                if !sql.is_empty() {
                    compiled.push(CompiledQuery::new(sql, Vec::new(), QueryOperation::Ddl));
                }
            }
        }
        Ok(compiled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectInPlan {
    parent_table: String,
    child_table: String,
    parent_key_columns: Vec<String>,
    child_key_columns: Vec<String>,
    batch_size: usize,
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
    plan: SelectInPlan,
    param_names: Vec<String>,
}

impl SelectInQuery {
    pub fn compile(&self, dialect: &impl Dialect) -> OrmdanticResult<CompiledQuery> {
        let mut bind_index = 1;
        let selected = "*";
        let mut params = Vec::new();
        let predicates = self
            .plan
            .child_key_columns
            .iter()
            .map(|column| {
                let placeholders = self
                    .param_names
                    .iter()
                    .map(|param| {
                        params.push(param.clone());
                        let placeholder = dialect.placeholder(bind_index);
                        bind_index += 1;
                        placeholder
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} IN ({placeholders})", dialect.quote_ident(column))
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        Ok(CompiledQuery::new(
            format!(
                "SELECT {selected} FROM {} WHERE {predicates}",
                dialect.quote_ident(&self.plan.child_table)
            ),
            params,
            QueryOperation::Select,
        ))
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
        order_by: Vec<OrderBy>,
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
        match self {
            Self::Select {
                table,
                columns,
                filters,
                order_by,
                limit,
                offset,
            } => compile_select(dialect, table, columns, filters, order_by, *limit, *offset),
            Self::JoinedSelect {
                table,
                columns,
                joins,
                filters,
                order_by,
                limit,
                offset,
            } => compile_joined_select(
                dialect, table, columns, joins, filters, order_by, *limit, *offset,
            ),
            Self::Count { table, filters } => compile_count(dialect, table, filters),
            Self::Insert { table, columns } => compile_insert(dialect, table, columns),
            Self::Update { table, columns, pk } => compile_update(dialect, table, columns, pk),
            Self::Upsert { table, columns, pk } => compile_upsert(dialect, table, columns, pk),
            Self::Delete { table, pk } => compile_delete(dialect, table, pk),
        }
    }
}

fn render_select_ast(
    dialect: &impl Dialect,
    select: &SelectAst,
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<String> {
    if select.projections.is_empty() {
        return Err(OrmdanticError::SqlCompile {
            message: "select query requires at least one projection".to_string(),
        });
    }

    let mut sql = String::new();
    if !select.ctes.is_empty() {
        let recursive = select.ctes.iter().any(|cte| cte.recursive);
        sql.push_str(if recursive {
            "WITH RECURSIVE "
        } else {
            "WITH "
        });
        sql.push_str(
            &select
                .ctes
                .iter()
                .map(|cte| {
                    render_select_ast(dialect, &cte.query, params, bind_index)
                        .map(|query| format!("{} AS ({query})", dialect.quote_ident(&cte.name)))
                })
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", "),
        );
        sql.push(' ');
    }

    sql.push_str("SELECT ");
    if select.distinct {
        sql.push_str("DISTINCT ");
    }
    sql.push_str(
        &select
            .projections
            .iter()
            .map(|projection| {
                let rendered = render_expr(dialect, &projection.expr, params, bind_index)?;
                Ok(match &projection.alias {
                    Some(alias) => format!("{rendered} AS {}", dialect.quote_ident(alias)),
                    None => rendered,
                })
            })
            .collect::<OrmdanticResult<Vec<_>>>()?
            .join(", "),
    );

    if let Some(source) = &select.from {
        sql.push_str(" FROM ");
        sql.push_str(&render_table_source(dialect, source, params, bind_index)?);
    }
    for join in &select.joins {
        sql.push(' ');
        sql.push_str(match join.kind {
            JoinKind::Inner => "JOIN",
            JoinKind::Left => "LEFT JOIN",
            JoinKind::Right => "RIGHT JOIN",
            JoinKind::Full => "FULL JOIN",
            JoinKind::Cross => "CROSS JOIN",
        });
        sql.push(' ');
        sql.push_str(&render_table_source(
            dialect,
            &join.source,
            params,
            bind_index,
        )?);
        if let Some(on) = &join.on {
            sql.push_str(" ON ");
            sql.push_str(&render_expr(dialect, on, params, bind_index)?);
        }
    }
    if let Some(where_expr) = &select.where_expr {
        sql.push_str(" WHERE ");
        sql.push_str(&render_expr(dialect, where_expr, params, bind_index)?);
    }
    if !select.group_by.is_empty() {
        sql.push_str(" GROUP BY ");
        sql.push_str(
            &select
                .group_by
                .iter()
                .map(|expr| render_expr(dialect, expr, params, bind_index))
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", "),
        );
    }
    if let Some(having) = &select.having {
        sql.push_str(" HAVING ");
        sql.push_str(&render_expr(dialect, having, params, bind_index)?);
    }
    if !select.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(
            &select
                .order_by
                .iter()
                .map(|order| render_order_expr(dialect, order, params, bind_index))
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", "),
        );
    }
    if let Some(limit) = select.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = select.offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    Ok(sql)
}

fn render_dml_ast(
    dialect: &impl Dialect,
    dml: &DmlAst,
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<(String, QueryOperation)> {
    match dml {
        DmlAst::Insert {
            table,
            columns,
            rows,
            returning,
        } => {
            require_columns(columns, "insert")?;
            if rows.is_empty() {
                return Err(OrmdanticError::SqlCompile {
                    message: "insert query requires at least one row".to_string(),
                });
            }
            let rendered_rows = rows
                .iter()
                .map(|row| {
                    Ok(format!(
                        "({})",
                        row.iter()
                            .map(|expr| render_expr(dialect, expr, params, bind_index))
                            .collect::<OrmdanticResult<Vec<_>>>()?
                            .join(", ")
                    ))
                })
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ");
            let mut sql = format!(
                "INSERT INTO {} ({}) VALUES {rendered_rows}",
                render_table_source(dialect, table, params, bind_index)?,
                column_list(dialect, columns)
            );
            append_returning(dialect, &mut sql, returning, params, bind_index)?;
            Ok((sql, QueryOperation::Insert))
        }
        DmlAst::Update {
            table,
            assignments,
            where_expr,
            returning,
        } => {
            if assignments.is_empty() {
                return Err(OrmdanticError::SqlCompile {
                    message: "update query requires at least one assignment".to_string(),
                });
            }
            let assignments = assignments
                .iter()
                .map(|(column, expr)| {
                    Ok(format!(
                        "{} = {}",
                        dialect.quote_ident(column),
                        render_expr(dialect, expr, params, bind_index)?
                    ))
                })
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ");
            let mut sql = format!(
                "UPDATE {} SET {assignments}",
                render_table_source(dialect, table, params, bind_index)?
            );
            if let Some(where_expr) = where_expr {
                sql.push_str(" WHERE ");
                sql.push_str(&render_expr(dialect, where_expr, params, bind_index)?);
            }
            append_returning(dialect, &mut sql, returning, params, bind_index)?;
            Ok((sql, QueryOperation::Update))
        }
        DmlAst::Delete {
            table,
            where_expr,
            returning,
        } => {
            let mut sql = format!(
                "DELETE FROM {}",
                render_table_source(dialect, table, params, bind_index)?
            );
            if let Some(where_expr) = where_expr {
                sql.push_str(" WHERE ");
                sql.push_str(&render_expr(dialect, where_expr, params, bind_index)?);
            }
            append_returning(dialect, &mut sql, returning, params, bind_index)?;
            Ok((sql, QueryOperation::Delete))
        }
        DmlAst::Upsert {
            table,
            columns,
            rows,
            conflict_target,
            update_assignments,
            returning,
        } => {
            require_columns(columns, "upsert")?;
            if rows.is_empty() {
                return Err(OrmdanticError::SqlCompile {
                    message: "upsert query requires at least one row".to_string(),
                });
            }
            let rendered_rows = rows
                .iter()
                .map(|row| {
                    Ok(format!(
                        "({})",
                        row.iter()
                            .map(|expr| render_expr(dialect, expr, params, bind_index))
                            .collect::<OrmdanticResult<Vec<_>>>()?
                            .join(", ")
                    ))
                })
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ");
            let mut sql = format!(
                "INSERT INTO {} ({}) VALUES {rendered_rows}",
                render_table_source(dialect, table, params, bind_index)?,
                column_list(dialect, columns)
            );
            let target = conflict_target
                .first()
                .or_else(|| columns.first())
                .ok_or_else(|| OrmdanticError::SqlCompile {
                    message: "upsert query requires a conflict target".to_string(),
                })?;
            let update_columns = if update_assignments.is_empty() {
                columns
                    .iter()
                    .filter(|column| *column != target)
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                update_assignments
                    .iter()
                    .map(|(column, _)| column.clone())
                    .collect::<Vec<_>>()
            };
            sql.push(' ');
            sql.push_str(&dialect.upsert_conflict_clause(target, &update_columns));
            append_returning(dialect, &mut sql, returning, params, bind_index)?;
            Ok((sql, QueryOperation::Upsert))
        }
    }
}

fn render_expr(
    dialect: &impl Dialect,
    expr: &Expr,
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<String> {
    Ok(match expr {
        Expr::Column { table, name } => match table {
            Some(table) => format!(
                "{}.{}",
                dialect.quote_ident(table),
                dialect.quote_ident(name)
            ),
            None => dialect.quote_ident(name),
        },
        Expr::Param(name) => {
            params.push(name.clone());
            let placeholder = dialect.placeholder(*bind_index);
            *bind_index += 1;
            placeholder
        }
        Expr::Literal(literal) => render_literal(literal),
        Expr::Binary { left, op, right } => format!(
            "({} {} {})",
            render_expr(dialect, left, params, bind_index)?,
            render_binary_op(op),
            render_expr(dialect, right, params, bind_index)?
        ),
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => format!("(NOT {})", render_expr(dialect, expr, params, bind_index)?),
            UnaryOp::Neg => format!("(-{})", render_expr(dialect, expr, params, bind_index)?),
            UnaryOp::IsNull => {
                format!(
                    "({} IS NULL)",
                    render_expr(dialect, expr, params, bind_index)?
                )
            }
            UnaryOp::IsNotNull => format!(
                "({} IS NOT NULL)",
                render_expr(dialect, expr, params, bind_index)?
            ),
        },
        Expr::Function { name, args, over } => {
            let args = args
                .iter()
                .map(|arg| render_expr(dialect, arg, params, bind_index))
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ");
            let mut sql = format!("{name}({args})");
            if let Some(window) = over {
                sql.push_str(" OVER (");
                let mut parts = Vec::new();
                if !window.partition_by.is_empty() {
                    parts.push(format!(
                        "PARTITION BY {}",
                        window
                            .partition_by
                            .iter()
                            .map(|expr| render_expr(dialect, expr, params, bind_index))
                            .collect::<OrmdanticResult<Vec<_>>>()?
                            .join(", ")
                    ));
                }
                if !window.order_by.is_empty() {
                    parts.push(format!(
                        "ORDER BY {}",
                        window
                            .order_by
                            .iter()
                            .map(|order| render_order_expr(dialect, order, params, bind_index))
                            .collect::<OrmdanticResult<Vec<_>>>()?
                            .join(", ")
                    ));
                }
                sql.push_str(&parts.join(" "));
                sql.push(')');
            }
            sql
        }
        Expr::Between { expr, low, high } => format!(
            "({} BETWEEN {} AND {})",
            render_expr(dialect, expr, params, bind_index)?,
            render_expr(dialect, low, params, bind_index)?,
            render_expr(dialect, high, params, bind_index)?
        ),
        Expr::InList {
            expr,
            values,
            negated,
        } => {
            let operator = if *negated { "NOT IN" } else { "IN" };
            format!(
                "({} {operator} ({}))",
                render_expr(dialect, expr, params, bind_index)?,
                values
                    .iter()
                    .map(|value| render_expr(dialect, value, params, bind_index))
                    .collect::<OrmdanticResult<Vec<_>>>()?
                    .join(", ")
            )
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let operator = if *negated { "NOT IN" } else { "IN" };
            format!(
                "({} {operator} ({}))",
                render_expr(dialect, expr, params, bind_index)?,
                render_select_ast(dialect, subquery, params, bind_index)?
            )
        }
        Expr::Exists(subquery) => format!(
            "EXISTS ({})",
            render_select_ast(dialect, subquery, params, bind_index)?
        ),
        Expr::Case { whens, else_expr } => {
            let mut sql = "CASE".to_string();
            for (condition, value) in whens {
                sql.push_str(" WHEN ");
                sql.push_str(&render_expr(dialect, condition, params, bind_index)?);
                sql.push_str(" THEN ");
                sql.push_str(&render_expr(dialect, value, params, bind_index)?);
            }
            if let Some(else_expr) = else_expr {
                sql.push_str(" ELSE ");
                sql.push_str(&render_expr(dialect, else_expr, params, bind_index)?);
            }
            sql.push_str(" END");
            sql
        }
        Expr::Cast { expr, type_name } => format!(
            "CAST({} AS {type_name})",
            render_expr(dialect, expr, params, bind_index)?
        ),
        Expr::Tuple(values) => format!(
            "({})",
            values
                .iter()
                .map(|value| render_expr(dialect, value, params, bind_index))
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ")
        ),
        Expr::RawSafe(sql) => sql.clone(),
    })
}

fn render_table_source(
    dialect: &impl Dialect,
    source: &TableSource,
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<String> {
    Ok(match source {
        TableSource::Table { name, alias } => match alias {
            Some(alias) => format!(
                "{} AS {}",
                dialect.quote_ident(name),
                dialect.quote_ident(alias)
            ),
            None => dialect.quote_ident(name),
        },
        TableSource::Subquery { subquery, alias } => format!(
            "({}) AS {}",
            render_select_ast(dialect, subquery, params, bind_index)?,
            dialect.quote_ident(alias)
        ),
        TableSource::RawSafe(sql) => sql.clone(),
    })
}

fn render_order_expr(
    dialect: &impl Dialect,
    order: &OrderExpr,
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<String> {
    let direction = match order.direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    let nulls = match order.nulls {
        Some(OrderNulls::First) => " NULLS FIRST",
        Some(OrderNulls::Last) => " NULLS LAST",
        None => "",
    };
    Ok(format!(
        "{} {direction}{nulls}",
        render_expr(dialect, &order.expr, params, bind_index)?
    ))
}

fn append_returning(
    dialect: &impl Dialect,
    sql: &mut String,
    returning: &[Expr],
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<()> {
    if returning.is_empty() {
        return Ok(());
    }
    if !dialect.supports_returning() {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "RETURNING".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    sql.push_str(" RETURNING ");
    sql.push_str(
        &returning
            .iter()
            .map(|expr| render_expr(dialect, expr, params, bind_index))
            .collect::<OrmdanticResult<Vec<_>>>()?
            .join(", "),
    );
    Ok(())
}

fn render_literal(literal: &SqlLiteral) -> String {
    match literal {
        SqlLiteral::Null => "NULL".to_string(),
        SqlLiteral::Integer(value) => value.to_string(),
        SqlLiteral::String(value) => format!("'{}'", value.replace('\'', "''")),
        SqlLiteral::Boolean(true) => "TRUE".to_string(),
        SqlLiteral::Boolean(false) => "FALSE".to_string(),
    }
}

fn render_binary_op(op: &BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Like => "LIKE",
        BinaryOp::ILike => "ILIKE",
    }
}

#[allow(clippy::too_many_arguments)]
fn compile_joined_select(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[JoinedSelectColumn],
    joins: &[JoinSpec],
    filters: &[Filter],
    order_by: &[OrderBy],
    limit: Option<usize>,
    offset: Option<usize>,
) -> OrmdanticResult<CompiledQuery> {
    require_joined_select_columns(columns)?;
    let mut bind_index = 1;
    let selected = columns
        .iter()
        .map(|column| joined_selected_column(dialect, column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "SELECT {selected} FROM {}",
        dialect.quote_ident(table.name())
    );
    for join in joins {
        sql.push_str(" LEFT JOIN ");
        sql.push_str(&dialect.quote_ident(join.table()));
        sql.push_str(" AS ");
        sql.push_str(&dialect.quote_ident(join.alias()));
        sql.push_str(" ON ");
        sql.push_str(&qualified_alias_column(
            dialect,
            join.left_alias(),
            join.left_column(),
        ));
        sql.push_str(" = ");
        sql.push_str(&qualified_alias_column(
            dialect,
            join.right_alias(),
            join.right_column(),
        ));
    }
    let params =
        append_filters_with_alias(dialect, &mut sql, filters, &mut bind_index, table.name());
    append_ordering_with_alias(dialect, &mut sql, order_by, table.name());
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    Ok(CompiledQuery::new(sql, params, QueryOperation::Select))
}

fn compile_select(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[SelectColumn],
    filters: &[Filter],
    order_by: &[OrderBy],
    limit: Option<usize>,
    offset: Option<usize>,
) -> OrmdanticResult<CompiledQuery> {
    require_select_columns(columns)?;
    let mut bind_index = 1;
    let selected = columns
        .iter()
        .map(|column| selected_column(dialect, table, column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "SELECT {selected} FROM {}",
        dialect.quote_ident(table.name())
    );
    let params = append_filters(dialect, &mut sql, filters, &mut bind_index);
    append_ordering(dialect, &mut sql, order_by);
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    Ok(CompiledQuery::new(sql, params, QueryOperation::Select))
}

fn compile_count(
    dialect: &impl Dialect,
    table: &TableRef,
    filters: &[Filter],
) -> OrmdanticResult<CompiledQuery> {
    let mut bind_index = 1;
    let mut sql = format!("SELECT COUNT(*) FROM {}", dialect.quote_ident(table.name()));
    let params = append_filters(dialect, &mut sql, filters, &mut bind_index);
    Ok(CompiledQuery::new(sql, params, QueryOperation::Count))
}

fn compile_insert(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[String],
) -> OrmdanticResult<CompiledQuery> {
    require_columns(columns, "insert")?;
    let rendered_columns = column_list(dialect, columns);
    let placeholders = placeholders(dialect, 1, columns.len());
    Ok(CompiledQuery::new(
        format!(
            "INSERT INTO {} ({rendered_columns}) VALUES ({placeholders})",
            dialect.quote_ident(table.name())
        ),
        columns.to_vec(),
        QueryOperation::Insert,
    ))
}

fn compile_update(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[String],
    pk: &str,
) -> OrmdanticResult<CompiledQuery> {
    require_columns(columns, "update")?;
    let assignments = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            format!(
                "{} = {}",
                dialect.quote_ident(column),
                dialect.placeholder(idx + 1)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let pk_placeholder = dialect.placeholder(columns.len() + 1);
    let mut params = columns.to_vec();
    params.push(pk.to_string());
    Ok(CompiledQuery::new(
        format!(
            "UPDATE {} SET {assignments} WHERE {} = {pk_placeholder}",
            dialect.quote_ident(table.name()),
            dialect.quote_ident(pk)
        ),
        params,
        QueryOperation::Update,
    ))
}

fn compile_upsert(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[String],
    pk: &str,
) -> OrmdanticResult<CompiledQuery> {
    require_columns(columns, "upsert")?;
    let insert = compile_insert(dialect, table, columns)?;
    let update_columns = columns
        .iter()
        .filter(|column| column.as_str() != pk)
        .cloned()
        .collect::<Vec<_>>();
    Ok(CompiledQuery::new(
        format!(
            "{} {}",
            insert.sql(),
            dialect.upsert_conflict_clause(pk, &update_columns)
        ),
        columns.to_vec(),
        QueryOperation::Upsert,
    ))
}

fn compile_delete(
    dialect: &impl Dialect,
    table: &TableRef,
    pk: &str,
) -> OrmdanticResult<CompiledQuery> {
    Ok(CompiledQuery::new(
        format!(
            "DELETE FROM {} WHERE {} = {}",
            dialect.quote_ident(table.name()),
            dialect.quote_ident(pk),
            dialect.placeholder(1)
        ),
        vec![pk.to_string()],
        QueryOperation::Delete,
    ))
}

fn append_filters(
    dialect: &impl Dialect,
    sql: &mut String,
    filters: &[Filter],
    bind_index: &mut usize,
) -> Vec<String> {
    if filters.is_empty() {
        return Vec::new();
    }

    let mut params = Vec::new();
    let rendered = filters
        .iter()
        .map(|filter| {
            render_filter(dialect, filter, bind_index, &mut params, |column| {
                dialect.quote_ident(column)
            })
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    sql.push_str(" WHERE ");
    sql.push_str(&rendered);
    params
}

fn append_ordering(dialect: &impl Dialect, sql: &mut String, order_by: &[OrderBy]) {
    if order_by.is_empty() {
        return;
    }

    let rendered = order_by
        .iter()
        .map(|order| {
            let direction = match order.direction() {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            format!("{} {direction}", dialect.quote_ident(order.column()))
        })
        .collect::<Vec<_>>()
        .join(", ");
    sql.push_str(" ORDER BY ");
    sql.push_str(&rendered);
}

fn append_filters_with_alias(
    dialect: &impl Dialect,
    sql: &mut String,
    filters: &[Filter],
    bind_index: &mut usize,
    table_alias: &str,
) -> Vec<String> {
    if filters.is_empty() {
        return Vec::new();
    }

    let mut params = Vec::new();
    let rendered = filters
        .iter()
        .map(|filter| {
            render_filter(dialect, filter, bind_index, &mut params, |column| {
                qualified_alias_column(dialect, table_alias, column)
            })
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    sql.push_str(" WHERE ");
    sql.push_str(&rendered);
    params
}

fn render_filter(
    dialect: &impl Dialect,
    filter: &Filter,
    bind_index: &mut usize,
    params: &mut Vec<String>,
    render_column: impl Fn(&str) -> String + Copy,
) -> String {
    let expression = PredicateExpr::from(filter);
    render_expression(dialect, &expression, bind_index, params, render_column)
}

fn render_expression(
    dialect: &impl Dialect,
    expression: &PredicateExpr,
    bind_index: &mut usize,
    params: &mut Vec<String>,
    render_column: impl Fn(&str) -> String + Copy,
) -> String {
    match expression {
        PredicateExpr::Compare { left, op, right } => render_comparison_expression(
            dialect,
            render_column(left.name()),
            op,
            right,
            bind_index,
            params,
        ),
        PredicateExpr::InList {
            left,
            params: names,
            negated,
        } => render_in_expression(
            dialect,
            render_column(left.name()),
            *negated,
            names,
            bind_index,
            params,
        ),
        PredicateExpr::NullCheck { expr, negated } => {
            let operator = if *negated { "IS NOT NULL" } else { "IS NULL" };
            format!("{} {operator}", render_column(expr.name()))
        }
        PredicateExpr::Bool { op, exprs } => {
            render_bool_expression(dialect, op, exprs, bind_index, params, render_column)
        }
    }
}

fn render_comparison_expression(
    dialect: &impl Dialect,
    column: String,
    operator: &ComparisonOp,
    param: &BindParam,
    bind_index: &mut usize,
    params: &mut Vec<String>,
) -> String {
    params.push(param.name().to_string());
    let placeholder = dialect.placeholder(*bind_index);
    *bind_index += 1;
    if operator == &ComparisonOp::ILike {
        return format!("LOWER({column}) LIKE LOWER({placeholder})");
    }
    format!("{} {} {placeholder}", column, operator.sql_operator())
}

fn render_in_expression(
    dialect: &impl Dialect,
    column: String,
    negated: bool,
    names: &[BindParam],
    bind_index: &mut usize,
    params: &mut Vec<String>,
) -> String {
    let operator = if negated { "NOT IN" } else { "IN" };
    if names.is_empty() {
        return if negated {
            "1 = 1".to_string()
        } else {
            "1 = 0".to_string()
        };
    }
    let placeholders = names
        .iter()
        .map(|name| {
            params.push(name.name().to_string());
            let placeholder = dialect.placeholder(*bind_index);
            *bind_index += 1;
            placeholder
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{column} {operator} ({placeholders})")
}

fn render_bool_expression(
    dialect: &impl Dialect,
    operator: &BoolOp,
    expressions: &[PredicateExpr],
    bind_index: &mut usize,
    params: &mut Vec<String>,
    render_column: impl Fn(&str) -> String + Copy,
) -> String {
    let rendered = expressions
        .iter()
        .map(|expression| render_expression(dialect, expression, bind_index, params, render_column))
        .collect::<Vec<_>>()
        .join(&format!(" {} ", operator.sql_operator()));
    format!("({rendered})")
}

fn append_ordering_with_alias(
    dialect: &impl Dialect,
    sql: &mut String,
    order_by: &[OrderBy],
    table_alias: &str,
) {
    if order_by.is_empty() {
        return;
    }

    let rendered = order_by
        .iter()
        .map(|order| {
            let direction = match order.direction() {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            format!(
                "{} {direction}",
                qualified_alias_column(dialect, table_alias, order.column())
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    sql.push_str(" ORDER BY ");
    sql.push_str(&rendered);
}

fn require_columns(columns: &[String], operation: &str) -> OrmdanticResult<()> {
    if columns.is_empty() {
        return Err(OrmdanticError::SqlCompile {
            message: format!("{operation} query requires at least one column"),
        });
    }
    Ok(())
}

fn require_select_columns(columns: &[SelectColumn]) -> OrmdanticResult<()> {
    if columns.is_empty() {
        return Err(OrmdanticError::SqlCompile {
            message: "select query requires at least one column".to_string(),
        });
    }
    Ok(())
}

fn require_joined_select_columns(columns: &[JoinedSelectColumn]) -> OrmdanticResult<()> {
    if columns.is_empty() {
        return Err(OrmdanticError::SqlCompile {
            message: "joined select query requires at least one column".to_string(),
        });
    }
    Ok(())
}

fn selected_column(dialect: &impl Dialect, table: &TableRef, column: &SelectColumn) -> String {
    let rendered = qualified_column(dialect, table, column.name());
    match column.alias() {
        Some(alias) => format!("{rendered} AS {}", dialect.quote_ident(alias)),
        None => rendered,
    }
}

fn qualified_column(dialect: &impl Dialect, table: &TableRef, column: &str) -> String {
    format!(
        "{}.{}",
        dialect.quote_ident(table.name()),
        dialect.quote_ident(column)
    )
}

fn qualified_alias_column(dialect: &impl Dialect, table_alias: &str, column: &str) -> String {
    format!(
        "{}.{}",
        dialect.quote_ident(table_alias),
        dialect.quote_ident(column)
    )
}

fn joined_selected_column(dialect: &impl Dialect, column: &JoinedSelectColumn) -> String {
    format!(
        "{} AS {}",
        qualified_alias_column(dialect, column.table_alias(), column.column()),
        dialect.quote_ident(column.alias())
    )
}

fn column_list(dialect: &impl Dialect, columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn placeholders(dialect: &impl Dialect, start: usize, count: usize) -> String {
    (start..start + count)
        .map(|index| dialect.placeholder(index))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::{
        Filter, JoinSpec, JoinedSelectColumn, OrderBy, QueryAst, QueryOperation, SelectColumn,
        SortDirection, TableRef,
    };
    use ormdantic_dialects::{PostgresDialect, SqliteDialect};

    #[test]
    fn compiles_select_for_sqlite() {
        let query = QueryAst::Select {
            table: TableRef::new("flavors"),
            columns: vec![SelectColumn::new("id"), SelectColumn::new("name")],
            filters: vec![Filter::Eq {
                column: "id".to_string(),
                param: "id".to_string(),
            }],
            order_by: vec![],
            limit: Some(10),
            offset: Some(20),
        }
        .compile(&SqliteDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "SELECT \"flavors\".\"id\", \"flavors\".\"name\" FROM \"flavors\" WHERE \"id\" = ? LIMIT 10 OFFSET 20"
        );
        assert_eq!(query.params(), &["id".to_string()]);
        assert_eq!(query.operation(), &QueryOperation::Select);
    }

    #[test]
    fn compiles_select_with_aliases_and_ordering_for_postgres() {
        let query = QueryAst::Select {
            table: TableRef::new("flavors"),
            columns: vec![
                SelectColumn::aliased("id", "flavors\\id"),
                SelectColumn::aliased("name", "flavors\\name"),
            ],
            filters: vec![Filter::Eq {
                column: "name".to_string(),
                param: "name".to_string(),
            }],
            order_by: vec![OrderBy::new("name", SortDirection::Desc)],
            limit: Some(5),
            offset: None,
        }
        .compile(&PostgresDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "SELECT \"flavors\".\"id\" AS \"flavors\\id\", \"flavors\".\"name\" AS \"flavors\\name\" FROM \"flavors\" WHERE \"name\" = $1 ORDER BY \"name\" DESC LIMIT 5"
        );
        assert_eq!(query.params(), &["name".to_string()]);
    }

    #[test]
    fn compiles_joined_select_for_relationships() {
        let query = QueryAst::JoinedSelect {
            table: TableRef::new("coffee"),
            columns: vec![
                JoinedSelectColumn::aliased("coffee", "id", "coffee\\id"),
                JoinedSelectColumn::aliased("coffee/flavor", "id", "coffee/flavor\\id"),
                JoinedSelectColumn::aliased("coffee/flavor", "name", "coffee/flavor\\name"),
            ],
            joins: vec![JoinSpec::left_join(
                "flavors",
                "coffee/flavor",
                "coffee",
                "flavor",
                "coffee/flavor",
                "id",
            )],
            filters: vec![Filter::Eq {
                column: "id".to_string(),
                param: "id".to_string(),
            }],
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
        .compile(&SqliteDialect)
        .expect("joined select should compile");

        assert_eq!(
            query.sql(),
            "SELECT \"coffee\".\"id\" AS \"coffee\\id\", \"coffee/flavor\".\"id\" AS \"coffee/flavor\\id\", \"coffee/flavor\".\"name\" AS \"coffee/flavor\\name\" FROM \"coffee\" LEFT JOIN \"flavors\" AS \"coffee/flavor\" ON \"coffee\".\"flavor\" = \"coffee/flavor\".\"id\" WHERE \"coffee\".\"id\" = ?"
        );
        assert_eq!(query.params(), &["id".to_string()]);
    }

    #[test]
    fn compiles_count_for_sqlite() {
        let query = QueryAst::Count {
            table: TableRef::new("flavors"),
            filters: vec![Filter::Eq {
                column: "name".to_string(),
                param: "name".to_string(),
            }],
        }
        .compile(&SqliteDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "SELECT COUNT(*) FROM \"flavors\" WHERE \"name\" = ?"
        );
        assert_eq!(query.params(), &["name".to_string()]);
        assert_eq!(query.operation(), &QueryOperation::Count);
    }

    #[test]
    fn compiles_insert_for_postgres() {
        let query = QueryAst::Insert {
            table: TableRef::new("flavors"),
            columns: vec!["id".to_string(), "name".to_string()],
        }
        .compile(&PostgresDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "INSERT INTO \"flavors\" (\"id\", \"name\") VALUES ($1, $2)"
        );
        assert_eq!(query.params(), &["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn compiles_upsert_for_postgres() {
        let query = QueryAst::Upsert {
            table: TableRef::new("flavors"),
            columns: vec!["id".to_string(), "name".to_string()],
            pk: "id".to_string(),
        }
        .compile(&PostgresDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "INSERT INTO \"flavors\" (\"id\", \"name\") VALUES ($1, $2) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = excluded.\"name\""
        );
    }

    #[test]
    fn compiles_pk_only_upsert_as_do_nothing() {
        let query = QueryAst::Upsert {
            table: TableRef::new("flavors"),
            columns: vec!["id".to_string()],
            pk: "id".to_string(),
        }
        .compile(&SqliteDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "INSERT INTO \"flavors\" (\"id\") VALUES (?) ON CONFLICT (\"id\") DO NOTHING"
        );
    }

    #[test]
    fn compiles_update_for_postgres() {
        let query = QueryAst::Update {
            table: TableRef::new("flavors"),
            columns: vec!["name".to_string(), "strength".to_string()],
            pk: "id".to_string(),
        }
        .compile(&PostgresDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "UPDATE \"flavors\" SET \"name\" = $1, \"strength\" = $2 WHERE \"id\" = $3"
        );
        assert_eq!(
            query.params(),
            &["name".to_string(), "strength".to_string(), "id".to_string()]
        );
    }

    #[test]
    fn compiles_delete_for_sqlite() {
        let query = QueryAst::Delete {
            table: TableRef::new("flavors"),
            pk: "id".to_string(),
        }
        .compile(&SqliteDialect)
        .expect("query should compile");

        assert_eq!(query.sql(), "DELETE FROM \"flavors\" WHERE \"id\" = ?");
        assert_eq!(query.params(), &["id".to_string()]);
    }
}

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
    DecimalEq { column: String, param: String },
    DecimalNe { column: String, param: String },
    DecimalLt { column: String, param: String },
    DecimalLe { column: String, param: String },
    DecimalGt { column: String, param: String },
    DecimalGe { column: String, param: String },
    DecimalIn { column: String, params: Vec<String> },
    DecimalNotIn { column: String, params: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnRef {
    name: String,
}

impl ColumnRef {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BindParam {
    name: String,
}

impl BindParam {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ComparisonOp {
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
    pub(crate) fn sql_operator(&self) -> &'static str {
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
pub(crate) enum BoolOp {
    And,
    Or,
}

impl BoolOp {
    pub(crate) fn sql_operator(&self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::Or => "OR",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PredicateExpr {
    Compare {
        left: ColumnRef,
        op: ComparisonOp,
        right: BindParam,
    },
    DecimalCompare {
        left: ColumnRef,
        op: ComparisonOp,
        right: BindParam,
    },
    InList {
        left: ColumnRef,
        params: Vec<BindParam>,
        negated: bool,
    },
    DecimalInList {
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
            Filter::DecimalEq { column, param } => {
                decimal_comparison_expr(column, ComparisonOp::Eq, param)
            }
            Filter::DecimalNe { column, param } => {
                decimal_comparison_expr(column, ComparisonOp::Ne, param)
            }
            Filter::DecimalLt { column, param } => {
                decimal_comparison_expr(column, ComparisonOp::Lt, param)
            }
            Filter::DecimalLe { column, param } => {
                decimal_comparison_expr(column, ComparisonOp::Le, param)
            }
            Filter::DecimalGt { column, param } => {
                decimal_comparison_expr(column, ComparisonOp::Gt, param)
            }
            Filter::DecimalGe { column, param } => {
                decimal_comparison_expr(column, ComparisonOp::Ge, param)
            }
            Filter::DecimalIn { column, params } => PredicateExpr::DecimalInList {
                left: ColumnRef::new(column.clone()),
                params: params.iter().cloned().map(BindParam::new).collect(),
                negated: false,
            },
            Filter::DecimalNotIn { column, params } => PredicateExpr::DecimalInList {
                left: ColumnRef::new(column.clone()),
                params: params.iter().cloned().map(BindParam::new).collect(),
                negated: true,
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

fn decimal_comparison_expr(column: &str, op: ComparisonOp, param: &str) -> PredicateExpr {
    PredicateExpr::DecimalCompare {
        left: ColumnRef::new(column),
        op,
        right: BindParam::new(param),
    }
}

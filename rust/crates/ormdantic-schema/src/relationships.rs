#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDef {
    field: String,
    target_table: String,
    target_field: String,
    cardinality: RelationshipCardinality,
    back_reference: Option<String>,
    local_columns: Vec<String>,
    remote_columns: Vec<String>,
    direction: RelationshipDirection,
    uselist: bool,
    nullable: bool,
    secondary_table: Option<String>,
    cascade: Vec<CascadeAction>,
    loader_strategy: LoaderStrategy,
}

impl RelationshipDef {
    pub fn new(
        field: impl Into<String>,
        target_table: impl Into<String>,
        target_field: impl Into<String>,
        cardinality: RelationshipCardinality,
    ) -> Self {
        Self {
            field: field.into(),
            target_table: target_table.into(),
            target_field: target_field.into(),
            cardinality,
            back_reference: None,
            local_columns: Vec::new(),
            remote_columns: Vec::new(),
            direction: match cardinality {
                RelationshipCardinality::One => RelationshipDirection::ManyToOne,
                RelationshipCardinality::Many => RelationshipDirection::OneToMany,
            },
            uselist: cardinality == RelationshipCardinality::Many,
            nullable: true,
            secondary_table: None,
            cascade: Vec::new(),
            loader_strategy: LoaderStrategy::Lazy,
        }
    }

    pub fn with_back_reference(mut self, back_reference: impl Into<String>) -> Self {
        self.back_reference = Some(back_reference.into());
        self
    }

    pub fn with_columns(mut self, local_columns: Vec<String>, remote_columns: Vec<String>) -> Self {
        self.local_columns = local_columns;
        self.remote_columns = remote_columns;
        self
    }

    pub fn with_direction(mut self, direction: RelationshipDirection) -> Self {
        self.direction = direction;
        self
    }

    pub fn uselist(mut self, uselist: bool) -> Self {
        self.uselist = uselist;
        self
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn secondary_table(mut self, secondary_table: impl Into<String>) -> Self {
        self.secondary_table = Some(secondary_table.into());
        self
    }

    pub fn cascade(mut self, cascade: Vec<CascadeAction>) -> Self {
        self.cascade = cascade;
        self
    }

    pub fn loader_strategy(mut self, loader_strategy: LoaderStrategy) -> Self {
        self.loader_strategy = loader_strategy;
        self
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn target_table(&self) -> &str {
        &self.target_table
    }

    pub fn target_field(&self) -> &str {
        &self.target_field
    }

    pub fn cardinality(&self) -> &RelationshipCardinality {
        &self.cardinality
    }

    pub fn back_reference(&self) -> Option<&str> {
        self.back_reference.as_deref()
    }

    pub fn local_columns(&self) -> &[String] {
        &self.local_columns
    }

    pub fn remote_columns(&self) -> &[String] {
        &self.remote_columns
    }

    pub fn direction(&self) -> &RelationshipDirection {
        &self.direction
    }

    pub fn is_uselist(&self) -> bool {
        self.uselist
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn secondary_table_name(&self) -> Option<&str> {
        self.secondary_table.as_deref()
    }

    pub fn cascade_actions(&self) -> &[CascadeAction] {
        &self.cascade
    }

    pub fn loader_strategy_ref(&self) -> &LoaderStrategy {
        &self.loader_strategy
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipCardinality {
    One,
    Many,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipDirection {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeAction {
    SaveUpdate,
    Merge,
    Delete,
    DeleteOrphan,
    RefreshExpire,
    Expunge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoaderStrategy {
    Lazy,
    Joined,
    SelectIn,
    Raise,
    NoLoad,
}

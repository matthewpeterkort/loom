use crate::mapping::{Expr, OutputType, Predicate};
use crate::source::{SourceTableDescriptor, SourceTableRef};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TransformPlanKind {
    #[default]
    Compiled,
    Fallback,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransformRef {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransformSpec {
    pub input: SourceTableRef,
    pub output_table_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub operations: Vec<TransformOperation>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PreviewTransformRequest {
    pub spec: TransformSpec,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum TransformOperation {
    ChooseHeaderRow { header_row_index: usize },
    SetDataStartRow { data_start_row_index: usize },
    RenameColumn { from: String, to: String },
    DropColumn { column: String },
    Trim { columns: Vec<String> },
    SplitColumn(SplitColumnOp),
    ExplodeColumn(ExplodeColumnOp),
    CoerceType {
        column: String,
        output_type: OutputType,
        #[serde(default)]
        on_error: CoerceOnErrorPolicy,
    },
    FilterRows(FilterRowsOp),
    DeriveColumn(DeriveColumnOp),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SplitColumnOp {
    pub column: String,
    pub delimiter: String,
    pub into: Vec<String>,
    #[serde(default)]
    pub behavior: SplitColumnBehavior,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SplitColumnBehavior {
    #[default]
    KeepOriginal,
    DropOriginal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExplodeColumnOp {
    pub column: String,
    pub delimiter: String,
    #[serde(default)]
    pub trim_values: bool,
    #[serde(default)]
    pub drop_empty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FilterRowsOp {
    pub predicate: Predicate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeriveColumnOp {
    pub column: String,
    pub expr: DeriveColumnExpr,
}

pub type DeriveColumnExpr = Expr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CoerceOnErrorPolicy {
    #[default]
    Strict,
    NullOnError,
    KeepOriginalText,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransformDescriptor {
    pub id: String,
    pub spec: TransformSpec,
    pub output_table: SourceTableDescriptor,
    pub status: TransformDescriptorStatus,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub created_unix_seconds: u64,
    #[serde(default)]
    pub updated_unix_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransformDescriptorStatus {
    Registered,
    Valid,
    Error,
}

impl Default for TransformDescriptorStatus {
    fn default() -> Self {
        Self::Registered
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TransformValidationReport {
    pub valid: bool,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub output_table: Option<SourceTableDescriptor>,
    #[serde(default)]
    pub plan_kind: TransformPlanKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransformPreviewResult {
    #[serde(default)]
    pub rows: Vec<BTreeMap<String, String>>,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub output_table: SourceTableDescriptor,
    #[serde(default)]
    pub plan_kind: TransformPlanKind,
}

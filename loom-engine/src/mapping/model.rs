use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::source::SourceFormat;

fn default_manifest_version() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphMappingManifest {
    #[serde(default = "default_manifest_version")]
    pub version: u32,
    #[serde(default)]
    pub graph: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    pub sources: BTreeMap<String, SourceMapping>,
    #[serde(default, alias = "nodes")]
    pub vertices: Vec<VertexMapping>,
    #[serde(default)]
    pub edges: Vec<EdgeMapping>,
    #[serde(default)]
    pub validation: MappingValidationPolicy,
    #[serde(default)]
    pub identity: BTreeMap<String, IdentityRule>,
    #[serde(default)]
    pub references: Vec<ReferenceRule>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

pub type NodeMapping = VertexMapping;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceMapping {
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub format: SourceFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VertexMapping {
    pub source: String,
    pub label: String,
    pub id: Expr,
    #[serde(default, rename = "where")]
    pub predicate: Option<Predicate>,
    #[serde(default)]
    pub columns: BTreeMap<String, ColumnMapping>,
    #[serde(default)]
    pub props: PropsMapping,
    #[serde(default)]
    pub prop_types: BTreeMap<String, OutputType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EdgeMapping {
    pub source: String,
    pub label: String,
    #[serde(default)]
    pub from_label: Option<String>,
    #[serde(default)]
    pub to_label: Option<String>,
    pub from: Expr,
    pub to: Expr,
    #[serde(default)]
    pub id: Option<Expr>,
    #[serde(default, rename = "where")]
    pub predicate: Option<Predicate>,
    #[serde(default)]
    pub columns: BTreeMap<String, ColumnMapping>,
    #[serde(default)]
    pub props: PropsMapping,
    #[serde(default)]
    pub prop_types: BTreeMap<String, OutputType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IdentityRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_id: Option<Expr>,
    #[serde(default)]
    pub aliases: BTreeMap<String, Expr>,
    #[serde(default)]
    pub normalizer: ReferenceNormalizer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReferenceRule {
    pub name: String,
    pub source: String,
    pub from_label: String,
    pub to_label: String,
    pub from_key: Expr,
    pub to_key: Expr,
    #[serde(default, rename = "where")]
    pub predicate: Option<Predicate>,
    #[serde(default)]
    pub normalizer: ReferenceNormalizer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceNormalizer {
    Raw,
    TrimLower,
    FhirCanonical,
}

impl Default for ReferenceNormalizer {
    fn default() -> Self {
        Self::Raw
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ColumnMapping {
    Expr(Expr),
    Typed(TypedColumnMapping),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TypedColumnMapping {
    pub expr: Expr,
    #[serde(rename = "type", default)]
    pub output_type: Option<OutputType>,
    #[serde(default)]
    pub coerce: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputType {
    String,
    Integer,
    Float,
    Boolean,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MappingValidationPolicy {
    #[serde(default)]
    pub duplicate_vertex_ids: DuplicateIdPolicy,
    #[serde(default)]
    pub duplicate_edge_ids: DuplicateIdPolicy,
    #[serde(default)]
    pub missing_edge_endpoints: MissingEndpointPolicy,
    #[serde(default)]
    pub empty_ids: EmptyIdPolicy,
    #[serde(default)]
    pub type_coercion: TypeCoercionPolicy,
    #[serde(default)]
    pub duplicate_alias_keys: AliasConflictPolicy,
    #[serde(default)]
    pub unresolved_references: ReferenceResolutionPolicy,
    #[serde(default)]
    pub ambiguous_references: AmbiguousReferencePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AliasConflictPolicy {
    Error,
    First,
    Warn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceResolutionPolicy {
    Error,
    Warn,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AmbiguousReferencePolicy {
    Error,
    First,
    Warn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateIdPolicy {
    Error,
    First,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MissingEndpointPolicy {
    Error,
    Warn,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmptyIdPolicy {
    Error,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TypeCoercionPolicy {
    Strict,
    BestEffort,
}

impl Default for MappingValidationPolicy {
    fn default() -> Self {
        Self {
            duplicate_vertex_ids: DuplicateIdPolicy::Error,
            duplicate_edge_ids: DuplicateIdPolicy::Error,
            missing_edge_endpoints: MissingEndpointPolicy::Error,
            empty_ids: EmptyIdPolicy::Error,
            type_coercion: TypeCoercionPolicy::Strict,
            duplicate_alias_keys: AliasConflictPolicy::Error,
            unresolved_references: ReferenceResolutionPolicy::Warn,
            ambiguous_references: AmbiguousReferencePolicy::Error,
        }
    }
}

impl Default for DuplicateIdPolicy {
    fn default() -> Self {
        Self::Error
    }
}

impl Default for MissingEndpointPolicy {
    fn default() -> Self {
        Self::Error
    }
}

impl Default for EmptyIdPolicy {
    fn default() -> Self {
        Self::Error
    }
}

impl Default for TypeCoercionPolicy {
    fn default() -> Self {
        Self::Strict
    }
}

impl Default for AliasConflictPolicy {
    fn default() -> Self {
        Self::Error
    }
}

impl Default for ReferenceResolutionPolicy {
    fn default() -> Self {
        Self::Warn
    }
}

impl Default for AmbiguousReferencePolicy {
    fn default() -> Self {
        Self::Error
    }
}

impl ColumnMapping {
    pub fn expr(&self) -> &Expr {
        match self {
            ColumnMapping::Expr(expr) => expr,
            ColumnMapping::Typed(mapping) => &mapping.expr,
        }
    }

    pub fn output_type(&self) -> OutputType {
        match self {
            ColumnMapping::Expr(_) => OutputType::String,
            ColumnMapping::Typed(mapping) => {
                mapping.output_type.clone().unwrap_or(OutputType::String)
            }
        }
    }

    pub fn coerce(&self) -> bool {
        match self {
            ColumnMapping::Expr(_) => false,
            ColumnMapping::Typed(mapping) => mapping.coerce,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PropsMapping {
    None,
    All,
    Except(Vec<String>),
    Only(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Expr {
    Column { column: String },
    Literal { literal: String },
    Concat { concat: Vec<Expr> },
    Coalesce { coalesce: Vec<Expr> },
    RowNumber { row_number: bool },
    Lower { lower: Box<Expr> },
    Upper { upper: Box<Expr> },
    Trim { trim: Box<Expr> },
    Replace { replace: ReplaceExpr },
    NullIf { null_if: Vec<Expr> },
    Sha256 { sha256: Box<Expr> },
    Text(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplaceExpr {
    pub value: Box<Expr>,
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Predicate {
    Eq { eq: BinaryPredicate },
    Neq { neq: BinaryPredicate },
    IsEmpty { is_empty: Expr },
    IsNotEmpty { is_not_empty: Expr },
    And { and: Vec<Predicate> },
    Or { or: Vec<Predicate> },
    Not { not: Box<Predicate> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryPredicate {
    pub left: Expr,
    pub right: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompiledVirtualGraphPlan {
    pub graph: String,
    pub vertex_table: String,
    pub edge_table: String,
    pub vertex_columns: Vec<String>,
    pub edge_columns: Vec<String>,
    pub node_views: Vec<CompiledNodeViewPlan>,
    pub edge_views: Vec<CompiledEdgeViewPlan>,
    pub identity_views: Vec<CompiledIdentityViewPlan>,
    pub reference_views: Vec<CompiledReferenceViewPlan>,
    pub source_dependencies: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompiledNodeViewPlan {
    pub mapping_index: usize,
    pub mapping_path: String,
    pub source_alias: String,
    pub source_id: String,
    pub source_table_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_table_ref: Option<crate::source::SourceTableRef>,
    pub view_name: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explain: Option<String>,
    pub projected_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompiledEdgeViewPlan {
    pub mapping_index: usize,
    pub mapping_path: String,
    pub source_alias: String,
    pub source_id: String,
    pub source_table_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_table_ref: Option<crate::source::SourceTableRef>,
    pub view_name: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explain: Option<String>,
    pub projected_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompiledIdentityViewPlan {
    pub label: String,
    pub view_name: String,
    pub alias_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explain: Option<String>,
    pub projected_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompiledReferenceViewPlan {
    pub name: String,
    pub source_alias: String,
    pub source_id: String,
    pub source_table_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_table_ref: Option<crate::source::SourceTableRef>,
    pub view_name: String,
    pub from_label: String,
    pub to_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explain: Option<String>,
    pub projected_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VirtualGraphBinding {
    pub graph: String,
    pub vertex_table: String,
    pub edge_table: String,
    pub node_view_names: Vec<String>,
    pub edge_view_names: Vec<String>,
    pub identity_view_names: Vec<String>,
    pub reference_view_names: Vec<String>,
    pub source_dependencies: Vec<String>,
    pub source_fingerprints: BTreeMap<String, String>,
    pub registered_unix_seconds: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct VirtualGraphIdentitySummary {
    #[serde(default)]
    pub labels: Vec<IdentityLabelSummary>,
    #[serde(default)]
    pub references: Vec<ReferenceSummary>,
    #[serde(default)]
    pub duplicate_alias_keys: BTreeMap<String, usize>,
    #[serde(default)]
    pub unresolved_references: BTreeMap<String, usize>,
    #[serde(default)]
    pub ambiguous_references: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdentityLabelSummary {
    pub label: String,
    pub view_name: String,
    #[serde(default)]
    pub alias_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReferenceSummary {
    pub name: String,
    pub view_name: String,
    pub from_label: String,
    pub to_label: String,
}

#[derive(Debug, Clone)]
pub struct MappingCompileResult {
    pub vertices: RecordBatch,
    pub edges: RecordBatch,
    pub report: MappingCompileReport,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MappingCompileReport {
    pub vertices: usize,
    pub edges: usize,
    #[serde(default)]
    pub vertex_plans: usize,
    #[serde(default)]
    pub edge_plans: usize,
    pub vertex_labels: BTreeMap<String, usize>,
    pub edge_labels: BTreeMap<String, usize>,
    #[serde(default)]
    pub filtered_vertex_rows: usize,
    #[serde(default)]
    pub filtered_edge_rows: usize,
    #[serde(default)]
    pub duplicate_vertices: usize,
    #[serde(default)]
    pub duplicate_edges: usize,
    #[serde(default)]
    pub unresolved_edge_endpoints: usize,
    #[serde(default)]
    pub coercion_failures: usize,
    #[serde(default)]
    pub provenance_missing: usize,
    #[serde(default)]
    pub vertices_uri: Option<String>,
    #[serde(default)]
    pub edges_uri: Option<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MappingValidationReport {
    pub valid: bool,
    pub errors: Vec<MappingValidationError>,
    pub warnings: Vec<String>,
    #[serde(default)]
    pub policy: MappingValidationPolicy,
    #[serde(default)]
    pub metrics: MappingValidationMetrics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_preview: Option<CompiledVirtualGraphPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MappingValidationError {
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MappingValidationMetrics {
    pub input_rows: BTreeMap<String, usize>,
    pub emitted_rows: BTreeMap<String, usize>,
    pub filtered_empty_ids: usize,
    pub duplicate_vertex_ids: usize,
    pub duplicate_edge_ids: usize,
    pub duplicate_alias_keys: BTreeMap<String, usize>,
    pub unresolved_edge_endpoints: usize,
    pub unresolved_edge_endpoint_counts: BTreeMap<String, usize>,
    pub unresolved_references: BTreeMap<String, usize>,
    pub ambiguous_references: BTreeMap<String, usize>,
    pub coercion_failures: BTreeMap<String, usize>,
    pub provenance_missing: usize,
    pub missing_required_fields: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphMappingStatus {
    Registered,
    Compiled,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphMappingDescriptor {
    pub graph: String,
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bound_schema: Option<String>,
    pub manifest: GraphMappingManifest,
    pub source_dependencies: Vec<String>,
    pub status: GraphMappingStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_binding: Option<VirtualGraphBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_graph: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_summary: Option<VirtualGraphIdentitySummary>,
    pub last_report: Option<MappingCompileReport>,
    pub last_error: Option<String>,
    pub created_unix_seconds: u64,
    pub updated_unix_seconds: u64,
}

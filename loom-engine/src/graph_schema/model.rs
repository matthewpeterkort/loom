use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::mapping::{
    ColumnMapping, Expr, GraphMappingManifest, MappingValidationError, MappingValidationReport,
    OutputType, Predicate, PropsMapping,
};
use crate::schema_binding::{ObservedColumnCluster, SchemaEdgeCandidate, SchemaEntityCandidate};
use crate::source::{SourceColumnProfile, SourceProfileSuggestion, SourceTableRef};

fn default_spec_version() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphSchemaSpec {
    #[serde(default = "default_spec_version")]
    pub version: u32,
    #[serde(default)]
    pub id: Option<String>,
    pub graph: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub bound_schema: Option<String>,
    #[serde(default)]
    pub nodes: Vec<GraphNodeSpec>,
    #[serde(default)]
    pub edges: Vec<GraphEdgeSpec>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphNodeSpec {
    pub source: SourceTableRef,
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
    #[serde(default)]
    pub schema_entity: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphEdgeSpec {
    pub source: SourceTableRef,
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
    #[serde(default)]
    pub schema_relation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphSchemaStatus {
    Draft,
    Registered,
    Published,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphSchemaDescriptor {
    pub id: String,
    pub graph: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub bound_schema: Option<String>,
    pub spec: GraphSchemaSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_manifest: Option<GraphMappingManifest>,
    pub status: GraphSchemaStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_validation: Option<GraphSchemaValidationReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_preview: Option<GraphSchemaPreview>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered_mapping_graph: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_graph: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_unix_seconds: u64,
    pub updated_unix_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphSchemaBinding {
    pub graph_schema_id: String,
    pub graph: String,
    #[serde(default)]
    pub bound_schema: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GraphSchemaValidationReport {
    pub valid: bool,
    #[serde(default)]
    pub errors: Vec<GraphSchemaValidationError>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub source_hints: Vec<GraphSchemaSourceHint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<GraphMappingManifest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mapping_validation: Option<MappingValidationReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphSchemaValidationError {
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GraphSchemaPreview {
    pub graph: String,
    #[serde(default)]
    pub node_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub edge_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub sample_node_rows: BTreeMap<String, Vec<serde_json::Value>>,
    #[serde(default)]
    pub sample_edge_rows: BTreeMap<String, Vec<serde_json::Value>>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub source_hints: Vec<GraphSchemaSourceHint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<GraphMappingManifest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mapping_validation: Option<MappingValidationReport>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GraphSchemaSourceHint {
    pub source: SourceTableRef,
    pub table_name: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub possible_id_columns: Vec<String>,
    #[serde(default)]
    pub semantic_suggestions: Vec<SourceProfileSuggestion>,
    #[serde(default)]
    pub likely_id_columns: Vec<String>,
    #[serde(default)]
    pub likely_reference_columns: Vec<String>,
    #[serde(default)]
    pub column_profiles: Vec<SourceColumnProfile>,
    #[serde(default)]
    pub clusters: Vec<ObservedColumnCluster>,
    #[serde(default)]
    pub entity_candidates: Vec<SchemaEntityCandidate>,
    #[serde(default)]
    pub edge_candidates: Vec<SchemaEdgeCandidate>,
    #[serde(default)]
    pub schema_derived: bool,
}

impl GraphSchemaValidationError {
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl From<MappingValidationError> for GraphSchemaValidationError {
    fn from(value: MappingValidationError) -> Self {
        Self {
            path: value.path,
            message: value.message,
        }
    }
}

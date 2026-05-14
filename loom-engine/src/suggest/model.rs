use serde::{Deserialize, Serialize};

use crate::mapping::GraphMappingManifest;
use crate::source::{SourceDescriptor, SourceProfile, SourceTable};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphSuggestionReport {
    pub graph: String,
    pub display_name: Option<String>,
    pub target_vocabulary: String,
    pub source_profiles: Vec<SourceProfile>,
    pub candidates: Vec<MappingSuggestionCandidate>,
    pub manifest: GraphMappingManifest,
    pub validation: crate::mapping::MappingValidationReport,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MappingSuggestionCandidate {
    pub rule_id: String,
    pub kind: SuggestionKind,
    pub selected: bool,
    pub confidence: f64,
    pub label: String,
    pub source: String,
    pub reason: String,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionKind {
    Vertex,
    Edge,
    Property,
}

#[derive(Debug, Clone)]
pub struct SuggestionInput {
    pub graph: String,
    pub display_name: Option<String>,
    pub target_vocabulary: Option<String>,
    pub sources: Vec<(SourceDescriptor, SourceTable, SourceProfile)>,
}

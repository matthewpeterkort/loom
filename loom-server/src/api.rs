use loom_engine::{graph, mapping, schema, source, suggest, transform, Engine};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::schema_ingest::{ArrowResourceShape, PromotedColumn};

fn default_compile_mapping() -> bool {
    false
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) engine: Arc<Engine>,
    pub(crate) schemas: Arc<RwLock<HashMap<String, StoredSchema>>>,
    pub(crate) graph_schema_bindings: Arc<RwLock<HashMap<String, String>>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IngestParquetToLanceRequest {
    pub(crate) vertices_parquet_path: String,
    pub(crate) edges_parquet_path: String,
    pub(crate) vortex_root_uri: String,
    pub(crate) mode: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IngestNdjsonWithSchemaRequest {
    pub(crate) schema_name: String,
    pub(crate) ndjson_dir: String,
    pub(crate) vortex_root_uri: String,
    pub(crate) mode: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SqlQueryRequest {
    pub(crate) sql: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueryAutocompleteRequest {
    #[serde(default)]
    pub(crate) labels: Option<String>,
    #[serde(default)]
    pub(crate) direction: Option<String>,
    #[serde(default)]
    pub(crate) prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReferencePreviewRequest {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SourceSampleRequest {
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct SourceProfileQuery {
    #[serde(default)]
    pub(crate) graph: Option<String>,
    #[serde(default)]
    pub(crate) schema_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GraphSuggestionRequest {
    pub(crate) graph: String,
    pub(crate) display_name: Option<String>,
    pub(crate) source_ids: Vec<String>,
    pub(crate) target_vocabulary: Option<String>,
    pub(crate) max_sample_rows: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GraphMappingRequest {
    pub(crate) manifest: mapping::GraphMappingManifest,
    #[serde(default = "default_compile_mapping")]
    pub(crate) compile: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GraphSchemaSpecRequest {
    pub(crate) spec: loom_engine::graph_schema::GraphSchemaSpec,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TransformRequest {
    pub(crate) spec: transform::TransformSpec,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TransformPreviewRequest {
    pub(crate) spec: transform::TransformSpec,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ApiResponse {
    pub(crate) status: &'static str,
    pub(crate) detail: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct QueryResponse {
    pub(crate) status: &'static str,
    pub(crate) graph: String,
    pub(crate) row_count: usize,
    pub(crate) rows: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub(crate) struct IngestResponse {
    pub(crate) status: &'static str,
    pub(crate) graph: String,
    pub(crate) detail: String,
    pub(crate) vertices_uri: String,
    pub(crate) edges_uri: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct SchemaIngestResponse {
    pub(crate) status: &'static str,
    pub(crate) graph: String,
    pub(crate) detail: String,
    pub(crate) vertices_uri: String,
    pub(crate) edges_uri: String,
    pub(crate) loaded_vertices: usize,
    pub(crate) typed_columns: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct SqlQueryResponse {
    pub(crate) status: &'static str,
    pub(crate) graph: String,
    pub(crate) row_count: usize,
    pub(crate) rows: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListGraphsResponse {
    pub(crate) graphs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphColumnsResponse {
    pub(crate) graph: String,
    pub(crate) vertex_columns: Vec<graph::GraphColumn>,
    pub(crate) edge_columns: Vec<graph::GraphColumn>,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphSchemaSpecResponse {
    pub(crate) status: &'static str,
    pub(crate) graph_schema: loom_engine::graph_schema::GraphSchemaDescriptor,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListGraphSchemasResponse {
    pub(crate) graph_schemas: Vec<loom_engine::graph_schema::GraphSchemaDescriptor>,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphSchemaValidationApiResponse {
    pub(crate) graph_schema_id: String,
    pub(crate) validation: loom_engine::graph_schema::GraphSchemaValidationReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphSchemaPreviewApiResponse {
    pub(crate) graph_schema_id: String,
    pub(crate) preview: loom_engine::graph_schema::GraphSchemaPreview,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphStatsResponse {
    pub(crate) graph: String,
    pub(crate) stats: graph::GraphStats,
}

#[derive(Debug, Serialize)]
pub(crate) struct VirtualGraphResponse {
    pub(crate) graph: String,
    pub(crate) mode: String,
    pub(crate) mapping: mapping::GraphMappingDescriptor,
    pub(crate) published_graph: Option<graph::GraphDescriptor>,
}

#[derive(Debug, Serialize)]
pub(crate) struct VirtualGraphViewsResponse {
    pub(crate) graph: String,
    pub(crate) views: Vec<VirtualGraphView>,
}

#[derive(Debug, Serialize)]
pub(crate) struct VirtualGraphView {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct VirtualGraphIdentityResponse {
    pub(crate) graph: String,
    pub(crate) identity: mapping::VirtualGraphIdentitySummary,
}

#[derive(Debug, Serialize)]
pub(crate) struct SchemaGraphResponse {
    pub(crate) name: String,
    pub(crate) graph_schema: schema::CompiledGraphSchema,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphBoundSchemaResponse {
    pub(crate) graph: String,
    pub(crate) schema_name: String,
    pub(crate) graph_schema: schema::CompiledGraphSchema,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphSchemaEntitiesResponse {
    pub(crate) graph: String,
    pub(crate) schema_name: String,
    pub(crate) entities: Vec<schema::EntityType>,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphSchemaEntityResponse {
    pub(crate) graph: String,
    pub(crate) schema_name: String,
    pub(crate) entity: schema::EntityType,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphSchemaLinksResponse {
    pub(crate) graph: String,
    pub(crate) schema_name: String,
    pub(crate) links: Vec<schema::LinkDefinition>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BindGraphSchemaResponse {
    pub(crate) status: &'static str,
    pub(crate) graph: String,
    pub(crate) schema_name: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct ExplainQueryResponse {
    pub(crate) graph: String,
    pub(crate) explain: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct SchemaVocabularyResponse {
    pub(crate) graph: String,
    pub(crate) schema_name: String,
    pub(crate) entities: Vec<SchemaEntityRelations>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SchemaEntityRelations {
    pub(crate) entity: String,
    pub(crate) outgoing: Vec<SchemaRelationSummary>,
    pub(crate) incoming: Vec<SchemaRelationSummary>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SchemaRelationSummary {
    pub(crate) rel: String,
    pub(crate) counterpart_labels: Vec<String>,
    pub(crate) wildcard_target: bool,
    pub(crate) runtime_available: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuerySchemaExplainResponse {
    pub(crate) graph: String,
    pub(crate) schema_name: String,
    pub(crate) valid: bool,
    pub(crate) steps: Vec<QuerySchemaStepExplain>,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuerySchemaStepExplain {
    pub(crate) index: usize,
    pub(crate) op: String,
    pub(crate) labels: Vec<String>,
    pub(crate) active_labels_before: Vec<String>,
    pub(crate) active_labels_after: Vec<String>,
    pub(crate) allowed_targets: Vec<String>,
    pub(crate) schema_valid: bool,
    pub(crate) runtime_available: bool,
    pub(crate) detail: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuerySchemaAutocompleteResponse {
    pub(crate) graph: String,
    pub(crate) schema_name: String,
    pub(crate) current_labels: Vec<String>,
    pub(crate) direction: String,
    pub(crate) relations: Vec<SchemaRelationSummary>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SourceResponse {
    pub(crate) status: &'static str,
    pub(crate) source: source::SourceDescriptor,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListSourcesResponse {
    pub(crate) sources: Vec<source::SourceDescriptor>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SourceProfileResponse {
    pub(crate) source_id: String,
    pub(crate) table_id: Option<String>,
    pub(crate) profile: source::SourceProfile,
}

#[derive(Debug, Serialize)]
pub(crate) struct SourceSampleResponse {
    pub(crate) source_id: String,
    pub(crate) table_id: Option<String>,
    pub(crate) rows: Vec<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SourceTablesResponse {
    pub(crate) source_id: String,
    pub(crate) tables: Vec<source::SourceTableDescriptor>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SourceTableResponse {
    pub(crate) source_id: String,
    pub(crate) table: source::SourceTableDescriptor,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphSuggestionResponse {
    pub(crate) suggestion: suggest::GraphSuggestionReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphMappingResponse {
    pub(crate) status: &'static str,
    pub(crate) mapping: mapping::GraphMappingDescriptor,
    pub(crate) graph: Option<graph::GraphDescriptor>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListGraphMappingsResponse {
    pub(crate) mappings: Vec<mapping::GraphMappingDescriptor>,
}

#[derive(Debug, Serialize)]
pub(crate) struct GraphMappingValidationResponse {
    pub(crate) graph: String,
    pub(crate) validation: mapping::MappingValidationReport,
    pub(crate) plan: Option<mapping::CompiledVirtualGraphPlan>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TransformResponse {
    pub(crate) status: &'static str,
    pub(crate) transform: transform::TransformDescriptor,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListTransformsResponse {
    pub(crate) transforms: Vec<transform::TransformDescriptor>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TransformValidationResponse {
    pub(crate) transform_id: Option<String>,
    pub(crate) validation: transform::TransformValidationReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct TransformPreviewResponse {
    pub(crate) transform_id: Option<String>,
    pub(crate) preview: transform::TransformPreviewResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SchemaSummary {
    pub(crate) name: String,
    pub(crate) schema_dialect: Option<String>,
    pub(crate) schema_id: Option<String>,
    pub(crate) defs_count: usize,
    pub(crate) resource_types: usize,
    pub(crate) link_relations: usize,
    pub(crate) has_hypermedia_links: bool,
    pub(crate) compiled_resource_shapes: usize,
    pub(crate) compiled_entity_types: usize,
    pub(crate) compiled_properties: usize,
    pub(crate) compiled_links: usize,
    pub(crate) wildcard_links: usize,
    pub(crate) created_unix_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredSchema {
    pub(crate) doc: Value,
    pub(crate) summary: SchemaSummary,
    pub(crate) arrow_shapes: Vec<ArrowResourceShape>,
    pub(crate) promoted_columns: Vec<PromotedColumn>,
    pub(crate) graph_schema: schema::CompiledGraphSchema,
    #[serde(default)]
    pub(crate) compile_warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListSchemasResponse {
    pub(crate) schemas: Vec<SchemaSummary>,
}

#[derive(Debug, Serialize)]
pub(crate) struct UpsertSchemaResponse {
    pub(crate) status: &'static str,
    pub(crate) detail: String,
    pub(crate) summary: SchemaSummary,
}

#[derive(Debug, Serialize)]
pub(crate) struct GetSchemaResponse {
    pub(crate) summary: SchemaSummary,
    pub(crate) arrow_shapes: Vec<ArrowResourceShape>,
    pub(crate) graph_schema: schema::CompiledGraphSchema,
    pub(crate) schema: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct PersistedSchemaState {
    pub(crate) schemas: HashMap<String, StoredSchema>,
    pub(crate) graph_schema_bindings: HashMap<String, String>,
}

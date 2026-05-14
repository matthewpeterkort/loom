use crate::*;
use axum::extract::Query;
use loom_engine::transform;

pub(crate) async fn list_graphs(
    State(state): State<AppState>,
) -> Result<Json<ListGraphsResponse>, (StatusCode, Json<ApiResponse>)> {
    let graphs = state.engine.list_graphs().map_err(internal_err)?;
    Ok(Json(ListGraphsResponse { graphs }))
}

pub(crate) async fn get_graph(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<graph::GraphDescriptor>, (StatusCode, Json<ApiResponse>)> {
    state
        .engine
        .get_graph_descriptor(&graph)
        .map(Json)
        .map_err(internal_err)
}

pub(crate) async fn bind_graph_schema(
    State(state): State<AppState>,
    Path((graph, schema_name)): Path<(String, String)>,
) -> Result<Json<BindGraphSchemaResponse>, (StatusCode, Json<ApiResponse>)> {
    bind_schema_to_graph(&state, &graph, &schema_name).map_err(bad_req)?;
    save_schema_state(&state).map_err(internal_err)?;
    Ok(Json(BindGraphSchemaResponse {
        status: "ok",
        graph,
        schema_name: sanitize_name(&schema_name),
    }))
}

pub(crate) async fn get_graph_bound_schema(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<GraphBoundSchemaResponse>, (StatusCode, Json<ApiResponse>)> {
    let schema_name = get_bound_schema_name_for_graph(&state, &graph)
        .map_err(internal_err)?
        .ok_or_else(|| bad_req(format!("graph `{graph}` is not schema-bound")))?;
    let guard = state
        .schemas
        .read()
        .map_err(|_| internal_err("schema registry lock poisoned"))?;
    let stored = guard
        .get(&schema_name)
        .ok_or_else(|| bad_req(format!("schema `{schema_name}` not found")))?;
    Ok(Json(GraphBoundSchemaResponse {
        graph,
        schema_name,
        graph_schema: stored.graph_schema.clone(),
    }))
}

pub(crate) async fn list_graph_schema_entities(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<GraphSchemaEntitiesResponse>, (StatusCode, Json<ApiResponse>)> {
    let schema_name = get_bound_schema_name_for_graph(&state, &graph)
        .map_err(internal_err)?
        .ok_or_else(|| bad_req(format!("graph `{graph}` is not schema-bound")))?;
    let guard = state
        .schemas
        .read()
        .map_err(|_| internal_err("schema registry lock poisoned"))?;
    let stored = guard
        .get(&schema_name)
        .ok_or_else(|| bad_req(format!("schema `{schema_name}` not found")))?;
    Ok(Json(GraphSchemaEntitiesResponse {
        graph,
        schema_name,
        entities: stored.graph_schema.entities.values().cloned().collect(),
    }))
}

pub(crate) async fn get_graph_schema_entity(
    State(state): State<AppState>,
    Path((graph, entity)): Path<(String, String)>,
) -> Result<Json<GraphSchemaEntityResponse>, (StatusCode, Json<ApiResponse>)> {
    let entity_name = entity.clone();
    let schema_name = get_bound_schema_name_for_graph(&state, &graph)
        .map_err(internal_err)?
        .ok_or_else(|| bad_req(format!("graph `{graph}` is not schema-bound")))?;
    let guard = state
        .schemas
        .read()
        .map_err(|_| internal_err("schema registry lock poisoned"))?;
    let stored = guard
        .get(&schema_name)
        .ok_or_else(|| bad_req(format!("schema `{schema_name}` not found")))?;
    let entity = stored
        .graph_schema
        .entity(&entity_name)
        .cloned()
        .ok_or_else(|| bad_req(format!("entity `{}` not found in bound schema", entity_name)))?;
    Ok(Json(GraphSchemaEntityResponse {
        graph,
        schema_name,
        entity,
    }))
}

pub(crate) async fn list_graph_schema_links(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<GraphSchemaLinksResponse>, (StatusCode, Json<ApiResponse>)> {
    let schema_name = get_bound_schema_name_for_graph(&state, &graph)
        .map_err(internal_err)?
        .ok_or_else(|| bad_req(format!("graph `{graph}` is not schema-bound")))?;
    let guard = state
        .schemas
        .read()
        .map_err(|_| internal_err("schema registry lock poisoned"))?;
    let stored = guard
        .get(&schema_name)
        .ok_or_else(|| bad_req(format!("schema `{schema_name}` not found")))?;
    let mut links = Vec::new();
    for entity in stored.graph_schema.entities.values() {
        links.extend(entity.links.iter().cloned());
    }
    Ok(Json(GraphSchemaLinksResponse {
        graph,
        schema_name,
        links,
    }))
}

pub(crate) async fn get_virtual_graph(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<VirtualGraphResponse>, (StatusCode, Json<ApiResponse>)> {
    let mapping = state
        .engine
        .get_virtual_graph_descriptor(&graph)
        .map_err(internal_err)?;
    let published_graph = state.engine.get_graph_descriptor(&graph).ok();
    let mode = if published_graph.is_some() {
        "published".to_string()
    } else {
        "virtual".to_string()
    };
    Ok(Json(VirtualGraphResponse {
        graph,
        mode,
        mapping,
        published_graph,
    }))
}

pub(crate) async fn list_virtual_graph_views(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<VirtualGraphViewsResponse>, (StatusCode, Json<ApiResponse>)> {
    let views = state
        .engine
        .list_virtual_graph_views(&graph)
        .await
        .map_err(internal_err)?
        .into_iter()
        .map(|(name, columns)| VirtualGraphView { name, columns })
        .collect();
    Ok(Json(VirtualGraphViewsResponse { graph, views }))
}

pub(crate) async fn get_virtual_graph_identity(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<VirtualGraphIdentityResponse>, (StatusCode, Json<ApiResponse>)> {
    let identity = state
        .engine
        .get_virtual_graph_identity_summary(&graph)
        .await
        .map_err(internal_err)?;
    Ok(Json(VirtualGraphIdentityResponse { graph, identity }))
}

pub(crate) async fn get_graph_columns(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<GraphColumnsResponse>, (StatusCode, Json<ApiResponse>)> {
    let (vertex_columns, edge_columns) = state
        .engine
        .get_graph_columns(&graph)
        .map_err(internal_err)?;
    Ok(Json(GraphColumnsResponse {
        graph,
        vertex_columns,
        edge_columns,
    }))
}

pub(crate) async fn get_graph_stats(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<GraphStatsResponse>, (StatusCode, Json<ApiResponse>)> {
    let stats = state.engine.get_graph_stats(&graph).map_err(internal_err)?;
    Ok(Json(GraphStatsResponse { graph, stats }))
}

pub(crate) async fn register_source(
    State(state): State<AppState>,
    Json(req): Json<source::SourceRegistration>,
) -> Result<Json<SourceResponse>, (StatusCode, Json<ApiResponse>)> {
    let descriptor = state
        .engine
        .register_source(req)
        .await
        .map_err(internal_err)?;
    save_source_catalog_state(&state).map_err(internal_err)?;
    Ok(Json(SourceResponse {
        status: "ok",
        source: descriptor,
    }))
}

pub(crate) async fn list_sources(
    State(state): State<AppState>,
) -> Result<Json<ListSourcesResponse>, (StatusCode, Json<ApiResponse>)> {
    let sources = state.engine.list_sources().map_err(internal_err)?;
    Ok(Json(ListSourcesResponse { sources }))
}

pub(crate) async fn get_source(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<source::SourceDescriptor>, (StatusCode, Json<ApiResponse>)> {
    state
        .engine
        .get_source(&source_id)
        .map(Json)
        .map_err(internal_err)
}

pub(crate) async fn list_source_tables(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<SourceTablesResponse>, (StatusCode, Json<ApiResponse>)> {
    let tables = state
        .engine
        .list_source_tables(&source_id)
        .map_err(internal_err)?;
    Ok(Json(SourceTablesResponse { source_id, tables }))
}

pub(crate) async fn get_source_table(
    State(state): State<AppState>,
    Path((source_id, table_id)): Path<(String, String)>,
) -> Result<Json<SourceTableResponse>, (StatusCode, Json<ApiResponse>)> {
    let table = state
        .engine
        .get_source_table(&source_id, &table_id)
        .map_err(internal_err)?;
    Ok(Json(SourceTableResponse { source_id, table }))
}

pub(crate) async fn refresh_source(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<SourceResponse>, (StatusCode, Json<ApiResponse>)> {
    let descriptor = state
        .engine
        .refresh_source(&source_id)
        .await
        .map_err(internal_err)?;
    save_source_catalog_state(&state).map_err(internal_err)?;
    Ok(Json(SourceResponse {
        status: "ok",
        source: descriptor,
    }))
}

pub(crate) async fn profile_source(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Query(query): Query<SourceProfileQuery>,
) -> Result<Json<SourceProfileResponse>, (StatusCode, Json<ApiResponse>)> {
    let source = state.engine.get_source(&source_id).map_err(internal_err)?;
    if source.is_multi_table() {
        return Err(bad_req(format!(
            "source `{source_id}` has multiple logical tables; use /v1/source/{source_id}/table/{{table_id}}/profile"
        )));
    }
    let bound_schema = resolve_profile_schema(&state, &query).map_err(internal_err)?;
    let profile = state
        .engine
        .profile_source_with_schema(&source_id, bound_schema.as_ref().map(|stored| &stored.graph_schema))
        .map_err(internal_err)?;
    Ok(Json(SourceProfileResponse {
        source_id,
        table_id: None,
        profile,
    }))
}

pub(crate) async fn sample_source(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Json(req): Json<SourceSampleRequest>,
) -> Result<Json<SourceSampleResponse>, (StatusCode, Json<ApiResponse>)> {
    let source = state.engine.get_source(&source_id).map_err(internal_err)?;
    if source.is_multi_table() {
        return Err(bad_req(format!(
            "source `{source_id}` has multiple logical tables; use /v1/source/{source_id}/table/{{table_id}}/sample"
        )));
    }
    let rows = state
        .engine
        .sample_source(&source_id, req.limit.unwrap_or(25))
        .map_err(internal_err)?;
    Ok(Json(SourceSampleResponse {
        source_id,
        table_id: None,
        rows,
    }))
}

pub(crate) async fn profile_source_table(
    State(state): State<AppState>,
    Path((source_id, table_id)): Path<(String, String)>,
    Query(query): Query<SourceProfileQuery>,
) -> Result<Json<SourceProfileResponse>, (StatusCode, Json<ApiResponse>)> {
    let bound_schema = resolve_profile_schema(&state, &query).map_err(internal_err)?;
    let profile = state
        .engine
        .profile_source_table_with_schema(
            &source_id,
            &table_id,
            bound_schema.as_ref().map(|stored| &stored.graph_schema),
        )
        .await
        .map_err(internal_err)?;
    Ok(Json(SourceProfileResponse {
        source_id,
        table_id: Some(table_id),
        profile,
    }))
}

pub(crate) async fn sample_source_table(
    State(state): State<AppState>,
    Path((source_id, table_id)): Path<(String, String)>,
    Json(req): Json<SourceSampleRequest>,
) -> Result<Json<SourceSampleResponse>, (StatusCode, Json<ApiResponse>)> {
    let rows = state
        .engine
        .sample_source_table(&source_id, &table_id, req.limit.unwrap_or(25))
        .await
        .map_err(internal_err)?;
    Ok(Json(SourceSampleResponse {
        source_id,
        table_id: Some(table_id),
        rows,
    }))
}

pub(crate) async fn preview_transform_for_source_table(
    State(state): State<AppState>,
    Path((source_id, table_id)): Path<(String, String)>,
    Json(req): Json<TransformPreviewRequest>,
) -> Result<Json<TransformPreviewResponse>, (StatusCode, Json<ApiResponse>)> {
    if req.spec.input.source_id != source_id || req.spec.input.table_id != table_id {
        return Err(bad_req("transform spec input must match route source/table"));
    }
    let preview = state
        .engine
        .preview_transform_spec(&req.spec, req.limit)
        .await
        .map_err(internal_err)?;
    Ok(Json(TransformPreviewResponse {
        transform_id: None,
        preview,
    }))
}

pub(crate) async fn register_transform(
    State(state): State<AppState>,
    Json(req): Json<TransformRequest>,
) -> Result<Json<TransformResponse>, (StatusCode, Json<ApiResponse>)> {
    let transform = state
        .engine
        .register_transform(req.spec)
        .await
        .map_err(internal_err)?;
    save_transform_catalog_state(&state).map_err(internal_err)?;
    save_source_catalog_state(&state).map_err(internal_err)?;
    Ok(Json(TransformResponse {
        status: "ok",
        transform,
    }))
}

pub(crate) async fn list_transforms(
    State(state): State<AppState>,
) -> Result<Json<ListTransformsResponse>, (StatusCode, Json<ApiResponse>)> {
    let transforms = state.engine.list_transforms().map_err(internal_err)?;
    Ok(Json(ListTransformsResponse { transforms }))
}

pub(crate) async fn get_transform(
    State(state): State<AppState>,
    Path(transform): Path<String>,
) -> Result<Json<transform::TransformDescriptor>, (StatusCode, Json<ApiResponse>)> {
    state
        .engine
        .get_transform(&transform)
        .map(Json)
        .map_err(internal_err)
}

pub(crate) async fn validate_transform(
    State(state): State<AppState>,
    Path(transform): Path<String>,
) -> Result<Json<TransformValidationResponse>, (StatusCode, Json<ApiResponse>)> {
    let descriptor = state.engine.get_transform(&transform).map_err(internal_err)?;
    let validation = state
        .engine
        .validate_transform_spec(&descriptor.spec)
        .await
        .map_err(internal_err)?;
    Ok(Json(TransformValidationResponse {
        transform_id: Some(transform),
        validation,
    }))
}

pub(crate) async fn preview_transform(
    State(state): State<AppState>,
    Path(transform): Path<String>,
    Json(req): Json<SourceSampleRequest>,
) -> Result<Json<TransformPreviewResponse>, (StatusCode, Json<ApiResponse>)> {
    let preview = state
        .engine
        .preview_transform(&transform, req.limit)
        .await
        .map_err(internal_err)?;
    Ok(Json(TransformPreviewResponse {
        transform_id: Some(transform),
        preview,
    }))
}

pub(crate) async fn suggest_graph_profile(
    State(state): State<AppState>,
    Json(req): Json<GraphSuggestionRequest>,
) -> Result<Json<GraphSuggestionResponse>, (StatusCode, Json<ApiResponse>)> {
    let bound_schema = get_stored_schema_for_graph(&state, &req.graph).map_err(internal_err)?;
    let mut suggestion = state
        .engine
        .suggest_graph_mapping_with_schema(
            req.graph,
            req.display_name,
            req.source_ids,
            req.target_vocabulary,
            req.max_sample_rows,
            bound_schema.as_ref().map(|stored| &stored.graph_schema),
        )
        .await
        .map_err(bad_req)?;
    if let Some(schema_name) = get_bound_schema_name_for_graph(&state, &suggestion.graph)
        .map_err(internal_err)?
    {
        suggestion
            .warnings
            .push(format!("suggestions generated against bound schema `{schema_name}`"));
    }
    Ok(Json(GraphSuggestionResponse { suggestion }))
}

fn resolve_profile_schema(
    state: &AppState,
    query: &SourceProfileQuery,
) -> anyhow::Result<Option<StoredSchema>> {
    if let Some(schema_name) = &query.schema_name {
        let guard = state
            .schemas
            .read()
            .map_err(|_| anyhow::anyhow!("schema registry lock poisoned"))?;
        return Ok(guard.get(schema_name).cloned());
    }
    if let Some(graph) = &query.graph {
        return get_stored_schema_for_graph(state, graph);
    }
    Ok(None)
}

pub(crate) async fn register_graph_mapping(
    State(state): State<AppState>,
    Json(req): Json<GraphMappingRequest>,
) -> Result<Json<GraphMappingResponse>, (StatusCode, Json<ApiResponse>)> {
    let manifest = req.manifest;
    let graph_id = mapping::graph_id_for_manifest(&manifest, None).map_err(bad_req)?;
    let bound_schema = get_stored_schema_for_graph(&state, &graph_id)
        .map_err(internal_err)?;
    let bound_schema_name =
        get_bound_schema_name_for_graph(&state, &graph_id).map_err(internal_err)?;
    let descriptor = state
        .engine
        .register_graph_mapping_with_schema(
            manifest,
            FsPath::new("."),
            req.compile,
            bound_schema.as_ref().map(|stored| &stored.graph_schema),
            bound_schema_name,
        )
        .await
        .map_err(internal_err)?;
    save_graph_mapping_catalog_state(&state).map_err(internal_err)?;
    save_graph_catalog_state(&state).map_err(internal_err)?;
    let graph = state.engine.get_graph_descriptor(&descriptor.graph).ok();
    Ok(Json(GraphMappingResponse {
        status: "ok",
        mapping: descriptor,
        graph,
    }))
}

pub(crate) async fn register_graph_schema(
    State(state): State<AppState>,
    Json(req): Json<GraphSchemaSpecRequest>,
) -> Result<Json<GraphSchemaSpecResponse>, (StatusCode, Json<ApiResponse>)> {
    let descriptor = state
        .engine
        .register_graph_schema(req.spec)
        .await
        .map_err(bad_req)?;
    save_graph_schema_catalog_state(&state).map_err(internal_err)?;
    Ok(Json(GraphSchemaSpecResponse {
        status: "ok",
        graph_schema: descriptor,
    }))
}

pub(crate) async fn list_graph_schemas(
    State(state): State<AppState>,
) -> Result<Json<ListGraphSchemasResponse>, (StatusCode, Json<ApiResponse>)> {
    let graph_schemas = state.engine.list_graph_schemas().map_err(internal_err)?;
    Ok(Json(ListGraphSchemasResponse { graph_schemas }))
}

pub(crate) async fn get_graph_schema(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<loom_engine::graph_schema::GraphSchemaDescriptor>, (StatusCode, Json<ApiResponse>)>
{
    state
        .engine
        .get_graph_schema(&id)
        .map(Json)
        .map_err(internal_err)
}

pub(crate) async fn validate_graph_schema(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GraphSchemaValidationApiResponse>, (StatusCode, Json<ApiResponse>)> {
    let graph_name = state
        .engine
        .get_graph_schema(&id)
        .map_err(internal_err)?
        .graph;
    let bound_schema = get_stored_schema_for_graph(&state, &graph_name).map_err(internal_err)?;
    let bound_schema_name = get_bound_schema_name_for_graph(&state, &graph_name).map_err(internal_err)?;
    let validation = state
        .engine
        .validate_graph_schema_with_schema(
            &id,
            FsPath::new("."),
            bound_schema.as_ref().map(|stored| &stored.graph_schema),
            bound_schema_name,
        )
        .await
        .map_err(bad_req)?;
    save_graph_schema_catalog_state(&state).map_err(internal_err)?;
    Ok(Json(GraphSchemaValidationApiResponse {
        graph_schema_id: id,
        validation,
    }))
}

pub(crate) async fn preview_graph_schema(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GraphSchemaPreviewApiResponse>, (StatusCode, Json<ApiResponse>)> {
    let graph_name = state
        .engine
        .get_graph_schema(&id)
        .map_err(internal_err)?
        .graph;
    let bound_schema = get_stored_schema_for_graph(&state, &graph_name).map_err(internal_err)?;
    let bound_schema_name = get_bound_schema_name_for_graph(&state, &graph_name).map_err(internal_err)?;
    let preview = state
        .engine
        .preview_graph_schema_with_schema(
            &id,
            FsPath::new("."),
            bound_schema.as_ref().map(|stored| &stored.graph_schema),
            bound_schema_name,
        )
        .await
        .map_err(bad_req)?;
    save_graph_schema_catalog_state(&state).map_err(internal_err)?;
    Ok(Json(GraphSchemaPreviewApiResponse {
        graph_schema_id: id,
        preview,
    }))
}

pub(crate) async fn register_graph_schema_runtime(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GraphSchemaSpecResponse>, (StatusCode, Json<ApiResponse>)> {
    let graph_name = state
        .engine
        .get_graph_schema(&id)
        .map_err(internal_err)?
        .graph;
    let bound_schema = get_stored_schema_for_graph(&state, &graph_name).map_err(internal_err)?;
    let bound_schema_name = get_bound_schema_name_for_graph(&state, &graph_name).map_err(internal_err)?;
    let descriptor = state
        .engine
        .register_graph_schema_runtime_with_schema(
            &id,
            FsPath::new("."),
            false,
            bound_schema.as_ref().map(|stored| &stored.graph_schema),
            bound_schema_name,
        )
        .await
        .map_err(internal_err)?;
    save_graph_schema_catalog_state(&state).map_err(internal_err)?;
    save_graph_mapping_catalog_state(&state).map_err(internal_err)?;
    save_graph_catalog_state(&state).map_err(internal_err)?;
    Ok(Json(GraphSchemaSpecResponse {
        status: "ok",
        graph_schema: descriptor,
    }))
}

pub(crate) async fn publish_graph_schema_runtime(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GraphSchemaSpecResponse>, (StatusCode, Json<ApiResponse>)> {
    let graph_name = state
        .engine
        .get_graph_schema(&id)
        .map_err(internal_err)?
        .graph;
    let bound_schema = get_stored_schema_for_graph(&state, &graph_name).map_err(internal_err)?;
    let bound_schema_name = get_bound_schema_name_for_graph(&state, &graph_name).map_err(internal_err)?;
    let descriptor = state
        .engine
        .register_graph_schema_runtime_with_schema(
            &id,
            FsPath::new("."),
            true,
            bound_schema.as_ref().map(|stored| &stored.graph_schema),
            bound_schema_name,
        )
        .await
        .map_err(internal_err)?;
    save_graph_schema_catalog_state(&state).map_err(internal_err)?;
    save_graph_mapping_catalog_state(&state).map_err(internal_err)?;
    save_graph_catalog_state(&state).map_err(internal_err)?;
    Ok(Json(GraphSchemaSpecResponse {
        status: "ok",
        graph_schema: descriptor,
    }))
}

pub(crate) async fn list_graph_mappings(
    State(state): State<AppState>,
) -> Result<Json<ListGraphMappingsResponse>, (StatusCode, Json<ApiResponse>)> {
    let mappings = state.engine.list_graph_mappings().map_err(internal_err)?;
    Ok(Json(ListGraphMappingsResponse { mappings }))
}

pub(crate) async fn get_graph_mapping(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<mapping::GraphMappingDescriptor>, (StatusCode, Json<ApiResponse>)> {
    state
        .engine
        .get_graph_mapping(&graph)
        .map(Json)
        .map_err(internal_err)
}

pub(crate) async fn validate_graph_mapping(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<GraphMappingValidationResponse>, (StatusCode, Json<ApiResponse>)> {
    let descriptor = state
        .engine
        .get_graph_mapping(&graph)
        .map_err(internal_err)?;
    let validation = state
        .engine
        .validate_graph_mapping_full_with_schema(
            &descriptor.manifest,
            FsPath::new("."),
            get_stored_schema_for_graph(&state, &graph)
                .map_err(internal_err)?
                .as_ref()
                .map(|stored| &stored.graph_schema),
        )
        .await;
    let plan = validation.plan_preview.clone();
    Ok(Json(GraphMappingValidationResponse {
        graph,
        validation,
        plan,
    }))
}

pub(crate) async fn compile_graph_mapping(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<GraphMappingResponse>, (StatusCode, Json<ApiResponse>)> {
    let descriptor = state
        .engine
        .compile_graph_mapping_with_schema(
            &graph,
            FsPath::new("."),
            get_stored_schema_for_graph(&state, &graph)
                .map_err(internal_err)?
                .as_ref()
                .map(|stored| &stored.graph_schema),
        )
        .await
        .map_err(internal_err)?;
    save_graph_mapping_catalog_state(&state).map_err(internal_err)?;
    save_graph_catalog_state(&state).map_err(internal_err)?;
    let graph_descriptor = state.engine.get_graph_descriptor(&graph).ok();
    Ok(Json(GraphMappingResponse {
        status: "ok",
        mapping: descriptor,
        graph: graph_descriptor,
    }))
}

pub(crate) async fn refresh_graph_mapping(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<GraphMappingResponse>, (StatusCode, Json<ApiResponse>)> {
    let descriptor = state
        .engine
        .refresh_graph_mapping_with_schema(
            &graph,
            FsPath::new("."),
            get_stored_schema_for_graph(&state, &graph)
                .map_err(internal_err)?
                .as_ref()
                .map(|stored| &stored.graph_schema),
        )
        .await
        .map_err(internal_err)?;
    save_source_catalog_state(&state).map_err(internal_err)?;
    save_graph_mapping_catalog_state(&state).map_err(internal_err)?;
    save_graph_catalog_state(&state).map_err(internal_err)?;
    let graph_descriptor = state.engine.get_graph_descriptor(&graph).ok();
    Ok(Json(GraphMappingResponse {
        status: "ok",
        mapping: descriptor,
        graph: graph_descriptor,
    }))
}

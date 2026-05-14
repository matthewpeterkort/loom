use crate::*;
use loom_engine::schema::compile_graph_schema;
use serde_json::Value;

pub(crate) async fn list_schemas(
    State(state): State<AppState>,
) -> Result<Json<ListSchemasResponse>, (StatusCode, Json<ApiResponse>)> {
    let guard = state
        .schemas
        .read()
        .map_err(|_| internal_err("schema registry lock poisoned"))?;
    let mut schemas = guard
        .values()
        .map(|v| v.summary.clone())
        .collect::<Vec<SchemaSummary>>();
    schemas.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(ListSchemasResponse { schemas }))
}

pub(crate) async fn upsert_schema(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(schema): Json<Value>,
) -> Result<Json<UpsertSchemaResponse>, (StatusCode, Json<ApiResponse>)> {
    let normalized_name = sanitize_name(&name);
    if normalized_name.is_empty() {
        return Err(bad_req("schema name cannot be empty"));
    }
    validate_schema_doc(&schema).map_err(bad_req)?;
    let arrow_shapes = compile_arrow_shapes(&schema).map_err(bad_req)?;
    let graph_schema = compile_graph_schema(&schema).map_err(bad_req)?;
    let promoted_columns = promoted_columns_from_shapes(&arrow_shapes);
    let summary = summarize_schema(&normalized_name, &schema, arrow_shapes.len(), &graph_schema);
    let mut guard = state
        .schemas
        .write()
        .map_err(|_| internal_err("schema registry lock poisoned"))?;
    guard.insert(
        normalized_name.clone(),
        StoredSchema {
            doc: schema,
            summary: summary.clone(),
            arrow_shapes,
            promoted_columns,
            compile_warnings: graph_schema.warnings.clone(),
            graph_schema,
        },
    );
    drop(guard);
    save_schema_state(&state).map_err(internal_err)?;
    Ok(Json(UpsertSchemaResponse {
        status: "ok",
        detail: format!("schema `{}` registered", normalized_name),
        summary,
    }))
}

pub(crate) async fn get_schema(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<GetSchemaResponse>, (StatusCode, Json<ApiResponse>)> {
    let normalized_name = sanitize_name(&name);
    let guard = state
        .schemas
        .read()
        .map_err(|_| internal_err("schema registry lock poisoned"))?;
    let item = guard
        .get(&normalized_name)
        .ok_or_else(|| bad_req(format!("schema `{}` not found", normalized_name)))?;
    Ok(Json(GetSchemaResponse {
        summary: item.summary.clone(),
        arrow_shapes: item.arrow_shapes.clone(),
        graph_schema: item.graph_schema.clone(),
        schema: item.doc.clone(),
    }))
}

pub(crate) async fn get_schema_graph(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<SchemaGraphResponse>, (StatusCode, Json<ApiResponse>)> {
    let normalized_name = sanitize_name(&name);
    let guard = state
        .schemas
        .read()
        .map_err(|_| internal_err("schema registry lock poisoned"))?;
    let item = guard
        .get(&normalized_name)
        .ok_or_else(|| bad_req(format!("schema `{}` not found", normalized_name)))?;
    Ok(Json(SchemaGraphResponse {
        name: normalized_name,
        graph_schema: item.graph_schema.clone(),
    }))
}

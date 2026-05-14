use crate::*;

pub(crate) async fn bulk_load_graph(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(req): Json<IngestParquetToLanceRequest>,
) -> Result<Json<IngestResponse>, (StatusCode, Json<ApiResponse>)> {
    let mode = parse_ingest_mode(req.mode.as_deref()).map_err(bad_req)?;
    let uris = state
        .engine
        .ingest_parquet_graph_to_vortex(
            &graph,
            &req.vertices_parquet_path,
            &req.edges_parquet_path,
            &req.vortex_root_uri,
            mode,
        )
        .await
        .map_err(internal_err)?;
    save_graph_catalog_state(&state).map_err(internal_err)?;

    Ok(Json(IngestResponse {
        status: "ok",
        graph: graph.clone(),
        detail: format!("bulk-loaded graph `{}` from parquet into vortex", graph),
        vertices_uri: uris.vertices_uri,
        edges_uri: uris.edges_uri,
    }))
}

pub(crate) async fn bulk_load_graph_schema_typed(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(req): Json<IngestNdjsonWithSchemaRequest>,
) -> Result<Json<SchemaIngestResponse>, (StatusCode, Json<ApiResponse>)> {
    let mode = parse_ingest_mode(req.mode.as_deref()).map_err(bad_req)?;
    let stored = {
        let guard = state
            .schemas
            .read()
            .map_err(|_| internal_err("schema registry lock poisoned"))?;
        guard
            .get(&sanitize_name(&req.schema_name))
            .cloned()
            .ok_or_else(|| bad_req(format!("schema `{}` not found", req.schema_name)))?
    };

    let temp = tempfile::tempdir().map_err(internal_err)?;
    let vertices_parquet = temp.path().join("typed_vertices.parquet");
    let edges_parquet = temp.path().join("typed_edges.parquet");

    let loaded_vertices = build_typed_vertex_parquet(
        FsPath::new(&req.ndjson_dir),
        &vertices_parquet,
        &stored.promoted_columns,
    )
    .map_err(internal_err)?;
    build_edges_parquet_from_ndjson(FsPath::new(&req.ndjson_dir), &edges_parquet)
        .map_err(internal_err)?;

    let mut vertex_columns = vec![
        VertexColumn {
            name: "id".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "label".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_codec".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_bin".to_string(),
            sql_type: "BLOB NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_json_bin".to_string(),
            sql_type: "BLOB NOT NULL".to_string(),
        },
    ];
    for col in &stored.promoted_columns {
        vertex_columns.push(VertexColumn {
            name: col.column_name.clone(),
            sql_type: promoted_type_to_sql(&col.kind).to_string(),
        });
    }

    let uris = state
        .engine
        .ingest_parquet_graph_to_vortex_with_vertex_columns(
            &graph,
            vertices_parquet
                .to_str()
                .ok_or_else(|| internal_err("invalid vertices parquet path"))?,
            edges_parquet
                .to_str()
                .ok_or_else(|| internal_err("invalid edges parquet path"))?,
            &req.vortex_root_uri,
            mode,
            &vertex_columns,
        )
        .await
        .map_err(internal_err)?;

    {
        let mut guard = state
            .graph_schema_bindings
            .write()
            .map_err(|_| internal_err("schema binding lock poisoned"))?;
        guard.insert(sanitize_name(&graph), sanitize_name(&req.schema_name));
    }
    save_schema_state(&state).map_err(internal_err)?;
    save_graph_catalog_state(&state).map_err(internal_err)?;

    Ok(Json(SchemaIngestResponse {
        status: "ok",
        graph: graph.clone(),
        detail: format!(
            "schema-aware NDJSON bulk load complete for graph `{}` using schema `{}`",
            graph, req.schema_name
        ),
        vertices_uri: uris.vertices_uri,
        edges_uri: uris.edges_uri,
        loaded_vertices,
        typed_columns: stored.promoted_columns.len(),
    }))
}

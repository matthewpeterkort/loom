use crate::*;
use arrow_json::LineDelimitedWriter;
use futures::StreamExt;
use loom_engine::{graph_name, parse_query_json};
use serde_json::Value;

pub(crate) async fn query_graph(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(payload): Json<Value>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ApiResponse>)> {
    let query = decode_graph_query(&graph, payload).map_err(bad_req)?;
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = with_default_http_projection(query);
    let batches = state
        .engine
        .query_batches(&query)
        .await
        .map_err(internal_err)?;
    let row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    if row_count > http_inline_row_limit() {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ApiResponse {
                status: "error",
                detail: "result set too large for inline JSON response; use /v1/graph/:graph/query/ndjson".to_string(),
            }),
        ));
    }
    let rows = batches_to_json_rows(&batches).map_err(internal_err)?;

    Ok(Json(QueryResponse {
        status: "ok",
        graph,
        row_count,
        rows,
    }))
}

pub(crate) async fn query_graph_sql(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(req): Json<SqlQueryRequest>,
) -> Result<Json<SqlQueryResponse>, (StatusCode, Json<ApiResponse>)> {
    let batches = state
        .engine
        .query_sql_batches(&req.sql)
        .await
        .map_err(internal_err)?;
    let row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    if row_count > http_inline_row_limit() {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ApiResponse {
                status: "error",
                detail: "result set too large for inline JSON response; use a narrower SQL projection or predicate".to_string(),
            }),
        ));
    }
    let rows = batches_to_json_rows(&batches).map_err(internal_err)?;
    Ok(Json(SqlQueryResponse {
        status: "ok",
        graph,
        row_count,
        rows,
    }))
}

pub(crate) async fn query_virtual_graph_sql(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(req): Json<SqlQueryRequest>,
) -> Result<Json<SqlQueryResponse>, (StatusCode, Json<ApiResponse>)> {
    let batches = state
        .engine
        .query_virtual_graph_sql_batches(&graph, &req.sql)
        .await
        .map_err(internal_err)?;
    let row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    if row_count > http_inline_row_limit() {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ApiResponse {
                status: "error",
                detail: "result set too large for inline JSON response; use a narrower SQL projection or predicate".to_string(),
            }),
        ));
    }
    let rows = batches_to_json_rows(&batches).map_err(internal_err)?;
    Ok(Json(SqlQueryResponse {
        status: "ok",
        graph,
        row_count,
        rows,
    }))
}

pub(crate) async fn explain_virtual_graph_sql(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(req): Json<SqlQueryRequest>,
) -> Result<Json<ExplainQueryResponse>, (StatusCode, Json<ApiResponse>)> {
    let explain = state
        .engine
        .explain_virtual_graph_sql(&graph, &req.sql)
        .await
        .map_err(internal_err)?;
    Ok(Json(ExplainQueryResponse { graph, explain }))
}

pub(crate) async fn preview_virtual_reference(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(req): Json<ReferencePreviewRequest>,
) -> Result<Json<SqlQueryResponse>, (StatusCode, Json<ApiResponse>)> {
    let rows = state
        .engine
        .preview_virtual_reference_rows(&graph, &req.name, req.limit.unwrap_or(25))
        .await
        .map_err(internal_err)?;
    Ok(Json(SqlQueryResponse {
        status: "ok",
        graph,
        row_count: rows.len(),
        rows: rows
            .into_iter()
            .map(|row| serde_json::from_str(&row))
            .collect::<Result<Vec<Value>, _>>()
            .map_err(internal_err)?,
    }))
}

pub(crate) async fn query_compat(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ApiResponse>)> {
    let query = parse_query_json(&payload.to_string()).map_err(bad_req)?;
    let graph = graph_name(&query).map_err(bad_req)?.to_string();
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = with_default_http_projection(query);
    let batches = state
        .engine
        .query_batches(&query)
        .await
        .map_err(internal_err)?;
    let row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    if row_count > http_inline_row_limit() {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ApiResponse {
                status: "error",
                detail: "result set too large for inline JSON response; use /v1/graph/:graph/query/ndjson".to_string(),
            }),
        ));
    }
    let rows = batches_to_json_rows(&batches).map_err(internal_err)?;

    Ok(Json(QueryResponse {
        status: "ok",
        graph,
        row_count,
        rows,
    }))
}

pub(crate) async fn query_graph_ndjson(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(payload): Json<Value>,
) -> Result<HttpResponse<Body>, (StatusCode, Json<ApiResponse>)> {
    let query = decode_graph_query(&graph, payload).map_err(bad_req)?;
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = with_default_http_projection(query);
    let stream = state
        .engine
        .query_batch_stream(&query)
        .await
        .map_err(internal_err)?;
    let stream = stream.map(|item| {
        let batch = item.map_err(|e| std::io::Error::other(e.to_string()))?;
        if batch.num_rows() == 0 {
            return Ok::<Bytes, std::io::Error>(Bytes::new());
        }
        let mut writer = LineDelimitedWriter::new(Vec::<u8>::new());
        let bytes = writer
            .write(&batch)
            .and_then(|_| writer.finish())
            .map(|_| writer.into_inner())
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok::<Bytes, std::io::Error>(Bytes::from(bytes))
    });
    let mut response = HttpResponse::new(Body::from_stream(stream));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/x-ndjson"),
    );
    Ok(response)
}

pub(crate) async fn query_graph_compact(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(payload): Json<Value>,
) -> Result<HttpResponse<Body>, (StatusCode, Json<ApiResponse>)> {
    let query = decode_graph_query(&graph, payload).map_err(bad_req)?;
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = with_default_http_projection(query);
    let bytes = state
        .engine
        .query_json_bytes(&query)
        .await
        .map_err(internal_err)?;
    Ok(json_bytes_response(bytes))
}

pub(crate) async fn query_compat_ndjson(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<HttpResponse<Body>, (StatusCode, Json<ApiResponse>)> {
    let query = parse_query_json(&payload.to_string()).map_err(bad_req)?;
    let graph = graph_name(&query).map_err(bad_req)?.to_string();
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = with_default_http_projection(query);
    let stream = state
        .engine
        .query_batch_stream(&query)
        .await
        .map_err(internal_err)?;
    let stream = stream.map(|item| {
        let batch = item.map_err(|e| std::io::Error::other(e.to_string()))?;
        if batch.num_rows() == 0 {
            return Ok::<Bytes, std::io::Error>(Bytes::new());
        }
        let mut writer = LineDelimitedWriter::new(Vec::<u8>::new());
        let bytes = writer
            .write(&batch)
            .and_then(|_| writer.finish())
            .map(|_| writer.into_inner())
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok::<Bytes, std::io::Error>(Bytes::from(bytes))
    });
    let mut response = HttpResponse::new(Body::from_stream(stream));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/x-ndjson"),
    );
    Ok(response)
}

pub(crate) async fn query_compat_compact(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<HttpResponse<Body>, (StatusCode, Json<ApiResponse>)> {
    let query = parse_query_json(&payload.to_string()).map_err(bad_req)?;
    let graph = graph_name(&query).map_err(bad_req)?.to_string();
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = with_default_http_projection(query);
    let bytes = state
        .engine
        .query_json_bytes(&query)
        .await
        .map_err(internal_err)?;
    Ok(json_bytes_response(bytes))
}

pub(crate) async fn query_graph_full(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(payload): Json<Value>,
) -> Result<HttpResponse<Body>, (StatusCode, Json<ApiResponse>)> {
    let query = decode_graph_query(&graph, payload).map_err(bad_req)?;
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = ensure_full_fast_projection(query);
    let batches = state
        .engine
        .query_batches(&query)
        .await
        .map_err(internal_err)?;
    let stored = get_stored_schema_for_graph(&state, &graph)
        .map_err(internal_err)?
        .ok_or_else(|| {
            bad_req(format!(
                "graph `{graph}` is not schema-bound; full mode requires schema-aware ingest"
            ))
        })?;
    let bytes =
        reconstruct_full_rows_fast_from_batches(&batches, Some(&stored)).map_err(internal_err)?;
    Ok(json_bytes_response(bytes))
}

pub(crate) async fn query_graph_full_fast(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(payload): Json<Value>,
) -> Result<HttpResponse<Body>, (StatusCode, Json<ApiResponse>)> {
    let query = decode_graph_query(&graph, payload).map_err(bad_req)?;
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = ensure_full_fast_projection(query);
    let batches = state
        .engine
        .query_batches(&query)
        .await
        .map_err(internal_err)?;
    let stored = get_stored_schema_for_graph(&state, &graph)
        .map_err(internal_err)?
        .ok_or_else(|| {
            bad_req(format!(
                "graph `{graph}` is not schema-bound; full mode requires schema-aware ingest"
            ))
        })?;
    let bytes =
        reconstruct_full_rows_fast_from_batches(&batches, Some(&stored)).map_err(internal_err)?;
    Ok(json_bytes_response(bytes))
}

pub(crate) async fn query_compat_full(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<HttpResponse<Body>, (StatusCode, Json<ApiResponse>)> {
    let query = parse_query_json(&payload.to_string()).map_err(bad_req)?;
    let graph = graph_name(&query).map_err(bad_req)?.to_string();
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = ensure_full_fast_projection(query);
    let batches = state
        .engine
        .query_batches(&query)
        .await
        .map_err(internal_err)?;
    let stored = get_stored_schema_for_graph(&state, &graph)
        .map_err(internal_err)?
        .ok_or_else(|| {
            bad_req(format!(
                "graph `{graph}` is not schema-bound; full mode requires schema-aware ingest"
            ))
        })?;
    let bytes =
        reconstruct_full_rows_fast_from_batches(&batches, Some(&stored)).map_err(internal_err)?;
    Ok(json_bytes_response(bytes))
}

pub(crate) async fn query_compat_full_fast(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<HttpResponse<Body>, (StatusCode, Json<ApiResponse>)> {
    let query = parse_query_json(&payload.to_string()).map_err(bad_req)?;
    let graph = graph_name(&query).map_err(bad_req)?.to_string();
    validate_query_against_schema(&state, &graph, &query)?;
    let query = rewrite_query_with_promoted_columns_for_graph(&state, &graph, query)?;
    let query = ensure_full_fast_projection(query);
    let batches = state
        .engine
        .query_batches(&query)
        .await
        .map_err(internal_err)?;
    let stored = get_stored_schema_for_graph(&state, &graph)
        .map_err(internal_err)?
        .ok_or_else(|| {
            bad_req(format!(
                "graph `{graph}` is not schema-bound; full mode requires schema-aware ingest"
            ))
        })?;
    let bytes =
        reconstruct_full_rows_fast_from_batches(&batches, Some(&stored)).map_err(internal_err)?;
    Ok(json_bytes_response(bytes))
}

pub(crate) async fn get_query_schema_vocabulary(
    State(state): State<AppState>,
    Path(graph): Path<String>,
) -> Result<Json<SchemaVocabularyResponse>, (StatusCode, Json<ApiResponse>)> {
    let vocabulary = schema_vocabulary_for_graph(&state, &graph)
        .map_err(internal_err)?
        .ok_or_else(|| bad_req(format!("graph `{graph}` is not schema-bound")))?;
    Ok(Json(vocabulary))
}

pub(crate) async fn explain_query_schema(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    Json(payload): Json<Value>,
) -> Result<Json<QuerySchemaExplainResponse>, (StatusCode, Json<ApiResponse>)> {
    let query = decode_graph_query(&graph, payload).map_err(bad_req)?;
    let explain = explain_query_against_schema(&state, &graph, &query)
        .map_err(internal_err)?
        .ok_or_else(|| bad_req(format!("graph `{graph}` is not schema-bound")))?;
    Ok(Json(explain))
}

pub(crate) async fn autocomplete_query_schema(
    State(state): State<AppState>,
    Path(graph): Path<String>,
    axum::extract::Query(req): axum::extract::Query<QueryAutocompleteRequest>,
) -> Result<Json<QuerySchemaAutocompleteResponse>, (StatusCode, Json<ApiResponse>)> {
    let labels = req
        .labels
        .unwrap_or_default()
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();
    let direction = req.direction.unwrap_or_else(|| "out".to_string());
    let response = autocomplete_query_against_schema(
        &state,
        &graph,
        labels,
        &direction,
        req.prefix.as_deref(),
    )
    .map_err(internal_err)?
    .ok_or_else(|| bad_req(format!("graph `{graph}` is not schema-bound")))?;
    Ok(Json(response))
}

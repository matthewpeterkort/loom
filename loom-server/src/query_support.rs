use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
};
use arrow::record_batch::RecordBatch;
use arrow_json::ArrayWriter;
use axum::{body::Body, extract::Json, http::StatusCode, response::Response as HttpResponse};
use loom_engine::{parse_query_json, step_op, step_string_list, steps, steps_mut, Query};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use serde_json::Value;

use crate::{
    bad_req, decode_payload_flexbuf, internal_err, sanitize_name, ApiResponse, AppState,
    PromotedColumn, PromotedType, QuerySchemaAutocompleteResponse, QuerySchemaExplainResponse,
    QuerySchemaStepExplain, SchemaEntityRelations, SchemaRelationSummary, SchemaVocabularyResponse,
    StoredSchema,
};

pub(crate) fn decode_graph_query(graph: &str, payload: Value) -> Result<Query, anyhow::Error> {
    if payload.get("graph").is_some() && payload.get("steps").is_some() {
        return parse_query_json(&payload.to_string());
    }

    let steps = payload
        .get("steps")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("payload must include `steps`"))?;
    let shaped = serde_json::json!({
        "graph": graph,
        "steps": steps
    });
    parse_query_json(&shaped.to_string())
}

pub(crate) fn rewrite_query_with_promoted_columns_for_graph(
    state: &AppState,
    graph: &str,
    query: Query,
) -> Result<Query, (StatusCode, Json<ApiResponse>)> {
    let graph_key = sanitize_name(graph);
    let schema_key = {
        let guard = state
            .graph_schema_bindings
            .read()
            .map_err(|_| internal_err("schema binding lock poisoned"))?;
        guard.get(&graph_key).cloned()
    };
    let Some(schema_key) = schema_key else {
        return Ok(query);
    };
    let stored = {
        let guard = state
            .schemas
            .read()
            .map_err(|_| internal_err("schema registry lock poisoned"))?;
        guard.get(&schema_key).cloned()
    };
    let Some(stored) = stored else {
        return Ok(query);
    };
    Ok(rewrite_query_with_promoted_columns(query, &stored))
}

pub(crate) fn rewrite_query_with_promoted_columns(
    mut query: Query,
    stored: &StoredSchema,
) -> Query {
    let mut active_label: Option<String> = None;
    if let Ok(step_list) = steps_mut(&mut query) {
        for step in step_list {
            match step.get("op").and_then(Value::as_str) {
                Some("has_label") => {
                    let labels = step
                        .get("labels")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|value| value.as_str().map(str::to_string))
                        .collect::<Vec<_>>();
                    active_label = if labels.len() == 1 {
                        labels.first().cloned()
                    } else {
                        None
                    };
                }
                Some("has") => {
                    if let Some(field) = step
                        .get("field")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                    {
                        if let Some(col) =
                            resolve_promoted_column(stored, active_label.as_deref(), &field)
                        {
                            if let Some(field_ref) = step.get_mut("field") {
                                *field_ref = Value::String(col);
                            }
                        }
                    }
                }
                Some("render") => {
                    if let Some(fields) = step.get_mut("fields").and_then(Value::as_array_mut) {
                        for field in fields.iter_mut() {
                            let Some(field_name) = field.as_str().map(str::to_string) else {
                                continue;
                            };
                            if field_name == "*" {
                                continue;
                            }
                            if let Some(col) = resolve_promoted_column(
                                stored,
                                active_label.as_deref(),
                                &field_name,
                            ) {
                                *field = Value::String(col);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    query
}

pub(crate) fn resolve_promoted_column(
    stored: &StoredSchema,
    active_label: Option<&str>,
    field: &str,
) -> Option<String> {
    if let Some(label) = active_label {
        let s_label = sanitize_name(label);
        for col in &stored.promoted_columns {
            if sanitize_name(&col.resource_type) == s_label
                && sanitize_name(&col.field_name) == sanitize_name(field)
            {
                return Some(col.column_name.clone());
            }
        }
        return None;
    }

    let matches = stored
        .promoted_columns
        .iter()
        .filter(|c| sanitize_name(&c.field_name) == sanitize_name(field))
        .collect::<Vec<&PromotedColumn>>();
    if matches.len() == 1 {
        return Some(matches[0].column_name.clone());
    }
    None
}

pub(crate) fn with_default_http_projection(mut query: Query) -> Query {
    let has_render = steps(&query)
        .map(|items| items.iter().any(|s| matches!(s.get("op").and_then(Value::as_str), Some("render"))))
        .unwrap_or(false);
    let has_count = steps(&query)
        .map(|items| items.iter().any(|s| matches!(s.get("op").and_then(Value::as_str), Some("count"))))
        .unwrap_or(false);
    if !has_render && !has_count {
        if let Ok(step_list) = steps_mut(&mut query) {
            step_list.push(serde_json::json!({
                "op": "render",
                "fields": ["id", "label"]
            }));
        }
    }
    query
}

pub(crate) fn ensure_full_fast_projection(mut query: Query) -> Query {
    let has_count = steps(&query)
        .map(|items| items.iter().any(|s| matches!(s.get("op").and_then(Value::as_str), Some("count"))))
        .unwrap_or(false);
    if has_count {
        return query;
    }
    let fast_fields = vec![
        "id".to_string(),
        "label".to_string(),
        "payload_json_bin".to_string(),
        "payload_codec".to_string(),
        "payload_bin".to_string(),
    ];
    if let Ok(step_list) = steps_mut(&mut query) {
        for step in &mut *step_list {
            if matches!(step.get("op").and_then(Value::as_str), Some("render")) {
                *step = serde_json::json!({
                    "op": "render",
                    "fields": fast_fields
                });
                return query;
            }
        }
        step_list.push(serde_json::json!({
            "op": "render",
            "fields": fast_fields
        }));
    }
    query
}

pub(crate) fn get_stored_schema_for_graph(
    state: &AppState,
    graph: &str,
) -> Result<Option<StoredSchema>, anyhow::Error> {
    let graph_key = sanitize_name(graph);
    let schema_key = {
        let guard = state
            .graph_schema_bindings
            .read()
            .map_err(|_| anyhow::anyhow!("schema binding lock poisoned"))?;
        guard.get(&graph_key).cloned()
    };
    let Some(schema_key) = schema_key else {
        return Ok(None);
    };
    let stored = {
        let guard = state
            .schemas
            .read()
            .map_err(|_| anyhow::anyhow!("schema registry lock poisoned"))?;
        guard.get(&schema_key).cloned()
    };
    Ok(stored)
}

pub(crate) fn get_bound_schema_name_for_graph(
    state: &AppState,
    graph: &str,
) -> Result<Option<String>, anyhow::Error> {
    let graph_key = sanitize_name(graph);
    let schema_key = {
        let guard = state
            .graph_schema_bindings
            .read()
            .map_err(|_| anyhow::anyhow!("schema binding lock poisoned"))?;
        guard.get(&graph_key).cloned()
    };
    Ok(schema_key)
}

pub(crate) fn bind_schema_to_graph(
    state: &AppState,
    graph: &str,
    schema_name: &str,
) -> Result<(), anyhow::Error> {
    let graph_key = sanitize_name(graph);
    let schema_key = sanitize_name(schema_name);
    {
        let schemas = state
            .schemas
            .read()
            .map_err(|_| anyhow::anyhow!("schema registry lock poisoned"))?;
        if !schemas.contains_key(&schema_key) {
            return Err(anyhow::anyhow!("schema `{schema_name}` not found"));
        }
    }
    let mut guard = state
        .graph_schema_bindings
        .write()
        .map_err(|_| anyhow::anyhow!("schema binding lock poisoned"))?;
    guard.insert(graph_key, schema_key);
    Ok(())
}

fn infer_labels_from_ids(ids: &[String]) -> Vec<String> {
    let labels = ids
        .iter()
        .filter_map(|id| id.split('/').next().map(str::to_string))
        .collect::<BTreeSet<_>>();
    labels.into_iter().collect()
}

fn runtime_edge_labels(
    state: &AppState,
    graph: &str,
) -> Result<HashSet<String>, anyhow::Error> {
    let mut labels = HashSet::new();
    if let Ok(mapping) = state.engine.get_graph_mapping(graph) {
        for edge in mapping.manifest.edges {
            labels.insert(edge.label);
        }
    }
    if let Ok(graph_desc) = state.engine.get_graph_descriptor(graph) {
        for label in graph_desc.active_snapshot.stats.edge_labels.keys() {
            labels.insert(label.clone());
        }
    }
    Ok(labels)
}

pub(crate) fn schema_vocabulary_for_graph(
    state: &AppState,
    graph: &str,
) -> Result<Option<SchemaVocabularyResponse>, anyhow::Error> {
    let Some(schema_name) = get_bound_schema_name_for_graph(state, graph)? else {
        return Ok(None);
    };
    let stored = get_stored_schema_for_graph(state, graph)?
        .ok_or_else(|| anyhow::anyhow!("schema `{schema_name}` not found"))?;
    let available = runtime_edge_labels(state, graph)?;
    let mut entities = Vec::new();
    for entity_name in stored.graph_schema.entity_names() {
        let outgoing = stored
            .graph_schema
            .outgoing_links(&entity_name)
            .into_iter()
            .map(|link| SchemaRelationSummary {
                rel: link.rel.clone(),
                counterpart_labels: vec![link.target_entity.clone()],
                wildcard_target: link.wildcard_target,
                runtime_available: available.contains(&link.rel),
            })
            .collect::<Vec<_>>();
        let incoming = stored
            .graph_schema
            .incoming_links(&entity_name)
            .into_iter()
            .map(|(source, link)| SchemaRelationSummary {
                rel: link.rel.clone(),
                counterpart_labels: vec![source.name.clone()],
                wildcard_target: link.wildcard_target,
                runtime_available: available.contains(&link.rel),
            })
            .collect::<Vec<_>>();
        entities.push(SchemaEntityRelations {
            entity: entity_name,
            outgoing,
            incoming,
        });
    }
    Ok(Some(SchemaVocabularyResponse {
        graph: graph.to_string(),
        schema_name,
        entities,
    }))
}

pub(crate) fn explain_query_against_schema(
    state: &AppState,
    graph: &str,
    query: &Query,
) -> Result<Option<QuerySchemaExplainResponse>, anyhow::Error> {
    let Some(schema_name) = get_bound_schema_name_for_graph(state, graph)? else {
        return Ok(None);
    };
    let stored = get_stored_schema_for_graph(state, graph)?
        .ok_or_else(|| anyhow::anyhow!("schema `{schema_name}` not found"))?;
    let available = runtime_edge_labels(state, graph)?;
    let mut active_labels = Vec::<String>::new();
    let mut explained_steps = Vec::new();
    let mut valid = true;

    for (index, step) in loom_engine::steps(query)?.iter().enumerate() {
        let before = active_labels.clone();
        let mut after = before.clone();
        let mut labels = Vec::new();
        let mut allowed_targets = Vec::new();
        let mut schema_valid = true;
        let mut runtime_available = true;
        let mut detail;
        let op = match step_op(step)? {
            "v" => {
                let ids = step_string_list(step, "ids")?;
                after = infer_labels_from_ids(&ids);
                detail = "seed vertices".to_string();
                "v"
            }
            "has_label" => {
                let step_labels = step_string_list(step, "labels")?;
                labels = step_labels.clone();
                after = step_labels.clone();
                detail = "restrict active labels".to_string();
                "has_label"
            }
            "out" | "out_e" => {
                let rels = step_string_list(step, "labels")?;
                labels = rels.clone();
                runtime_available = rels.iter().all(|rel| available.contains(rel));
                if before.is_empty() {
                    detail = "relation checked without known active label".to_string();
                } else {
                    let mut targets = BTreeSet::new();
                    for source in &before {
                        for rel in &rels {
                            let allowed = stored.graph_schema.allowed_targets_for_rel(source, &rel);
                            if allowed.is_empty() {
                                schema_valid = false;
                            }
                            targets.extend(allowed);
                        }
                    }
                    allowed_targets = targets.into_iter().collect();
                    if matches!(step_op(step)?, "out") {
                        after = allowed_targets.clone();
                    } else {
                        after = Vec::new();
                    }
                    detail = if schema_valid {
                        "schema-valid outbound traversal".to_string()
                    } else {
                        "outbound relation not allowed from current label set".to_string()
                    };
                }
                "out"
            }
            "in" | "in_e" => {
                let rels = step_string_list(step, "labels")?;
                labels = rels.clone();
                runtime_available = rels.iter().all(|rel| available.contains(rel));
                if before.is_empty() {
                    detail = "relation checked without known active label".to_string();
                } else {
                    let mut sources = BTreeSet::new();
                    for target in &before {
                        for rel in &rels {
                            let allowed = stored.graph_schema.reverse_sources_for_rel(target, &rel);
                            if allowed.is_empty() {
                                schema_valid = false;
                            }
                            sources.extend(allowed);
                        }
                    }
                    allowed_targets = sources.iter().cloned().collect();
                    if matches!(step_op(step)?, "in") {
                        after = allowed_targets.clone();
                    } else {
                        after = Vec::new();
                    }
                    detail = if schema_valid {
                        "schema-valid inbound traversal".to_string()
                    } else {
                        "inbound relation not allowed for current label set".to_string()
                    };
                }
                "in"
            }
            "both" | "both_e" => {
                let rels = step_string_list(step, "labels")?;
                labels = rels.clone();
                runtime_available = rels.iter().all(|rel| available.contains(rel));
                let mut candidates = BTreeSet::new();
                for label in &before {
                    for rel in &rels {
                        let out = stored.graph_schema.allowed_targets_for_rel(label, &rel);
                        let inward = stored.graph_schema.reverse_sources_for_rel(label, &rel);
                        if out.is_empty() && inward.is_empty() {
                            schema_valid = false;
                        }
                        candidates.extend(out);
                        candidates.extend(inward);
                    }
                }
                allowed_targets = candidates.into_iter().collect();
                if matches!(step_op(step)?, "both") {
                    after = allowed_targets.clone();
                } else {
                    after = Vec::new();
                }
                detail = if schema_valid {
                    "schema-valid bidirectional traversal".to_string()
                } else {
                    "relation not allowed in either direction for current label set".to_string()
                };
                "both"
            }
            "has" => {
                detail = "property filter".to_string();
                "has"
            }
            "has_id" => {
                let ids = step_string_list(step, "ids")?;
                after = infer_labels_from_ids(&ids);
                detail = "restrict ids".to_string();
                "has_id"
            }
            "limit" => {
                detail = "limit".to_string();
                "limit"
            }
            "skip" => {
                detail = "skip".to_string();
                "skip"
            }
            "count" => {
                detail = "count".to_string();
                "count"
            }
            "render" => {
                detail = "render".to_string();
                "render"
            }
            other => {
                detail = format!("unsupported step `{other}`");
                other
            }
        };
        if !schema_valid || !runtime_available {
            valid = false;
            if schema_valid && !runtime_available {
                detail.push_str("; relation exists in schema but has no registered runtime edge view");
            }
        }
        explained_steps.push(QuerySchemaStepExplain {
            index,
            op: op.to_string(),
            labels,
            active_labels_before: before,
            active_labels_after: after.clone(),
            allowed_targets,
            schema_valid,
            runtime_available,
            detail,
        });
        active_labels = after;
    }
    Ok(Some(QuerySchemaExplainResponse {
        graph: graph.to_string(),
        schema_name,
        valid,
        steps: explained_steps,
    }))
}

pub(crate) fn validate_query_against_schema(
    state: &AppState,
    graph: &str,
    query: &Query,
) -> Result<(), (StatusCode, Json<ApiResponse>)> {
    let Some(explain) = explain_query_against_schema(state, graph, query).map_err(internal_err)? else {
        return Ok(());
    };
    let bad = explain
        .steps
        .iter()
        .find(|step| !step.schema_valid)
        .map(|step| step.detail.clone());
    if let Some(detail) = bad {
        return Err(bad_req(format!("schema-bound query invalid: {detail}")));
    }
    Ok(())
}

pub(crate) fn autocomplete_query_against_schema(
    state: &AppState,
    graph: &str,
    labels: Vec<String>,
    direction: &str,
    prefix: Option<&str>,
) -> Result<Option<QuerySchemaAutocompleteResponse>, anyhow::Error> {
    let Some(schema_name) = get_bound_schema_name_for_graph(state, graph)? else {
        return Ok(None);
    };
    let stored = get_stored_schema_for_graph(state, graph)?
        .ok_or_else(|| anyhow::anyhow!("schema `{schema_name}` not found"))?;
    let available = runtime_edge_labels(state, graph)?;
    let prefix = prefix.unwrap_or("").to_ascii_lowercase();
    let mut relations = BTreeMap::<String, SchemaRelationSummary>::new();
    for label in &labels {
        match direction {
            "in" => {
                for (source, link) in stored.graph_schema.incoming_links(label) {
                    if !prefix.is_empty() && !link.rel.to_ascii_lowercase().starts_with(&prefix) {
                        continue;
                    }
                    relations
                        .entry(link.rel.clone())
                        .and_modify(|summary| {
                            if !summary.counterpart_labels.contains(&source.name) {
                                summary.counterpart_labels.push(source.name.clone());
                            }
                            summary.wildcard_target |= link.wildcard_target;
                            summary.runtime_available |= available.contains(&link.rel);
                        })
                        .or_insert_with(|| SchemaRelationSummary {
                            rel: link.rel.clone(),
                            counterpart_labels: vec![source.name.clone()],
                            wildcard_target: link.wildcard_target,
                            runtime_available: available.contains(&link.rel),
                        });
                }
            }
            _ => {
                for link in stored.graph_schema.outgoing_links(label) {
                    if !prefix.is_empty() && !link.rel.to_ascii_lowercase().starts_with(&prefix) {
                        continue;
                    }
                    relations
                        .entry(link.rel.clone())
                        .and_modify(|summary| {
                            if !summary.counterpart_labels.contains(&link.target_entity) {
                                summary.counterpart_labels.push(link.target_entity.clone());
                            }
                            summary.wildcard_target |= link.wildcard_target;
                            summary.runtime_available |= available.contains(&link.rel);
                        })
                        .or_insert_with(|| SchemaRelationSummary {
                            rel: link.rel.clone(),
                            counterpart_labels: vec![link.target_entity.clone()],
                            wildcard_target: link.wildcard_target,
                            runtime_available: available.contains(&link.rel),
                        });
                }
            }
        }
    }
    Ok(Some(QuerySchemaAutocompleteResponse {
        graph: graph.to_string(),
        schema_name,
        current_labels: labels,
        direction: direction.to_string(),
        relations: relations.into_values().collect(),
    }))
}

pub(crate) fn http_inline_row_limit() -> usize {
    std::env::var("LOOM_HTTP_MAX_INLINE_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10_000)
}

pub(crate) fn json_bytes_response(bytes: Vec<u8>) -> HttpResponse<Body> {
    let mut response = HttpResponse::new(Body::from(bytes));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response
}

pub(crate) fn batches_to_json_rows(batches: &[RecordBatch]) -> anyhow::Result<Vec<Value>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let refs = batches.iter().collect::<Vec<&RecordBatch>>();
    let mut writer = ArrayWriter::new(Vec::<u8>::new());
    writer.write_batches(&refs)?;
    writer.finish()?;
    let bytes = writer.into_inner();
    Ok(serde_json::from_slice(&bytes)?)
}

#[allow(dead_code)]
pub(crate) fn reconstruct_full_rows_fast(
    rows: Vec<Value>,
    stored: Option<&StoredSchema>,
) -> Result<Vec<Value>, anyhow::Error> {
    let stored = stored.ok_or_else(|| {
        anyhow::anyhow!("schema-bound full reconstruction requires stored schema")
    })?;
    let mut out = Vec::<Value>::with_capacity(rows.len());
    for row in rows {
        let id = row
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let label = row
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or("Resource")
            .to_string();

        let mut obj = serde_json::Map::<String, Value>::new();
        if !id.is_empty() {
            obj.insert(
                "id".to_string(),
                Value::String(strip_resource_type_prefix(&id)),
            );
        }
        obj.insert("resourceType".to_string(), Value::String(label.clone()));

        let mut cols = stored
            .promoted_columns
            .iter()
            .filter(|c| sanitize_name(&c.resource_type) == sanitize_name(&label))
            .collect::<Vec<&PromotedColumn>>();
        cols.sort_by_key(|c| c.field_name.matches('.').count());

        for col in cols {
            let Some(raw) = row.get(&col.column_name) else {
                continue;
            };
            if raw.is_null() {
                continue;
            }
            let value = match col.kind {
                PromotedType::Json => {
                    if let Some(s) = raw.as_str() {
                        serde_json::from_str::<Value>(s).unwrap_or_else(|_| raw.clone())
                    } else {
                        raw.clone()
                    }
                }
                _ => raw.clone(),
            };
            set_nested_value(&mut obj, &col.field_name, value);
        }

        out.push(Value::Object(obj));
    }
    Ok(out)
}

pub(crate) fn reconstruct_full_rows_fast_from_batches(
    batches: &[RecordBatch],
    stored: Option<&StoredSchema>,
) -> Result<Vec<u8>, anyhow::Error> {
    let stored = stored.ok_or_else(|| {
        anyhow::anyhow!("schema-bound full reconstruction requires stored schema")
    })?;
    let mut out = Vec::<u8>::new();
    out.push(b'[');
    let mut wrote_any = false;

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let id_arr = batch
            .column_by_name("id")
            .and_then(|a| a.as_any().downcast_ref::<StringArray>());
        let label_arr = batch
            .column_by_name("label")
            .and_then(|a| a.as_any().downcast_ref::<StringArray>());
        let payload_codec_arr = batch
            .column_by_name("payload_codec")
            .and_then(|a| a.as_any().downcast_ref::<StringArray>());
        let payload_bin_arr = batch
            .column_by_name("payload_bin")
            .and_then(|a| a.as_any().downcast_ref::<BinaryArray>());
        let payload_json_bin_arr = batch
            .column_by_name("payload_json_bin")
            .and_then(|a| a.as_any().downcast_ref::<BinaryArray>());
        let mut promoted_arrays = std::collections::HashMap::<String, &ArrayRef>::new();
        for col in &stored.promoted_columns {
            if let Some(arr) = batch.column_by_name(&col.column_name) {
                promoted_arrays.insert(col.column_name.clone(), arr);
            }
        }

        for row_idx in 0..batch.num_rows() {
            let id = id_arr
                .filter(|a| !a.is_null(row_idx))
                .map(|a| a.value(row_idx))
                .unwrap_or("");
            let label = label_arr
                .filter(|a| !a.is_null(row_idx))
                .map(|a| a.value(row_idx))
                .unwrap_or("Resource");

            if let Some(raw_json) = payload_json_bin_arr
                .filter(|a| !a.is_null(row_idx))
                .map(|a| a.value(row_idx))
            {
                if wrote_any {
                    out.push(b',');
                }
                out.extend_from_slice(raw_json);
                wrote_any = true;
                continue;
            }

            let codec = payload_codec_arr
                .filter(|a| !a.is_null(row_idx))
                .map(|a| a.value(row_idx));
            let payload_obj = payload_bin_arr
                .filter(|a| !a.is_null(row_idx))
                .map(|a| a.value(row_idx))
                .and_then(|bytes| {
                    if codec.is_none() || codec == Some("flexbuf_v1") {
                        decode_payload_flexbuf(bytes)
                    } else {
                        None
                    }
                })
                .and_then(|v| match v {
                    Value::Object(obj) => Some(obj),
                    _ => None,
                });

            let mut obj = if let Some(payload) = payload_obj {
                payload
            } else {
                let mut projected = serde_json::Map::<String, Value>::new();
                let mut cols = stored
                    .promoted_columns
                    .iter()
                    .filter(|c| sanitize_name(&c.resource_type) == sanitize_name(label))
                    .collect::<Vec<&PromotedColumn>>();
                cols.sort_by_key(|c| c.field_name.matches('.').count());

                for col in cols {
                    let Some(arr) = promoted_arrays.get(&col.column_name) else {
                        continue;
                    };
                    let Some(value) = promoted_value_from_array(arr.as_ref(), row_idx, &col.kind)
                    else {
                        continue;
                    };
                    set_nested_value(&mut projected, &col.field_name, value);
                }
                projected
            };

            if !obj.contains_key("id") && !id.is_empty() {
                obj.insert(
                    "id".to_string(),
                    Value::String(strip_resource_type_prefix(id)),
                );
            }
            if !obj.contains_key("resourceType") {
                obj.insert("resourceType".to_string(), Value::String(label.to_string()));
            }
            if wrote_any {
                out.push(b',');
            }
            serde_json::to_writer(&mut out, &Value::Object(obj))?;
            wrote_any = true;
        }
    }
    out.push(b']');
    Ok(out)
}

pub(crate) fn promoted_value_from_array(
    arr: &dyn Array,
    row_idx: usize,
    kind: &PromotedType,
) -> Option<Value> {
    if arr.is_null(row_idx) {
        return None;
    }
    match kind {
        PromotedType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| Value::String(a.value(row_idx).to_string())),
        PromotedType::Json => arr.as_any().downcast_ref::<StringArray>().map(|a| {
            serde_json::from_str::<Value>(a.value(row_idx))
                .unwrap_or_else(|_| Value::String(a.value(row_idx).to_string()))
        }),
        PromotedType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| Value::from(a.value(row_idx))),
        PromotedType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| Value::from(a.value(row_idx))),
        PromotedType::Bool => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::from(a.value(row_idx))),
    }
}

pub(crate) fn strip_resource_type_prefix(id: &str) -> String {
    let parts = id.split('/').collect::<Vec<&str>>();
    if parts.len() == 2 {
        parts[1].to_string()
    } else {
        id.to_string()
    }
}

pub(crate) fn set_nested_value(
    root: &mut serde_json::Map<String, Value>,
    dotted_path: &str,
    value: Value,
) {
    let parts = dotted_path
        .split('.')
        .filter(|p| !p.is_empty())
        .collect::<Vec<&str>>();
    if parts.is_empty() {
        return;
    }
    set_nested_value_parts(root, &parts, value);
}

pub(crate) fn set_nested_value_parts(
    root: &mut serde_json::Map<String, Value>,
    parts: &[&str],
    value: Value,
) {
    if parts.len() == 1 {
        root.insert(parts[0].to_string(), value);
        return;
    }
    let key = parts[0].to_string();
    let child = root
        .entry(key)
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if !child.is_object() {
        *child = Value::Object(serde_json::Map::new());
    }
    if let Some(child_obj) = child.as_object_mut() {
        set_nested_value_parts(child_obj, &parts[1..], value);
    }
}

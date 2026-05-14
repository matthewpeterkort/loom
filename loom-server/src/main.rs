mod api;
mod flight;
mod persistence;
mod query_support;
mod routes;
mod schema_ingest;

pub(crate) use api::*;
pub(crate) use flight::*;
pub(crate) use persistence::*;
pub(crate) use query_support::*;
pub(crate) use schema_ingest::*;

use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{
    body::{Body, Bytes},
    extract::{Json, Path, State},
    http::StatusCode,
    response::Response as HttpResponse,
    routing::{get, post},
    Router,
};
use loom_engine::{graph, mapping, source, Engine, EngineConfig, IngestMode, VertexColumn};
use std::env;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Server;

fn bad_req<E: std::fmt::Display>(e: E) -> (StatusCode, Json<ApiResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ApiResponse {
            status: "error",
            detail: e.to_string(),
        }),
    )
}

fn internal_err<E: std::fmt::Display>(e: E) -> (StatusCode, Json<ApiResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ApiResponse {
            status: "error",
            detail: e.to_string(),
        }),
    )
}

fn parse_ingest_mode(mode: Option<&str>) -> Result<IngestMode, anyhow::Error> {
    match mode.unwrap_or("overwrite").to_ascii_lowercase().as_str() {
        "create" => Ok(IngestMode::Create),
        "append" => Ok(IngestMode::Append),
        "overwrite" => Ok(IngestMode::Overwrite),
        other => Err(anyhow::anyhow!(
            "invalid mode `{}`; expected create|append|overwrite",
            other
        )),
    }
}

pub(crate) fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn sanitize_name(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

fn env_var_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_port_or(key: &str, default: u16) -> Result<u16, anyhow::Error> {
    match env::var(key) {
        Ok(value) => value
            .parse::<u16>()
            .map_err(|e| anyhow::anyhow!("invalid {key} value `{value}`: {e}")),
        Err(_) => Ok(default),
    }
}

fn env_socket_addr_or(
    host_key: &str,
    default_host: &str,
    port_key: &str,
    default_port: u16,
) -> Result<SocketAddr, anyhow::Error> {
    let host = env_var_or(host_key, default_host);
    let port = env_port_or(port_key, default_port)?;
    format!("{host}:{port}")
        .parse::<SocketAddr>()
        .map_err(|e| anyhow::anyhow!("invalid socket address from {host_key}/{port_key}: {e}"))
}

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let work_dir = env_var_or("LOOM_WORK_DIR", "/tmp");
    let engine = Arc::new(Engine::new(EngineConfig {
        work_dir,
    })?);
    let source_snapshot = load_source_catalog_state()?;
    let source_reload_errors = engine.load_source_catalog_snapshot(source_snapshot).await?;
    for err in source_reload_errors {
        eprintln!("source catalog reload warning: {err}");
    }
    let mapping_snapshot = load_graph_mapping_catalog_state()?;
    let mapping_reload_errors = engine
        .load_graph_mapping_catalog_snapshot(mapping_snapshot, FsPath::new("."))
        .await?;
    for err in mapping_reload_errors {
        eprintln!("graph mapping catalog reload warning: {err}");
    }
    let graph_schema_snapshot = load_graph_schema_catalog_state()?;
    engine.load_graph_schema_catalog_snapshot(graph_schema_snapshot)?;
    let transform_snapshot = load_transform_catalog_state()?;
    let transform_reload_errors = engine
        .load_transform_catalog_snapshot(transform_snapshot)
        .await?;
    for err in transform_reload_errors {
        eprintln!("transform catalog reload warning: {err}");
    }
    let graph_snapshot = load_graph_catalog_state()?;
    let graph_reload_errors = engine.load_graph_catalog_snapshot(graph_snapshot).await?;
    for err in graph_reload_errors {
        eprintln!("graph catalog reload warning: {err}");
    }
    let persisted = load_schema_state()?;
    let state = AppState {
        engine: engine.clone(),
        schemas: Arc::new(RwLock::new(persisted.schemas)),
        graph_schema_bindings: Arc::new(RwLock::new(persisted.graph_schema_bindings)),
    };
    let flight = LoomFlightService { engine };

    let http_app = Router::new()
        .route("/v1/schema", get(routes::schema::list_schemas))
        .route(
            "/v1/schema/{name}",
            get(routes::schema::get_schema).post(routes::schema::upsert_schema),
        )
        .route(
            "/v1/schema/{name}/graph",
            get(routes::schema::get_schema_graph),
        )
        .route(
            "/v1/source",
            get(routes::catalog::list_sources).post(routes::catalog::register_source),
        )
        .route("/v1/source/{source}", get(routes::catalog::get_source))
        .route(
            "/v1/source/{source}/table",
            get(routes::catalog::list_source_tables),
        )
        .route(
            "/v1/source/{source}/table/{table}",
            get(routes::catalog::get_source_table),
        )
        .route(
            "/v1/source/{source}/refresh",
            post(routes::catalog::refresh_source),
        )
        .route(
            "/v1/source/{source}/profile",
            post(routes::catalog::profile_source),
        )
        .route(
            "/v1/source/{source}/sample",
            post(routes::catalog::sample_source),
        )
        .route(
            "/v1/source/{source}/table/{table}/profile",
            post(routes::catalog::profile_source_table),
        )
        .route(
            "/v1/source/{source}/table/{table}/sample",
            post(routes::catalog::sample_source_table),
        )
        .route(
            "/v1/source/{source}/table/{table}/preview-transform",
            post(routes::catalog::preview_transform_for_source_table),
        )
        .route(
            "/v1/transform",
            get(routes::catalog::list_transforms).post(routes::catalog::register_transform),
        )
        .route(
            "/v1/transform/{transform}",
            get(routes::catalog::get_transform),
        )
        .route(
            "/v1/transform/{transform}/validate",
            post(routes::catalog::validate_transform),
        )
        .route(
            "/v1/transform/{transform}/preview",
            post(routes::catalog::preview_transform),
        )
        .route(
            "/v1/profile/suggest-graph",
            post(routes::catalog::suggest_graph_profile),
        )
        .route(
            "/v1/graph-mapping",
            get(routes::catalog::list_graph_mappings).post(routes::catalog::register_graph_mapping),
        )
        .route(
            "/v1/graph-mapping/{graph}",
            get(routes::catalog::get_graph_mapping),
        )
        .route(
            "/v1/graph-mapping/{graph}/validate",
            post(routes::catalog::validate_graph_mapping),
        )
        .route(
            "/v1/graph-mapping/{graph}/compile",
            post(routes::catalog::compile_graph_mapping),
        )
        .route(
            "/v1/graph-mapping/{graph}/refresh",
            post(routes::catalog::refresh_graph_mapping),
        )
        .route(
            "/v1/graph-schema",
            get(routes::catalog::list_graph_schemas).post(routes::catalog::register_graph_schema),
        )
        .route(
            "/v1/graph-schema/{id}",
            get(routes::catalog::get_graph_schema),
        )
        .route(
            "/v1/graph-schema/{id}/validate",
            post(routes::catalog::validate_graph_schema),
        )
        .route(
            "/v1/graph-schema/{id}/preview",
            post(routes::catalog::preview_graph_schema),
        )
        .route(
            "/v1/graph-schema/{id}/register",
            post(routes::catalog::register_graph_schema_runtime),
        )
        .route(
            "/v1/graph-schema/{id}/publish",
            post(routes::catalog::publish_graph_schema_runtime),
        )
        .route("/v1/graph", get(routes::catalog::list_graphs))
        .route("/v1/graph/{graph}", get(routes::catalog::get_graph))
        .route(
            "/v1/graph/{graph}/schema/{name}",
            post(routes::catalog::bind_graph_schema),
        )
        .route(
            "/v1/graph/{graph}/schema",
            get(routes::catalog::get_graph_bound_schema),
        )
        .route(
            "/v1/graph/{graph}/schema/entities",
            get(routes::catalog::list_graph_schema_entities),
        )
        .route(
            "/v1/graph/{graph}/schema/entities/{entity}",
            get(routes::catalog::get_graph_schema_entity),
        )
        .route(
            "/v1/graph/{graph}/schema/links",
            get(routes::catalog::list_graph_schema_links),
        )
        .route("/v1/graphs/{graph}", get(routes::catalog::get_virtual_graph))
        .route(
            "/v1/graphs/{graph}/views",
            get(routes::catalog::list_virtual_graph_views),
        )
        .route(
            "/v1/graphs/{graph}/identity",
            get(routes::catalog::get_virtual_graph_identity),
        )
        .route(
            "/v1/graphs/{graph}/query/sql",
            post(routes::query::query_virtual_graph_sql),
        )
        .route(
            "/v1/graphs/{graph}/query/explain",
            post(routes::query::explain_virtual_graph_sql),
        )
        .route(
            "/v1/graphs/{graph}/query/reference-preview",
            post(routes::query::preview_virtual_reference),
        )
        .route(
            "/v1/graph/{graph}/columns",
            get(routes::catalog::get_graph_columns),
        )
        .route(
            "/v1/graph/{graph}/stats",
            get(routes::catalog::get_graph_stats),
        )
        .route("/v1/graph/{graph}/query", post(routes::query::query_graph))
        .route(
            "/v1/graph/{graph}/query/schema-vocabulary",
            get(routes::query::get_query_schema_vocabulary),
        )
        .route(
            "/v1/graph/{graph}/query/explain-schema",
            post(routes::query::explain_query_schema),
        )
        .route(
            "/v1/graph/{graph}/query/autocomplete",
            get(routes::query::autocomplete_query_schema),
        )
        .route(
            "/v1/graph/{graph}/sql",
            post(routes::query::query_graph_sql),
        )
        .route(
            "/v1/graph/{graph}/query/compact",
            post(routes::query::query_graph_compact),
        )
        .route(
            "/v1/graph/{graph}/query/full",
            post(routes::query::query_graph_full),
        )
        .route(
            "/v1/graph/{graph}/query/full/fast",
            post(routes::query::query_graph_full_fast),
        )
        .route(
            "/v1/graph/{graph}/query/ndjson",
            post(routes::query::query_graph_ndjson),
        )
        .route(
            "/v1/graph/{graph}/bulk-load",
            post(routes::load::bulk_load_graph),
        )
        .route(
            "/v1/graph/{graph}/bulk-load/ndjson-schema",
            post(routes::load::bulk_load_graph_schema_typed),
        )
        // Compatibility endpoint during migration.
        .route("/v1/query", post(routes::query::query_compat))
        .route(
            "/v1/query/compact",
            post(routes::query::query_compat_compact),
        )
        .route("/v1/query/full", post(routes::query::query_compat_full))
        .route(
            "/v1/query/full/fast",
            post(routes::query::query_compat_full_fast),
        )
        .route("/v1/query/ndjson", post(routes::query::query_compat_ndjson))
        .with_state(state);

    let http_addr = env_socket_addr_or("LOOM_HTTP_HOST", "127.0.0.1", "LOOM_HTTP_PORT", 8080)?;
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;

    let flight_addr =
        env_socket_addr_or("LOOM_FLIGHT_HOST", "127.0.0.1", "LOOM_FLIGHT_PORT", 50051)?;

    let http_task = tokio::spawn(async move { axum::serve(http_listener, http_app).await });
    let flight_task = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(flight))
            .serve(flight_addr)
            .await
    });

    let _ = tokio::try_join!(http_task, flight_task)?;
    Ok(())
}

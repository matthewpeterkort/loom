use crate::*;
use loom_engine::transform;

fn data_dir() -> PathBuf {
    std::env::var("LOOM_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn catalog_path(env_key: &str, file_name: &str) -> PathBuf {
    std::env::var(env_key)
        .map(PathBuf::from)
        .unwrap_or_else(|_| data_dir().join(file_name))
}

pub(crate) fn schema_state_file() -> PathBuf {
    catalog_path("LOOM_SCHEMA_STATE_FILE", "loom_schema_state.json")
}

pub(crate) fn source_catalog_file() -> PathBuf {
    catalog_path("LOOM_SOURCE_CATALOG_FILE", "loom_source_catalog.json")
}

pub(crate) fn graph_mapping_catalog_file() -> PathBuf {
    catalog_path("LOOM_GRAPH_MAPPING_CATALOG_FILE", "loom_graph_mapping_catalog.json")
}

pub(crate) fn graph_schema_catalog_file() -> PathBuf {
    catalog_path("LOOM_GRAPH_SCHEMA_CATALOG_FILE", "loom_graph_schema_catalog.json")
}

pub(crate) fn transform_catalog_file() -> PathBuf {
    catalog_path("LOOM_TRANSFORM_CATALOG_FILE", "loom_transform_catalog.json")
}

pub(crate) fn graph_catalog_file() -> PathBuf {
    catalog_path("LOOM_GRAPH_CATALOG_FILE", "loom_graph_catalog.json")
}

pub(crate) fn save_schema_state(state: &AppState) -> Result<(), anyhow::Error> {
    let schemas = {
        let guard = state
            .schemas
            .read()
            .map_err(|_| anyhow::anyhow!("schema registry lock poisoned"))?;
        guard.clone()
    };
    let graph_schema_bindings = {
        let guard = state
            .graph_schema_bindings
            .read()
            .map_err(|_| anyhow::anyhow!("schema binding lock poisoned"))?;
        guard.clone()
    };
    let snapshot = PersistedSchemaState {
        schemas,
        graph_schema_bindings,
    };
    let path = schema_state_file();
    save_schema_state_to_path(&path, &snapshot)
}

pub(crate) fn load_schema_state() -> Result<PersistedSchemaState, anyhow::Error> {
    let path = schema_state_file();
    load_schema_state_from_path(&path)
}

pub(crate) fn save_source_catalog_state(state: &AppState) -> Result<(), anyhow::Error> {
    let snapshot = state.engine.source_catalog.snapshot()?;
    let path = source_catalog_file();
    save_source_catalog_state_to_path(&path, &snapshot)
}

pub(crate) fn load_source_catalog_state() -> Result<source::SourceCatalogSnapshot, anyhow::Error> {
    let path = source_catalog_file();
    load_source_catalog_state_from_path(&path)
}

pub(crate) fn save_graph_mapping_catalog_state(state: &AppState) -> Result<(), anyhow::Error> {
    let snapshot = state.engine.graph_mapping_catalog.snapshot()?;
    let path = graph_mapping_catalog_file();
    save_graph_mapping_catalog_state_to_path(&path, &snapshot)
}

pub(crate) fn save_graph_schema_catalog_state(state: &AppState) -> Result<(), anyhow::Error> {
    let snapshot = state.engine.graph_schema_catalog.snapshot()?;
    let path = graph_schema_catalog_file();
    save_graph_schema_catalog_state_to_path(&path, &snapshot)
}

pub(crate) fn save_transform_catalog_state(state: &AppState) -> Result<(), anyhow::Error> {
    let snapshot = state.engine.transform_catalog.snapshot()?;
    let path = transform_catalog_file();
    save_transform_catalog_state_to_path(&path, &snapshot)
}

pub(crate) fn load_transform_catalog_state(
) -> Result<transform::TransformCatalogSnapshot, anyhow::Error> {
    let path = transform_catalog_file();
    load_transform_catalog_state_from_path(&path)
}

pub(crate) fn load_graph_mapping_catalog_state(
) -> Result<mapping::GraphMappingCatalogSnapshot, anyhow::Error> {
    let path = graph_mapping_catalog_file();
    load_graph_mapping_catalog_state_from_path(&path)
}

pub(crate) fn load_graph_schema_catalog_state(
) -> Result<loom_engine::graph_schema::GraphSchemaCatalogSnapshot, anyhow::Error> {
    let path = graph_schema_catalog_file();
    load_graph_schema_catalog_state_from_path(&path)
}

pub(crate) fn save_graph_catalog_state(state: &AppState) -> Result<(), anyhow::Error> {
    let snapshot = state.engine.graph_catalog.snapshot()?;
    let path = graph_catalog_file();
    save_graph_catalog_state_to_path(&path, &snapshot)
}

pub(crate) fn load_graph_catalog_state() -> Result<graph::GraphCatalogSnapshot, anyhow::Error> {
    let path = graph_catalog_file();
    load_graph_catalog_state_from_path(&path)
}

pub(crate) fn save_schema_state_to_path(
    path: &FsPath,
    snapshot: &PersistedSchemaState,
) -> Result<(), anyhow::Error> {
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

pub(crate) fn load_schema_state_from_path(
    path: &FsPath,
) -> Result<PersistedSchemaState, anyhow::Error> {
    if !path.exists() {
        return Ok(PersistedSchemaState::default());
    }
    let bytes = std::fs::read(path)?;
    let state: PersistedSchemaState = serde_json::from_slice(&bytes)?;
    Ok(state)
}

pub(crate) fn save_source_catalog_state_to_path(
    path: &FsPath,
    snapshot: &source::SourceCatalogSnapshot,
) -> Result<(), anyhow::Error> {
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

pub(crate) fn load_source_catalog_state_from_path(
    path: &FsPath,
) -> Result<source::SourceCatalogSnapshot, anyhow::Error> {
    if !path.exists() {
        return Ok(source::SourceCatalogSnapshot::default());
    }
    let bytes = std::fs::read(path)?;
    let state: source::SourceCatalogSnapshot = serde_json::from_slice(&bytes)?;
    Ok(state)
}

pub(crate) fn save_graph_mapping_catalog_state_to_path(
    path: &FsPath,
    snapshot: &mapping::GraphMappingCatalogSnapshot,
) -> Result<(), anyhow::Error> {
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

pub(crate) fn save_graph_schema_catalog_state_to_path(
    path: &FsPath,
    snapshot: &loom_engine::graph_schema::GraphSchemaCatalogSnapshot,
) -> Result<(), anyhow::Error> {
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

pub(crate) fn load_graph_mapping_catalog_state_from_path(
    path: &FsPath,
) -> Result<mapping::GraphMappingCatalogSnapshot, anyhow::Error> {
    if !path.exists() {
        return Ok(mapping::GraphMappingCatalogSnapshot::default());
    }
    let bytes = std::fs::read(path)?;
    let state: mapping::GraphMappingCatalogSnapshot = serde_json::from_slice(&bytes)?;
    Ok(state)
}

pub(crate) fn load_graph_schema_catalog_state_from_path(
    path: &FsPath,
) -> Result<loom_engine::graph_schema::GraphSchemaCatalogSnapshot, anyhow::Error> {
    if !path.exists() {
        return Ok(loom_engine::graph_schema::GraphSchemaCatalogSnapshot::default());
    }
    let bytes = std::fs::read(path)?;
    let state: loom_engine::graph_schema::GraphSchemaCatalogSnapshot =
        serde_json::from_slice(&bytes)?;
    Ok(state)
}

pub(crate) fn save_transform_catalog_state_to_path(
    path: &FsPath,
    snapshot: &transform::TransformCatalogSnapshot,
) -> Result<(), anyhow::Error> {
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

pub(crate) fn load_transform_catalog_state_from_path(
    path: &FsPath,
) -> Result<transform::TransformCatalogSnapshot, anyhow::Error> {
    if !path.exists() {
        return Ok(transform::TransformCatalogSnapshot::default());
    }
    let bytes = std::fs::read(path)?;
    let state: transform::TransformCatalogSnapshot = serde_json::from_slice(&bytes)?;
    Ok(state)
}

pub(crate) fn save_graph_catalog_state_to_path(
    path: &FsPath,
    snapshot: &graph::GraphCatalogSnapshot,
) -> Result<(), anyhow::Error> {
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

pub(crate) fn load_graph_catalog_state_from_path(
    path: &FsPath,
) -> Result<graph::GraphCatalogSnapshot, anyhow::Error> {
    if !path.exists() {
        return Ok(graph::GraphCatalogSnapshot::default());
    }
    let bytes = std::fs::read(path)?;
    let state: graph::GraphCatalogSnapshot = serde_json::from_slice(&bytes)?;
    Ok(state)
}

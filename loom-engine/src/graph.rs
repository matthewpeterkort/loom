//! Graph catalog and published graph metadata.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

#[derive(Debug, Default)]
pub struct GraphCatalog {
    graphs: RwLock<HashMap<String, GraphDescriptor>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphCatalogSnapshot {
    pub graphs: Vec<GraphDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphDescriptor {
    pub name: String,
    pub active_version: u64,
    pub status: GraphStatus,
    pub source_kind: GraphSourceKind,
    pub vertex_table: String,
    pub edge_table: String,
    pub active_snapshot: GraphSnapshot,
    pub created_unix_seconds: u64,
    pub updated_unix_seconds: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphSnapshot {
    pub version: u64,
    pub vertices_uri: String,
    pub edges_uri: String,
    pub adjacency_uri: Option<String>,
    pub vertex_table: String,
    pub edge_table: String,
    pub mapping_graph: Option<String>,
    pub mapping_version: Option<u32>,
    pub source_dependencies: Vec<String>,
    pub source_fingerprints: BTreeMap<String, String>,
    pub vertex_columns: Vec<GraphColumn>,
    pub edge_columns: Vec<GraphColumn>,
    pub stats: GraphStats,
    pub provenance: GraphProvenance,
    pub created_unix_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphColumn {
    pub name: String,
    pub data_type: String,
    pub role: GraphColumnRole,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphColumnRole {
    Id,
    Label,
    Endpoint,
    Provenance,
    Property,
    Dynamic,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphStatus {
    Active,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphSourceKind {
    MappingManifest,
    BulkParquet,
    BulkNdjson,
    Memory,
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GraphStats {
    pub vertices: usize,
    pub edges: usize,
    pub vertex_labels: BTreeMap<String, usize>,
    pub edge_labels: BTreeMap<String, usize>,
    pub vertex_file_bytes: Option<u64>,
    pub edge_file_bytes: Option<u64>,
    pub compile_duration_ms: Option<u128>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphProvenance {
    pub source_kind: Option<GraphSourceKind>,
    pub mapping_graph: Option<String>,
    pub source_ids: Vec<String>,
    pub storage_paths: Vec<String>,
}

impl GraphCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, descriptor: GraphDescriptor) -> Result<()> {
        let mut guard = self
            .graphs
            .write()
            .map_err(|_| anyhow!("graph catalog lock poisoned"))?;
        guard.insert(descriptor.name.clone(), descriptor);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Option<GraphDescriptor>> {
        let guard = self
            .graphs
            .read()
            .map_err(|_| anyhow!("graph catalog lock poisoned"))?;
        Ok(guard.get(name).cloned())
    }

    pub fn require(&self, name: &str) -> Result<GraphDescriptor> {
        self.get(name)?
            .ok_or_else(|| anyhow!("graph `{name}` not found"))
    }

    pub fn list(&self) -> Result<Vec<GraphDescriptor>> {
        let guard = self
            .graphs
            .read()
            .map_err(|_| anyhow!("graph catalog lock poisoned"))?;
        let mut graphs = guard.values().cloned().collect::<Vec<_>>();
        graphs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(graphs)
    }

    pub fn names(&self) -> Result<Vec<String>> {
        Ok(self.list()?.into_iter().map(|graph| graph.name).collect())
    }

    pub fn snapshot(&self) -> Result<GraphCatalogSnapshot> {
        Ok(GraphCatalogSnapshot {
            graphs: self.list()?,
        })
    }

    pub fn replace_all(&self, snapshot: GraphCatalogSnapshot) -> Result<()> {
        let mut guard = self
            .graphs
            .write()
            .map_err(|_| anyhow!("graph catalog lock poisoned"))?;
        guard.clear();
        for graph in snapshot.graphs {
            guard.insert(graph.name.clone(), graph);
        }
        Ok(())
    }
}

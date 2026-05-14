use crate::schema::CalyprSchemaRegistry;
use anyhow::{anyhow, Result};
use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use datafusion::common::JoinType;
use datafusion::datasource::MemTable;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::{col, lit, ParquetReadOptions, SessionContext};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use vortex::array::arrays::PrimitiveArray;
use vortex::array::stream::ArrayStreamExt;
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, IntoArray};
use vortex::buffer::Buffer;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::VortexSessionDefault;

pub mod converter;
pub mod graph_schema;
pub mod graph;
pub mod json_rows;
pub mod mapping;
pub mod operators;
pub mod planner;
pub mod query_model;
pub mod schema;
pub mod schema_binding;
pub mod shredder;
pub mod source;
pub mod suggest;
pub mod transform;

mod services;

use crate::operators::traversal::TraversalDirection;
use crate::planner::{
    LoomExtensionPlanner, LoomFilterPushdownRule, LoomMaterializeNode, LoomQueryPlanner,
    LoomTraversalNode,
};
pub use query_model::{
    graph_name, lower_to_logical_plan, parse_query_json, step_eq, step_field, step_n, step_op,
    step_string_list, steps, steps_mut, LoweringContext, Query,
};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub work_dir: String,
}
pub struct Engine {
    pub config: EngineConfig,
    pub session: SessionContext,
    pub catalog: Arc<Catalog>,
    pub source_catalog: Arc<source::SourceCatalog>,
    pub transform_catalog: Arc<transform::TransformCatalog>,
    pub graph_schema_catalog: Arc<graph_schema::GraphSchemaCatalog>,
    pub graph_mapping_catalog: Arc<mapping::GraphMappingCatalog>,
    pub graph_catalog: Arc<graph::GraphCatalog>,
}
pub struct Catalog {
    pub root: String,
    pub graphs: RwLock<HashMap<String, GraphMeta>>,
}
#[derive(Debug, Clone)]
pub struct GraphMeta {
    pub name: String,
    pub vertices_uri: String,
    pub edges_uri: String,
}

#[derive(Debug, Clone)]
struct GraphCatalogInput {
    name: String,
    source_kind: graph::GraphSourceKind,
    vertices_uri: String,
    edges_uri: String,
    adjacency_uri: Option<String>,
    vertex_batches: Vec<RecordBatch>,
    edge_batches: Vec<RecordBatch>,
    source_dependencies: Vec<String>,
    source_fingerprints: BTreeMap<String, String>,
    mapping_graph: Option<String>,
    mapping_version: Option<u32>,
    compile_duration_ms: Option<u128>,
}

fn empty_edge_batches() -> Result<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("from_id", DataType::Utf8, true),
        Field::new("to_id", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("source_id", DataType::Utf8, true),
        Field::new("source_row", DataType::Utf8, true),
    ]));
    Ok(vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())) as arrow::array::ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())) as arrow::array::ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())) as arrow::array::ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())) as arrow::array::ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())) as arrow::array::ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())) as arrow::array::ArrayRef,
        ],
    )?])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeGraphMode {
    Published,
    Virtual,
    PublishedAndVirtual,
}

impl LoweringContext for Engine {
    fn table_vertices(&self, graph: &str) -> Result<String> {
        self.resolve_runtime_graph_mode(graph)?;
        Ok(format!("vertices_{}", graph))
    }
    fn table_edges(&self, graph: &str) -> Result<String> {
        self.resolve_runtime_graph_mode(graph)?;
        Ok(format!("edges_{}", graph))
    }
    fn traverse_out(
        &self,
        graph: &str,
        input: LogicalPlan,
        labels: &[String],
    ) -> Result<LogicalPlan> {
        match self.resolve_runtime_graph_mode(graph)? {
            RuntimeGraphMode::Virtual => self.traverse_virtual(graph, input, labels, TraversalDirection::Out),
            RuntimeGraphMode::Published | RuntimeGraphMode::PublishedAndVirtual => {
                self.traverse_published(graph, input, TraversalDirection::Out)
            }
        }
    }
    fn traverse_in(
        &self,
        graph: &str,
        input: LogicalPlan,
        labels: &[String],
    ) -> Result<LogicalPlan> {
        match self.resolve_runtime_graph_mode(graph)? {
            RuntimeGraphMode::Virtual => self.traverse_virtual(graph, input, labels, TraversalDirection::In),
            RuntimeGraphMode::Published | RuntimeGraphMode::PublishedAndVirtual => {
                self.traverse_published(graph, input, TraversalDirection::In)
            }
        }
    }
    fn traverse_both(
        &self,
        graph: &str,
        input: LogicalPlan,
        labels: &[String],
    ) -> Result<LogicalPlan> {
        match self.resolve_runtime_graph_mode(graph)? {
            RuntimeGraphMode::Virtual => {
                self.traverse_virtual(graph, input, labels, TraversalDirection::Both)
            }
            RuntimeGraphMode::Published | RuntimeGraphMode::PublishedAndVirtual => {
                self.traverse_published(graph, input, TraversalDirection::Both)
            }
        }
    }
    fn materialize(&self, graph: &str, input: LogicalPlan) -> Result<LogicalPlan> {
        match self.resolve_runtime_graph_mode(graph)? {
            RuntimeGraphMode::Virtual => self.materialize_virtual_vertices(graph, input),
            RuntimeGraphMode::Published | RuntimeGraphMode::PublishedAndVirtual => {
                self.materialize_published(graph, input)
            }
        }
    }
}

impl Engine {
    fn resolve_runtime_graph_mode(&self, graph: &str) -> Result<RuntimeGraphMode> {
        let has_published = self.catalog.graphs.read().unwrap().contains_key(graph);
        let has_virtual = self
            .graph_mapping_catalog
            .get(graph)?
            .and_then(|descriptor| descriptor.virtual_binding)
            .is_some();
        match (has_published, has_virtual) {
            (true, true) => Ok(RuntimeGraphMode::PublishedAndVirtual),
            (true, false) => Ok(RuntimeGraphMode::Published),
            (false, true) => Ok(RuntimeGraphMode::Virtual),
            (false, false) => Err(anyhow!(
                "graph `{graph}` has neither published storage nor registered virtual views"
            )),
        }
    }

    fn traverse_published(
        &self,
        graph: &str,
        input: LogicalPlan,
        direction: TraversalDirection,
    ) -> Result<LogicalPlan> {
        let csr = futures::executor::block_on(self.get_csr(graph))?;
        Ok(LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(LoomTraversalNode::new(
                    input,
                    graph.to_string(),
                    csr.offsets.clone(),
                    csr.targets.clone(),
                    csr.offsets_in.clone(),
                    csr.targets_in.clone(),
                    direction,
                )),
            },
        ))
    }

    fn materialize_published(&self, graph: &str, input: LogicalPlan) -> Result<LogicalPlan> {
        let cols = futures::executor::block_on(self.get_vertex_columns(graph))?;
        let schema = schema_from_vertex_columns(&cols)?;
        let v_uri = self
            .catalog
            .graph_root(graph)
            .map(|r| join_uri(&r, "vertices/vertices.vortex"))?;
        let vortex_arr = futures::executor::block_on(read_array_from_vortex(&v_uri))?;
        let mut columns = Vec::new();
        for col in &cols {
            let names = vortex_arr.children_names();
            let col_idx = names
                .iter()
                .position(|n| n == &col.name)
                .ok_or_else(|| anyhow!("missing col {}", col.name))?;
            let child = vortex_arr
                .nth_child(col_idx)
                .ok_or_else(|| anyhow!("missing field"))?;
            columns.push((col.name.clone(), child));
        }
        Ok(LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(LoomMaterializeNode::new(
                    input,
                    graph.to_string(),
                    columns,
                    None,
                    schema,
                )),
            },
        ))
    }

    fn traverse_virtual(
        &self,
        graph: &str,
        input: LogicalPlan,
        labels: &[String],
        direction: TraversalDirection,
    ) -> Result<LogicalPlan> {
        let input_df = self.materialize_virtual_input(input)?;
        let edge_table = format!("edges_{graph}");
        let edges = futures::executor::block_on(self.session.table(&edge_table))?;
        match direction {
            TraversalDirection::Out => {
                let edges = filter_edge_labels(edges, labels)?
                    .select(vec![col("from_id"), col("to_id")])?;
                let joined = input_df.join(edges, JoinType::Inner, &["id"], &["from_id"], None)?;
                let projected = joined.select(vec![col("to_id").alias("id")])?;
                distinct_vertex_ids(projected)
            }
            TraversalDirection::In => {
                let edges = filter_edge_labels(edges, labels)?
                    .select(vec![col("from_id"), col("to_id")])?;
                let joined = input_df.join(edges, JoinType::Inner, &["id"], &["to_id"], None)?;
                let projected = joined.select(vec![col("from_id").alias("id")])?;
                distinct_vertex_ids(projected)
            }
            TraversalDirection::Both => {
                let outward = {
                    let edges = filter_edge_labels(
                        futures::executor::block_on(self.session.table(&edge_table))?,
                        labels,
                    )?
                    .select(vec![col("from_id"), col("to_id")])?;
                    let joined =
                        input_df
                            .clone()
                            .join(edges, JoinType::Inner, &["id"], &["from_id"], None)?;
                    joined.select(vec![col("to_id").alias("id")])?
                };
                let inward = {
                    let edges = filter_edge_labels(
                        futures::executor::block_on(self.session.table(&edge_table))?,
                        labels,
                    )?
                    .select(vec![col("from_id"), col("to_id")])?;
                    let joined = input_df.join(edges, JoinType::Inner, &["id"], &["to_id"], None)?;
                    joined.select(vec![col("from_id").alias("id")])?
                };
                distinct_vertex_ids(outward.union(inward)?)
            }
        }
        .map(|dataframe| dataframe.into_unoptimized_plan())
    }

    fn materialize_virtual_vertices(&self, graph: &str, input: LogicalPlan) -> Result<LogicalPlan> {
        let input_df = self.materialize_virtual_input(input)?;
        let vertex_table = format!("vertices_{graph}");
        let vertices = futures::executor::block_on(self.session.table(&vertex_table))?;
        let vertex_columns = vertices
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        let join_projections = vertex_columns
            .iter()
            .map(|name| col(name).alias(&format!("__vertex_col_{name}")))
            .chain(std::iter::once(col("id").alias("__vertex_join_id")))
            .collect::<Vec<_>>();
        let vertices = vertices.select(join_projections)?;
        let joined =
            input_df.join(vertices, JoinType::Inner, &["id"], &["__vertex_join_id"], None)?;
        let projections = vertex_columns
            .iter()
            .map(|name| col(&format!("__vertex_col_{name}")).alias(name))
            .collect::<Vec<_>>();
        Ok(joined.select(projections)?.into_unoptimized_plan())
    }

    fn materialize_virtual_input(&self, input: LogicalPlan) -> Result<DataFrame> {
        let dataframe = DataFrame::new(self.session.state(), input);
        let batches = futures::executor::block_on(dataframe.collect())?;
        self.session.read_batches(batches).map_err(Into::into)
    }
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        let session = SessionContext::new();
        let catalog = Arc::new(Catalog {
            root: config.work_dir.clone(),
            graphs: RwLock::new(HashMap::new()),
        });
        let source_catalog = Arc::new(source::SourceCatalog::new());
        let transform_catalog = Arc::new(transform::TransformCatalog::new());
        let graph_schema_catalog = Arc::new(graph_schema::GraphSchemaCatalog::new());
        let graph_mapping_catalog = Arc::new(mapping::GraphMappingCatalog::new());
        let graph_catalog = Arc::new(graph::GraphCatalog::new());
        Ok(Self {
            config,
            session,
            catalog,
            source_catalog,
            transform_catalog,
            graph_schema_catalog,
            graph_mapping_catalog,
            graph_catalog,
        })
    }
}

impl Catalog {
    pub fn graph_root(&self, name: &str) -> Result<String> {
        Ok(join_uri(&self.root, name))
    }
}

fn arrow_table_from_dir(
    dir: &str,
    purpose: &str,
) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
    let path = std::path::PathBuf::from(dir);
    let mut unified_fields = std::collections::HashMap::new();
    let mut readers = Vec::new();

    if path.is_dir() {
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("arrow") {
                let file = std::fs::File::open(&path)?;
                let reader = arrow::ipc::reader::FileReader::try_new(file, None)?;
                let schema = reader.schema();
                for field in schema.fields() {
                    if !unified_fields.contains_key(field.name()) {
                        unified_fields.insert(field.name().clone(), field.clone());
                    }
                }
                readers.push(reader);
            }
        }
    }

    if readers.is_empty() {
        return Err(anyhow!("No {purpose} arrow files found in {dir}"));
    }

    let mut sorted_fields: Vec<_> = unified_fields.into_values().collect();
    sorted_fields.sort_by(|a, b| a.name().cmp(b.name()));
    let unified_schema = Arc::new(arrow::datatypes::Schema::new(sorted_fields));
    let mut all_batches = Vec::new();
    for reader in readers {
        let file_schema = reader.schema();
        for batch_result in reader {
            let batch = batch_result?;
            let mut padded_columns = Vec::with_capacity(unified_schema.fields().len());
            for field in unified_schema.fields() {
                match file_schema.index_of(field.name()) {
                    Ok(idx) => padded_columns.push(batch.column(idx).clone()),
                    Err(_) => {
                        let null_arr =
                            arrow::array::new_null_array(field.data_type(), batch.num_rows());
                        padded_columns.push(null_arr);
                    }
                }
            }
            all_batches.push(RecordBatch::try_new(
                unified_schema.clone(),
                padded_columns,
            )?);
        }
    }

    Ok(Arc::new(datafusion::datasource::MemTable::try_new(
        unified_schema,
        vec![all_batches],
    )?))
}

fn read_arrow_batches_from_dir(dir: &str) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("arrow") {
            continue;
        }
        let file = std::fs::File::open(&path)?;
        let reader = arrow::ipc::reader::FileReader::try_new(file, None)?;
        for batch in reader {
            batches.push(batch?);
        }
    }
    Ok(batches)
}

async fn write_batches_as_graph_storage(
    dir: &str,
    stem: &str,
    batches: &[RecordBatch],
) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let arrow_path = join_uri(dir, &format!("{stem}.arrow"));
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .ok_or_else(|| anyhow!("cannot write empty graph storage without schema"))?;
    let file = std::fs::File::create(&arrow_path)?;
    let mut writer = FileWriter::try_new(file, &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    write_batches_as_vortex(dir, stem, batches).await
}

async fn write_batches_as_vortex(dir: &str, stem: &str, batches: &[RecordBatch]) -> Result<()> {
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .ok_or_else(|| anyhow!("cannot write empty vortex storage without schema"))?;
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let columns = names
        .iter()
        .map(|name| {
            let mut values = Vec::with_capacity(row_count);
            for batch in batches {
                if let Some(array) = batch.column_by_name(name) {
                    for row_idx in 0..batch.num_rows() {
                        values.push(arrow_value_to_string(array.as_ref(), row_idx));
                    }
                }
            }
            vortex::array::arrays::VarBinArray::from(values).into_array()
        })
        .collect::<Vec<_>>();
    let field_names = names
        .iter()
        .map(|name| Arc::<str>::from(name.as_str()))
        .collect::<Vec<_>>();
    let struct_arr = vortex::array::arrays::StructArray::try_new(
        field_names.into(),
        columns,
        row_count,
        Validity::NonNullable,
    )?
    .into_array();
    let out = tokio::fs::File::create(join_uri(dir, &format!("{stem}.vortex"))).await?;
    vortex::session::VortexSession::default()
        .write_options()
        .write(out, ArrayStreamExt::boxed(struct_arr.to_array_stream()))
        .await?;
    Ok(())
}

fn arrow_value_to_string(array: &dyn Array, row_idx: usize) -> String {
    if array.is_null(row_idx) {
        return String::new();
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return values.value(row_idx).to_string();
    }
    if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
    {
        return values.value(row_idx).to_string();
    }
    if let Some(values) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return values.value(row_idx).to_string();
    }
    if let Some(values) = array.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        return values.value(row_idx).to_string();
    }
    if let Some(values) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return values.value(row_idx).to_string();
    }
    if let Some(values) = array.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return values.value(row_idx).to_string();
    }
    if let Some(values) = array.as_any().downcast_ref::<arrow::array::BinaryArray>() {
        return values
            .value(row_idx)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
    }
    String::new()
}

fn build_csr_from_graph_batches(
    vertices: &[RecordBatch],
    edges: &[RecordBatch],
) -> crate::operators::traversal::CsrArrays {
    let mut vertex_ordinals = HashMap::<String, usize>::new();
    for batch in vertices {
        if let Some(ids) = batch.column_by_name("id") {
            for row_idx in 0..batch.num_rows() {
                let id = arrow_value_to_string(ids.as_ref(), row_idx);
                if !id.is_empty() && !vertex_ordinals.contains_key(&id) {
                    vertex_ordinals.insert(id, vertex_ordinals.len());
                }
            }
        }
    }
    let vertex_count = vertex_ordinals.len();
    let mut out_adj = vec![Vec::<u64>::new(); vertex_count];
    let mut in_adj = vec![Vec::<u64>::new(); vertex_count];
    for batch in edges {
        let Some(from_ids) = batch.column_by_name("from_id") else {
            continue;
        };
        let Some(to_ids) = batch.column_by_name("to_id") else {
            continue;
        };
        for row_idx in 0..batch.num_rows() {
            let from_id = arrow_value_to_string(from_ids.as_ref(), row_idx);
            let to_id = arrow_value_to_string(to_ids.as_ref(), row_idx);
            let (Some(from), Some(to)) =
                (vertex_ordinals.get(&from_id), vertex_ordinals.get(&to_id))
            else {
                continue;
            };
            out_adj[*from].push(*to as u64);
            in_adj[*to].push(*from as u64);
        }
    }
    let (offsets, targets) = adjacency_to_csr(out_adj);
    let (offsets_in, targets_in) = adjacency_to_csr(in_adj);
    crate::operators::traversal::CsrArrays {
        offsets: Buffer::from(offsets),
        targets: Buffer::from(targets),
        offsets_in: Some(Buffer::from(offsets_in)),
        targets_in: Some(Buffer::from(targets_in)),
    }
}

fn adjacency_to_csr(adjacency: Vec<Vec<u64>>) -> (Vec<u64>, Vec<u64>) {
    let mut offsets = Vec::with_capacity(adjacency.len() + 1);
    let mut targets = Vec::new();
    offsets.push(0);
    for mut row in adjacency {
        row.sort_unstable();
        row.dedup();
        targets.extend(row);
        offsets.push(targets.len() as u64);
    }
    (offsets, targets)
}

fn graph_columns_from_batches(batches: &[RecordBatch], vertex: bool) -> Vec<graph::GraphColumn> {
    let Some(batch) = batches.first() else {
        return Vec::new();
    };
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| graph::GraphColumn {
            name: field.name().clone(),
            data_type: format!("{:?}", field.data_type()),
            role: graph_column_role(field.name(), vertex),
        })
        .collect()
}

fn graph_column_role(name: &str, vertex: bool) -> graph::GraphColumnRole {
    match name {
        "id" => graph::GraphColumnRole::Id,
        "label" => graph::GraphColumnRole::Label,
        "source_id" | "source_row" => graph::GraphColumnRole::Provenance,
        "from_id" | "to_id" if !vertex => graph::GraphColumnRole::Endpoint,
        _ if name.starts_with("prop_") => graph::GraphColumnRole::Property,
        _ => graph::GraphColumnRole::Dynamic,
    }
}

fn label_counts_from_batches(batches: &[RecordBatch]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for batch in batches {
        let Some(labels) = batch.column_by_name("label") else {
            continue;
        };
        for row_idx in 0..batch.num_rows() {
            let label = arrow_value_to_string(labels.as_ref(), row_idx);
            if !label.is_empty() {
                *counts.entry(label).or_default() += 1;
            }
        }
    }
    counts
}

fn directory_file_size(path: &str) -> Result<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total += metadata.len();
        }
    }
    Ok(total)
}

fn unix_seconds_local() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

pub async fn read_array_from_vortex(path: &str) -> Result<ArrayRef> {
    let session = vortex::session::VortexSession::default();
    let file = session
        .open_options()
        .open_path(path)
        .await
        .map_err(|e| anyhow!(e))?;
    let stream = file
        .scan()
        .map_err(|e| anyhow!(e))?
        .into_array_stream()
        .map_err(|e| anyhow!(e))?;
    Ok(stream.read_all().await.map_err(|e| anyhow!(e))?)
}

async fn write_csr_to_vortex(
    csr: &crate::operators::traversal::CsrArrays,
    dir: &str,
) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let off_v: ArrayRef =
        PrimitiveArray::new(csr.offsets.clone(), Validity::NonNullable).into_array();
    let tar_v: ArrayRef =
        PrimitiveArray::new(csr.targets.clone(), Validity::NonNullable).into_array();
    let out_off = tokio::fs::File::create(join_uri(dir, "offsets.vortex")).await?;
    let out_tar = tokio::fs::File::create(join_uri(dir, "targets.vortex")).await?;
    vortex::session::VortexSession::default()
        .write_options()
        .write(out_off, ArrayStreamExt::boxed(off_v.to_array_stream()))
        .await?;
    vortex::session::VortexSession::default()
        .write_options()
        .write(out_tar, ArrayStreamExt::boxed(tar_v.to_array_stream()))
        .await?;
    if let (Some(offsets_in), Some(targets_in)) = (&csr.offsets_in, &csr.targets_in) {
        let off_in_v: ArrayRef =
            PrimitiveArray::new(offsets_in.clone(), Validity::NonNullable).into_array();
        let tar_in_v: ArrayRef =
            PrimitiveArray::new(targets_in.clone(), Validity::NonNullable).into_array();
        let out_off_in = tokio::fs::File::create(join_uri(dir, "offsets_in.vortex")).await?;
        let out_tar_in = tokio::fs::File::create(join_uri(dir, "targets_in.vortex")).await?;
        vortex::session::VortexSession::default()
            .write_options()
            .write(
                out_off_in,
                ArrayStreamExt::boxed(off_in_v.to_array_stream()),
            )
            .await?;
        vortex::session::VortexSession::default()
            .write_options()
            .write(
                out_tar_in,
                ArrayStreamExt::boxed(tar_in_v.to_array_stream()),
            )
            .await?;
    }
    Ok(())
}

async fn read_csr_from_vortex(dir: &str) -> Result<crate::operators::traversal::CsrArrays> {
    let off_v = read_array_from_vortex(&join_uri(dir, "offsets.vortex")).await?;
    let tar_v = read_array_from_vortex(&join_uri(dir, "targets.vortex")).await?;
    let off_can = off_v.to_canonical().map_err(|e| anyhow!(e))?;
    let tar_can = tar_v.to_canonical().map_err(|e| anyhow!(e))?;
    let off_p = off_can.as_primitive();
    let tar_p = tar_can.as_primitive();
    let offsets_in_path = join_uri(dir, "offsets_in.vortex");
    let targets_in_path = join_uri(dir, "targets_in.vortex");
    let (offsets_in, targets_in) = if std::path::Path::new(&offsets_in_path).exists()
        && std::path::Path::new(&targets_in_path).exists()
    {
        let off_in_v = read_array_from_vortex(&offsets_in_path).await?;
        let tar_in_v = read_array_from_vortex(&targets_in_path).await?;
        let off_in_can = off_in_v.to_canonical().map_err(|e| anyhow!(e))?;
        let tar_in_can = tar_in_v.to_canonical().map_err(|e| anyhow!(e))?;
        (
            Some(Buffer::from(
                off_in_can
                    .as_primitive()
                    .to_buffer::<u64>()
                    .as_ref()
                    .to_vec(),
            )),
            Some(Buffer::from(
                tar_in_can
                    .as_primitive()
                    .to_buffer::<u64>()
                    .as_ref()
                    .to_vec(),
            )),
        )
    } else {
        (None, None)
    };
    Ok(crate::operators::traversal::CsrArrays {
        offsets: Buffer::from(off_p.to_buffer::<u64>().as_ref().to_vec()),
        targets: Buffer::from(tar_p.to_buffer::<u64>().as_ref().to_vec()),
        offsets_in,
        targets_in,
    })
}

pub fn schema_from_vertex_columns(cols: &[VertexColumn]) -> Result<SchemaRef> {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| Field::new(&c.name, DataType::Utf8, true))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

pub fn schema_from_vortex_dtype(dtype: &vortex::dtype::DType) -> Result<Schema> {
    if let vortex::dtype::DType::Struct(st, _) = dtype {
        let mut fields = Vec::new();
        for (name, _) in st.names().iter().zip(st.fields()) {
            fields.push(Field::new(name.as_ref(), DataType::Utf8, true));
        }
        return Ok(Schema::new(fields));
    }
    Err(anyhow!("unsupported dtype"))
}

fn join_uri(r: &str, c: &str) -> String {
    format!("{}/{}", r.trim_end_matches('/'), c.trim_start_matches('/'))
}
fn ensure_directory_uri(u: &str) -> String {
    if u.ends_with('/') {
        u.to_string()
    } else {
        format!("{u}/")
    }
}
fn graph_root_from_vertices_uri(vertices_uri: &str) -> Option<String> {
    let trimmed = vertices_uri.trim_end_matches('/');
    trimmed
        .strip_suffix("/vertices")
        .map(|root| root.to_string())
}
fn filter_edge_labels(edges: DataFrame, labels: &[String]) -> Result<DataFrame> {
    if labels.is_empty() {
        return Ok(edges);
    }
    let mut predicate = col("label").eq(lit(labels[0].clone()));
    for label in labels.iter().skip(1) {
        predicate = predicate.or(col("label").eq(lit(label.clone())));
    }
    edges.filter(predicate).map_err(Into::into)
}
fn distinct_vertex_ids(dataframe: DataFrame) -> Result<DataFrame> {
    dataframe
        .distinct_on(
            vec![col("id")],
            vec![col("id")],
            Some(vec![col("id").sort(true, true)]),
        )
        .map_err(Into::into)
}
fn validate_row_provenance(
    row: &HashMap<String, String>,
    path: &str,
    report: &mut mapping::MappingValidationReport,
) {
    let source_id = row.get("source_id").cloned().unwrap_or_default();
    let source_row = row.get("source_row").cloned().unwrap_or_default();
    if source_id.trim().is_empty() || source_row.trim().is_empty() {
        report.metrics.provenance_missing += 1;
        report.errors.push(mapping::MappingValidationError {
            path: format!("{path}.provenance"),
            message: "mapped row is missing source_id or source_row provenance".to_string(),
        });
    }
}

fn typed_columns_for_path(
    manifest: &mapping::GraphMappingManifest,
    path: &str,
) -> Vec<(String, mapping::OutputType)> {
    if let Some(idx) = path
        .strip_prefix("vertices[")
        .and_then(|value| value.strip_suffix(']'))
        .and_then(|value| value.parse::<usize>().ok())
    {
        return manifest
            .vertices
            .get(idx)
            .map(|vertex| {
                let mut columns = typed_columns_from_map(&vertex.columns);
                columns.extend(typed_property_columns(&vertex.prop_types));
                columns
            })
            .unwrap_or_default();
    }
    if let Some(idx) = path
        .strip_prefix("edges[")
        .and_then(|value| value.strip_suffix(']'))
        .and_then(|value| value.parse::<usize>().ok())
    {
        return manifest
            .edges
            .get(idx)
            .map(|edge| {
                let mut columns = typed_columns_from_map(&edge.columns);
                columns.extend(typed_property_columns(&edge.prop_types));
                columns
            })
            .unwrap_or_default();
    }
    Vec::new()
}

fn typed_columns_from_map(
    columns: &BTreeMap<String, mapping::ColumnMapping>,
) -> Vec<(String, mapping::OutputType)> {
    columns
        .iter()
        .filter_map(|(name, column)| match column {
            mapping::ColumnMapping::Typed(_) => Some((name.clone(), column.output_type())),
            mapping::ColumnMapping::Expr(_) => None,
        })
        .collect()
}

fn typed_property_columns(
    prop_types: &BTreeMap<String, mapping::OutputType>,
) -> Vec<(String, mapping::OutputType)> {
    prop_types
        .iter()
        .map(|(source_column, output_type)| {
            (
                property_column_name_local(source_column),
                output_type.clone(),
            )
        })
        .collect()
}

fn property_column_name_local(source_column: &str) -> String {
    format!("prop_{}", sanitize_output_column_local(source_column))
}

fn sanitize_output_column_local(value: &str) -> String {
    let out = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if out.is_empty() {
        "column".to_string()
    } else {
        out
    }
}

fn value_coerces_to_type(value: &str, output_type: &mapping::OutputType) -> bool {
    match output_type {
        mapping::OutputType::String => true,
        mapping::OutputType::Integer => value.parse::<i64>().is_ok(),
        mapping::OutputType::Float => value.parse::<f64>().is_ok(),
        mapping::OutputType::Boolean => matches!(
            value.to_ascii_lowercase().as_str(),
            "true" | "false" | "1" | "0" | "yes" | "no"
        ),
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestMode {
    Create,
    Append,
    Overwrite,
}
pub struct GraphStorageUris {
    pub vertices_uri: String,
    pub edges_uri: String,
}
pub struct VertexColumn {
    pub name: String,
    pub sql_type: String,
}

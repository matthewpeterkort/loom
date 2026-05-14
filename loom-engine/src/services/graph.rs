use crate::*;
use arrow::array::StringArray;

impl Engine {
    pub async fn ingest_ndjson_graph_to_vortex(
        &self,
        graph: &str,
        ndjson_dir: &str,
        vortex_root: &str,
        _mode: IngestMode,
    ) -> Result<GraphStorageUris> {
        let schema_path = "/Users/peterkor/Desktop/BMEG/grip-benchmark/calypr/calypr-schema.json";
        let registry = CalyprSchemaRegistry::load(schema_path)?;

        let graph_root = join_uri(vortex_root, graph);
        let v_uri = join_uri(&graph_root, "vertices");
        let e_uri = join_uri(&graph_root, "edges");
        std::fs::create_dir_all(&v_uri)?;
        std::fs::create_dir_all(&e_uri)?;
        std::fs::create_dir_all(join_uri(&graph_root, "adjacency"))?;

        let registry = std::sync::Mutex::new(registry);
        let ndjson_dir = ndjson_dir.to_string();

        let mut writers: HashMap<String, arrow::ipc::writer::FileWriter<std::fs::File>> =
            HashMap::new();
        let mut dir = tokio::fs::read_dir(&ndjson_dir).await?;
        let mut plans = HashMap::new();
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("ndjson") {
                let file_bytes = tokio::fs::read(&path).await?;
                let file_type = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("Vertex")
                    .to_string();
                eprintln!("Latching to file type: {}", file_type);

                let lines: Vec<&[u8]> = file_bytes
                    .split(|&b| b == b'\n')
                    .filter(|l| !l.is_empty())
                    .collect();

                let plan = plans.entry(file_type.clone()).or_insert_with(|| {
                    let schema = registry
                        .lock()
                        .unwrap()
                        .get_schema(&file_type)
                        .expect("schema not found");
                    std::sync::Arc::new(crate::shredder::ShredderCompiler::compile(schema).unwrap())
                });

                let (tx, rx) = std::sync::mpsc::sync_channel(32);
                let plan_ref = plan.clone();
                let file_type_ref = file_type.clone();

                std::thread::scope(|s| {
                    // Producer pool
                    s.spawn(move || {
                        use rayon::prelude::*;
                        lines.par_chunks(8192).for_each_with(tx, |s_tx, chunk| {
                            let t_shred = std::time::Instant::now();
                            let rb = crate::shredder::TypedShredder::shred_batch(&plan_ref, chunk)
                                .expect("shred_batch error");
                            let shred_ms = t_shred.elapsed().as_millis();
                            s_tx.send((rb, shred_ms, chunk.len())).expect("send error");
                        });
                    });

                    // Consumer sink
                    for (rb, shred_ms, chunk_len) in rx {
                        let t_write = std::time::Instant::now();
                        if !writers.contains_key(&file_type_ref) {
                            let out_path = join_uri(&v_uri, &format!("{}.arrow", file_type_ref));
                            let file = std::fs::File::create(&out_path).unwrap();
                            let w = arrow::ipc::writer::FileWriter::try_new(file, &rb.schema())
                                .unwrap();
                            writers.insert(file_type_ref.clone(), w);
                        }
                        writers
                            .get_mut(&file_type_ref)
                            .unwrap()
                            .write(&rb)
                            .expect("IPC write failure");
                        let write_ms = t_write.elapsed().as_millis();
                        eprintln!(
                            "[TIMING] shred_batch={}ms  writer.write={}ms  ({} rows) ({})",
                            shred_ms, write_ms, chunk_len, file_type_ref
                        );
                    }
                });
            }
        }
        for (_, mut w) in writers {
            w.finish().map_err(|e| anyhow!("IPC finish: {}", e))?;
        }
        eprintln!("Ingestion finished successfully.");

        let csr = crate::operators::traversal::CsrArrays {
            offsets: vortex::buffer::Buffer::from(vec![0u64]),
            targets: vortex::buffer::Buffer::from(vec![]),
            offsets_in: None,
            targets_in: None,
        };
        write_csr_to_vortex(&csr, &join_uri(&graph_root, "adjacency")).await?;

        self.register_graph_vortex(graph, &v_uri, &e_uri).await?;
        let vertex_batches = read_arrow_batches_from_dir(&v_uri).unwrap_or_else(|_| Vec::new());
        let edge_batches = read_arrow_batches_from_dir(&e_uri)
            .unwrap_or_else(|_| empty_edge_batches().unwrap_or_default());
        self.upsert_graph_catalog_from_batches(GraphCatalogInput {
            name: graph.to_string(),
            source_kind: graph::GraphSourceKind::BulkNdjson,
            vertices_uri: v_uri.clone(),
            edges_uri: e_uri.clone(),
            adjacency_uri: Some(join_uri(&graph_root, "adjacency")),
            vertex_batches,
            edge_batches,
            source_dependencies: Vec::new(),
            source_fingerprints: BTreeMap::new(),
            mapping_graph: None,
            mapping_version: None,
            compile_duration_ms: None,
        })?;
        Ok(GraphStorageUris {
            vertices_uri: v_uri,
            edges_uri: e_uri,
        })
    }

    pub async fn ingest_parquet_graph_to_vortex(
        &self,
        graph: &str,
        v_p: &str,
        e_p: &str,
        vortex_root: &str,
        _m: IngestMode,
    ) -> Result<GraphStorageUris> {
        let df = self
            .session
            .read_parquet(v_p, datafusion::prelude::ParquetReadOptions::default())
            .await?;
        let batches = df.collect().await?;
        let graph_root = join_uri(vortex_root, graph);
        let v_uri = join_uri(&graph_root, "vertices");
        let e_uri = join_uri(&graph_root, "edges");
        std::fs::create_dir_all(&v_uri)?;
        std::fs::create_dir_all(&e_uri)?;
        std::fs::create_dir_all(join_uri(&graph_root, "adjacency"))?;

        // Simple vortex conversion for parquet columns
        let mut ids = Vec::new();
        let mut labels = Vec::new();
        let mut props = Vec::new();
        let mut v_ids = Vec::new();
        let mut cur = 0u64;
        let mut arrow_writer: Option<FileWriter<std::fs::File>> = None;
        let mut vertex_batches_for_catalog = Vec::new();
        for batch in batches {
            vertex_batches_for_catalog.push(batch.clone());
            if arrow_writer.is_none() {
                let arrow_path = join_uri(&v_uri, "vertices.arrow");
                let file = std::fs::File::create(&arrow_path)?;
                arrow_writer = Some(FileWriter::try_new(file, &batch.schema())?);
            }
            let id_col = batch
                .column_by_name("id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let label_col = batch
                .column_by_name("label")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let props_col = batch
                .column_by_name("props")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            for i in 0..batch.num_rows() {
                ids.push(
                    id_col
                        .map(|col| col.value(i).to_string())
                        .unwrap_or_else(|| format!("{}_{}", graph, cur)),
                );
                labels.push(
                    label_col
                        .map(|col| col.value(i).to_string())
                        .unwrap_or_else(|| "Vertex".to_string()),
                );
                props.push(
                    props_col
                        .map(|col| col.value(i).to_string())
                        .unwrap_or_else(|| "{}".to_string()),
                );
                v_ids.push(cur);
                cur += 1;
            }
            if let Some(writer) = arrow_writer.as_mut() {
                writer.write(&batch)?;
            }
        }
        if let Some(mut writer) = arrow_writer {
            writer.finish()?;
        }

        let mut edge_batches_for_catalog = Vec::new();
        if !e_p.is_empty() {
            let edges_df = self
                .session
                .read_parquet(e_p, datafusion::prelude::ParquetReadOptions::default())
                .await?;
            let edge_batches = edges_df.collect().await?;
            edge_batches_for_catalog = edge_batches.clone();
            if let Some(first) = edge_batches.first() {
                let edge_path = join_uri(&e_uri, "edges.arrow");
                let file = std::fs::File::create(&edge_path)?;
                let mut writer = FileWriter::try_new(file, &first.schema())?;
                for batch in edge_batches {
                    writer.write(&batch)?;
                }
                writer.finish()?;
            }
        }

        let struct_arr = vortex::array::arrays::StructArray::try_new(
            ["id", "label", "props", "_v_id"].into(),
            vec![
                vortex::array::arrays::VarBinArray::from(ids).into_array(),
                vortex::array::arrays::VarBinArray::from(labels).into_array(),
                vortex::array::arrays::VarBinArray::from(props).into_array(),
                PrimitiveArray::new(Buffer::from(v_ids), Validity::NonNullable).into_array(),
            ],
            cur as usize,
            Validity::NonNullable,
        )?
        .into_array();
        let out = tokio::fs::File::create(join_uri(&v_uri, "vertices.vortex")).await?;
        vortex::session::VortexSession::default()
            .write_options()
            .write(out, ArrayStreamExt::boxed(struct_arr.to_array_stream()))
            .await?;

        write_csr_to_vortex(
            &crate::operators::traversal::CsrArrays {
                offsets: Buffer::from(vec![0u64]),
                targets: Buffer::from(vec![]),
                offsets_in: None,
                targets_in: None,
            },
            &join_uri(&graph_root, "adjacency"),
        )
        .await?;
        self.register_graph_vortex(graph, &v_uri, &e_uri).await?;
        self.upsert_graph_catalog_from_batches(GraphCatalogInput {
            name: graph.to_string(),
            source_kind: graph::GraphSourceKind::BulkParquet,
            vertices_uri: v_uri.clone(),
            edges_uri: e_uri.clone(),
            adjacency_uri: Some(join_uri(&graph_root, "adjacency")),
            vertex_batches: vertex_batches_for_catalog,
            edge_batches: edge_batches_for_catalog,
            source_dependencies: Vec::new(),
            source_fingerprints: BTreeMap::new(),
            mapping_graph: None,
            mapping_version: None,
            compile_duration_ms: None,
        })?;
        Ok(GraphStorageUris {
            vertices_uri: v_uri,
            edges_uri: e_uri,
        })
    }

    pub async fn ingest_parquet_graph_to_vortex_with_vertex_columns(
        &self,
        g: &str,
        vp: &str,
        ep: &str,
        vr: &str,
        m: IngestMode,
        _c: &[VertexColumn],
    ) -> Result<GraphStorageUris> {
        self.ingest_parquet_graph_to_vortex(g, vp, ep, vr, m).await
    }

    pub async fn register_graph_vortex(&self, name: &str, v_uri: &str, e_uri: &str) -> Result<()> {
        let v_uri = ensure_directory_uri(v_uri);
        let e_uri = ensure_directory_uri(e_uri);
        self.catalog.graphs.write().unwrap().insert(
            name.to_string(),
            GraphMeta {
                name: name.to_string(),
                vertices_uri: v_uri,
                edges_uri: e_uri,
            },
        );

        let vt_v = self.get_vertex_table(name).await?;
        let et_v = self.get_edge_table(name).await?;
        let vertex_table_name = format!("vertices_{}", name);
        let edge_table_name = format!("edges_{}", name);
        let _ = self.session.deregister_table(vertex_table_name.as_str());
        let _ = self.session.deregister_table(edge_table_name.as_str());
        self.session
            .register_table(vertex_table_name, vt_v.clone())?;
        self.session.register_table(edge_table_name, et_v)?;
        Ok(())
    }

    pub async fn register_graph_batches(
        &self,
        name: &str,
        vertices: RecordBatch,
        edges: RecordBatch,
    ) -> Result<()> {
        self.catalog.graphs.write().unwrap().insert(
            name.to_string(),
            GraphMeta {
                name: name.to_string(),
                vertices_uri: format!("memory://{name}/vertices"),
                edges_uri: format!("memory://{name}/edges"),
            },
        );
        let vertices_table = datafusion::datasource::MemTable::try_new(
            vertices.schema(),
            vec![vec![vertices.clone()]],
        )?;
        let edges_table =
            datafusion::datasource::MemTable::try_new(edges.schema(), vec![vec![edges.clone()]])?;
        let vertex_table_name = format!("vertices_{}", name);
        let edge_table_name = format!("edges_{}", name);
        let _ = self.session.deregister_table(vertex_table_name.as_str());
        let _ = self.session.deregister_table(edge_table_name.as_str());
        self.session
            .register_table(vertex_table_name, Arc::new(vertices_table))?;
        self.session
            .register_table(edge_table_name, Arc::new(edges_table))?;
        self.upsert_graph_catalog_from_batches(GraphCatalogInput {
            name: name.to_string(),
            source_kind: graph::GraphSourceKind::Memory,
            vertices_uri: format!("memory://{name}/vertices"),
            edges_uri: format!("memory://{name}/edges"),
            adjacency_uri: None,
            vertex_batches: vec![vertices],
            edge_batches: vec![edges],
            source_dependencies: Vec::new(),
            source_fingerprints: BTreeMap::new(),
            mapping_graph: None,
            mapping_version: None,
            compile_duration_ms: None,
        })?;
        Ok(())
    }
}

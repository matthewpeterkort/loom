use crate::*;
use datafusion::common::{Column, JoinType};
use datafusion::dataframe::DataFrame;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::prelude::{col, ident, lit};
use datafusion::scalar::ScalarValue;

impl Engine {
    fn query_uses_virtual_graph_runtime(&self, graph: &str) -> Result<bool> {
        let has_published = self.catalog.graphs.read().unwrap().contains_key(graph);
        let has_virtual = self
            .graph_mapping_catalog
            .get(graph)?
            .and_then(|descriptor| descriptor.virtual_binding)
            .is_some();
        Ok(!has_published && has_virtual)
    }

    pub async fn get_vertex_table(
        &self,
        graph: &str,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let meta = self
            .catalog
            .graphs
            .read()
            .unwrap()
            .get(graph)
            .cloned()
            .ok_or_else(|| anyhow!("graph not found"))?;

        let v_dir = std::path::PathBuf::from(&meta.vertices_uri);
        let mut unified_fields = std::collections::HashMap::new();
        let mut readers = Vec::new();

        // 1. Discover all Arrow files and merge schemas
        if v_dir.is_dir() {
            for entry in std::fs::read_dir(&v_dir)? {
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
            return Err(anyhow!(
                "No vertex arrow files found in {}",
                meta.vertices_uri
            ));
        }

        let mut sorted_fields: Vec<_> = unified_fields.into_values().collect();
        sorted_fields.sort_by(|a, b| a.name().cmp(b.name()));
        let unified_schema = Arc::new(arrow::datatypes::Schema::new(sorted_fields));

        // 2. Read batches and pad them into the unified schema
        let mut all_batches = Vec::new();
        for reader in readers {
            let file_schema = reader.schema();
            for batch_result in reader {
                let batch = batch_result?;
                let mut padded_columns = Vec::with_capacity(unified_schema.fields().len());
                for field in unified_schema.fields() {
                    match file_schema.index_of(field.name()) {
                        Ok(idx) => {
                            padded_columns.push(batch.column(idx).clone());
                        }
                        Err(_) => {
                            let null_arr =
                                arrow::array::new_null_array(field.data_type(), batch.num_rows());
                            padded_columns.push(null_arr);
                        }
                    }
                }
                let padded_batch = RecordBatch::try_new(unified_schema.clone(), padded_columns)?;
                all_batches.push(padded_batch);
            }
        }

        Ok(Arc::new(datafusion::datasource::MemTable::try_new(
            unified_schema,
            vec![all_batches],
        )?))
    }

    pub async fn get_edge_table(
        &self,
        graph: &str,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let meta = self
            .catalog
            .graphs
            .read()
            .unwrap()
            .get(graph)
            .cloned()
            .ok_or_else(|| anyhow!("graph not found"))?;
        arrow_table_from_dir(&meta.edges_uri, "edge")
    }

    pub async fn get_vertex_columns(&self, graph: &str) -> Result<Vec<VertexColumn>> {
        let meta = self
            .catalog
            .graphs
            .read()
            .unwrap()
            .get(graph)
            .cloned()
            .ok_or_else(|| anyhow!("graph not found"))?;
        let v_path = join_uri(&meta.vertices_uri, "vertices.vortex");
        let array = read_array_from_vortex(&v_path).await?;
        let mut cols = Vec::new();
        if let vortex::dtype::DType::Struct(st, _) = array.dtype() {
            for (name, dtype) in st.names().iter().zip(st.fields()) {
                cols.push(VertexColumn {
                    name: name.to_string(),
                    sql_type: format!("{:?}", dtype),
                });
            }
        }
        Ok(cols)
    }

    pub async fn get_csr(
        &self,
        graph: &str,
    ) -> Result<Arc<crate::operators::traversal::CsrArrays>> {
        let meta = self
            .catalog
            .graphs
            .read()
            .unwrap()
            .get(graph)
            .cloned()
            .ok_or_else(|| anyhow!("graph not found"))?;
        let adj_dir = graph_root_from_vertices_uri(&meta.vertices_uri)
            .map(|root| join_uri(&root, "adjacency"))
            .unwrap_or_else(|| {
                join_uri(
                    &self.catalog.graph_root(&meta.name).unwrap_or_default(),
                    "adjacency",
                )
            });
        Ok(Arc::new(read_csr_from_vortex(&adj_dir).await?))
    }

    pub fn list_graphs(&self) -> Result<Vec<String>> {
        let mut names = self.graph_catalog.names()?;
        let runtime = self.catalog.graphs.read().unwrap();
        for name in runtime.keys() {
            if !names.iter().any(|existing| existing == name) {
                names.push(name.clone());
            }
        }
        names.sort();
        Ok(names)
    }

    pub async fn query_batch_stream(
        &self,
        query: &Query,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let graph = graph_name(query)?;
        if self.query_uses_virtual_graph_runtime(graph)? {
            let batches = self.query_virtual_graph_batches(query).await?;
            let schema = batches
                .first()
                .map(|batch| batch.schema())
                .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
            return Ok(Box::pin(MemoryStream::try_new(batches, schema, None)?));
        }
        let state = self.session.state();
        let plan = lower_to_logical_plan(&self.session, query, self).await?;
        let mut optimizer = state.optimizer().clone();
        optimizer.rules.push(Arc::new(LoomFilterPushdownRule {}));
        let optimized_plan = optimizer.optimize(plan, &state, |_, _| {})?;

        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            LoomExtensionPlanner {},
        )]);
        let planner = LoomQueryPlanner {
            physical_planner: Arc::new(physical_planner),
        };
        let state = SessionStateBuilder::new_from_existing(state)
            .with_query_planner(Arc::new(planner))
            .build();

        let physical_plan = state.create_physical_plan(&optimized_plan).await?;
        let task_ctx = state.task_ctx();
        Ok(physical_plan.execute(0, task_ctx).map_err(|e| anyhow!(e))?)
    }

    pub async fn query_batches(&self, query: &Query) -> Result<Vec<RecordBatch>> {
        if self.query_uses_virtual_graph_runtime(graph_name(query)?)? {
            return self.query_virtual_graph_batches(query).await;
        }
        eprintln!("query_batches: Start");
        let state = self.session.state();
        let plan = lower_to_logical_plan(&self.session, query, self).await?;
        eprintln!("query_batches: Lowering logical plan");
        let mut optimizer = state.optimizer().clone();
        optimizer.rules.push(Arc::new(LoomFilterPushdownRule {}));
        let optimized_plan = optimizer.optimize(plan, &state, |_, _| {})?;
        eprintln!("query_batches: Optimizer complete");

        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            LoomExtensionPlanner {},
        )]);
        let planner = LoomQueryPlanner {
            physical_planner: Arc::new(physical_planner),
        };
        let state = SessionStateBuilder::new_from_existing(state)
            .with_query_planner(Arc::new(planner))
            .build();

        eprintln!("query_batches: physical plan creating...");
        let physical_plan = state.create_physical_plan(&optimized_plan).await?;
        eprintln!("query_batches: collect execution starting...");
        let task_ctx = state.task_ctx();
        let batches = datafusion::physical_plan::collect(physical_plan, task_ctx).await?;
        eprintln!("query_batches: collect done!");
        Ok(batches)
    }

    pub async fn query_json_rows(&self, query: &Query) -> Result<Vec<String>> {
        let batches = self.query_batches(query).await?;
        json_rows::batches_to_json_rows(&batches)
    }

    pub async fn query_sql_batches(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.session.sql(sql).await?;
        Ok(df.collect().await?)
    }

    pub async fn query_sql_json_rows(&self, sql: &str) -> Result<Vec<String>> {
        let batches = self.query_sql_batches(sql).await?;
        json_rows::batches_to_json_rows(&batches)
    }

    pub async fn query_json_rows_sonic(&self, query: &Query) -> Result<Vec<String>> {
        self.query_json_rows(query).await
    }
    pub async fn query_json_bytes(&self, query: &Query) -> Result<Vec<u8>> {
        let rows = self.query_json_rows(query).await?;
        Ok(format!("[{}]", rows.join(",")).into_bytes())
    }
    pub async fn query_sonic_bytes(&self, query: &Query) -> Result<Vec<u8>> {
        self.query_json_bytes(query).await
    }
    pub async fn query_ndjson_chunks(&self, query: &Query) -> Result<Vec<Vec<u8>>> {
        let rows = self.query_json_rows(query).await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let mut b = r.into_bytes();
                b.push(b'\n');
                b
            })
            .collect())
    }

    async fn query_virtual_graph_batches(&self, query: &Query) -> Result<Vec<RecordBatch>> {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum StreamKind {
            Vertex,
            Edge,
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        enum VertexState {
            IdOnly,
            Materialized,
        }

        let graph = graph_name(query)?;
        let vertices_base = self.session.table(&format!("vertices_{graph}")).await?;
        let edges_base = self.session.table(&format!("edges_{graph}")).await?;
        let edge_columns = dataframe_column_names(&edges_base);
        let edge_relation = format!("edges_{graph}");

        let mut current = self.materialize_dataframe(vertices_base.clone()).await?;
        let mut kind = StreamKind::Vertex;
        let mut vertex_state = VertexState::Materialized;

        for step in steps(query)? {
            match step_op(step)? {
                "v" => {
                    let ids = step_string_list(step, "ids")?;
                    current = self.materialize_dataframe(vertices_base.clone()).await?;
                    if !ids.is_empty() {
                        current = self
                            .materialize_dataframe(current.filter(in_list_expr_local("id", &ids))?)
                            .await?;
                    }
                    kind = StreamKind::Vertex;
                    vertex_state = VertexState::Materialized;
                }
                "out" => {
                    let labels = step_string_list(step, "labels")?;
                    ensure_vertex_stream(kind == StreamKind::Vertex, "out")?;
                    current = self
                        .materialize_dataframe(self.virtual_out_dataframe(current, edges_base.clone(), &labels)?)
                        .await?;
                    kind = StreamKind::Vertex;
                    vertex_state = VertexState::IdOnly;
                }
                "in" => {
                    let labels = step_string_list(step, "labels")?;
                    ensure_vertex_stream(kind == StreamKind::Vertex, "in")?;
                    current = self
                        .materialize_dataframe(self.virtual_in_dataframe(current, edges_base.clone(), &labels)?)
                        .await?;
                    kind = StreamKind::Vertex;
                    vertex_state = VertexState::IdOnly;
                }
                "both" => {
                    let labels = step_string_list(step, "labels")?;
                    ensure_vertex_stream(kind == StreamKind::Vertex, "both")?;
                    current = self
                        .materialize_dataframe(self.virtual_both_dataframe(current, edges_base.clone(), &labels)?)
                        .await?;
                    kind = StreamKind::Vertex;
                    vertex_state = VertexState::IdOnly;
                }
                "out_e" => {
                    let labels = step_string_list(step, "labels")?;
                    ensure_vertex_stream(kind == StreamKind::Vertex, "out_e")?;
                    let current_vertices = self.materialize_dataframe(current).await?;
                    let edges = with_edge_label_filter_local(edges_base.clone(), &labels)?;
                    let joined = current_vertices.join(edges, JoinType::Inner, &["id"], &["from_id"], None)?;
                    current = self
                        .materialize_dataframe(select_columns_by_relation_local(
                            joined,
                            &edge_relation,
                            &edge_columns,
                        )?)
                        .await?;
                    kind = StreamKind::Edge;
                }
                "in_e" => {
                    let labels = step_string_list(step, "labels")?;
                    ensure_vertex_stream(kind == StreamKind::Vertex, "in_e")?;
                    let current_vertices = self.materialize_dataframe(current).await?;
                    let edges = with_edge_label_filter_local(edges_base.clone(), &labels)?;
                    let joined = current_vertices.join(edges, JoinType::Inner, &["id"], &["to_id"], None)?;
                    current = self
                        .materialize_dataframe(select_columns_by_relation_local(
                            joined,
                            &edge_relation,
                            &edge_columns,
                        )?)
                        .await?;
                    kind = StreamKind::Edge;
                }
                "both_e" => {
                    let labels = step_string_list(step, "labels")?;
                    ensure_vertex_stream(kind == StreamKind::Vertex, "both_e")?;
                    let current_vertices = self.materialize_dataframe(current).await?;
                    let out = {
                        let edges = with_edge_label_filter_local(edges_base.clone(), &labels)?;
                        let joined = current_vertices
                            .clone()
                            .join(edges, JoinType::Inner, &["id"], &["from_id"], None)?;
                        select_columns_by_relation_local(joined, &edge_relation, &edge_columns)?
                    };
                    let inn = {
                        let edges = with_edge_label_filter_local(edges_base.clone(), &labels)?;
                        let joined = current_vertices.join(edges, JoinType::Inner, &["id"], &["to_id"], None)?;
                        select_columns_by_relation_local(joined, &edge_relation, &edge_columns)?
                    };
                    current = self.materialize_dataframe(out.union(inn)?).await?;
                    kind = StreamKind::Edge;
                }
                "has" => {
                    let field = step_field(step)?;
                    let eq = step_eq(step)?;
                    if kind == StreamKind::Vertex && vertex_state == VertexState::IdOnly {
                        current = self
                            .materialize_dataframe(self.materialize_virtual_vertex_dataframe(current, vertices_base.clone())?)
                            .await?;
                        vertex_state = VertexState::Materialized;
                    }
                    let cols = dataframe_column_names(&current);
                    let expr = if cols.iter().any(|column| column == field) {
                        col(field)
                    } else {
                        col(field)
                    };
                    current = self
                        .materialize_dataframe(if eq.is_null() {
                            current.filter(expr.is_null())?
                        } else {
                            current.filter(expr.eq(json_literal_expr_local(eq)?))?
                        })
                        .await?;
                }
                "has_label" => {
                    let labels = step_string_list(step, "labels")?;
                    let cols = dataframe_column_names(&current);
                    current = self
                        .materialize_dataframe(if cols.iter().any(|column| column == "label") {
                            current.filter(in_list_expr_local("label", &labels))?
                        } else if cols.iter().any(|column| column == "resourceType") {
                            current.filter(in_list_expr_local("resourceType", &labels))?
                        } else {
                            current.filter(in_list_expr_local("label", &labels))?
                        })
                        .await?;
                }
                "has_id" => {
                    let ids = step_string_list(step, "ids")?;
                    current = self
                        .materialize_dataframe(current.filter(in_list_expr_local("id", &ids))?)
                        .await?;
                }
                "limit" => {
                    current = self.materialize_dataframe(current.limit(0, Some(step_n(step)? as usize))?).await?;
                }
                "skip" => {
                    current = self.materialize_dataframe(current.limit(step_n(step)? as usize, None)?).await?;
                }
                "count" => {
                    current = self
                        .materialize_dataframe(current.aggregate(vec![], vec![count(lit(1)).alias("count")])?)
                        .await?;
                }
                "render" => {
                    let fields = step_string_list(step, "fields")?;
                    if fields.is_empty() {
                        continue;
                    }
                    if kind == StreamKind::Vertex && vertex_state == VertexState::IdOnly {
                        current = self
                            .materialize_dataframe(self.materialize_virtual_vertex_dataframe(current, vertices_base.clone())?)
                            .await?;
                        vertex_state = VertexState::Materialized;
                    }
                    let all_cols = dataframe_column_names(&current);
                    let mut exprs = Vec::new();
                    for field in &fields {
                        if field == "*" {
                            exprs.extend(all_cols.iter().map(|name| ident(name)));
                        } else {
                            exprs.push(col(field));
                        }
                    }
                    current = self.materialize_dataframe(current.select(exprs)?).await?;
                }
                other => return Err(anyhow!("unsupported step op `{other}`")),
            }
        }

        if kind == StreamKind::Vertex && vertex_state == VertexState::IdOnly {
            current = self
                .materialize_dataframe(self.materialize_virtual_vertex_dataframe(current, vertices_base)?)
                .await?;
        }

        current.collect().await.map_err(Into::into)
    }

    async fn materialize_dataframe(&self, dataframe: DataFrame) -> Result<DataFrame> {
        let batches = dataframe.collect().await?;
        self.session.read_batches(batches).map_err(Into::into)
    }

    fn virtual_out_dataframe(
        &self,
        input: DataFrame,
        edges: DataFrame,
        labels: &[String],
    ) -> Result<DataFrame> {
        let edges = with_edge_label_filter_local(edges, labels)?
            .select(vec![col("from_id"), col("to_id")])?;
        let joined = input.join(edges, JoinType::Inner, &["id"], &["from_id"], None)?;
        joined.select(vec![col("to_id").alias("id")]).map_err(Into::into)
    }

    fn virtual_in_dataframe(
        &self,
        input: DataFrame,
        edges: DataFrame,
        labels: &[String],
    ) -> Result<DataFrame> {
        let edges = with_edge_label_filter_local(edges, labels)?
            .select(vec![col("from_id"), col("to_id")])?;
        let joined = input.join(edges, JoinType::Inner, &["id"], &["to_id"], None)?;
        joined.select(vec![col("from_id").alias("id")]).map_err(Into::into)
    }

    fn virtual_both_dataframe(
        &self,
        input: DataFrame,
        edges: DataFrame,
        labels: &[String],
    ) -> Result<DataFrame> {
        let out = self.virtual_out_dataframe(input.clone(), edges.clone(), labels)?;
        let inn = self.virtual_in_dataframe(input, edges, labels)?;
        out.union(inn)?.distinct().map_err(Into::into)
    }

    fn materialize_virtual_vertex_dataframe(
        &self,
        input: DataFrame,
        vertices: DataFrame,
    ) -> Result<DataFrame> {
        let vertex_columns = dataframe_column_names(&vertices);
        let join_projections = vertex_columns
            .iter()
            .map(|name| ident(name).alias(&format!("__vertex_col_{name}")))
            .chain(std::iter::once(ident("id").alias("__vertex_join_id")))
            .collect::<Vec<_>>();
        let vertices = vertices.select(join_projections)?;
        let joined = input.join(vertices, JoinType::Inner, &["id"], &["__vertex_join_id"], None)?;
        let projections = vertex_columns
            .iter()
            .map(|name| ident(&format!("__vertex_col_{name}")).alias(name))
            .collect::<Vec<_>>();
        joined.select(projections).map_err(Into::into)
    }
}

fn dataframe_column_names(dataframe: &DataFrame) -> Vec<String> {
    dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

fn with_edge_label_filter_local(edges: DataFrame, labels: &[String]) -> Result<DataFrame> {
    if labels.is_empty() {
        return Ok(edges);
    }
    edges.filter(in_list_expr_local("label", labels)).map_err(Into::into)
}

fn select_columns_by_relation_local(
    dataframe: DataFrame,
    relation: &str,
    names: &[String],
) -> Result<DataFrame> {
    let exprs = names
        .iter()
        .map(|name| Expr::Column(Column::new(Some(relation.to_string()), name.clone())))
        .collect::<Vec<_>>();
    dataframe.select(exprs).map_err(Into::into)
}

fn in_list_expr_local(column: &str, values: &[String]) -> datafusion::logical_expr::Expr {
    if values.is_empty() {
        return lit(true);
    }
    let mut expr = col(column).eq(lit(values[0].clone()));
    for value in values.iter().skip(1) {
        expr = expr.or(col(column).eq(lit(value.clone())));
    }
    expr
}

fn json_literal_expr_local(value: &serde_json::Value) -> Result<datafusion::logical_expr::Expr> {
    Ok(match value {
        serde_json::Value::Null => {
            datafusion::logical_expr::Expr::Literal(ScalarValue::Null, None)
        }
        serde_json::Value::Bool(value) => lit(*value),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                lit(value)
            } else if let Some(value) = number.as_u64() {
                lit(value)
            } else if let Some(value) = number.as_f64() {
                lit(value)
            } else {
                return Err(anyhow!("unsupported numeric literal"));
            }
        }
        serde_json::Value::String(value) => lit(value.clone()),
        _ => return Err(anyhow!("unsupported literal value in Has predicate")),
    })
}

fn ensure_vertex_stream(is_vertex: bool, step: &str) -> Result<()> {
    if is_vertex {
        Ok(())
    } else {
        Err(anyhow!("cannot apply `{step}` to an edge stream"))
    }
}

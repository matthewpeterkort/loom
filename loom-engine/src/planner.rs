use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchemaRef, DataFusionError};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{
    LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::{ApplyOrder, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use vortex::array::ArrayRef;
use vortex::array::ArrayVisitor;
use vortex::buffer::Buffer;
use vortex::expr::Expression as VortexExpression;

use crate::converter::convert_expr;
use crate::Query;

use crate::operators::materialize::MaterializeExec;
use crate::operators::traversal::{CsrArrays, TraversalDirection, TraversalExec};

#[derive(Debug)]
pub struct LoomTraversalNode {
    pub input: LogicalPlan,
    pub graph: String,
    pub offsets: Buffer<u64>,
    pub targets: Buffer<u64>,
    pub offsets_in: Option<Buffer<u64>>,
    pub targets_in: Option<Buffer<u64>>,
    pub direction: TraversalDirection,
    pub schema: DFSchemaRef,
}

impl LoomTraversalNode {
    pub fn new(
        input: LogicalPlan,
        graph: String,
        offsets: Buffer<u64>,
        targets: Buffer<u64>,
        offsets_in: Option<Buffer<u64>>,
        targets_in: Option<Buffer<u64>>,
        direction: TraversalDirection,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::UInt64,
            false,
        )]));
        let df_schema = DFSchemaRef::new(schema.clone().try_into().unwrap());
        Self {
            input,
            graph,
            offsets,
            targets,
            offsets_in,
            targets_in,
            direction,
            schema: df_schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for LoomTraversalNode {
    fn name(&self) -> &str {
        "LoomTraversalNode"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "LoomTraversalNode(graph={}, direction={:?})",
            self.graph, self.direction
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self::new(
            inputs[0].clone(),
            self.graph.clone(),
            self.offsets.clone(),
            self.targets.clone(),
            self.offsets_in.clone(),
            self.targets_in.clone(),
            self.direction,
        ))
    }
}

impl Hash for LoomTraversalNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.graph.hash(state);
        self.direction.hash(state);
    }
}
impl PartialEq for LoomTraversalNode {
    fn eq(&self, other: &Self) -> bool {
        self.graph == other.graph && self.direction == other.direction
    }
}
impl Eq for LoomTraversalNode {}
impl PartialOrd for LoomTraversalNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        Some(Ordering::Equal)
    }
}

#[derive(Debug)]
pub struct LoomMaterializeNode {
    pub input: LogicalPlan,
    pub graph: String,
    pub columns: Vec<(String, ArrayRef)>,
    pub pushed_filter: Option<VortexExpression>,
    pub schema: DFSchemaRef,
}

impl LoomMaterializeNode {
    pub fn new(
        input: LogicalPlan,
        graph: String,
        columns: Vec<(String, ArrayRef)>,
        pushed_filter: Option<VortexExpression>,
        schema: SchemaRef,
    ) -> Self {
        let df_schema = DFSchemaRef::new(schema.try_into().unwrap());
        Self {
            input,
            graph,
            columns,
            pushed_filter,
            schema: df_schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for LoomMaterializeNode {
    fn name(&self) -> &str {
        "LoomMaterializeNode"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "LoomMaterializeNode(graph={}, cols={}, filter={:?})",
            self.graph,
            self.columns.len(),
            self.pushed_filter
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        let schema_ref: SchemaRef = self.schema.as_arrow().clone().into();
        Ok(Self::new(
            inputs[0].clone(),
            self.graph.clone(),
            self.columns.clone(),
            self.pushed_filter.clone(),
            schema_ref,
        ))
    }
}

impl Hash for LoomMaterializeNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.graph.hash(state);
        for (name, _) in &self.columns {
            name.hash(state);
        }
    }
}
impl PartialEq for LoomMaterializeNode {
    fn eq(&self, other: &Self) -> bool {
        if self.graph != other.graph || self.columns.len() != other.columns.len() {
            return false;
        }
        self.columns
            .iter()
            .zip(&other.columns)
            .all(|(a, b)| a.0 == b.0)
    }
}
impl Eq for LoomMaterializeNode {}
impl PartialOrd for LoomMaterializeNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        Some(Ordering::Equal)
    }
}

pub struct LoomQueryPlanner {
    pub physical_planner: Arc<dyn PhysicalPlanner>,
}

impl Debug for LoomQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoomQueryPlanner").finish()
    }
}

#[async_trait]
impl QueryPlanner for LoomQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

pub struct LoomExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for LoomExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        if let Some(traversal) = any.downcast_ref::<LoomTraversalNode>() {
            return Ok(Some(Arc::new(
                TraversalExec::try_new(
                    physical_inputs[0].clone(),
                    traversal.offsets.clone(),
                    traversal.targets.clone(),
                    traversal.offsets_in.clone(),
                    traversal.targets_in.clone(),
                    traversal.direction,
                )
                .map_err(|e| DataFusionError::External(e.into()))?,
            )));
        } else if let Some(mat) = any.downcast_ref::<LoomMaterializeNode>() {
            let schema_ref: SchemaRef = mat.schema.as_arrow().clone().into();
            return Ok(Some(Arc::new(
                MaterializeExec::try_new(
                    physical_inputs[0].clone(),
                    mat.columns.clone(),
                    mat.pushed_filter.clone(),
                    schema_ref,
                )
                .map_err(|e| DataFusionError::External(e.into()))?,
            )));
        }
        Ok(None)
    }
}

pub async fn lower_to_logical_plan(
    _ctx: &datafusion::prelude::SessionContext,
    query: &Query,
    engine: &crate::Engine,
) -> datafusion::error::Result<LogicalPlan> {
    let mut plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false).build()?;
    let current_graph = crate::graph_name(query)
        .map_err(|e| DataFusionError::External(e.into()))?
        .to_string();

    for step in crate::steps(query).map_err(|e| DataFusionError::External(e.into()))? {
        match crate::step_op(step).map_err(|e| DataFusionError::External(e.into()))? {
            "v" => {
                let vt = format!("vertices_{}", current_graph);
                let table_provider = engine
                    .get_vertex_table(&current_graph)
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))?;
                let table_source = datafusion::datasource::DefaultTableSource::new(table_provider);
                let scan = LogicalPlanBuilder::scan(vt, Arc::new(table_source), None)?.build()?;

                let cols = engine
                    .get_vertex_columns(&current_graph)
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))?;
                let schema = crate::schema_from_vertex_columns(&cols)
                    .map_err(|e| DataFusionError::External(e.into()))?;

                let v_uri = engine
                    .catalog
                    .graph_root(&current_graph)
                    .map(|r| crate::join_uri(&r, "vertices/vertices.vortex"))
                    .map_err(|e| DataFusionError::External(e.into()))?;
                let vortex_arr = crate::read_array_from_vortex(&v_uri)
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))?;
                let mut columns = Vec::new();
                for col in &cols {
                    let names = vortex_arr.children_names();
                    let col_idx = names.iter().position(|n| n == &col.name).ok_or_else(|| {
                        DataFusionError::Internal(format!("missing col {}", col.name))
                    })?;
                    let child = vortex_arr
                        .nth_child(col_idx)
                        .ok_or_else(|| DataFusionError::Internal("missing field".to_string()))?;
                    columns.push((col.name.clone(), child));
                }

                plan = LogicalPlan::Extension(datafusion::logical_expr::Extension {
                    node: Arc::new(LoomMaterializeNode::new(
                        scan,
                        current_graph.clone(),
                        columns,
                        None,
                        schema,
                    )),
                });
            }
            "has_label" => {
                let labels = crate::step_string_list(step, "labels")
                    .map_err(|e| DataFusionError::External(e.into()))?;
                let Some(first_label) = labels.first() else {
                    continue;
                };
                plan = LogicalPlanBuilder::from(plan)
                    .filter(
                        datafusion::logical_expr::col("label")
                            .eq(datafusion::logical_expr::lit(first_label.clone())),
                    )?
                    .build()?;
            }
            "out" => {
                let csr: Arc<CsrArrays> = engine
                    .get_csr(&current_graph)
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))?;
                plan = LogicalPlan::Extension(datafusion::logical_expr::Extension {
                    node: Arc::new(LoomTraversalNode::new(
                        plan,
                        current_graph.clone(),
                        csr.offsets.clone(),
                        csr.targets.clone(),
                        csr.offsets_in.clone(),
                        csr.targets_in.clone(),
                        TraversalDirection::Out,
                    )),
                });
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Step {:?} not implemented",
                    step
                )));
            }
        }
    }
    Ok(plan)
}

#[derive(Debug)]
pub struct LoomFilterPushdownRule {}

impl OptimizerRule for LoomFilterPushdownRule {
    fn name(&self) -> &str {
        "LoomFilterPushdownRule"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> datafusion::error::Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                if let LogicalPlan::Extension(ext) = filter.input.as_ref() {
                    let any = ext.node.as_any();
                    if let Some(mat) = any.downcast_ref::<LoomMaterializeNode>() {
                        // Attempt to convert the predicate
                        if let Ok(vortex_expr) = convert_expr(&filter.predicate) {
                            // FUSE IT!
                            let new_filter = match &mat.pushed_filter {
                                Some(existing) => vortex::expr::and(existing.clone(), vortex_expr),
                                None => vortex_expr,
                            };
                            let new_mat = LoomMaterializeNode::new(
                                mat.input.clone(),
                                mat.graph.clone(),
                                mat.columns.clone(),
                                Some(new_filter),
                                mat.schema.as_arrow().clone().into(),
                            );
                            return Ok(Transformed::yes(LogicalPlan::Extension(
                                datafusion::logical_expr::Extension {
                                    node: Arc::new(new_mat),
                                },
                            )));
                        }
                    }
                }
                Ok(Transformed::no(LogicalPlan::Filter(filter)))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};
    use datafusion::optimizer::OptimizerContext;

    #[test]
    fn test_filter_pushdown_logic() -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::UInt64, false),
        ]));
        let _df_schema = DFSchemaRef::new(schema.clone().try_into()?);

        let input = LogicalPlanBuilder::empty(false).build()?;
        let mat = LoomMaterializeNode::new(
            input,
            "test_graph".to_string(),
            vec![],
            None,
            schema.clone(),
        );
        let plan = LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(mat),
        });

        // Add a filter: age > 30
        let filter_plan = LogicalPlanBuilder::from(plan)
            .filter(col("age").gt(lit(30i64)))?
            .build()?;

        let rule = LoomFilterPushdownRule {};
        let config = OptimizerContext::new();
        let optimized = rule.rewrite(filter_plan, &config)?;

        assert!(optimized.transformed);
        let opt_plan = optimized.data;

        // The top level should now be the Extension (MaterializeNode), NOT the Filter
        if let LogicalPlan::Extension(ext) = opt_plan {
            let any = ext.node.as_any();
            let mat = any
                .downcast_ref::<LoomMaterializeNode>()
                .expect("must be materialize node");
            assert!(mat.pushed_filter.is_some(), "filter should be pushed down");
        } else {
            panic!("Expected Extension plan, got {:?}", opt_plan);
        }

        Ok(())
    }
}

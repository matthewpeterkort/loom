use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::array::{Array, UInt64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::stream::Stream;
use futures::StreamExt;

use vortex::array::arrays::{PrimitiveArray, StructArray};
use vortex::array::validity::Validity;
use vortex::array::Canonical;
use vortex::array::{ArrayRef, IntoArray};
use vortex::buffer::Buffer;
use vortex::dtype::FieldNames;
use vortex::expr::Expression as VortexExpression;

#[derive(Debug)]
pub struct MaterializeExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub columns: Vec<(String, ArrayRef)>,
    pub pushed_filter: Option<VortexExpression>,
    pub schema: SchemaRef,
    pub properties: PlanProperties,
}

impl MaterializeExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        columns: Vec<(String, ArrayRef)>,
        pushed_filter: Option<VortexExpression>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            input,
            columns,
            pushed_filter,
            schema,
            properties,
        })
    }
}

impl DisplayAs for MaterializeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MaterializeExec(cols={}, filter={:?})",
            self.columns.len(),
            self.pushed_filter
        )
    }
}

impl ExecutionPlan for MaterializeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "MaterializeExec"
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            Self::try_new(
                children[0].clone(),
                self.columns.clone(),
                self.pushed_filter.clone(),
                self.schema.clone(),
            )
            .map_err(|e| DataFusionError::External(e.into()))?,
        ))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(MaterializeStream {
            input: input_stream,
            columns: self.columns.clone(),
            pushed_filter: self.pushed_filter.clone(),
            schema: self.schema.clone(),
        }))
    }
}

struct MaterializeStream {
    input: SendableRecordBatchStream,
    columns: Vec<(String, ArrayRef)>,
    pushed_filter: Option<VortexExpression>,
    schema: SchemaRef,
}

impl Stream for MaterializeStream {
    type Item = datafusion::error::Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(materialize_batch(
                &batch,
                &self.columns,
                &self.pushed_filter,
                &self.schema,
            ))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for MaterializeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn materialize_batch(
    batch: &RecordBatch,
    columns: &[(String, ArrayRef)],
    pushed_filter: &Option<VortexExpression>,
    schema: &SchemaRef,
) -> datafusion::error::Result<RecordBatch> {
    let ids = batch
        .column_by_name("_v_id")
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| DataFusionError::Internal("missing _v_id".to_string()))?;

    if ids.len() == 0 {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let indices_buffer = Buffer::from(ids.values().to_vec());
    let indices = PrimitiveArray::new(indices_buffer, Validity::NonNullable).into_array();

    let mut taken_columns = Vec::with_capacity(columns.len());
    let mut names = Vec::with_capacity(columns.len());

    for (name, vortex_arr) in columns {
        let taken = vortex_arr
            .take(indices.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        names.push(name.clone());
        taken_columns.push(taken);
    }

    let mut final_vortex_columns = taken_columns;

    // Apply pushed filter if present
    if let Some(expr) = pushed_filter {
        let struct_array = StructArray::try_new(
            FieldNames::from_iter(names.clone()),
            final_vortex_columns.clone(),
            indices.len(),
            Validity::NonNullable,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .into_array();

        let mask_array = struct_array
            .apply(expr)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mask = {
            let canonical = mask_array
                .to_canonical()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let bool_arr = match canonical {
                Canonical::Bool(b) => b,
                _ => {
                    return Err(DataFusionError::Internal(
                        "expected boolean result from expression".to_string(),
                    ))
                }
            };
            bool_arr.to_mask_fill_null_false()
        };

        // Filter all columns
        let mut filtered_cols = Vec::with_capacity(final_vortex_columns.len());
        for col in final_vortex_columns {
            filtered_cols.push(
                col.filter(mask.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            );
        }
        final_vortex_columns = filtered_cols;
    }

    let mut arrow_columns = Vec::with_capacity(final_vortex_columns.len());
    for taken in final_vortex_columns {
        let arrow_arr = vortex::array::arrow::IntoArrowArray::into_arrow_preferred(taken)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        arrow_columns.push(arrow_arr);
    }

    RecordBatch::try_new(schema.clone(), arrow_columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

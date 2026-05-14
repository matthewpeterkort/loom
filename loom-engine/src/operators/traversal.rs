use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::array::{Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::Stream;
use futures::StreamExt;
use vortex::buffer::Buffer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TraversalDirection {
    Out,
    In,
    Both,
}

#[derive(Debug, Clone)]
pub struct CsrArrays {
    pub offsets: Buffer<u64>,
    pub targets: Buffer<u64>,
    pub offsets_in: Option<Buffer<u64>>,
    pub targets_in: Option<Buffer<u64>>,
}

#[derive(Debug)]
pub struct TraversalExec {
    input: Arc<dyn ExecutionPlan>,
    offsets: Buffer<u64>,
    targets: Buffer<u64>,
    offsets_in: Option<Buffer<u64>>,
    targets_in: Option<Buffer<u64>>,
    direction: TraversalDirection,
    properties: PlanProperties,
}

impl TraversalExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        offsets: Buffer<u64>,
        targets: Buffer<u64>,
        offsets_in: Option<Buffer<u64>>,
        targets_in: Option<Buffer<u64>>,
        direction: TraversalDirection,
    ) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            input,
            offsets,
            targets,
            offsets_in,
            targets_in,
            direction,
            properties,
        })
    }
}

impl DisplayAs for TraversalExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TraversalExec(direction={:?})", self.direction)
    }
}

impl ExecutionPlan for TraversalExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "TraversalExec"
    }
    fn schema(&self) -> SchemaRef {
        self.properties.eq_properties.schema().clone()
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
            TraversalExec::try_new(
                children[0].clone(),
                self.offsets.clone(),
                self.targets.clone(),
                self.offsets_in.clone(),
                self.targets_in.clone(),
                self.direction,
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
        Ok(Box::pin(TraversalStream {
            input: input_stream,
            offsets: self.offsets.clone(),
            targets: self.targets.clone(),
            offsets_in: self.offsets_in.clone(),
            targets_in: self.targets_in.clone(),
            direction: self.direction,
            schema: self.schema(),
        }))
    }
    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

struct TraversalStream {
    input: SendableRecordBatchStream,
    offsets: Buffer<u64>,
    targets: Buffer<u64>,
    offsets_in: Option<Buffer<u64>>,
    targets_in: Option<Buffer<u64>>,
    direction: TraversalDirection,
    schema: SchemaRef,
}

impl Stream for TraversalStream {
    type Item = datafusion::error::Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(traverse_batch(
                &batch,
                &self.offsets,
                &self.targets,
                self.offsets_in.as_deref(),
                self.targets_in.as_deref(),
                self.direction,
                &self.schema,
            ))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for TraversalStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn traverse_batch(
    batch: &RecordBatch,
    offsets_out: &[u64],
    targets_out: &[u64],
    offsets_in: Option<&[u64]>,
    targets_in: Option<&[u64]>,
    direction: TraversalDirection,
    schema: &SchemaRef,
) -> datafusion::error::Result<RecordBatch> {
    let ids = batch
        .column_by_name("id")
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| DataFusionError::Internal("missing id".to_string()))?;

    let mut result_ids = Vec::new();
    for i in 0..ids.len() {
        if ids.is_null(i) {
            continue;
        }
        let id = ids.value(i) as usize;

        if direction == TraversalDirection::Out || direction == TraversalDirection::Both {
            if id < offsets_out.len() - 1 {
                let start = offsets_out[id] as usize;
                let end = offsets_out[id + 1] as usize;
                for j in start..end {
                    result_ids.push(targets_out[j]);
                }
            }
        }

        if (direction == TraversalDirection::In || direction == TraversalDirection::Both)
            && offsets_in.is_some()
            && targets_in.is_some()
        {
            let off = offsets_in.unwrap();
            let tar = targets_in.unwrap();
            if id < off.len() - 1 {
                let start = off[id] as usize;
                let end = off[id + 1] as usize;
                for j in start..end {
                    result_ids.push(tar[j]);
                }
            }
        }
    }

    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(result_ids))],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

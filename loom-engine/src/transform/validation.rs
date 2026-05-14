use anyhow::{anyhow, Result};

use crate::transform;

pub fn validate_transform_spec_shape(spec: &transform::TransformSpec) -> Result<()> {
    if spec.output_table_id.trim().is_empty() {
        return Err(anyhow!("transform output_table_id must not be empty"));
    }
    let mut seen_non_layout = false;
    for op in &spec.operations {
        match op {
            transform::TransformOperation::ChooseHeaderRow { .. }
            | transform::TransformOperation::SetDataStartRow { .. } => {
                if seen_non_layout {
                    return Err(anyhow!(
                        "layout operations must appear before non-layout transform operations"
                    ));
                }
            }
            _ => seen_non_layout = true,
        }
    }
    Ok(())
}

use anyhow::{anyhow, Result};
use datafusion::prelude::{col, lit, DataFrame};

use crate::transform::{self, TransformPlanKind};

pub struct CompiledTransformPlan {
    pub plan_kind: TransformPlanKind,
    pub dataframe: Option<DataFrame>,
    pub output_headers: Vec<String>,
    pub header_row_index: Option<usize>,
    pub data_start_row_index: Option<usize>,
    pub original_column_names: Vec<String>,
    pub warnings: Vec<String>,
}

pub fn compile_transform_dataframe(
    spec: &transform::TransformSpec,
    mut dataframe: DataFrame,
    input_headers: Vec<String>,
    header_row_index: Option<usize>,
    data_start_row_index: Option<usize>,
    original_column_names: Vec<String>,
) -> Result<CompiledTransformPlan> {
    if spec.operations.iter().any(|op| matches!(op, transform::TransformOperation::ExplodeColumn(_))) {
        return Ok(CompiledTransformPlan {
            plan_kind: TransformPlanKind::Fallback,
            dataframe: None,
            output_headers: input_headers,
            header_row_index,
            data_start_row_index,
            original_column_names,
            warnings: Vec::new(),
        });
    }

    let mut warnings = Vec::new();
    let mut current_headers = input_headers;
    let mut current_header_row_index = header_row_index;
    let mut current_data_start_row_index = data_start_row_index;

    for op in &spec.operations {
        match op {
            transform::TransformOperation::ChooseHeaderRow { header_row_index } => {
                current_header_row_index = Some(*header_row_index);
            }
            transform::TransformOperation::SetDataStartRow { data_start_row_index } => {
                current_data_start_row_index = Some(*data_start_row_index);
            }
            transform::TransformOperation::RenameColumn { from, to } => {
                require_column(&current_headers, from)?;
                if current_headers.iter().any(|header| header == to && header != from) {
                    return Err(anyhow!("rename target `{to}` already exists"));
                }
                dataframe = dataframe.with_column_renamed(from, to)?;
                for header in &mut current_headers {
                    if header == from {
                        *header = to.clone();
                    }
                }
            }
            transform::TransformOperation::DropColumn { column } => {
                require_column(&current_headers, column)?;
                dataframe = dataframe.drop_columns(&[column.as_str()])?;
                current_headers.retain(|header| header != column);
            }
            transform::TransformOperation::Trim { columns } => {
                for column in columns {
                    require_column(&current_headers, column)?;
                    dataframe = dataframe.with_column(
                        column,
                        transform::expr::compile_expr(&crate::mapping::Expr::Trim {
                            trim: Box::new(crate::mapping::Expr::Column {
                                column: column.clone(),
                            }),
                        })?,
                    )?;
                }
            }
            transform::TransformOperation::SplitColumn(op) => {
                require_column(&current_headers, &op.column)?;
                for target in &op.into {
                    if current_headers.iter().any(|header| header == target) {
                        return Err(anyhow!("split target `{target}` already exists"));
                    }
                }
                for (idx, target) in op.into.iter().enumerate() {
                    let expr = if op.delimiter.is_empty() {
                        if idx == 0 {
                            col(&op.column)
                        } else {
                            lit("")
                        }
                    } else {
                        datafusion::prelude::split_part(
                            col(&op.column),
                            lit(op.delimiter.clone()),
                            lit((idx + 1) as i64),
                        )
                    };
                    dataframe = dataframe.with_column(target, expr)?;
                }
                if matches!(op.behavior, transform::SplitColumnBehavior::DropOriginal) {
                    dataframe = dataframe.drop_columns(&[op.column.as_str()])?;
                    current_headers.retain(|header| header != &op.column);
                }
                current_headers.extend(op.into.clone());
            }
            transform::TransformOperation::ExplodeColumn(_) => unreachable!("explode handled earlier"),
            transform::TransformOperation::CoerceType {
                column,
                output_type,
                on_error,
            } => {
                require_column(&current_headers, column)?;
                if matches!(on_error, transform::CoerceOnErrorPolicy::KeepOriginalText) {
                    warnings.push(format!(
                        "column `{column}` keep_original_text coercion remains stringly typed in compiled mode"
                    ));
                }
                dataframe = dataframe.with_column(
                    column,
                    transform::expr::compile_coerce_expr(column, output_type, on_error),
                )?;
            }
            transform::TransformOperation::FilterRows(op) => {
                dataframe = dataframe.filter(transform::expr::compile_predicate(&op.predicate)?)?;
            }
            transform::TransformOperation::DeriveColumn(op) => {
                validate_expr_columns(&op.expr, &current_headers)?;
                dataframe =
                    dataframe.with_column(&op.column, transform::expr::compile_expr(&op.expr)?)?;
                if !current_headers.iter().any(|header| header == &op.column) {
                    current_headers.push(op.column.clone());
                }
            }
        }
        ensure_unique_columns(&current_headers)?;
    }

    let mut projection = current_headers
        .iter()
        .map(|header| col(header))
        .collect::<Vec<_>>();
    for provenance in ["__source_row", "__source_table", "__source_id"] {
        projection.push(col(provenance));
    }
    dataframe = dataframe.with_column(
        "__transform_id",
        lit(transform_id_for_spec(spec)),
    )?;
    projection.push(col("__transform_id"));
    dataframe = dataframe.select(projection)?;

    Ok(CompiledTransformPlan {
        plan_kind: TransformPlanKind::Compiled,
        dataframe: Some(dataframe),
        output_headers: current_headers,
        header_row_index: current_header_row_index,
        data_start_row_index: current_data_start_row_index,
        original_column_names,
        warnings,
    })
}

fn require_column(headers: &[String], column: &str) -> Result<()> {
    if headers.iter().any(|header| header == column) {
        Ok(())
    } else {
        Err(anyhow!("column `{column}` not found"))
    }
}

fn ensure_unique_columns(headers: &[String]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for header in headers {
        if header.trim().is_empty() {
            return Err(anyhow!("output column names must be non-empty"));
        }
        if !seen.insert(header.clone()) {
            return Err(anyhow!("duplicate output column `{header}`"));
        }
    }
    Ok(())
}

fn validate_expr_columns(expr: &crate::mapping::Expr, headers: &[String]) -> Result<()> {
    match expr {
        crate::mapping::Expr::Column { column } => require_column(headers, column),
        crate::mapping::Expr::Concat { concat }
        | crate::mapping::Expr::Coalesce { coalesce: concat } => {
            for expr in concat {
                validate_expr_columns(expr, headers)?;
            }
            Ok(())
        }
        crate::mapping::Expr::Lower { lower }
        | crate::mapping::Expr::Upper { upper: lower }
        | crate::mapping::Expr::Trim { trim: lower }
        | crate::mapping::Expr::Sha256 { sha256: lower } => validate_expr_columns(lower, headers),
        crate::mapping::Expr::Replace { replace } => validate_expr_columns(&replace.value, headers),
        crate::mapping::Expr::NullIf { null_if } => {
            for expr in null_if {
                validate_expr_columns(expr, headers)?;
            }
            Ok(())
        }
        crate::mapping::Expr::Literal { .. }
        | crate::mapping::Expr::RowNumber { .. }
        | crate::mapping::Expr::Text(_) => Ok(()),
    }
}

fn sanitize_id_component(value: &str) -> String {
    value
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
        .to_string()
}

fn transform_id_for_spec(spec: &transform::TransformSpec) -> String {
    format!(
        "transform_{}_{}",
        sanitize_id_component(&spec.input.source_id),
        sanitize_id_component(&spec.output_table_id)
    )
}

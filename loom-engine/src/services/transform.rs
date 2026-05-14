use crate::*;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, ViewTable};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

const TRANSFORM_ID_KEY: &str = "transform_id";
const INPUT_SOURCE_ID_KEY: &str = "input_source_id";
const INPUT_TABLE_ID_KEY: &str = "input_table_id";

impl Engine {
    pub async fn register_transform(
        &self,
        spec: transform::TransformSpec,
    ) -> Result<transform::TransformDescriptor> {
        let validation = self.validate_transform_spec(&spec).await?;
        if !validation.valid {
            return Err(anyhow!(
                "invalid transform spec: {}",
                validation.errors.join("; ")
            ));
        }
        let output_table = validation
            .output_table
            .clone()
            .ok_or_else(|| anyhow!("transform validation did not produce an output table"))?;
        let transform_id = transform_id_for_spec(&spec);
        let now = now_unix_seconds_local();
        let descriptor = transform::TransformDescriptor {
            id: transform_id,
            spec,
            output_table: output_table.clone(),
            status: transform::TransformDescriptorStatus::Valid,
            last_error: None,
            created_unix_seconds: now,
            updated_unix_seconds: now,
        };
        self.transform_catalog.upsert(descriptor.clone())?;
        self.source_catalog
            .upsert_table(&descriptor.spec.input.source_id, output_table)?;
        Ok(descriptor)
    }

    pub fn list_transforms(&self) -> Result<Vec<transform::TransformDescriptor>> {
        self.transform_catalog.list()
    }

    pub fn get_transform(&self, id: &str) -> Result<transform::TransformDescriptor> {
        self.transform_catalog.require(id)
    }

    pub async fn validate_transform_spec(
        &self,
        spec: &transform::TransformSpec,
    ) -> Result<transform::TransformValidationReport> {
        let mut report = transform::TransformValidationReport::default();
        match self.describe_transform_spec(spec).await {
            Ok(exec) => {
                report.valid = true;
                report.warnings = exec.warnings;
                report.output_table = Some(exec.output_table);
                report.plan_kind = exec.plan_kind;
            }
            Err(err) => {
                report.valid = false;
                report.errors.push(err.to_string());
            }
        }
        Ok(report)
    }

    pub async fn preview_transform_spec(
        &self,
        spec: &transform::TransformSpec,
        limit: Option<usize>,
    ) -> Result<transform::TransformPreviewResult> {
        let exec = self.describe_transform_preview(spec, limit).await?;
        Ok(transform::TransformPreviewResult {
            rows: exec
                .table
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().collect::<BTreeMap<_, _>>())
                .collect(),
            warnings: exec.warnings,
            output_table: exec.output_table,
            plan_kind: exec.plan_kind,
        })
    }

    pub async fn preview_transform(
        &self,
        transform_id: &str,
        limit: Option<usize>,
    ) -> Result<transform::TransformPreviewResult> {
        let descriptor = self.transform_catalog.require(transform_id)?;
        self.preview_transform_spec(&descriptor.spec, limit).await
    }

    pub async fn load_transform_catalog_snapshot(
        &self,
        snapshot: transform::TransformCatalogSnapshot,
    ) -> Result<Vec<String>> {
        self.transform_catalog.replace_all(snapshot)?;
        let mut errors = Vec::new();
        for descriptor in self.transform_catalog.list()? {
            if let Err(err) = self
                .source_catalog
                .upsert_table(&descriptor.spec.input.source_id, descriptor.output_table.clone())
            {
                errors.push(format!("{}: {}", descriptor.id, err));
            }
        }
        Ok(errors)
    }

    pub async fn ensure_transform_table_registered(
        &self,
        table_ref: &source::SourceTableRef,
    ) -> Result<source::SourceTableDescriptor> {
        let table = self
            .source_catalog
            .require_table(&table_ref.source_id, &table_ref.table_id)?;
        if table.registered {
            return Ok(table);
        }
        let transform_id = table
            .metadata
            .get(TRANSFORM_ID_KEY)
            .cloned()
            .ok_or_else(|| anyhow!("derived table `{}` missing transform id metadata", table.id))?;
        let descriptor = self.transform_catalog.require(&transform_id)?;
        let _ = self.session.deregister_table(table.table_name.as_str());
        let compiled = self.compile_transform_spec(&descriptor.spec).await?;
        match compiled.plan_kind {
            transform::TransformPlanKind::Compiled => {
                let plan = compiled
                    .dataframe
                    .ok_or_else(|| anyhow!("compiled transform missing dataframe"))?
                    .into_unoptimized_plan();
                self.session.register_table(
                    table.table_name.clone(),
                    Arc::new(ViewTable::new(plan, None)),
                )?;
            }
            transform::TransformPlanKind::Fallback => {
                let exec = self
                    .execute_transform_spec(&descriptor.spec, None, false, &mut HashSet::new())
                    .await?;
                let batch = table_to_batch_with_transform_provenance(&exec.table, &transform_id)?;
                let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
                self.session
                    .register_table(table.table_name.clone(), Arc::new(provider))?;
            }
        }
        self.source_catalog.set_table_registered(table_ref, true)?;
        self.source_catalog
            .require_table(&table_ref.source_id, &table_ref.table_id)
    }

    async fn describe_transform_spec(
        &self,
        spec: &transform::TransformSpec,
    ) -> Result<TransformExecutionSummary> {
        let compiled = self.compile_transform_spec(spec).await?;
        if matches!(compiled.plan_kind, transform::TransformPlanKind::Fallback) {
            let exec = self
                .execute_transform_spec(spec, None, false, &mut HashSet::new())
                .await?;
            return Ok(TransformExecutionSummary {
                table: exec.table,
                output_table: exec.output_table,
                warnings: exec.warnings,
                plan_kind: transform::TransformPlanKind::Fallback,
            });
        }
        let dataframe = compiled
            .dataframe
            .clone()
            .ok_or_else(|| anyhow!("compiled transform missing dataframe"))?;
        let batches = dataframe.collect().await?;
        let table = source_table_from_batches(
            &batches,
            &spec.input.source_id,
            &spec.output_table_id,
            &compiled.output_headers,
        )?;
        let output_table = build_transform_output_descriptor(spec, &compiled, &table)?;
        Ok(TransformExecutionSummary {
            table,
            output_table,
            warnings: compiled.warnings,
            plan_kind: transform::TransformPlanKind::Compiled,
        })
    }

    async fn describe_transform_preview(
        &self,
        spec: &transform::TransformSpec,
        limit: Option<usize>,
    ) -> Result<TransformExecutionSummary> {
        let compiled = self.compile_transform_spec(spec).await?;
        if matches!(compiled.plan_kind, transform::TransformPlanKind::Fallback) {
            let exec = self
                .execute_transform_spec(spec, limit, true, &mut HashSet::new())
                .await?;
            return Ok(TransformExecutionSummary {
                table: exec.table,
                output_table: exec.output_table,
                warnings: exec.warnings,
                plan_kind: transform::TransformPlanKind::Fallback,
            });
        }
        let mut dataframe = compiled
            .dataframe
            .clone()
            .ok_or_else(|| anyhow!("compiled transform missing dataframe"))?;
        if let Some(limit) = limit {
            dataframe = dataframe.limit(0, Some(limit))?;
        }
        let batches = dataframe.collect().await?;
        let table = source_table_from_batches(
            &batches,
            &spec.input.source_id,
            &spec.output_table_id,
            &compiled.output_headers,
        )?;
        let output_table = build_transform_output_descriptor(spec, &compiled, &table)?;
        Ok(TransformExecutionSummary {
            table,
            output_table,
            warnings: compiled.warnings,
            plan_kind: transform::TransformPlanKind::Compiled,
        })
    }

    async fn compile_transform_spec(
        &self,
        spec: &transform::TransformSpec,
    ) -> Result<transform::compiler::CompiledTransformPlan> {
        transform::validation::validate_transform_spec_shape(spec)?;
        let input_descriptor = self.source_catalog.require(&spec.input.source_id)?;
        let input_table_descriptor = self
            .source_catalog
            .require_table(&spec.input.source_id, &spec.input.table_id)?;
        let (dataframe, headers, header_row_index, data_start_row_index, original_column_names) =
            self.build_transform_input_dataframe(&input_descriptor, &input_table_descriptor, spec)
                .await?;
        transform::compiler::compile_transform_dataframe(
            spec,
            dataframe,
            headers,
            header_row_index,
            data_start_row_index,
            original_column_names,
        )
    }

    async fn build_transform_input_dataframe(
        &self,
        source_descriptor: &source::SourceDescriptor,
        table_descriptor: &source::SourceTableDescriptor,
        spec: &transform::TransformSpec,
    ) -> Result<(
        datafusion::prelude::DataFrame,
        Vec<String>,
        Option<usize>,
        Option<usize>,
        Vec<String>,
    )> {
        match table_descriptor.kind {
            source::SourceTableKind::Derived => {
                let table_ref = source::SourceTableRef {
                    source_id: spec.input.source_id.clone(),
                    table_id: spec.input.table_id.clone(),
                };
                let registered = Box::pin(self.ensure_source_table_registered(&table_ref)).await?;
                let dataframe = self.session.table(registered.table_name.as_str()).await?;
                Ok((
                    dataframe,
                    registered.inferred_column_names.clone(),
                    registered.header_row_index,
                    registered.data_start_row_index,
                    registered.original_column_names.clone(),
                ))
            }
            _ => {
                if transform_has_layout_ops(spec) {
                    let table = read_source_table_rows_with_transform_layout(
                        source_descriptor,
                        table_descriptor,
                        spec,
                        None,
                    )?;
                    let headers = table.headers.clone();
                    let original_column_names = headers.clone();
                    let batch = source::source_rows_to_batch_with_provenance(&table)?;
                    let dataframe = self.session.read_batch(batch)?;
                    Ok((
                        dataframe,
                        headers,
                        Some(table.skipped_comment_rows),
                        Some(table.skipped_comment_rows + 1),
                        original_column_names,
                    ))
                } else {
                    let table_ref = source::SourceTableRef {
                        source_id: spec.input.source_id.clone(),
                        table_id: spec.input.table_id.clone(),
                    };
                    let registered = Box::pin(self.ensure_source_table_registered(&table_ref)).await?;
                    let dataframe = self.session.table(registered.table_name.as_str()).await?;
                    Ok((
                        dataframe,
                        registered.inferred_column_names.clone(),
                        registered.header_row_index,
                        registered.data_start_row_index,
                        registered.original_column_names.clone(),
                    ))
                }
            }
        }
    }

    pub(crate) async fn execute_transform_spec(
        &self,
        spec: &transform::TransformSpec,
        limit: Option<usize>,
        for_preview: bool,
        stack: &mut HashSet<String>,
    ) -> Result<ExecutedTransform> {
        transform::validation::validate_transform_spec_shape(spec)?;
        let input_descriptor = self.source_catalog.require(&spec.input.source_id)?;
        let input_table_descriptor = self
            .source_catalog
            .require_table(&spec.input.source_id, &spec.input.table_id)?;
        let input_table = self
            .load_transform_input_table(&input_descriptor, &input_table_descriptor, spec, limit, stack)
            .await?;
        let exec = execute_transform_pipeline(
            spec,
            input_table,
            &input_descriptor.id,
            input_table_descriptor.created_unix_seconds,
            input_table_descriptor.updated_unix_seconds,
            for_preview,
        )?;
        Ok(exec)
    }

    async fn load_transform_input_table(
        &self,
        source_descriptor: &source::SourceDescriptor,
        table_descriptor: &source::SourceTableDescriptor,
        spec: &transform::TransformSpec,
        limit: Option<usize>,
        stack: &mut HashSet<String>,
    ) -> Result<source::SourceTable> {
        let key = format!("{}:{}", spec.input.source_id, spec.input.table_id);
        if !stack.insert(key.clone()) {
            return Err(anyhow!("transform cycle detected at {}", key));
        }
        let result = match table_descriptor.kind {
            source::SourceTableKind::Derived => {
                let transform_id = table_descriptor
                    .metadata
                    .get(TRANSFORM_ID_KEY)
                    .cloned()
                    .ok_or_else(|| anyhow!("derived input table missing transform metadata"))?;
                let descriptor = self.transform_catalog.require(&transform_id)?;
                let exec = Box::pin(self.execute_transform_spec(&descriptor.spec, limit, true, stack)).await?;
                Ok(exec.table)
            }
            _ => {
                if transform_has_layout_ops(spec) {
                    read_source_table_rows_with_transform_layout(
                        source_descriptor,
                        table_descriptor,
                        spec,
                        limit,
                    )
                } else {
                    source::read_source_table_rows(source_descriptor, &spec.input, limit)
                }
            }
        };
        stack.remove(&key);
        result
    }
}

pub(crate) struct ExecutedTransform {
    pub(crate) table: source::SourceTable,
    pub(crate) output_table: source::SourceTableDescriptor,
    pub(crate) warnings: Vec<String>,
}

struct TransformExecutionSummary {
    table: source::SourceTable,
    output_table: source::SourceTableDescriptor,
    warnings: Vec<String>,
    plan_kind: transform::TransformPlanKind,
}

fn build_transform_output_descriptor(
    spec: &transform::TransformSpec,
    compiled: &transform::compiler::CompiledTransformPlan,
    table: &source::SourceTable,
) -> Result<source::SourceTableDescriptor> {
    let input_table_descriptor = source::infer_table_descriptor_from_table(
        &spec.input.source_id,
        &spec.output_table_id,
        spec.display_name.clone(),
        source::SourceTableKind::Derived,
        table,
        [
            (TRANSFORM_ID_KEY.to_string(), transform_id_for_spec(spec)),
            (INPUT_SOURCE_ID_KEY.to_string(), spec.input.source_id.clone()),
            (INPUT_TABLE_ID_KEY.to_string(), spec.input.table_id.clone()),
        ]
        .into_iter()
        .collect(),
        now_unix_seconds_local(),
        now_unix_seconds_local(),
        compiled.header_row_index,
        compiled.data_start_row_index,
        compiled.original_column_names.clone(),
        compiled.output_headers.clone(),
    );
    Ok(input_table_descriptor)
}

fn source_table_from_batches(
    batches: &[RecordBatch],
    source_id: &str,
    table_id: &str,
    headers: &[String],
) -> Result<source::SourceTable> {
    source::record_batches_to_source_table(batches, source_id, table_id, headers)
}

fn execute_transform_pipeline(
    spec: &transform::TransformSpec,
    mut table: source::SourceTable,
    source_id: &str,
    created_unix_seconds: u64,
    updated_unix_seconds: u64,
    _for_preview: bool,
) -> Result<ExecutedTransform> {
    let mut warnings = Vec::new();
    let mut current_headers = table.headers.clone();
    let original_column_names = current_headers.clone();
    let mut header_row_index = Some(table.skipped_comment_rows);
    let mut data_start_row_index = Some(table.skipped_comment_rows + 1);

    for op in &spec.operations {
        match op {
            transform::TransformOperation::ChooseHeaderRow { header_row_index: value } => {
                header_row_index = Some(*value);
            }
            transform::TransformOperation::SetDataStartRow { data_start_row_index: value } => {
                data_start_row_index = Some(*value);
            }
            transform::TransformOperation::RenameColumn { from, to } => {
                require_column(&current_headers, from)?;
                if current_headers.iter().any(|header| header == to && header != from) {
                    return Err(anyhow!("rename target `{to}` already exists"));
                }
                for row in &mut table.rows {
                    let value = row.values.remove(from).unwrap_or_default();
                    row.values.insert(to.clone(), value);
                }
                for header in &mut current_headers {
                    if header == from {
                        *header = to.clone();
                    }
                }
            }
            transform::TransformOperation::DropColumn { column } => {
                require_column(&current_headers, column)?;
                current_headers.retain(|header| header != column);
                for row in &mut table.rows {
                    row.values.remove(column);
                }
            }
            transform::TransformOperation::Trim { columns } => {
                for column in columns {
                    require_column(&current_headers, column)?;
                }
                for row in &mut table.rows {
                    for column in columns {
                        if let Some(value) = row.values.get_mut(column) {
                            *value = value.trim().to_string();
                        }
                    }
                }
            }
            transform::TransformOperation::SplitColumn(op) => {
                require_column(&current_headers, &op.column)?;
                for target in &op.into {
                    if current_headers.iter().any(|header| header == target) {
                        return Err(anyhow!("split target `{target}` already exists"));
                    }
                }
                let drop_original = matches!(op.behavior, transform::SplitColumnBehavior::DropOriginal);
                for row in &mut table.rows {
                    let value = row.values.get(&op.column).cloned().unwrap_or_default();
                    let parts = if op.delimiter.is_empty() {
                        vec![value]
                    } else {
                        value.split(&op.delimiter).map(|part| part.to_string()).collect::<Vec<_>>()
                    };
                    for (idx, target) in op.into.iter().enumerate() {
                        row.values
                            .insert(target.clone(), parts.get(idx).cloned().unwrap_or_default());
                    }
                    if drop_original {
                        row.values.remove(&op.column);
                    }
                }
                if drop_original {
                    current_headers.retain(|header| header != &op.column);
                }
                current_headers.extend(op.into.clone());
            }
            transform::TransformOperation::ExplodeColumn(op) => {
                require_column(&current_headers, &op.column)?;
                let mut next_rows = Vec::new();
                for row in &table.rows {
                    let value = row.values.get(&op.column).cloned().unwrap_or_default();
                    let mut parts = if op.delimiter.is_empty() {
                        vec![value]
                    } else {
                        value.split(&op.delimiter).map(|part| part.to_string()).collect::<Vec<_>>()
                    };
                    if op.trim_values {
                        for part in &mut parts {
                            *part = part.trim().to_string();
                        }
                    }
                    if op.drop_empty {
                        parts.retain(|part| !part.is_empty());
                    }
                    if parts.is_empty() {
                        if !op.drop_empty {
                            next_rows.push(row.clone());
                        }
                        continue;
                    }
                    for part in parts {
                        let mut new_row = row.clone();
                        new_row.values.insert(op.column.clone(), part);
                        next_rows.push(new_row);
                    }
                }
                table.rows = next_rows;
            }
            transform::TransformOperation::CoerceType {
                column,
                output_type,
                on_error,
            } => {
                require_column(&current_headers, column)?;
                for row in &mut table.rows {
                    let current = row.values.get(column).cloned().unwrap_or_default();
                    if current.is_empty() {
                        continue;
                    }
                    match coerce_value(&current, output_type) {
                        Ok(value) => {
                            row.values.insert(column.clone(), value);
                        }
                        Err(err) => match on_error {
                            transform::CoerceOnErrorPolicy::Strict => {
                                return Err(anyhow!("column `{column}` coercion failed: {err}"));
                            }
                            transform::CoerceOnErrorPolicy::NullOnError => {
                                row.values.insert(column.clone(), String::new());
                                warnings.push(format!("column `{column}` coercion failed for `{current}`"));
                            }
                            transform::CoerceOnErrorPolicy::KeepOriginalText => {
                                warnings.push(format!("column `{column}` coercion failed for `{current}`"));
                            }
                        },
                    }
                }
            }
            transform::TransformOperation::FilterRows(op) => {
                table
                    .rows
                    .retain(|row| eval_predicate(&op.predicate, row));
            }
            transform::TransformOperation::DeriveColumn(op) => {
                validate_expr_columns(&op.expr, &current_headers)?;
                if !current_headers.iter().any(|header| header == &op.column) {
                    current_headers.push(op.column.clone());
                }
                for row in &mut table.rows {
                    let value = eval_expr(&op.expr, row);
                    row.values.insert(op.column.clone(), value);
                }
            }
        }
        ensure_unique_columns(&current_headers)?;
        for row in &mut table.rows {
            retain_only_headers(row, &current_headers);
        }
    }

    table.headers = current_headers.clone();
    let mut metadata = BTreeMap::new();
    metadata.insert(TRANSFORM_ID_KEY.to_string(), transform_id_for_spec(spec));
    metadata.insert(INPUT_SOURCE_ID_KEY.to_string(), spec.input.source_id.clone());
    metadata.insert(INPUT_TABLE_ID_KEY.to_string(), spec.input.table_id.clone());
    let output_table = source::infer_table_descriptor_from_table(
        source_id,
        &spec.output_table_id,
        spec.display_name.clone(),
        source::SourceTableKind::Derived,
        &table,
        metadata.into_iter().collect(),
        created_unix_seconds,
        updated_unix_seconds,
        header_row_index,
        data_start_row_index,
        original_column_names.clone(),
        current_headers.clone(),
    );
    Ok(ExecutedTransform {
        table,
        output_table,
        warnings,
    })
}

fn transform_has_layout_ops(spec: &transform::TransformSpec) -> bool {
    spec.operations.iter().any(|op| {
        matches!(
            op,
            transform::TransformOperation::ChooseHeaderRow { .. }
                | transform::TransformOperation::SetDataStartRow { .. }
        )
    })
}

fn transform_id_for_spec(spec: &transform::TransformSpec) -> String {
    format!(
        "transform_{}_{}",
        sanitize_id_component(&spec.input.source_id),
        sanitize_id_component(&spec.output_table_id)
    )
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

fn require_column(headers: &[String], column: &str) -> Result<()> {
    if headers.iter().any(|header| header == column) {
        Ok(())
    } else {
        Err(anyhow!("column `{column}` not found"))
    }
}

fn ensure_unique_columns(headers: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
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

fn retain_only_headers(row: &mut source::SourceRow, headers: &[String]) {
    let keep = headers.iter().cloned().collect::<HashSet<_>>();
    row.values.retain(|key, _| keep.contains(key));
    for header in headers {
        row.values.entry(header.clone()).or_default();
    }
}

fn coerce_value(value: &str, output_type: &mapping::OutputType) -> Result<String> {
    match output_type {
        mapping::OutputType::String => Ok(value.to_string()),
        mapping::OutputType::Integer => Ok(value.parse::<i64>()?.to_string()),
        mapping::OutputType::Float => Ok(value.parse::<f64>()?.to_string()),
        mapping::OutputType::Boolean => {
            let lower = value.to_ascii_lowercase();
            match lower.as_str() {
                "true" | "1" | "yes" => Ok("true".to_string()),
                "false" | "0" | "no" => Ok("false".to_string()),
                _ => Err(anyhow!("`{value}` is not a boolean")),
            }
        }
    }
}

fn validate_expr_columns(expr: &mapping::Expr, headers: &[String]) -> Result<()> {
    match expr {
        mapping::Expr::Column { column } => require_column(headers, column),
        mapping::Expr::Concat { concat } | mapping::Expr::Coalesce { coalesce: concat } => {
            for expr in concat {
                validate_expr_columns(expr, headers)?;
            }
            Ok(())
        }
        mapping::Expr::Lower { lower }
        | mapping::Expr::Upper { upper: lower }
        | mapping::Expr::Trim { trim: lower }
        | mapping::Expr::Sha256 { sha256: lower } => validate_expr_columns(lower, headers),
        mapping::Expr::Replace { replace } => {
            validate_expr_columns(&replace.value, headers)
        }
        mapping::Expr::NullIf { null_if } => {
            for expr in null_if {
                validate_expr_columns(expr, headers)?;
            }
            Ok(())
        }
        mapping::Expr::Literal { .. } | mapping::Expr::RowNumber { .. } | mapping::Expr::Text(_) => {
            Ok(())
        }
    }
}

fn eval_expr(expr: &mapping::Expr, row: &source::SourceRow) -> String {
    match expr {
        mapping::Expr::Column { column } => row.values.get(column).cloned().unwrap_or_default(),
        mapping::Expr::Literal { literal } => literal.clone(),
        mapping::Expr::Concat { concat } => concat.iter().map(|expr| eval_expr(expr, row)).collect(),
        mapping::Expr::Coalesce { coalesce } => coalesce
            .iter()
            .map(|expr| eval_expr(expr, row))
            .find(|value| !value.is_empty())
            .unwrap_or_default(),
        mapping::Expr::RowNumber { row_number } => {
            if *row_number {
                row.line_number.to_string()
            } else {
                String::new()
            }
        }
        mapping::Expr::Lower { lower } => eval_expr(lower, row).to_ascii_lowercase(),
        mapping::Expr::Upper { upper } => eval_expr(upper, row).to_ascii_uppercase(),
        mapping::Expr::Trim { trim } => eval_expr(trim, row).trim().to_string(),
        mapping::Expr::Replace { replace } => eval_expr(&replace.value, row)
            .replace(&replace.from, &replace.to),
        mapping::Expr::NullIf { null_if } => {
            if null_if.len() == 2 {
                let left = eval_expr(&null_if[0], row);
                let right = eval_expr(&null_if[1], row);
                if left == right { String::new() } else { left }
            } else {
                String::new()
            }
        }
        mapping::Expr::Sha256 { sha256 } => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(eval_expr(sha256, row));
            format!("{:x}", hasher.finalize())
        }
        mapping::Expr::Text(value) => value.clone(),
    }
}

fn eval_predicate(predicate: &mapping::Predicate, row: &source::SourceRow) -> bool {
    match predicate {
        mapping::Predicate::Eq { eq } => eval_expr(&eq.left, row) == eval_expr(&eq.right, row),
        mapping::Predicate::Neq { neq } => eval_expr(&neq.left, row) != eval_expr(&neq.right, row),
        mapping::Predicate::IsEmpty { is_empty } => eval_expr(is_empty, row).is_empty(),
        mapping::Predicate::IsNotEmpty { is_not_empty } => !eval_expr(is_not_empty, row).is_empty(),
        mapping::Predicate::And { and } => and.iter().all(|predicate| eval_predicate(predicate, row)),
        mapping::Predicate::Or { or } => or.iter().any(|predicate| eval_predicate(predicate, row)),
        mapping::Predicate::Not { not } => !eval_predicate(not, row),
    }
}

fn table_to_batch_with_transform_provenance(
    table: &source::SourceTable,
    transform_id: &str,
) -> Result<RecordBatch> {
    let mut fields = table
        .headers
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect::<Vec<_>>();
    fields.push(Field::new("__source_row", DataType::Utf8, false));
    fields.push(Field::new("__source_table", DataType::Utf8, false));
    fields.push(Field::new("__source_id", DataType::Utf8, false));
    fields.push(Field::new("__transform_id", DataType::Utf8, false));
    let schema = Arc::new(Schema::new(fields));
    let arrays = schema
        .fields()
        .iter()
        .map(|field| {
            Arc::new(StringArray::from(
                table
                    .rows
                    .iter()
                    .map(|row| match field.name().as_str() {
                        "__source_row" => row.line_number.to_string(),
                        "__source_table" => table.table_id.clone(),
                        "__source_id" => table.source_id.clone(),
                        "__transform_id" => transform_id.to_string(),
                        name => row.values.get(name).cloned().unwrap_or_default(),
                    })
                    .collect::<Vec<_>>(),
            )) as arrow::array::ArrayRef
        })
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn read_source_table_rows_with_transform_layout(
    descriptor: &source::SourceDescriptor,
    table: &source::SourceTableDescriptor,
    spec: &transform::TransformSpec,
    limit: Option<usize>,
) -> Result<source::SourceTable> {
    let mut header_row_index = table.header_row_index.unwrap_or(0);
    let mut data_start_row_index = table.data_start_row_index.unwrap_or(header_row_index + 1);
    for op in &spec.operations {
        match op {
            transform::TransformOperation::ChooseHeaderRow { header_row_index: value } => {
                header_row_index = *value;
            }
            transform::TransformOperation::SetDataStartRow { data_start_row_index: value } => {
                data_start_row_index = *value;
            }
            _ => break,
        }
    }
    let raw_rows = match descriptor.format {
        source::SourceFormat::Csv | source::SourceFormat::Tsv | source::SourceFormat::CbioTsv => {
            read_delimited_raw_rows(descriptor)?
        }
        source::SourceFormat::Xlsx => read_xlsx_raw_rows(descriptor, table)?,
        _ => {
            return Err(anyhow!(
                "layout operations are only supported for raw delimited and xlsx tables in v1"
            ))
        }
    };
    build_table_from_raw_rows(
        &descriptor.id,
        &table.id,
        raw_rows,
        header_row_index,
        data_start_row_index,
        limit,
    )
}

fn read_delimited_raw_rows(descriptor: &source::SourceDescriptor) -> Result<Vec<(usize, Vec<String>)>> {
    let source::SourceLocation::Local { path } = &descriptor.location;
    let delimiter = descriptor
        .read_options
        .delimiter
        .unwrap_or(match descriptor.format {
            source::SourceFormat::Csv => ',',
            _ => '\t',
        });
    let comment_prefix = descriptor
        .read_options
        .comment_prefix
        .clone()
        .unwrap_or_else(|| {
            if descriptor.format == source::SourceFormat::CbioTsv {
                "#".to_string()
            } else {
                String::new()
            }
        });
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        if !comment_prefix.is_empty() && line.starts_with(&comment_prefix) {
            continue;
        }
        rows.push((idx + 1, line.split(delimiter).map(|part| part.trim().to_string()).collect()));
    }
    Ok(rows)
}

fn read_xlsx_raw_rows(
    descriptor: &source::SourceDescriptor,
    table: &source::SourceTableDescriptor,
) -> Result<Vec<(usize, Vec<String>)>> {
    use calamine::{open_workbook, Reader, Xlsx};
    let source::SourceLocation::Local { path } = &descriptor.location;
    let sheet_name = table
        .metadata
        .get("sheet_name")
        .cloned()
        .unwrap_or_else(|| table.display_name.clone().unwrap_or_else(|| table.id.clone()));
    let mut workbook: Xlsx<_> = open_workbook(path)?;
    let range = workbook.worksheet_range(&sheet_name)?;
    Ok(range
        .rows()
        .enumerate()
        .map(|(idx, row)| (idx + 1, row.iter().map(|cell| cell.to_string()).collect()))
        .collect())
}

fn build_table_from_raw_rows(
    source_id: &str,
    table_id: &str,
    raw_rows: Vec<(usize, Vec<String>)>,
    header_row_index: usize,
    data_start_row_index: usize,
    limit: Option<usize>,
) -> Result<source::SourceTable> {
    let header = raw_rows
        .get(header_row_index)
        .ok_or_else(|| anyhow!("header_row_index {} out of range", header_row_index))?
        .1
        .clone();
    let headers = normalize_headers(&header);
    let mut rows = Vec::new();
    for (physical_idx, values) in raw_rows.into_iter().skip(data_start_row_index) {
        let mut map = HashMap::new();
        for (idx, header) in headers.iter().enumerate() {
            map.insert(header.clone(), values.get(idx).cloned().unwrap_or_default());
        }
        rows.push(source::SourceRow {
            line_number: physical_idx,
            values: map,
        });
        if let Some(limit) = limit {
            if rows.len() >= limit {
                break;
            }
        }
    }
    Ok(source::SourceTable {
        source_id: source_id.to_string(),
        table_id: table_id.to_string(),
        headers,
        skipped_comment_rows: header_row_index,
        rows,
    })
}

fn normalize_headers(values: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let mut normalized = value
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
            if normalized.is_empty() {
                normalized = format!("column_{}", idx + 1);
            }
            let base = normalized.clone();
            let mut suffix = 2usize;
            while !seen.insert(normalized.clone()) {
                normalized = format!("{}_{}", base, suffix);
                suffix += 1;
            }
            normalized
        })
        .collect()
}

fn now_unix_seconds_local() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

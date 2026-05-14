use super::model::{
    ColumnType, SourceColumn, SourceDescriptor, SourceFormat, SourceLocation, SourceRegistration,
    SourceRow, SourceSchema, SourceStats, SourceTable, SourceTableDescriptor, SourceTableKind,
    SourceTableQualitySummary, SourceTableRef,
};
use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use calamine::{open_workbook, Data, Reader, SheetVisible, Xlsx};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

struct TableInference {
    schema: SourceSchema,
    row_count: usize,
    header_row_index: Option<usize>,
    data_start_row_index: Option<usize>,
    original_column_names: Vec<String>,
    inferred_column_names: Vec<String>,
    quality: SourceTableQualitySummary,
}

struct XlsxPreview {
    table: SourceTable,
    header_row_index: usize,
    data_start_row_index: usize,
    original_column_names: Vec<String>,
    inferred_column_names: Vec<String>,
}

pub fn infer_and_read_source(
    registration: SourceRegistration,
) -> Result<(SourceDescriptor, Vec<SourceTable>)> {
    let path = local_path(&registration.location).to_string();
    validate_source_id(&registration.id)?;
    if !Path::new(&path).exists() {
        return Err(anyhow!("source path does not exist: {path}"));
    }
    let metadata = fs::metadata(&path)?;
    let now = unix_now();
    let (mut descriptor, tables) = match registration.format {
        SourceFormat::Csv | SourceFormat::Tsv | SourceFormat::CbioTsv => {
            infer_delimited_source(registration.clone(), now)?
        }
        SourceFormat::CbioCaseList => infer_cbio_case_list_source(registration.clone(), now)?,
        SourceFormat::Parquet => infer_parquet_source(registration.clone(), now)?,
        SourceFormat::Xlsx => infer_xlsx_source(registration.clone(), now)?,
    };
    descriptor.stats = SourceStats {
        row_count: Some(descriptor.tables.iter().filter_map(|table| table.stats.row_count).sum()),
        file_size_bytes: Some(metadata.len()),
        modified_unix_seconds: metadata.modified().ok().and_then(unix_time),
        fingerprint: Some(fingerprint(&path, metadata.len(), metadata.modified().ok())),
    };
    if descriptor.table_name.is_none() {
        if let Some(table) = descriptor.default_table() {
            descriptor.table_name = Some(table.table_name.clone());
        }
    }
    if descriptor.schema.is_none() {
        descriptor.schema = descriptor.default_table().map(|table| table.schema.clone());
    }
    if descriptor.registered.is_none() {
        descriptor.registered = Some(false);
    }
    Ok((descriptor, tables))
}

pub fn read_source_rows(descriptor: &SourceDescriptor, limit: Option<usize>) -> Result<SourceTable> {
    let table = descriptor.require_single_table()?.clone();
    read_source_table_rows(
        descriptor,
        &SourceTableRef {
            source_id: descriptor.id.clone(),
            table_id: table.id,
        },
        limit,
    )
}

pub fn read_source_table_rows(
    descriptor: &SourceDescriptor,
    table_ref: &SourceTableRef,
    limit: Option<usize>,
) -> Result<SourceTable> {
    let table = descriptor
        .tables
        .iter()
        .find(|table| table.id == table_ref.table_id)
        .ok_or_else(|| {
            anyhow!(
                "source `{}` does not contain table `{}`",
                descriptor.id,
                table_ref.table_id
            )
        })?
        .clone();
    let mut table = match descriptor.format {
        SourceFormat::Csv | SourceFormat::Tsv | SourceFormat::CbioTsv => {
            read_delimited_source_from_descriptor(descriptor, &table)?
        }
        SourceFormat::CbioCaseList => read_cbio_case_list_source_rows(descriptor, &table)?,
        SourceFormat::Parquet => read_parquet_sample_source(&descriptor.id, &table)?,
        SourceFormat::Xlsx => read_xlsx_sheet_rows(descriptor, &table)?,
    };
    if let Some(limit) = limit {
        table.rows.truncate(limit);
    }
    Ok(table)
}

pub fn source_rows_to_batch(table: &SourceTable) -> Result<RecordBatch> {
    source_rows_to_batch_with_schema(table, source_table_schema(table))
}

pub fn source_rows_to_batch_with_provenance(table: &SourceTable) -> Result<RecordBatch> {
    let mut fields = source_table_schema(table).fields().to_vec();
    fields.push(Arc::new(Field::new("__source_row", DataType::Utf8, false)));
    fields.push(Arc::new(Field::new("__source_table", DataType::Utf8, false)));
    fields.push(Arc::new(Field::new("__source_id", DataType::Utf8, false)));
    source_rows_to_batch_with_schema(table, Arc::new(Schema::new(fields)))
}

pub fn source_rows_to_batch_with_schema(table: &SourceTable, schema: SchemaRef) -> Result<RecordBatch> {
    let columns = schema
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
                        name => row.values.get(name).cloned().unwrap_or_default(),
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn source_table_schema(table: &SourceTable) -> SchemaRef {
    Arc::new(Schema::new(
        table
            .headers
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ))
}

pub fn record_batches_to_source_table(
    batches: &[RecordBatch],
    source_id: &str,
    table_id: &str,
    headers: &[String],
) -> Result<SourceTable> {
    let json_rows = crate::json_rows::batches_to_json_rows(batches)?;
    let mut rows = Vec::with_capacity(json_rows.len());
    for row in json_rows {
        let value: Value = serde_json::from_str(&row)?;
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("expected object row in transform preview"))?;
        let mut values = HashMap::new();
        for header in headers {
            values.insert(
                header.clone(),
                json_value_to_string(object.get(header).unwrap_or(&Value::Null)),
            );
        }
        let line_number = object
            .get("__source_row")
            .map(json_value_to_string)
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or_default();
        rows.push(SourceRow { line_number, values });
    }
    Ok(SourceTable {
        source_id: source_id.to_string(),
        table_id: table_id.to_string(),
        headers: headers.to_vec(),
        skipped_comment_rows: 0,
        rows,
    })
}

fn json_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}

pub fn source_table_name(source_id: &str, table_id: &str) -> String {
    if table_id == "primary" {
        format!("source_{}", sanitize_identifier(source_id))
    } else {
        format!(
            "source_{}_{}",
            sanitize_identifier(source_id),
            sanitize_identifier(table_id)
        )
    }
}

pub fn infer_table_descriptor_from_table(
    source_id: &str,
    table_id: &str,
    display_name: Option<String>,
    kind: SourceTableKind,
    table: &SourceTable,
    metadata: HashMap<String, String>,
    created_unix_seconds: u64,
    updated_unix_seconds: u64,
    header_row_index: Option<usize>,
    data_start_row_index: Option<usize>,
    original_column_names: Vec<String>,
    inferred_column_names: Vec<String>,
) -> SourceTableDescriptor {
    let inference = match (header_row_index, data_start_row_index) {
        (Some(header_row_index), Some(data_start_row_index)) => infer_table_with_header(
            table,
            header_row_index,
            data_start_row_index,
            original_column_names,
            inferred_column_names,
        ),
        _ => infer_table(table),
    };
    SourceTableDescriptor {
        id: table_id.to_string(),
        display_name,
        table_name: source_table_name(source_id, table_id),
        schema: inference.schema,
        stats: SourceStats {
            row_count: Some(inference.row_count),
            ..Default::default()
        },
        created_unix_seconds,
        updated_unix_seconds,
        registered: false,
        kind,
        header_row_index: inference.header_row_index,
        data_start_row_index: inference.data_start_row_index,
        original_column_names: inference.original_column_names,
        inferred_column_names: inference.inferred_column_names,
        quality: inference.quality,
        metadata,
    }
}

fn infer_delimited_source(
    registration: SourceRegistration,
    now: u64,
) -> Result<(SourceDescriptor, Vec<SourceTable>)> {
    let table_id = "primary".to_string();
    let table = read_delimited_source(&registration.id, &registration, &table_id)?;
    Ok((
        new_source_descriptor(
            &registration,
            now,
            vec![new_table_descriptor(
                &registration,
                &table_id,
                registration.display_name.clone(),
                SourceTableKind::RawFile,
                infer_table(&table),
                HashMap::new(),
                now,
            )],
        ),
        vec![table],
    ))
}

fn infer_cbio_case_list_source(
    registration: SourceRegistration,
    now: u64,
) -> Result<(SourceDescriptor, Vec<SourceTable>)> {
    let table_id = "primary".to_string();
    let table = read_cbio_case_list_source(&registration.id, local_path(&registration.location), &table_id)?;
    Ok((
        new_source_descriptor(
            &registration,
            now,
            vec![new_table_descriptor(
                &registration,
                &table_id,
                registration.display_name.clone(),
                SourceTableKind::RawFile,
                infer_table(&table),
                HashMap::new(),
                now,
            )],
        ),
        vec![table],
    ))
}

fn infer_parquet_source(
    registration: SourceRegistration,
    now: u64,
) -> Result<(SourceDescriptor, Vec<SourceTable>)> {
    let table_id = "primary".to_string();
    let mut metadata = HashMap::new();
    metadata.insert("path".to_string(), local_path(&registration.location).to_string());
    let descriptor_for_read = new_table_descriptor(
        &registration,
        &table_id,
        registration.display_name.clone(),
        SourceTableKind::RawFile,
        empty_inference(),
        metadata.clone(),
        now,
    );
    let table = read_parquet_sample_source(&registration.id, &descriptor_for_read)?;
    let inference = infer_table(&table);
    let descriptor = new_source_descriptor(
        &registration,
        now,
        vec![new_table_descriptor(
            &registration,
            &table_id,
            registration.display_name.clone(),
            SourceTableKind::RawFile,
            inference,
            metadata,
            now,
        )],
    );
    Ok((descriptor, Vec::new()))
}

fn infer_xlsx_source(
    registration: SourceRegistration,
    now: u64,
) -> Result<(SourceDescriptor, Vec<SourceTable>)> {
    let source_path = local_path(&registration.location);
    let workbook: Xlsx<_> = open_workbook(source_path)?;
    let sheet_metadata = workbook.sheets_metadata().to_vec();
    let visible = sheet_metadata
        .iter()
        .filter(|sheet| sheet.visible == SheetVisible::Visible)
        .collect::<Vec<_>>();
    let mut seen = BTreeSet::new();
    let mut tables = Vec::new();
    for sheet in visible {
        let mut metadata = HashMap::new();
        metadata.insert("sheet_name".to_string(), sheet.name.clone());
        metadata.insert("sheet_visibility".to_string(), "visible".to_string());
        let ordinal = sheet_metadata
            .iter()
            .position(|candidate| candidate.name == sheet.name)
            .unwrap_or_default()
            + 1;
        metadata.insert("sheet_ordinal".to_string(), ordinal.to_string());
        let table_id = unique_table_id(
            &format!("sheet_{ordinal:04}_{}", sanitize_identifier(&sheet.name)),
            &mut seen,
        );
        let preview = read_xlsx_sheet_preview(
            &registration.id,
            &table_id,
            source_path,
            &sheet.name,
            registration.read_options.has_header.unwrap_or(true),
        )?;
        let inference = infer_table_with_header(
            &preview.table,
            preview.header_row_index,
            preview.data_start_row_index,
            preview.original_column_names.clone(),
            preview.inferred_column_names.clone(),
        );
        tables.push(new_table_descriptor(
            &registration,
            &table_id,
            Some(sheet.name.clone()),
            SourceTableKind::RawSheet,
            inference,
            metadata,
            now,
        ));
    }
    let mut descriptor = new_source_descriptor(&registration, now, tables);
    descriptor
        .metadata
        .insert("sheet_count".to_string(), sheet_metadata.len().to_string());
    descriptor
        .metadata
        .insert("visible_sheet_count".to_string(), descriptor.tables.len().to_string());
    Ok((descriptor, Vec::new()))
}

fn new_source_descriptor(
    registration: &SourceRegistration,
    now: u64,
    tables: Vec<SourceTableDescriptor>,
) -> SourceDescriptor {
    let mut metadata = HashMap::new();
    let SourceLocation::Local { path } = &registration.location;
    metadata.insert("path".to_string(), path.clone());
    let table_name = tables.first().map(|table| table.table_name.clone());
    let schema = tables.first().map(|table| table.schema.clone());
    SourceDescriptor {
        id: registration.id.clone(),
        display_name: registration.display_name.clone(),
        format: registration.format.clone(),
        location: registration.location.clone(),
        read_options: registration.read_options.clone(),
        tables,
        metadata,
        stats: SourceStats::default(),
        created_unix_seconds: now,
        updated_unix_seconds: now,
        table_name,
        schema,
        registered: Some(false),
    }
}

fn new_table_descriptor(
    registration: &SourceRegistration,
    table_id: &str,
    display_name: Option<String>,
    kind: SourceTableKind,
    inference: TableInference,
    metadata: HashMap<String, String>,
    now: u64,
) -> SourceTableDescriptor {
    SourceTableDescriptor {
        id: table_id.to_string(),
        display_name,
        table_name: source_table_name(&registration.id, table_id),
        schema: inference.schema,
        stats: SourceStats {
            row_count: Some(inference.row_count),
            ..Default::default()
        },
        created_unix_seconds: now,
        updated_unix_seconds: now,
        registered: false,
        kind,
        header_row_index: inference.header_row_index,
        data_start_row_index: inference.data_start_row_index,
        original_column_names: inference.original_column_names,
        inferred_column_names: inference.inferred_column_names,
        quality: inference.quality,
        metadata,
    }
}

fn read_delimited_source(id: &str, registration: &SourceRegistration, table_id: &str) -> Result<SourceTable> {
    let table = SourceTableDescriptor {
        id: table_id.to_string(),
        display_name: registration.display_name.clone(),
        table_name: source_table_name(id, table_id),
        schema: SourceSchema::default(),
        stats: SourceStats::default(),
        created_unix_seconds: 0,
        updated_unix_seconds: 0,
        registered: false,
        kind: SourceTableKind::RawFile,
        header_row_index: None,
        data_start_row_index: None,
        original_column_names: Vec::new(),
        inferred_column_names: Vec::new(),
        quality: SourceTableQualitySummary::default(),
        metadata: HashMap::new(),
    };
    let descriptor = SourceDescriptor {
        id: id.to_string(),
        display_name: registration.display_name.clone(),
        format: registration.format.clone(),
        location: registration.location.clone(),
        read_options: registration.read_options.clone(),
        tables: vec![table.clone()],
        metadata: HashMap::new(),
        stats: SourceStats::default(),
        created_unix_seconds: 0,
        updated_unix_seconds: 0,
        table_name: Some(table.table_name.clone()),
        schema: Some(SourceSchema::default()),
        registered: Some(false),
    };
    read_delimited_source_from_descriptor(&descriptor, &table)
}

fn read_delimited_source_from_descriptor(
    descriptor: &SourceDescriptor,
    table: &SourceTableDescriptor,
) -> Result<SourceTable> {
    let path = local_path(&descriptor.location);
    let delimiter = descriptor
        .read_options
        .delimiter
        .unwrap_or_else(|| default_delimiter(&descriptor.format));
    let comment_prefix = descriptor
        .read_options
        .comment_prefix
        .clone()
        .unwrap_or_else(|| {
            if descriptor.format == SourceFormat::CbioTsv {
                "#".to_string()
            } else {
                String::new()
            }
        });
    let skip_comments = !comment_prefix.is_empty();
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut headers = Vec::new();
    let mut rows = Vec::new();
    let mut skipped_comment_rows = 0usize;

    for (zero_idx, line) in reader.lines().enumerate() {
        let line_number = zero_idx + 1;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if headers.is_empty() && skip_comments && line.starts_with(&comment_prefix) {
            skipped_comment_rows += 1;
            continue;
        }
        if headers.is_empty() {
            headers = split_delimited(&line, delimiter);
            continue;
        }
        rows.push(SourceRow {
            line_number,
            values: values_for_headers(&headers, &split_delimited(&line, delimiter)),
        });
    }
    if headers.is_empty() {
        return Err(anyhow!("{path} did not contain a header row"));
    }
    Ok(SourceTable {
        source_id: descriptor.id.clone(),
        table_id: table.id.clone(),
        headers,
        skipped_comment_rows,
        rows,
    })
}

fn read_cbio_case_list_source(id: &str, path: &str, table_id: &str) -> Result<SourceTable> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut fields = HashMap::<String, String>::new();
    for line in reader.lines() {
        let line = line?;
        if let Some((key, value)) = line.split_once(':') {
            fields.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    let stable_id = fields
        .get("stable_id")
        .cloned()
        .ok_or_else(|| anyhow!("case list {path} missing stable_id"))?;
    let name = fields
        .get("case_list_name")
        .cloned()
        .unwrap_or_else(|| stable_id.clone());
    let sample_ids = fields
        .get("case_list_ids")
        .map(|ids| {
            ids.split('\t')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let headers = vec![
        "stable_id".to_string(),
        "case_list_name".to_string(),
        "sample_id".to_string(),
    ];
    let rows = sample_ids
        .into_iter()
        .map(|sample_id| SourceRow {
            line_number: 1,
            values: HashMap::from([
                ("stable_id".to_string(), stable_id.clone()),
                ("case_list_name".to_string(), name.clone()),
                ("sample_id".to_string(), sample_id),
            ]),
        })
        .collect();
    Ok(SourceTable {
        source_id: id.to_string(),
        table_id: table_id.to_string(),
        headers,
        skipped_comment_rows: 0,
        rows,
    })
}

fn read_cbio_case_list_source_rows(
    descriptor: &SourceDescriptor,
    table: &SourceTableDescriptor,
) -> Result<SourceTable> {
    read_cbio_case_list_source(&descriptor.id, local_path(&descriptor.location), &table.id)
}

fn read_parquet_sample_source(id: &str, table: &SourceTableDescriptor) -> Result<SourceTable> {
    let path = table
        .metadata
        .get("path")
        .cloned()
        .ok_or_else(|| anyhow!("parquet source table `{}` missing path metadata", table.id))?;
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let headers = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    Ok(SourceTable {
        source_id: id.to_string(),
        table_id: table.id.clone(),
        headers,
        skipped_comment_rows: 0,
        rows: Vec::new(),
    })
}

fn read_xlsx_sheet_preview(
    source_id: &str,
    table_id: &str,
    path: &str,
    sheet_name: &str,
    has_header: bool,
) -> Result<XlsxPreview> {
    let mut workbook: Xlsx<_> = open_workbook(path)?;
    let range = workbook.worksheet_range(sheet_name)?;
    let rows = range.rows().collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(XlsxPreview {
            table: SourceTable {
                source_id: source_id.to_string(),
                table_id: table_id.to_string(),
                headers: Vec::new(),
                skipped_comment_rows: 0,
                rows: Vec::new(),
            },
            header_row_index: 0,
            data_start_row_index: 0,
            original_column_names: Vec::new(),
            inferred_column_names: Vec::new(),
        });
    }
    let header_idx = first_non_empty_row(&rows).unwrap_or(0);
    let original_headers = if has_header {
        cells_to_values(rows[header_idx])
    } else {
        synthetic_headers(rows[header_idx].len())
    };
    let headers = normalize_headers(&original_headers);
    let start_idx = if has_header { header_idx + 1 } else { header_idx };
    let body = rows
        .iter()
        .enumerate()
        .skip(start_idx)
        .filter(|(_, row)| row.iter().any(|cell| !cell_string(cell).trim().is_empty()))
        .map(|(idx, row)| SourceRow {
            line_number: idx + 1,
            values: values_for_headers(&headers, &cells_to_values(row)),
        })
        .collect::<Vec<_>>();
    Ok(XlsxPreview {
        table: SourceTable {
            source_id: source_id.to_string(),
            table_id: table_id.to_string(),
            headers: headers.clone(),
            skipped_comment_rows: header_idx,
            rows: body,
        },
        header_row_index: header_idx,
        data_start_row_index: start_idx,
        original_column_names: original_headers,
        inferred_column_names: headers,
    })
}

fn read_xlsx_sheet_rows(
    descriptor: &SourceDescriptor,
    table: &SourceTableDescriptor,
) -> Result<SourceTable> {
    let sheet_name = table
        .metadata
        .get("sheet_name")
        .cloned()
        .unwrap_or_else(|| table.display_name.clone().unwrap_or_else(|| table.id.clone()));
    Ok(read_xlsx_sheet_preview(
        &descriptor.id,
        &table.id,
        local_path(&descriptor.location),
        &sheet_name,
        descriptor.read_options.has_header.unwrap_or(true),
    )?
    .table)
}

fn infer_schema(table: &SourceTable) -> SourceSchema {
    let columns = table
        .headers
        .iter()
        .enumerate()
        .map(|(ordinal, name)| {
            let mut sample_values = Vec::new();
            let mut nullable = false;
            let mut seen_non_empty = Vec::new();
            for row in &table.rows {
                let value = row.values.get(name).cloned().unwrap_or_default();
                if value.is_empty() {
                    nullable = true;
                    continue;
                }
                if sample_values.len() < 5 && !sample_values.iter().any(|v| v == &value) {
                    sample_values.push(value.clone());
                }
                seen_non_empty.push(value);
            }
            SourceColumn {
                name: name.clone(),
                ordinal,
                inferred_type: infer_type(&seen_non_empty),
                nullable,
                sample_values,
            }
        })
        .collect::<Vec<_>>();
    SourceSchema {
        columns,
        skipped_comment_rows: table.skipped_comment_rows,
        metadata: HashMap::new(),
    }
}

fn infer_table(table: &SourceTable) -> TableInference {
    infer_table_with_header(table, 0, 1, table.headers.clone(), table.headers.clone())
}

fn infer_table_with_header(
    table: &SourceTable,
    header_row_index: usize,
    data_start_row_index: usize,
    original_column_names: Vec<String>,
    inferred_column_names: Vec<String>,
) -> TableInference {
    TableInference {
        schema: infer_schema(table),
        row_count: table.rows.len(),
        header_row_index: Some(header_row_index),
        data_start_row_index: Some(data_start_row_index),
        original_column_names,
        inferred_column_names,
        quality: infer_quality_summary(table),
    }
}

fn empty_inference() -> TableInference {
    TableInference {
        schema: SourceSchema::default(),
        row_count: 0,
        header_row_index: None,
        data_start_row_index: None,
        original_column_names: Vec::new(),
        inferred_column_names: Vec::new(),
        quality: SourceTableQualitySummary::default(),
    }
}

fn infer_type(values: &[String]) -> ColumnType {
    if values.is_empty() {
        return ColumnType::String;
    }
    if values
        .iter()
        .all(|v| matches!(v.as_str(), "true" | "false" | "TRUE" | "FALSE"))
    {
        return ColumnType::Boolean;
    }
    if values.iter().all(|v| v.parse::<i64>().is_ok()) {
        return ColumnType::Integer;
    }
    if values.iter().all(|v| v.parse::<f64>().is_ok()) {
        return ColumnType::Float;
    }
    ColumnType::String
}

fn local_path(location: &SourceLocation) -> &str {
    match location {
        SourceLocation::Local { path } => path,
    }
}

fn validate_source_id(id: &str) -> Result<()> {
    if sanitize_identifier(id).is_empty() {
        return Err(anyhow!("source id must contain at least one alphanumeric character"));
    }
    Ok(())
}

fn sanitize_identifier(value: &str) -> String {
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

fn default_delimiter(format: &SourceFormat) -> char {
    match format {
        SourceFormat::Csv => ',',
        _ => '\t',
    }
}

fn split_delimited(line: &str, delimiter: char) -> Vec<String> {
    line.split(delimiter).map(|item| item.trim().to_string()).collect()
}

fn values_for_headers(headers: &[String], values: &[String]) -> HashMap<String, String> {
    headers
        .iter()
        .enumerate()
        .map(|(idx, header)| {
            (
                header.clone(),
                values.get(idx).cloned().unwrap_or_default(),
            )
        })
        .collect()
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

fn unix_time(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs())
}

fn fingerprint(path: &str, len: u64, modified: Option<SystemTime>) -> String {
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    len.hash(&mut hasher);
    modified.and_then(unix_time).hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

fn unique_table_id(base: &str, seen: &mut BTreeSet<String>) -> String {
    let base = if base.is_empty() { "sheet".to_string() } else { base.to_string() };
    if seen.insert(base.clone()) {
        return base;
    }
    let mut idx = 2usize;
    loop {
        let candidate = format!("{base}_{idx}");
        if seen.insert(candidate.clone()) {
            return candidate;
        }
        idx += 1;
    }
}

fn first_non_empty_row(rows: &[&[Data]]) -> Option<usize> {
    rows.iter()
        .position(|row| row.iter().any(|cell| !cell_string(cell).trim().is_empty()))
}

fn normalize_headers(values: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let base = if value.trim().is_empty() {
                format!("column_{}", idx + 1)
            } else {
                value.trim().to_string()
            };
            unique_table_id(&sanitize_identifier(&base), &mut seen)
        })
        .collect()
}

fn synthetic_headers(len: usize) -> Vec<String> {
    (0..len).map(|idx| format!("column_{}", idx + 1)).collect()
}

fn cells_to_values(row: &[Data]) -> Vec<String> {
    row.iter().map(cell_string).collect()
}

fn cell_string(cell: &Data) -> String {
    match cell {
        Data::Empty => String::new(),
        other => other.to_string(),
    }
}

fn infer_quality_summary(table: &SourceTable) -> SourceTableQualitySummary {
    let empty_column_count = table
        .headers
        .iter()
        .filter(|header| {
            table
                .rows
                .iter()
                .all(|row| row.values.get(*header).map(|value| value.trim().is_empty()).unwrap_or(true))
        })
        .count();
    let mut duplicate_seen = HashMap::<String, usize>::new();
    for header in &table.headers {
        *duplicate_seen.entry(header.clone()).or_default() += 1;
    }
    let duplicate_header_count = duplicate_seen
        .values()
        .filter(|count| **count > 1)
        .map(|count| count - 1)
        .sum();
    let mut possible_id_columns = Vec::new();
    let mut type_conflict_count = 0usize;
    for header in &table.headers {
        let mut non_empty = Vec::new();
        for row in &table.rows {
            let value = row.values.get(header).cloned().unwrap_or_default();
            if !value.trim().is_empty() {
                non_empty.push(value);
            }
        }
        let distinct = non_empty.iter().collect::<BTreeSet<_>>().len();
        if !non_empty.is_empty() && distinct == non_empty.len() {
            possible_id_columns.push(header.clone());
        }
        let has_integer = non_empty.iter().any(|value| value.parse::<i64>().is_ok());
        let has_float = non_empty.iter().any(|value| value.parse::<f64>().is_ok());
        let has_text = non_empty
            .iter()
            .any(|value| value.parse::<f64>().is_err() && !matches!(value.as_str(), "true" | "false" | "TRUE" | "FALSE"));
        if ((has_integer || has_float) && has_text) || (has_integer && has_float) {
            type_conflict_count += 1;
        }
    }
    SourceTableQualitySummary {
        row_count_sampled: table.rows.len(),
        column_count: table.headers.len(),
        empty_column_count,
        duplicate_header_count,
        type_conflict_count,
        possible_id_columns,
    }
}

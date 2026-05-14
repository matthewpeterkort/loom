use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;
use loom_engine::schema as graph_schema;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use crate::{now_unix_seconds, sanitize_name, SchemaSummary};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArrowResourceShape {
    pub(crate) resource_type: String,
    pub(crate) fields: Vec<ArrowFieldShape>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArrowFieldShape {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) children: Vec<ArrowFieldShape>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PromotedType {
    Utf8,
    Int64,
    Float64,
    Bool,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PromotedColumn {
    pub(crate) resource_type: String,
    pub(crate) field_name: String,
    pub(crate) column_name: String,
    pub(crate) kind: PromotedType,
}

pub(crate) fn validate_schema_doc(schema: &Value) -> Result<(), anyhow::Error> {
    let obj = schema
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("schema payload must be a JSON object"))?;
    if !obj.contains_key("$schema") {
        return Err(anyhow::anyhow!("schema payload must include `$schema`"));
    }
    if !(obj.contains_key("$defs") || obj.contains_key("definitions")) {
        return Err(anyhow::anyhow!(
            "schema payload should include `$defs` (or `definitions`)"
        ));
    }
    let _ = jsonschema::validator_for(schema)
        .map_err(|e| anyhow::anyhow!("jsonschema compile error: {}", e))?;
    Ok(())
}

pub(crate) fn summarize_schema(
    name: &str,
    schema: &Value,
    compiled_resource_shapes: usize,
    compiled_graph_schema: &graph_schema::CompiledGraphSchema,
) -> SchemaSummary {
    let schema_dialect = schema
        .get("$schema")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let schema_id = schema
        .get("$id")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let defs = schema
        .get("$defs")
        .or_else(|| schema.get("definitions"))
        .and_then(Value::as_object);
    let defs_count = defs.map(|d| d.len()).unwrap_or(0);

    let mut resource_types = 0usize;
    let mut link_relations = 0usize;
    let mut has_hypermedia_links = false;

    if let Some(defs_map) = defs {
        for def in defs_map.values() {
            if def.get("properties").is_some() {
                resource_types += 1;
            }
            if let Some(links) = def.get("links").and_then(Value::as_array) {
                has_hypermedia_links = true;
                link_relations += links
                    .iter()
                    .filter(|entry| entry.get("rel").is_some())
                    .count();
            }
        }
    }

    SchemaSummary {
        name: name.to_string(),
        schema_dialect,
        schema_id,
        defs_count,
        resource_types,
        link_relations,
        has_hypermedia_links,
        compiled_resource_shapes,
        compiled_entity_types: compiled_graph_schema.descriptor.entity_type_count,
        compiled_properties: compiled_graph_schema.descriptor.property_count,
        compiled_links: compiled_graph_schema.descriptor.link_count,
        wildcard_links: compiled_graph_schema.descriptor.wildcard_link_count,
        created_unix_seconds: now_unix_seconds(),
    }
}

pub(crate) fn compile_arrow_shapes(
    schema: &Value,
) -> Result<Vec<ArrowResourceShape>, anyhow::Error> {
    let defs = schema
        .get("$defs")
        .or_else(|| schema.get("definitions"))
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("schema missing `$defs` object"))?;
    let mut out = Vec::new();
    for (name, def) in defs {
        if !is_object_schema(def) {
            continue;
        }
        let data_type = infer_arrow_type(def, defs, 0)?;
        if let DataType::Struct(fields) = data_type {
            let shape = ArrowResourceShape {
                resource_type: name.clone(),
                fields: fields.iter().map(|f| field_to_shape(f.as_ref())).collect(),
            };
            out.push(shape);
        }
    }
    out.sort_by(|a, b| a.resource_type.cmp(&b.resource_type));
    Ok(out)
}

fn is_object_schema(v: &Value) -> bool {
    matches!(v.get("type").and_then(Value::as_str), Some("object")) || v.get("properties").is_some()
}

fn infer_arrow_type(
    schema_node: &Value,
    defs: &serde_json::Map<String, Value>,
    depth: usize,
) -> Result<DataType, anyhow::Error> {
    if depth > 32 {
        return Err(anyhow::anyhow!("schema nesting too deep"));
    }
    if let Some(reference) = schema_node.get("$ref").and_then(Value::as_str) {
        let resolved = resolve_ref(reference, defs)
            .ok_or_else(|| anyhow::anyhow!("unresolved $ref: {}", reference))?;
        return infer_arrow_type(resolved, defs, depth + 1);
    }

    if let Some(one_of) = schema_node.get("oneOf").and_then(Value::as_array) {
        if let Some(chosen) = one_of.iter().find(|v| !is_null_type(v)) {
            return infer_arrow_type(chosen, defs, depth + 1);
        }
    }
    if let Some(any_of) = schema_node.get("anyOf").and_then(Value::as_array) {
        if let Some(chosen) = any_of.iter().find(|v| !is_null_type(v)) {
            return infer_arrow_type(chosen, defs, depth + 1);
        }
    }

    let types = normalized_types(schema_node);
    if types.iter().any(|t| t == "object") || schema_node.get("properties").is_some() {
        let required = schema_node
            .get("required")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect::<std::collections::HashSet<String>>()
            })
            .unwrap_or_default();
        let mut fields = Vec::<Field>::new();
        if let Some(props) = schema_node.get("properties").and_then(Value::as_object) {
            let mut names = props.keys().cloned().collect::<Vec<String>>();
            names.sort();
            for name in names {
                if let Some(node) = props.get(&name) {
                    let nullable = is_nullable_schema(node) || !required.contains(&name);
                    let data_type = infer_arrow_type(node, defs, depth + 1)?;
                    fields.push(Field::new(name, data_type, nullable));
                }
            }
        }
        return Ok(DataType::Struct(Fields::from(fields)));
    }

    if types.iter().any(|t| t == "array") {
        let item_node = schema_node.get("items").unwrap_or(&Value::Null);
        let item_type = if item_node.is_null() {
            DataType::Utf8
        } else {
            infer_arrow_type(item_node, defs, depth + 1)?
        };
        return Ok(DataType::List(Arc::new(Field::new(
            "item", item_type, true,
        ))));
    }

    if types.iter().any(|t| t == "integer") {
        return Ok(DataType::Int64);
    }
    if types.iter().any(|t| t == "number") {
        return Ok(DataType::Float64);
    }
    if types.iter().any(|t| t == "boolean") {
        return Ok(DataType::Boolean);
    }
    if types.iter().any(|t| t == "string") {
        return Ok(DataType::Utf8);
    }

    Ok(DataType::Utf8)
}

fn resolve_ref<'a>(reference: &str, defs: &'a serde_json::Map<String, Value>) -> Option<&'a Value> {
    if let Some(name) = reference.strip_prefix("#/$defs/") {
        return defs.get(name);
    }
    if let Some(name) = reference.strip_prefix("#/definitions/") {
        return defs.get(name);
    }
    let tail = reference.rsplit('/').next()?;
    defs.get(tail)
}

fn normalized_types(schema_node: &Value) -> Vec<String> {
    match schema_node.get("type") {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

fn is_null_type(node: &Value) -> bool {
    matches!(node.get("type").and_then(Value::as_str), Some("null"))
}

fn is_nullable_schema(schema_node: &Value) -> bool {
    normalized_types(schema_node).iter().any(|t| t == "null")
}

fn field_to_shape(field: &Field) -> ArrowFieldShape {
    let (data_type, children) = data_type_to_shape(field.data_type());
    ArrowFieldShape {
        name: field.name().to_string(),
        data_type,
        nullable: field.is_nullable(),
        children,
    }
}

fn data_type_to_shape(dt: &DataType) -> (String, Vec<ArrowFieldShape>) {
    match dt {
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|f| field_to_shape(f.as_ref()))
                .collect::<Vec<ArrowFieldShape>>();
            ("struct".to_string(), children)
        }
        DataType::List(item) => {
            let (child_dtype, child_children) = data_type_to_shape(item.data_type());
            let child = ArrowFieldShape {
                name: item.name().to_string(),
                data_type: child_dtype,
                nullable: item.is_nullable(),
                children: child_children,
            };
            ("list".to_string(), vec![child])
        }
        DataType::Int64 => ("int64".to_string(), Vec::new()),
        DataType::Float64 => ("float64".to_string(), Vec::new()),
        DataType::Boolean => ("bool".to_string(), Vec::new()),
        DataType::Utf8 => ("utf8".to_string(), Vec::new()),
        other => (format!("{other:?}").to_lowercase(), Vec::new()),
    }
}

pub(crate) fn promoted_columns_from_shapes(shapes: &[ArrowResourceShape]) -> Vec<PromotedColumn> {
    let mut out = Vec::<PromotedColumn>::new();
    for shape in shapes {
        collect_promoted_columns_for_shape(&shape.resource_type, &shape.fields, "", &mut out);
    }
    out.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    out.dedup_by(|a, b| a.column_name == b.column_name);
    out
}

fn collect_promoted_columns_for_shape(
    resource_type: &str,
    fields: &[ArrowFieldShape],
    prefix: &str,
    out: &mut Vec<PromotedColumn>,
) {
    for field in fields {
        let path = if prefix.is_empty() {
            field.name.clone()
        } else {
            format!("{prefix}.{}", field.name)
        };
        if let Some(kind) = shape_data_type_to_promoted(&field.data_type) {
            if field.name == "id" || field.name == "resourceType" {
                continue;
            }
            out.push(PromotedColumn {
                resource_type: resource_type.to_string(),
                field_name: path.clone(),
                column_name: format!(
                    "t_{}_{}",
                    sanitize_name(resource_type),
                    sanitize_name(&path)
                ),
                kind,
            });
            if field.data_type != "struct" {
                continue;
            }
        }
        if field.data_type == "struct" && !field.children.is_empty() {
            collect_promoted_columns_for_shape(resource_type, &field.children, &path, out);
        }
    }
}

fn shape_data_type_to_promoted(dtype: &str) -> Option<PromotedType> {
    match dtype {
        "utf8" => Some(PromotedType::Utf8),
        "int64" => Some(PromotedType::Int64),
        "float64" => Some(PromotedType::Float64),
        "bool" => Some(PromotedType::Bool),
        "struct" | "list" => Some(PromotedType::Json),
        _ => None,
    }
}

pub(crate) fn promoted_type_to_sql(kind: &PromotedType) -> &'static str {
    match kind {
        PromotedType::Utf8 => "VARCHAR",
        PromotedType::Int64 => "BIGINT",
        PromotedType::Float64 => "DOUBLE",
        PromotedType::Bool => "BOOLEAN",
        PromotedType::Json => "VARCHAR",
    }
}

enum TypedColumnBuilder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Bool(BooleanBuilder),
    Json(StringBuilder),
}

impl TypedColumnBuilder {
    fn with_kind(kind: &PromotedType, capacity: usize) -> Self {
        match kind {
            PromotedType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 8)),
            PromotedType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            PromotedType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            PromotedType::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            PromotedType::Json => Self::Json(StringBuilder::with_capacity(capacity, capacity * 16)),
        }
    }

    fn append_from_json(&mut self, value: Option<&Value>) {
        match self {
            TypedColumnBuilder::Utf8(builder) => match value {
                Some(Value::String(s)) => builder.append_value(s),
                Some(Value::Number(n)) => builder.append_value(n.to_string()),
                Some(Value::Bool(b)) => builder.append_value(if *b { "true" } else { "false" }),
                _ => builder.append_null(),
            },
            TypedColumnBuilder::Int64(builder) => match value.and_then(Value::as_i64) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            },
            TypedColumnBuilder::Float64(builder) => match value.and_then(Value::as_f64) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            },
            TypedColumnBuilder::Bool(builder) => match value.and_then(Value::as_bool) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            },
            TypedColumnBuilder::Json(builder) => match value {
                Some(v) => {
                    if let Ok(s) = serde_json::to_string(v) {
                        builder.append_value(s);
                    } else {
                        builder.append_null();
                    }
                }
                None => builder.append_null(),
            },
        }
    }

    fn append_null(&mut self) {
        match self {
            TypedColumnBuilder::Utf8(builder) => builder.append_null(),
            TypedColumnBuilder::Int64(builder) => builder.append_null(),
            TypedColumnBuilder::Float64(builder) => builder.append_null(),
            TypedColumnBuilder::Bool(builder) => builder.append_null(),
            TypedColumnBuilder::Json(builder) => builder.append_null(),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            TypedColumnBuilder::Utf8(_) => DataType::Utf8,
            TypedColumnBuilder::Int64(_) => DataType::Int64,
            TypedColumnBuilder::Float64(_) => DataType::Float64,
            TypedColumnBuilder::Bool(_) => DataType::Boolean,
            TypedColumnBuilder::Json(_) => DataType::Utf8,
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            TypedColumnBuilder::Utf8(builder) => Arc::new(builder.finish()),
            TypedColumnBuilder::Int64(builder) => Arc::new(builder.finish()),
            TypedColumnBuilder::Float64(builder) => Arc::new(builder.finish()),
            TypedColumnBuilder::Bool(builder) => Arc::new(builder.finish()),
            TypedColumnBuilder::Json(builder) => Arc::new(builder.finish()),
        }
    }
}

pub(crate) fn build_typed_vertex_parquet(
    ndjson_dir: &FsPath,
    out_path: &FsPath,
    promoted_columns: &[PromotedColumn],
) -> Result<usize, anyhow::Error> {
    build_typed_vertex_parquet_limited(ndjson_dir, out_path, promoted_columns, None)
}

pub(crate) fn build_typed_vertex_parquet_limited(
    ndjson_dir: &FsPath,
    out_path: &FsPath,
    promoted_columns: &[PromotedColumn],
    max_rows: Option<usize>,
) -> Result<usize, anyhow::Error> {
    let mut files = std::fs::read_dir(ndjson_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("ndjson"))
        .collect::<Vec<PathBuf>>();
    files.sort();

    let mut id_builder = StringBuilder::new();
    let mut label_builder = StringBuilder::new();
    let mut payload_codec_builder = StringBuilder::new();
    let mut payload_bin_builder = BinaryBuilder::new();
    let mut payload_json_bin_builder = BinaryBuilder::new();
    let mut typed_builders = promoted_columns
        .iter()
        .map(|c| TypedColumnBuilder::with_kind(&c.kind, 0))
        .collect::<Vec<TypedColumnBuilder>>();

    let mut total = 0usize;
    for file in files {
        let label = file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("Unknown")
            .to_string();
        let rdr = BufReader::new(File::open(&file)?);
        for line in rdr.lines() {
            if let Some(limit) = max_rows {
                if total >= limit {
                    break;
                }
            }
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let parsed: Value = serde_json::from_str(&line)?;
            let id = vertex_id_for_record(&label, &parsed, total + 1);
            id_builder.append_value(&id);
            label_builder.append_value(&label);
            payload_codec_builder.append_value("flexbuf_v1");
            let payload = encode_payload_flexbuf(&parsed)?;
            payload_bin_builder.append_value(payload.as_slice());
            payload_json_bin_builder.append_value(line.as_bytes());

            for (idx, col) in promoted_columns.iter().enumerate() {
                if sanitize_name(&label) == sanitize_name(&col.resource_type) {
                    typed_builders[idx]
                        .append_from_json(get_value_at_dotted_path(&parsed, &col.field_name));
                } else {
                    typed_builders[idx].append_null();
                }
            }
            total += 1;
        }
        if let Some(limit) = max_rows {
            if total >= limit {
                break;
            }
        }
    }

    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("payload_codec", DataType::Utf8, false),
        Field::new("payload_bin", DataType::Binary, false),
        Field::new("payload_json_bin", DataType::Binary, false),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(id_builder.finish()),
        Arc::new(label_builder.finish()),
        Arc::new(payload_codec_builder.finish()),
        Arc::new(payload_bin_builder.finish()),
        Arc::new(payload_json_bin_builder.finish()),
    ];
    for (idx, col) in promoted_columns.iter().enumerate() {
        fields.push(Field::new(
            &col.column_name,
            typed_builders[idx].data_type(),
            true,
        ));
        arrays.push(typed_builders[idx].finish());
    }
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let file = File::create(out_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(total)
}

pub(crate) fn build_edges_parquet_from_ndjson(
    ndjson_dir: &FsPath,
    out_path: &FsPath,
) -> Result<usize, anyhow::Error> {
    build_edges_parquet_from_ndjson_limited(ndjson_dir, out_path, None)
}

pub(crate) fn build_edges_parquet_from_ndjson_limited(
    ndjson_dir: &FsPath,
    out_path: &FsPath,
    max_rows: Option<usize>,
) -> Result<usize, anyhow::Error> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("from_id", DataType::Utf8, false),
        Field::new("to_id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("props", DataType::Utf8, true),
    ]));
    let out = File::create(out_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(out, schema, Some(props))?;

    let mut files = std::fs::read_dir(ndjson_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("ndjson"))
        .collect::<Vec<PathBuf>>();
    files.sort();

    let mut edge_ids = Vec::<String>::new();
    let mut from_ids = Vec::<String>::new();
    let mut to_ids = Vec::<String>::new();
    let mut labels = Vec::<String>::new();
    let mut props_col = Vec::<String>::new();
    let mut total_edges = 0usize;
    let mut total_rows = 0usize;

    for file in files {
        let resource_label = file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("Unknown")
            .to_string();
        let rdr = BufReader::new(File::open(&file)?);
        let mut line_no = 0usize;
        for line in rdr.lines() {
            if let Some(limit) = max_rows {
                if total_rows >= limit {
                    break;
                }
            }
            let line = line?;
            line_no += 1;
            if line.trim().is_empty() {
                continue;
            }
            total_rows += 1;
            let parsed: Value = serde_json::from_str(&line)?;
            let from_id = vertex_id_for_record(&resource_label, &parsed, line_no);

            let mut refs = Vec::<(String, String)>::new();
            collect_reference_edges(&parsed, "", &mut refs);
            for (path, reference_raw) in refs {
                let Some(to_id) = canonical_reference(&reference_raw) else {
                    continue;
                };
                total_edges += 1;
                edge_ids.push(format!("eref-{total_edges}"));
                from_ids.push(from_id.clone());
                to_ids.push(to_id);
                labels.push(format!("ref:{}", path));
                props_col.push(format!(
                    "{{\"path\":\"{}\",\"reference\":\"{}\"}}",
                    json_escape_string(&path),
                    json_escape_string(&reference_raw)
                ));
            }

            if edge_ids.len() >= 20_000 {
                flush_edge_batch(
                    &mut writer,
                    &edge_ids,
                    &from_ids,
                    &to_ids,
                    &labels,
                    &props_col,
                )?;
                edge_ids.clear();
                from_ids.clear();
                to_ids.clear();
                labels.clear();
                props_col.clear();
            }
        }
        if let Some(limit) = max_rows {
            if total_rows >= limit {
                break;
            }
        }
    }

    if !edge_ids.is_empty() {
        flush_edge_batch(
            &mut writer,
            &edge_ids,
            &from_ids,
            &to_ids,
            &labels,
            &props_col,
        )?;
    }

    writer.close()?;
    Ok(total_edges)
}

fn flush_edge_batch(
    writer: &mut ArrowWriter<File>,
    edge_ids: &[String],
    from_ids: &[String],
    to_ids: &[String],
    labels: &[String],
    props_col: &[String],
) -> Result<(), anyhow::Error> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("from_id", DataType::Utf8, false),
        Field::new("to_id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("props", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(edge_ids.to_vec())),
            Arc::new(arrow::array::StringArray::from(from_ids.to_vec())),
            Arc::new(arrow::array::StringArray::from(to_ids.to_vec())),
            Arc::new(arrow::array::StringArray::from(labels.to_vec())),
            Arc::new(arrow::array::StringArray::from(props_col.to_vec())),
        ],
    )?;
    writer.write(&batch)?;
    Ok(())
}

fn vertex_id_for_record(label: &str, parsed: &Value, fallback_idx: usize) -> String {
    let raw_id = parsed.get("id").and_then(Value::as_str).unwrap_or("");
    if raw_id.is_empty() {
        return format!("{label}/auto-{fallback_idx}");
    }
    if raw_id.contains('/') {
        raw_id.to_string()
    } else {
        format!("{label}/{raw_id}")
    }
}

fn collect_reference_edges(value: &Value, path: &str, out: &mut Vec<(String, String)>) {
    match value {
        Value::Object(map) => {
            if let Some(reference) = map.get("reference").and_then(Value::as_str) {
                out.push((path.to_string(), reference.to_string()));
            }
            for (k, v) in map {
                let child = if path.is_empty() {
                    k.to_string()
                } else {
                    format!("{path}.{k}")
                };
                collect_reference_edges(v, &child, out);
            }
        }
        Value::Array(items) => {
            for (idx, item) in items.iter().enumerate() {
                let child = if path.is_empty() {
                    format!("[{idx}]")
                } else {
                    format!("{path}[{idx}]")
                };
                collect_reference_edges(item, &child, out);
            }
        }
        _ => {}
    }
}

fn canonical_reference(reference: &str) -> Option<String> {
    let trimmed = reference.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let no_fragment = trimmed.split('#').next().unwrap_or(trimmed);
    let no_query = no_fragment.split('?').next().unwrap_or(no_fragment);
    let parts = no_query
        .trim_matches('/')
        .split('/')
        .filter(|p| !p.is_empty())
        .collect::<Vec<&str>>();
    if parts.len() >= 2 {
        Some(format!(
            "{}/{}",
            parts[parts.len() - 2],
            parts[parts.len() - 1]
        ))
    } else {
        Some(no_query.to_string())
    }
}

pub(crate) fn get_value_at_dotted_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(root);
    }
    let mut cur = root;
    for segment in path.split('.') {
        if segment.is_empty() {
            return None;
        }
        match cur {
            Value::Object(map) => {
                cur = map.get(segment)?;
            }
            _ => return None,
        }
    }
    Some(cur)
}

fn encode_payload_flexbuf(value: &Value) -> Result<Vec<u8>, anyhow::Error> {
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    value.serialize(&mut serializer)?;
    Ok(serializer.take_buffer())
}

pub(crate) fn decode_payload_flexbuf(bytes: &[u8]) -> Option<Value> {
    let reader = flexbuffers::Reader::get_root(bytes).ok()?;
    Value::deserialize(reader).ok()
}

fn json_escape_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

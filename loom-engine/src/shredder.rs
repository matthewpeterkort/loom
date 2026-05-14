use anyhow::{anyhow, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, Int8Builder, ListArray, StringBuilder,
    StructArray, UnionArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, SchemaRef, UnionFields, UnionMode};
use arrow::record_batch::RecordBatch;
use sonic_rs::{JsonValueTrait, LazyValue};
use std::collections::HashMap;
use std::sync::Arc;

// ── Plan ──────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub enum PluckInstruction {
    Leaf {
        name: String,
        key: String,
        data_type: DataType,
    },
    Struct {
        name: String,
        key: String,
        fields: Vec<PluckInstruction>,
        /// Maps sub-object JSON key → index in `fields`
        field_key_idx: HashMap<String, usize>,
    },
    Union {
        name: String,
        key: String,
        variants: Vec<(String, PluckInstruction)>,
        type_ids: Vec<i8>,
    },
    List {
        name: String,
        key: String,
        element: Box<PluckInstruction>,
    },
}

impl PluckInstruction {
    pub fn name(&self) -> &str {
        match self {
            Self::Leaf { name, .. }
            | Self::Struct { name, .. }
            | Self::Union { name, .. }
            | Self::List { name, .. } => name,
        }
    }
    pub fn key(&self) -> &str {
        match self {
            Self::Leaf { key, .. }
            | Self::Struct { key, .. }
            | Self::Union { key, .. }
            | Self::List { key, .. } => key,
        }
    }

    pub fn to_field(&self) -> Field {
        match self {
            Self::Leaf {
                name, data_type, ..
            } => Field::new(name, data_type.clone(), true),
            Self::Struct { name, fields, .. } => {
                let sub: Vec<Field> = fields.iter().map(|f| f.to_field()).collect();
                Field::new(name, DataType::Struct(Fields::from(sub)), true)
            }
            Self::Union {
                name,
                variants,
                type_ids,
                ..
            } => {
                let uf: Vec<(i8, Arc<Field>)> = variants
                    .iter()
                    .enumerate()
                    .map(|(i, (_, instr))| {
                        (
                            type_ids[i],
                            Arc::new(Field::new(
                                instr.name(),
                                instr.to_field().data_type().clone(),
                                true,
                            )),
                        )
                    })
                    .collect();
                Field::new(
                    name,
                    DataType::Union(UnionFields::from_iter(uf), UnionMode::Sparse),
                    true,
                )
            }
            Self::List { name, element, .. } => {
                Field::new(name, DataType::List(Arc::new(element.to_field())), true)
            }
        }
    }
}

pub struct ShreddingPlan {
    pub instructions: Vec<PluckInstruction>,
    pub schema: SchemaRef,
    pub key_index: HashMap<String, usize>,
}

// ── Compiler ──────────────────────────────────────────────────────────────────

pub struct ShredderCompiler;

impl ShredderCompiler {
    pub fn compile(schema: SchemaRef) -> Result<ShreddingPlan> {
        let instructions: Vec<PluckInstruction> = schema
            .fields()
            .iter()
            .map(|f| Self::compile_field(f))
            .collect::<Result<_>>()?;

        let mut key_index: HashMap<String, usize> = HashMap::new();
        for (i, instr) in instructions.iter().enumerate() {
            if let PluckInstruction::Union { variants, .. } = instr {
                for (var_key, _) in variants {
                    key_index.insert(var_key.clone(), i);
                }
            } else {
                key_index.insert(instr.key().to_string(), i);
            }
        }

        // Regenerate the schema dynamically to apply the Utf8 fallback for Unions.
        let lowered_fields: Vec<Field> = instructions.iter().map(|i| i.to_field()).collect();
        let safe_schema = Arc::new(arrow::datatypes::Schema::new(lowered_fields));

        Ok(ShreddingPlan {
            instructions,
            schema: safe_schema,
            key_index,
        })
    }

    fn compile_field(field: &Field) -> Result<PluckInstruction> {
        let key = field.name().to_string();
        match field.data_type() {
            DataType::Struct(sub_fields) => {
                let fields: Vec<PluckInstruction> = sub_fields
                    .iter()
                    .map(|f| Self::compile_field(f))
                    .collect::<Result<_>>()?;
                let mut field_key_idx: HashMap<String, usize> = HashMap::new();
                for (i, f) in fields.iter().enumerate() {
                    if let PluckInstruction::Union { variants, .. } = f {
                        for (var_key, _) in variants {
                            field_key_idx.insert(var_key.clone(), i);
                        }
                    } else {
                        field_key_idx.insert(f.key().to_string(), i);
                    }
                }
                Ok(PluckInstruction::Struct {
                    name: key.clone(),
                    key,
                    fields,
                    field_key_idx,
                })
            }
            DataType::Union(uf, _) => {
                let mut variants = Vec::new();
                let mut type_ids = Vec::new();
                for (id, f) in uf.iter() {
                    variants.push((f.name().to_string(), Self::compile_field(f)?));
                    type_ids.push(id);
                }
                Ok(PluckInstruction::Union {
                    name: key.clone(),
                    key,
                    variants,
                    type_ids,
                })
            }
            DataType::List(inner) => Ok(PluckInstruction::List {
                name: key.clone(),
                key,
                element: Box::new(Self::compile_field(inner)?),
            }),
            dt => Ok(PluckInstruction::Leaf {
                name: key.clone(),
                key,
                data_type: dt.clone(),
            }),
        }
    }
}

// ── Column accumulator ────────────────────────────────────────────────────────
//
// One `ColAccum` per output column.  The hot path is:
//
//   1.  `shred_batch` runs ONE `to_object_iter` scan per row.
//   2.  For each key-value pair encountered it calls `ColAccum::push(val)`.
//   3.  For struct sub-fields a second `to_object_iter` scan runs on the
//       (typically small) nested value slice — still O(fragment bytes), not
//       O(full row bytes).
//   4.  Any column not hit by the scan gets `push_null()`.
//
// This means each byte of the JSON is read at most twice (once for the top
// level, once if the byte lives inside a Struct field), vs. the previous
// approach of N separate scans for N columns.

enum ColAccum {
    Str(StringBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    Bool(BooleanBuilder),
    Struct {
        field_key_idx: HashMap<String, usize>,
        sub_instrs: Vec<PluckInstruction>,
        sub: Vec<ColAccum>,
    },
    List {
        element_instr: Box<PluckInstruction>,
        values: Box<ColAccum>,
        offsets: Vec<i32>,
    },
    Union {
        type_ids_builder: Int8Builder,
        variant_ids: Vec<i8>,
        variants: Vec<ColAccum>,
        variant_key_idx: HashMap<String, usize>,
    },
    /// Fallback for unsupported types: stores raw JSON fragment.
    Raw(StringBuilder),
}

impl ColAccum {
    fn new(instr: &PluckInstruction, capacity: usize) -> Self {
        match instr {
            PluckInstruction::Leaf { data_type, .. } => match data_type {
                DataType::Utf8 => {
                    ColAccum::Str(StringBuilder::with_capacity(capacity, capacity * 24))
                }
                DataType::Int64 => ColAccum::I64(Int64Builder::with_capacity(capacity)),
                DataType::Float64 => ColAccum::F64(Float64Builder::with_capacity(capacity)),
                DataType::Boolean => ColAccum::Bool(BooleanBuilder::with_capacity(capacity)),
                _ => ColAccum::Raw(StringBuilder::with_capacity(capacity, 0)),
            },
            PluckInstruction::Struct {
                fields,
                field_key_idx,
                ..
            } => {
                let sub = fields.iter().map(|f| ColAccum::new(f, capacity)).collect();
                ColAccum::Struct {
                    field_key_idx: field_key_idx.clone(),
                    sub_instrs: fields.clone(),
                    sub,
                }
            }
            PluckInstruction::List { element, .. } => {
                let values = Box::new(ColAccum::new(element, capacity * 4));
                ColAccum::List {
                    element_instr: element.clone(),
                    values,
                    offsets: vec![0i32],
                }
            }
            PluckInstruction::Union {
                variants, type_ids, ..
            } => {
                let mut v_accums = Vec::new();
                let mut v_idx = HashMap::new();
                for (i, (var_key, instr)) in variants.iter().enumerate() {
                    v_accums.push(ColAccum::new(instr, capacity));
                    v_idx.insert(var_key.clone(), i);
                }
                ColAccum::Union {
                    type_ids_builder: Int8Builder::with_capacity(capacity),
                    variant_ids: type_ids.clone(),
                    variants: v_accums,
                    variant_key_idx: v_idx,
                }
            }
        }
    }

    /// Append a value from a `LazyValue` (a zero-copy slice of the original
    /// JSON record).  Optionally passes the matched JSON key to identify flatten-variants.
    fn push<'a>(&mut self, key_match: Option<&str>, val: LazyValue<'a>) {
        match self {
            ColAccum::Str(b) => {
                if val.is_null() {
                    b.append_null();
                } else {
                    b.append_option(val.as_str());
                }
            }
            ColAccum::I64(b) => {
                if val.is_null() {
                    b.append_null();
                } else {
                    b.append_option(val.as_i64());
                }
            }
            ColAccum::F64(b) => {
                if val.is_null() {
                    b.append_null();
                } else {
                    b.append_option(val.as_f64());
                }
            }
            ColAccum::Bool(b) => {
                if val.is_null() {
                    b.append_null();
                } else {
                    b.append_option(val.as_bool());
                }
            }

            // ── Struct: one inner scan over the sub-object fragment ───────
            ColAccum::Struct {
                field_key_idx,
                sub_instrs: _,
                sub,
            } => {
                if val.is_null() {
                    // Null the whole struct: recurse null into sub-columns.
                    sub.iter_mut().for_each(|s| s.push_null());
                    return;
                }
                let raw = val.as_raw_str();
                let ncols = sub.len();
                // Small inline seen-bitfield (u64 covers up to 64 sub-fields).
                let mut seen: u64 = 0;
                for item in sonic_rs::to_object_iter(raw) {
                    let (k, v) = match item {
                        Ok(kv) => kv,
                        Err(_) => continue,
                    };
                    if let Some(&ci) = field_key_idx.get(&*k) {
                        sub[ci].push(Some(&*k), v);
                        seen |= 1u64 << ci;
                    }
                }
                for ci in 0..ncols {
                    if seen & (1u64 << ci) == 0 {
                        sub[ci].push_null();
                    }
                }
            }

            // ── List: iterate array items into the values accumulator ─────
            ColAccum::List {
                values, offsets, ..
            } => {
                let start = *offsets.last().unwrap_or(&0);
                let mut count = 0i32;
                if !val.is_null() {
                    let raw = val.as_raw_str();
                    for item in sonic_rs::to_array_iter(raw) {
                        let v = match item {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        values.push(None, v);
                        count += 1;
                    }
                }
                offsets.push(start + count);
            }

            // ── Union: flat map over mapped object keys ───────────────────
            ColAccum::Union {
                type_ids_builder,
                variant_ids,
                variants,
                variant_key_idx,
            } => {
                if val.is_null() {
                    type_ids_builder.append_value(variant_ids.first().copied().unwrap_or(0));
                    variants.iter_mut().for_each(|v| v.push_null());
                } else {
                    let mut matched_vidx = None;
                    if let Some(k_str) = key_match {
                        if let Some(&idx) = variant_key_idx.get(k_str) {
                            matched_vidx = Some(idx);
                        }
                    }
                    if let Some(idx) = matched_vidx {
                        type_ids_builder.append_value(variant_ids[idx]);
                        for (i, v) in variants.iter_mut().enumerate() {
                            if i == idx {
                                v.push(None, val.clone());
                            } else {
                                v.push_null();
                            }
                        }
                    } else {
                        type_ids_builder.append_value(variant_ids.first().copied().unwrap_or(0));
                        variants.iter_mut().for_each(|v| v.push_null());
                    }
                }
            }

            // ── Raw fallback: store JSON fragment ─────────────────────────
            ColAccum::Raw(b) => {
                if val.is_null() {
                    b.append_null();
                } else {
                    b.append_value(val.as_raw_str());
                }
            }
        }
    }

    fn push_null(&mut self) {
        match self {
            ColAccum::Str(b) => b.append_null(),
            ColAccum::I64(b) => b.append_null(),
            ColAccum::F64(b) => b.append_null(),
            ColAccum::Bool(b) => b.append_null(),
            ColAccum::Struct { sub, .. } => sub.iter_mut().for_each(|s| s.push_null()),
            ColAccum::List { offsets, .. } => {
                let last = *offsets.last().unwrap_or(&0);
                offsets.push(last);
            }
            ColAccum::Union {
                type_ids_builder,
                variant_ids,
                variants,
                ..
            } => {
                type_ids_builder.append_value(variant_ids.first().copied().unwrap_or(0));
                variants.iter_mut().for_each(|v| v.push_null());
            }
            ColAccum::Raw(b) => b.append_null(),
        }
    }

    fn finish(self, instr: &PluckInstruction) -> Result<ArrayRef> {
        match self {
            ColAccum::Str(mut b) => Ok(Arc::new(b.finish()) as ArrayRef),
            ColAccum::I64(mut b) => Ok(Arc::new(b.finish()) as ArrayRef),
            ColAccum::F64(mut b) => Ok(Arc::new(b.finish()) as ArrayRef),
            ColAccum::Bool(mut b) => Ok(Arc::new(b.finish()) as ArrayRef),
            ColAccum::Raw(mut b) => Ok(Arc::new(b.finish()) as ArrayRef),

            ColAccum::Struct {
                sub_instrs, sub, ..
            } => {
                let arrow_fields: Vec<Field> = sub_instrs.iter().map(|f| f.to_field()).collect();
                let arrays: Vec<ArrayRef> = sub
                    .into_iter()
                    .zip(sub_instrs.iter())
                    .map(|(acc, i)| acc.finish(i))
                    .collect::<Result<_>>()?;
                Ok(Arc::new(StructArray::try_new(
                    Fields::from(arrow_fields),
                    arrays,
                    None,
                )?) as ArrayRef)
            }

            ColAccum::List {
                element_instr,
                values,
                offsets,
            } => {
                let values_arr = values.finish(&element_instr)?;
                let field = Arc::new(element_instr.to_field());
                Ok(Arc::new(ListArray::try_new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    values_arr,
                    None,
                )?) as ArrayRef)
            }

            ColAccum::Union {
                mut type_ids_builder,
                variants,
                ..
            } => {
                if let PluckInstruction::Union {
                    variants: instr_variants,
                    type_ids,
                    ..
                } = instr
                {
                    let mut arrays = Vec::new();
                    for (acc, (_, sub_instr)) in variants.into_iter().zip(instr_variants) {
                        arrays.push(acc.finish(sub_instr)?);
                    }
                    let union_fields = UnionFields::from_iter(
                        type_ids
                            .iter()
                            .zip(instr_variants.iter())
                            .map(|(&i, (_, sub_instr))| (i, Arc::new(sub_instr.to_field()))),
                    );
                    let type_ids_array = type_ids_builder.finish();
                    Ok(Arc::new(UnionArray::try_new(
                        union_fields,
                        type_ids_array.values().clone(),
                        None,
                        arrays,
                    )?) as ArrayRef)
                } else {
                    unreachable!()
                }
            }
        }
    }
}

// ── Public shredder ───────────────────────────────────────────────────────────

pub struct TypedShredder;

impl TypedShredder {
    /// Shred a batch of JSON records into an Arrow `RecordBatch`.
    ///
    /// **Algorithm**: One `to_object_iter` scan per row.  Each byte is read at
    /// most twice (top-level + one possible struct inner scan).  No heap
    /// allocation for the row itself; column builders accumulate values
    /// directly from borrowed `LazyValue` slices.
    pub fn shred_batch(plan: &ShreddingPlan, records: &[&[u8]]) -> Result<RecordBatch> {
        let n = records.len();
        let ncols = plan.instructions.len();

        // One accumulator per column, pre-allocated to the batch capacity.
        let mut accums: Vec<ColAccum> = plan
            .instructions
            .iter()
            .map(|instr| ColAccum::new(instr, n))
            .collect();

        for &record in records {
            // Bitfield: marks which columns received a value from this row.
            // u64 supports up to 64 top-level columns; extend to [u64; 2] if needed.
            let mut seen: u64 = 0;

            // ── Single pass over top-level key-value pairs ────────────────
            for item in sonic_rs::to_object_iter(record) {
                let (k, v) = match item {
                    Ok(kv) => kv,
                    Err(_) => continue,
                };
                if let Some(&col_idx) = plan.key_index.get(&*k) {
                    accums[col_idx].push(Some(&*k), v);
                    seen |= 1u64 << col_idx;
                }
            }

            // ── Null-fill any column not present in this row ──────────────
            for ci in 0..ncols {
                if seen & (1u64 << ci) == 0 {
                    accums[ci].push_null();
                }
            }
        }

        // Finish builders → ArrayRef columns.
        let columns: Vec<ArrayRef> = accums
            .into_iter()
            .zip(plan.instructions.iter())
            .map(|(acc, instr)| acc.finish(instr))
            .collect::<Result<_>>()?;

        let rb = RecordBatch::try_new(plan.schema.clone(), columns).map_err(|e| anyhow!(e))?;
        eprintln!(
            "Materialized batch of {} rows × {} cols",
            rb.num_rows(),
            rb.num_columns()
        );
        Ok(rb)
    }
}

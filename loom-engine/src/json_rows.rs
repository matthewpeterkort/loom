use anyhow::{anyhow, Result};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, ListArray, StringArray, StringViewArray, StructArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

pub fn batches_to_json_rows(batches: &[RecordBatch]) -> Result<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(batch_to_json_rows(batch)?);
    }
    Ok(rows)
}

fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<String>> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut out = String::with_capacity(128);
        out.push('{');
        for (col_idx, field) in schema.fields().iter().enumerate() {
            if col_idx > 0 {
                out.push(',');
            }
            push_json_string(&mut out, field.name());
            out.push(':');
            write_array_value(batch.column(col_idx).as_ref(), row_idx, &mut out)?;
        }
        out.push('}');
        rows.push(out);
    }

    Ok(rows)
}

fn write_array_value(array: &dyn Array, row_idx: usize, out: &mut String) -> Result<()> {
    if array.is_null(row_idx) {
        out.push_str("null");
        return Ok(());
    }

    match array.data_type() {
        DataType::Boolean => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected BooleanArray"))?;
            out.push_str(if array.value(row_idx) {
                "true"
            } else {
                "false"
            });
        }
        DataType::Int32 => {
            let array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow!("expected Int32Array"))?;
            out.push_str(&array.value(row_idx).to_string());
        }
        DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64Array"))?;
            out.push_str(&array.value(row_idx).to_string());
        }
        DataType::UInt32 => {
            let array = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow!("expected UInt32Array"))?;
            out.push_str(&array.value(row_idx).to_string());
        }
        DataType::UInt64 => {
            let array = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("expected UInt64Array"))?;
            out.push_str(&array.value(row_idx).to_string());
        }
        DataType::Float32 => {
            let array = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow!("expected Float32Array"))?;
            write_float(array.value(row_idx) as f64, out);
        }
        DataType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("expected Float64Array"))?;
            write_float(array.value(row_idx), out);
        }
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected StringArray"))?;
            push_json_string(out, array.value(row_idx));
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| anyhow!("expected LargeStringArray"))?;
            push_json_string(out, array.value(row_idx));
        }
        DataType::Utf8View => {
            let array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| anyhow!("expected StringViewArray"))?;
            push_json_string(out, array.value(row_idx));
        }
        DataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("expected BinaryArray"))?;
            push_json_string(out, &hex_encode(array.value(row_idx)));
        }
        DataType::LargeBinary => {
            let array = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| anyhow!("expected LargeBinaryArray"))?;
            push_json_string(out, &hex_encode(array.value(row_idx)));
        }
        DataType::List(_) => {
            let array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("expected ListArray"))?;
            let values = array.value(row_idx);
            write_array_slice(values.as_ref(), out)?;
        }
        DataType::Struct(_) => {
            let array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("expected StructArray"))?;
            write_struct_value(array, row_idx, out)?;
        }
        DataType::Null => out.push_str("null"),
        other => {
            return Err(anyhow!("unsupported output type: {other:?}"));
        }
    }

    Ok(())
}

fn write_array_slice(array: &dyn Array, out: &mut String) -> Result<()> {
    out.push('[');
    for idx in 0..array.len() {
        if idx > 0 {
            out.push(',');
        }
        write_array_value(array, idx, out)?;
    }
    out.push(']');
    Ok(())
}

fn write_struct_value(array: &StructArray, row_idx: usize, out: &mut String) -> Result<()> {
    out.push('{');
    for (idx, field) in array.fields().iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        push_json_string(out, field.name());
        out.push(':');
        write_array_value(array.column(idx).as_ref(), row_idx, out)?;
    }
    out.push('}');
    Ok(())
}

fn write_float(value: f64, out: &mut String) {
    if value.is_finite() {
        out.push_str(&value.to_string());
    } else {
        out.push_str("null");
    }
}

fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => {
                let code = c as u32;
                out.push_str("\\u");
                out.push(hex_digit((code >> 12) & 0xF));
                out.push(hex_digit((code >> 8) & 0xF));
                out.push(hex_digit((code >> 4) & 0xF));
                out.push(hex_digit(code & 0xF));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn hex_digit(n: u32) -> char {
    match n {
        0..=9 => char::from_u32(b'0' as u32 + n).unwrap(),
        10..=15 => char::from_u32(b'a' as u32 + (n - 10)).unwrap(),
        _ => unreachable!(),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(hex_digit((byte >> 4) as u32));
        out.push(hex_digit((byte & 0x0F) as u32));
    }
    out
}

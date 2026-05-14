use anyhow::{anyhow, Result};
use arrow::datatypes::DataType;
use datafusion::prelude::{
    cast, coalesce, col, concat, lit, lower, nullif, replace, sha256, trim, try_cast,
    upper, when, Expr,
};
use std::ops::Not;

use crate::mapping;
use crate::transform::CoerceOnErrorPolicy;

fn as_string(expr: Expr) -> Expr {
    coalesce(vec![cast(expr, DataType::Utf8), lit("")])
}

fn null_if_empty(expr: Expr) -> Expr {
    nullif(as_string(expr), lit(""))
}

pub fn compile_expr(expr: &mapping::Expr) -> Result<Expr> {
    Ok(match expr {
        mapping::Expr::Column { column } => col(column),
        mapping::Expr::Literal { literal } => lit(literal.clone()),
        mapping::Expr::Text(text) => lit(text.clone()),
        mapping::Expr::Concat { concat: parts } => {
            let compiled = parts
                .iter()
                .map(compile_expr)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(as_string)
                .collect::<Vec<_>>();
            concat(compiled)
        }
        mapping::Expr::Coalesce { coalesce: parts } => {
            let compiled = parts
                .iter()
                .map(compile_expr)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(null_if_empty)
                .collect::<Vec<_>>();
            coalesce(compiled)
        }
        mapping::Expr::RowNumber { row_number } => {
            if *row_number {
                col("__source_row")
            } else {
                lit("")
            }
        }
        mapping::Expr::Lower { lower: inner } => lower(as_string(compile_expr(inner)?)),
        mapping::Expr::Upper { upper: inner } => upper(as_string(compile_expr(inner)?)),
        mapping::Expr::Trim { trim: inner } => trim(vec![as_string(compile_expr(inner)?)]),
        mapping::Expr::Replace { replace: op } => replace(
            as_string(compile_expr(&op.value)?),
            lit(op.from.clone()),
            lit(op.to.clone()),
        ),
        mapping::Expr::NullIf { null_if: values } => {
            if values.len() != 2 {
                return Err(anyhow!("null_if requires exactly two expressions"));
            }
            nullif(
                as_string(compile_expr(&values[0])?),
                as_string(compile_expr(&values[1])?),
            )
        }
        mapping::Expr::Sha256 { sha256: inner } => sha256(as_string(compile_expr(inner)?)),
    })
}

pub fn compile_predicate(predicate: &mapping::Predicate) -> Result<Expr> {
    Ok(match predicate {
        mapping::Predicate::Eq { eq } => {
            as_string(compile_expr(&eq.left)?).eq(as_string(compile_expr(&eq.right)?))
        }
        mapping::Predicate::Neq { neq } => {
            as_string(compile_expr(&neq.left)?).not_eq(as_string(compile_expr(&neq.right)?))
        }
        mapping::Predicate::IsEmpty { is_empty } => {
            let expr = compile_expr(is_empty)?;
            expr.clone().is_null().or(as_string(expr).eq(lit("")))
        }
        mapping::Predicate::IsNotEmpty { is_not_empty } => {
            let expr = compile_expr(is_not_empty)?;
            expr.clone().is_not_null().and(as_string(expr).not_eq(lit("")))
        }
        mapping::Predicate::And { and } => and
            .iter()
            .map(compile_predicate)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|left, right| left.and(right))
            .unwrap_or_else(|| lit(true)),
        mapping::Predicate::Or { or } => or
            .iter()
            .map(compile_predicate)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|left, right| left.or(right))
            .unwrap_or_else(|| lit(false)),
        mapping::Predicate::Not { not } => compile_predicate(not)?.not(),
    })
}

pub fn compile_coerce_expr(
    column: &str,
    output_type: &mapping::OutputType,
    policy: &CoerceOnErrorPolicy,
) -> Expr {
    let input = as_string(col(column));
    let empty = input.clone().eq(lit(""));
    match output_type {
        mapping::OutputType::String => input,
        mapping::OutputType::Integer => compile_numeric_string(input, DataType::Int64, empty, policy),
        mapping::OutputType::Float => compile_numeric_string(input, DataType::Float64, empty, policy),
        mapping::OutputType::Boolean => compile_boolean_string(input, empty, policy),
    }
}

fn compile_numeric_string(
    input: Expr,
    data_type: DataType,
    empty: Expr,
    policy: &CoerceOnErrorPolicy,
) -> Expr {
    match policy {
        CoerceOnErrorPolicy::Strict => when(empty.clone(), lit(""))
            .otherwise(cast(cast(input, data_type), DataType::Utf8))
            .unwrap(),
        CoerceOnErrorPolicy::NullOnError => when(empty.clone(), lit(""))
            .otherwise(coalesce(vec![cast(try_cast(input, data_type), DataType::Utf8), lit("")]))
            .unwrap(),
        CoerceOnErrorPolicy::KeepOriginalText => when(empty.clone(), lit(""))
            .otherwise(coalesce(vec![cast(try_cast(input.clone(), data_type), DataType::Utf8), input]))
            .unwrap(),
    }
}

fn compile_boolean_string(input: Expr, empty: Expr, policy: &CoerceOnErrorPolicy) -> Expr {
    let normalized = lower(trim(vec![input.clone()]));
    let canonical = when(
        normalized
            .clone()
            .eq(lit("true"))
            .or(normalized.clone().eq(lit("1")))
            .or(normalized.clone().eq(lit("yes"))),
        lit("true"),
    )
    .when(
        normalized
            .clone()
            .eq(lit("false"))
            .or(normalized.clone().eq(lit("0")))
            .or(normalized.clone().eq(lit("no"))),
        lit("false"),
    )
    .otherwise(lit(""))
    .unwrap();
    match policy {
        CoerceOnErrorPolicy::Strict => when(empty.clone(), lit(""))
            .otherwise(canonical.clone())
            .unwrap(),
        CoerceOnErrorPolicy::NullOnError => when(empty.clone(), lit(""))
            .otherwise(canonical.clone())
            .unwrap(),
        CoerceOnErrorPolicy::KeepOriginalText => when(empty.clone(), lit(""))
            .otherwise(
                when(canonical.clone().eq(lit("")), input)
                    .otherwise(canonical)
                    .unwrap(),
            )
            .unwrap(),
    }
}

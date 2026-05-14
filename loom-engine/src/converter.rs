use anyhow::{anyhow, Result};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;
use vortex::expr::Expression as VortexExpression;
use vortex::expr::{and, col, eq, gt, gt_eq, lit, lt, lt_eq, not_eq, or};
use vortex::scalar::Scalar as VortexScalar;

pub fn convert_expr(expr: &Expr) -> Result<VortexExpression> {
    match expr {
        Expr::BinaryExpr(binary) => {
            let lhs = convert_expr(&binary.left)?;
            let rhs = convert_expr(&binary.right)?;
            match binary.op {
                Operator::Eq => Ok(eq(lhs, rhs)),
                Operator::NotEq => Ok(not_eq(lhs, rhs)),
                Operator::Gt => Ok(gt(lhs, rhs)),
                Operator::GtEq => Ok(gt_eq(lhs, rhs)),
                Operator::Lt => Ok(lt(lhs, rhs)),
                Operator::LtEq => Ok(lt_eq(lhs, rhs)),
                Operator::And => Ok(and(lhs, rhs)),
                Operator::Or => Ok(or(lhs, rhs)),
                _ => Err(anyhow!("unsupported operator: {:?}", binary.op)),
            }
        }
        Expr::Column(column) => Ok(col(column.name.clone())),
        Expr::Literal(value, _) => {
            let vortex_scalar = convert_scalar_value(value)?;
            Ok(lit(vortex_scalar))
        }
        _ => Err(anyhow!("unsupported expression type: {:?}", expr)),
    }
}

fn convert_scalar_value(value: &ScalarValue) -> Result<VortexScalar> {
    match value {
        ScalarValue::Boolean(Some(b)) => Ok(VortexScalar::from(*b)),
        ScalarValue::Int8(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::Int32(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::Int64(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::UInt8(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::UInt32(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::UInt64(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::Float32(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::Float64(Some(v)) => Ok(VortexScalar::from(*v)),
        ScalarValue::Utf8(Some(v)) => Ok(VortexScalar::from(v.clone())),
        _ => Err(anyhow!("unsupported scalar value: {:?}", value)),
    }
}

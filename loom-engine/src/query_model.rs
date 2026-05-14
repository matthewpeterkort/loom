use anyhow::{anyhow, Context, Result};
use datafusion::common::{Column, JoinType};
use datafusion::dataframe::DataFrame;
use datafusion::execution::FunctionRegistry;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::{col, lit, SessionContext};
use serde_json::Value;

pub type Query = Value;

pub fn parse_query_json(input: &str) -> Result<Query> {
    let query: Query = serde_json::from_str(input).context("invalid LoomQL JSON payload")?;
    validate_query(&query)?;
    Ok(query)
}

pub fn validate_query(query: &Query) -> Result<()> {
    graph_name(query)?;
    steps(query)?;
    Ok(())
}

pub fn graph_name(query: &Query) -> Result<&str> {
    query
        .get("graph")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("query must include string `graph`"))
}

pub fn steps(query: &Query) -> Result<&Vec<Value>> {
    query
        .get("steps")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("query must include array `steps`"))
}

pub fn steps_mut(query: &mut Query) -> Result<&mut Vec<Value>> {
    query
        .get_mut("steps")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow!("query must include array `steps`"))
}

pub fn step_op(step: &Value) -> Result<&str> {
    step.get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("step must include string `op`"))
}

pub fn step_string_list(step: &Value, key: &str) -> Result<Vec<String>> {
    match step.get(key) {
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| anyhow!("step `{}` must contain only strings", key))
            })
            .collect(),
        Some(_) => Err(anyhow!("step `{}` must be an array", key)),
        None => Ok(Vec::new()),
    }
}

pub fn step_n(step: &Value) -> Result<u64> {
    step.get("n")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("step must include numeric `n`"))
}

pub fn step_field(step: &Value) -> Result<&str> {
    step.get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("step must include string `field`"))
}

pub fn step_eq(step: &Value) -> Result<&Value> {
    step.get("eq")
        .ok_or_else(|| anyhow!("step must include `eq`"))
}

pub trait LoweringContext: Send + Sync {
    fn table_vertices(&self, graph: &str) -> Result<String>;
    fn table_edges(&self, graph: &str) -> Result<String>;
    fn traverse_out(
        &self,
        graph: &str,
        input: LogicalPlan,
        labels: &[String],
    ) -> Result<LogicalPlan>;
    fn traverse_in(
        &self,
        graph: &str,
        input: LogicalPlan,
        labels: &[String],
    ) -> Result<LogicalPlan>;
    fn traverse_both(
        &self,
        graph: &str,
        input: LogicalPlan,
        labels: &[String],
    ) -> Result<LogicalPlan>;
    fn materialize(&self, graph: &str, input: LogicalPlan) -> Result<LogicalPlan>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamKind {
    Vertex,
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VertexState {
    IdOnly,
    Materialized,
}

pub async fn lower_to_logical_plan(
    session: &SessionContext,
    query: &Query,
    ctx: &dyn LoweringContext,
) -> Result<LogicalPlan> {
    let graph = graph_name(query)?;
    let vertices_table = ctx.table_vertices(graph)?;
    let edges_table = ctx.table_edges(graph)?;

    let vertices_base = session.table(vertices_table).await?;
    let edges_base = session.table(edges_table.clone()).await?;

    let edge_cols = column_names(&edges_base);

    let mut current = vertices_base.clone();
    let mut kind = StreamKind::Vertex;
    let mut v_state = VertexState::Materialized;

    for step in steps(query)? {
        match step_op(step)? {
            "v" => {
                let ids = step_string_list(step, "ids")?;
                current = vertices_base.clone();
                if !ids.is_empty() {
                    current = current.filter(in_list_expr("id", &ids))?;
                }
                kind = StreamKind::Vertex;
                v_state = VertexState::Materialized;
            }
            "out" => {
                let labels = step_string_list(step, "labels")?;
                ensure_vertex(kind, "out")?;
                current = DataFrame::new(
                    session.state(),
                    ctx.traverse_out(graph, current.into_unoptimized_plan(), &labels)?,
                );
                kind = StreamKind::Vertex;
                v_state = VertexState::IdOnly;
            }
            "in" => {
                let labels = step_string_list(step, "labels")?;
                ensure_vertex(kind, "in")?;
                current = DataFrame::new(
                    session.state(),
                    ctx.traverse_in(graph, current.into_unoptimized_plan(), &labels)?,
                );
                kind = StreamKind::Vertex;
                v_state = VertexState::IdOnly;
            }
            "both" => {
                let labels = step_string_list(step, "labels")?;
                ensure_vertex(kind, "both")?;
                current = DataFrame::new(
                    session.state(),
                    ctx.traverse_both(graph, current.into_unoptimized_plan(), &labels)?,
                );
                kind = StreamKind::Vertex;
                v_state = VertexState::IdOnly;
            }
            "out_e" => {
                let labels = step_string_list(step, "labels")?;
                ensure_vertex(kind, "out_e")?;
                let edges = with_edge_label_filter(edges_base.clone(), &labels)?;
                let joined = current.join(edges, JoinType::Inner, &["id"], &["from_id"], None)?;
                current = select_columns_by_qualified_name(joined, Some(&edges_table), &edge_cols)?;
                kind = StreamKind::Edge;
            }
            "in_e" => {
                let labels = step_string_list(step, "labels")?;
                ensure_vertex(kind, "in_e")?;
                let edges = with_edge_label_filter(edges_base.clone(), &labels)?;
                let joined = current.join(edges, JoinType::Inner, &["id"], &["to_id"], None)?;
                current = select_columns_by_qualified_name(joined, Some(&edges_table), &edge_cols)?;
                kind = StreamKind::Edge;
            }
            "both_e" => {
                let labels = step_string_list(step, "labels")?;
                ensure_vertex(kind, "both_e")?;
                let out = {
                    let edges = with_edge_label_filter(edges_base.clone(), &labels)?;
                    let joined = current
                        .clone()
                        .join(edges, JoinType::Inner, &["id"], &["from_id"], None)?;
                    select_columns_by_qualified_name(joined, Some(&edges_table), &edge_cols)?
                };
                let inn = {
                    let edges = with_edge_label_filter(edges_base.clone(), &labels)?;
                    let joined = current
                        .clone()
                        .join(edges, JoinType::Inner, &["id"], &["to_id"], None)?;
                    select_columns_by_qualified_name(joined, Some(&edges_table), &edge_cols)?
                };
                current = out.union(inn)?;
                kind = StreamKind::Edge;
            }
            "has" => {
                let field = step_field(step)?;
                let eq = step_eq(step)?;
                if kind == StreamKind::Vertex && v_state == VertexState::IdOnly {
                    current = DataFrame::new(
                        session.state(),
                        ctx.materialize(graph, current.into_unoptimized_plan())?,
                    );
                    v_state = VertexState::Materialized;
                }

                let cols = column_names(&current);
                if cols.iter().any(|c| c == field) {
                    let expr = col(field);
                    current = if eq.is_null() {
                        current.filter(expr.is_null())?
                    } else {
                        current.filter(expr.eq(json_literal_expr(eq)?))?
                    };
                } else if kind == StreamKind::Vertex && cols.iter().any(|c| c == "payload_json_bin")
                {
                    let udf = session
                        .udf("payload_extract")
                        .map_err(|_| anyhow!("payload_extract UDF not registered"))?;
                    let extracted = udf.call(vec![col("payload_json_bin"), lit(field.to_string())]);
                    current = if eq.is_null() {
                        current.filter(extracted.is_null())?
                    } else {
                        current.filter(extracted.eq(lit(json_literal_as_string(eq)?)))?
                    };
                } else {
                    let expr = col(field);
                    current = if eq.is_null() {
                        current.filter(expr.is_null())?
                    } else {
                        current.filter(expr.eq(json_literal_expr(eq)?))?
                    };
                }
            }
            "has_label" => {
                let labels = step_string_list(step, "labels")?;
                let cols = column_names(&current);
                if cols.iter().any(|c| c == "label") {
                    current = current.filter(in_list_expr("label", &labels))?;
                } else if cols.iter().any(|c| c == "resourceType") {
                    current = current.filter(in_list_expr("\"resourceType\"", &labels))?;
                } else {
                    current = current.filter(in_list_expr("label", &labels))?;
                }
            }
            "has_id" => {
                let ids = step_string_list(step, "ids")?;
                current = current.filter(in_list_expr("id", &ids))?;
            }
            "limit" => {
                current = current.limit(0, Some(step_n(step)? as usize))?;
            }
            "skip" => {
                current = current.limit(step_n(step)? as usize, None)?;
            }
            "count" => {
                current = current.aggregate(vec![], vec![count(lit(1)).alias("count")])?;
            }
            "render" => {
                let fields = step_string_list(step, "fields")?;
                if fields.is_empty() {
                    continue;
                }
                if kind == StreamKind::Vertex && v_state == VertexState::IdOnly {
                    current = DataFrame::new(
                        session.state(),
                        ctx.materialize(graph, current.into_unoptimized_plan())?,
                    );
                    v_state = VertexState::Materialized;
                }
                let all_cols = column_names(&current);
                let mut exprs = Vec::<Expr>::new();
                for field in &fields {
                    if field == "*" {
                        exprs.extend(all_cols.iter().map(|name| col(name)));
                    } else {
                        exprs.push(col(field));
                    }
                }
                current = current.select(exprs)?;
            }
            other => return Err(anyhow!("unsupported step op `{other}`")),
        }
    }

    if kind == StreamKind::Vertex && v_state == VertexState::IdOnly {
        current = DataFrame::new(
            session.state(),
            ctx.materialize(graph, current.into_unoptimized_plan())?,
        );
    }

    Ok(current.into_unoptimized_plan())
}

fn column_names(df: &DataFrame) -> Vec<String> {
    df.schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect()
}

fn select_columns_by_qualified_name(
    df: DataFrame,
    relation: Option<&str>,
    names: &[String],
) -> Result<DataFrame> {
    let exprs = names
        .iter()
        .map(|name| Expr::Column(Column::new(relation, name.clone())))
        .collect::<Vec<Expr>>();
    Ok(df.select(exprs)?)
}

fn with_edge_label_filter(edges: DataFrame, labels: &[String]) -> Result<DataFrame> {
    if labels.is_empty() {
        Ok(edges)
    } else {
        Ok(edges.filter(in_list_expr("label", labels))?)
    }
}

fn in_list_expr(field: &str, values: &[String]) -> Expr {
    let lits = values.iter().map(|v| lit(v.clone())).collect::<Vec<_>>();
    col(field).in_list(lits, false)
}

fn ensure_vertex(kind: StreamKind, op: &str) -> Result<()> {
    if kind != StreamKind::Vertex {
        return Err(anyhow!("`{op}` requires a vertex stream"));
    }
    Ok(())
}

fn json_literal_expr(value: &Value) -> Result<Expr> {
    match value {
        Value::Null => Ok(lit(datafusion::scalar::ScalarValue::Null)),
        Value::Bool(v) => Ok(lit(*v)),
        Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                Ok(lit(i))
            } else if let Some(f) = v.as_f64() {
                Ok(lit(f))
            } else {
                Err(anyhow!("unsupported numeric literal"))
            }
        }
        Value::String(v) => Ok(lit(v.clone())),
        _ => Err(anyhow!("unsupported JSON literal in query filter")),
    }
}

fn json_literal_as_string(value: &Value) -> Result<String> {
    match value {
        Value::Null => Ok("null".to_string()),
        Value::Bool(v) => Ok(v.to_string()),
        Value::Number(v) => Ok(v.to_string()),
        Value::String(v) => Ok(v.clone()),
        _ => Err(anyhow!("unsupported JSON literal in payload filter")),
    }
}

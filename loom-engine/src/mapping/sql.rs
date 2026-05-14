use anyhow::{anyhow, Result};
use datafusion::functions_aggregate::expr_fn::{count_distinct, min};
use datafusion::logical_expr::JoinType;
use datafusion::prelude::{
    cast, coalesce, col, concat, ident, lit, lower, nullif, regexp_replace, replace, sha256, trim,
    upper, when, DataFrame, Expr as DfExpr, SessionContext,
};
use std::collections::{BTreeMap, HashSet};
use std::ops::Not;

use crate::source::{SourceDescriptor, SourceSchema, SourceTableRef};

use super::model::{
    ColumnMapping, CompiledEdgeViewPlan, CompiledIdentityViewPlan, CompiledNodeViewPlan,
    CompiledReferenceViewPlan, EdgeMapping, Expr, IdentityRule, PropsMapping,
    ReferenceNormalizer, ReferenceRule, VertexMapping,
};

pub(super) fn compile_node_view_plan(
    graph: &str,
    idx: usize,
    vertex: &VertexMapping,
    source: &SourceDescriptor,
) -> Result<CompiledNodeViewPlan> {
    let table = source.require_single_table()?;
    Ok(CompiledNodeViewPlan {
        mapping_index: idx,
        mapping_path: format!("vertices[{idx}]"),
        source_alias: vertex.source.clone(),
        source_id: source.id.clone(),
        source_table_name: table.table_name.clone(),
        source_table_ref: Some(SourceTableRef {
            source_id: source.id.clone(),
            table_id: table.id.clone(),
        }),
        view_name: format!(
            "graph_{}_node_{:04}_{}",
            sanitize_output_column(graph),
            idx,
            sanitize_output_column(&vertex.label)
        ),
        label: vertex.label.clone(),
        explain: None,
        projected_columns: node_projected_columns(vertex, &table.schema)?,
    })
}

pub(super) fn compile_edge_view_plan(
    graph: &str,
    idx: usize,
    edge: &EdgeMapping,
    source: &SourceDescriptor,
) -> Result<CompiledEdgeViewPlan> {
    let table = source.require_single_table()?;
    Ok(CompiledEdgeViewPlan {
        mapping_index: idx,
        mapping_path: format!("edges[{idx}]"),
        source_alias: edge.source.clone(),
        source_id: source.id.clone(),
        source_table_name: table.table_name.clone(),
        source_table_ref: Some(SourceTableRef {
            source_id: source.id.clone(),
            table_id: table.id.clone(),
        }),
        view_name: format!(
            "graph_{}_edge_{:04}_{}",
            sanitize_output_column(graph),
            idx,
            sanitize_output_column(&edge.label)
        ),
        label: edge.label.clone(),
        explain: None,
        projected_columns: edge_projected_columns(edge, &table.schema)?,
    })
}

pub(super) fn compile_identity_view_plan(
    graph: &str,
    label: &str,
    _vertices: &[(&VertexMapping, &SourceDescriptor)],
    rule: Option<&IdentityRule>,
) -> Result<CompiledIdentityViewPlan> {
    let rule = rule.cloned().unwrap_or(IdentityRule {
        canonical_id: None,
        aliases: BTreeMap::new(),
        normalizer: ReferenceNormalizer::Raw,
    });
    let mut alias_names = vec!["canonical".to_string()];
    alias_names.extend(rule.aliases.keys().cloned());
    Ok(CompiledIdentityViewPlan {
        label: label.to_string(),
        view_name: format!(
            "graph_{}_identity_{}",
            sanitize_output_column(graph),
            sanitize_output_column(label)
        ),
        alias_names,
        explain: None,
        projected_columns: vec![
            "label".to_string(),
            "canonical_id".to_string(),
            "alias_name".to_string(),
            "alias_key".to_string(),
            "source_id".to_string(),
            "source_row".to_string(),
            "transform_id".to_string(),
        ],
    })
}

pub(super) fn compile_reference_view_plan(
    graph: &str,
    reference: &ReferenceRule,
    source: &SourceDescriptor,
) -> Result<CompiledReferenceViewPlan> {
    let table = source.require_single_table()?;
    Ok(CompiledReferenceViewPlan {
        name: reference.name.clone(),
        source_alias: reference.source.clone(),
        source_id: source.id.clone(),
        source_table_name: table.table_name.clone(),
        source_table_ref: Some(SourceTableRef {
            source_id: source.id.clone(),
            table_id: table.id.clone(),
        }),
        view_name: format!(
            "graph_{}_reference_{}",
            sanitize_output_column(graph),
            sanitize_output_column(&reference.name)
        ),
        from_label: reference.from_label.clone(),
        to_label: reference.to_label.clone(),
        explain: None,
        projected_columns: vec![
            "from_label".to_string(),
            "to_label".to_string(),
            "from_key".to_string(),
            "to_key".to_string(),
            "resolved_from_id".to_string(),
            "resolved_to_id".to_string(),
            "match_count_from".to_string(),
            "match_count_to".to_string(),
            "resolution_status".to_string(),
            "source_id".to_string(),
            "source_row".to_string(),
        ],
    })
}

pub(crate) async fn build_node_view_dataframe(
    session: &SessionContext,
    vertex: &VertexMapping,
    source: &SourceDescriptor,
) -> Result<DataFrame> {
    let table = source.require_single_table()?;
    let dataframe = session.table(table.table_name.as_str()).await?;
    let properties = property_projection_pairs(&vertex.props, &vertex.columns, &table.schema)?;
    let mut projections = vec![
        compile_expr(&vertex.id)?.alias("id"),
        lit(vertex.label.clone()).alias("label"),
        lit(source.id.clone()).alias("source_id"),
        col("__source_row").alias("source_row"),
    ];
    projections.extend(
        properties
            .iter()
            .map(|(source_column, prop_column)| ident(source_column).alias(prop_column))
            .collect::<Vec<_>>(),
    );
    projections.extend(
        vertex
            .columns
            .iter()
            .map(|(column, mapping)| compile_expr(mapping.expr()).map(|expr| expr.alias(column)))
            .collect::<Result<Vec<_>>>()?,
    );

    let mut predicate = is_not_empty_mapping_expr(&vertex.id)?;
    if let Some(extra) = &vertex.predicate {
        predicate = predicate.and(compile_predicate(extra)?);
    }

    dataframe.filter(predicate)?.select(projections).map_err(Into::into)
}

pub(crate) async fn build_edge_view_dataframe(
    session: &SessionContext,
    edge: &EdgeMapping,
    source: &SourceDescriptor,
) -> Result<DataFrame> {
    let table = source.require_single_table()?;
    let dataframe = session.table(table.table_name.as_str()).await?;
    let properties = property_projection_pairs(&edge.props, &edge.columns, &table.schema)?;
    let id_expr = match &edge.id {
        Some(id) => compile_expr(id)?,
        None => compile_expr(&default_edge_id_expr(edge))?,
    };
    let from_expr = compile_expr(&edge.from)?;
    let to_expr = compile_expr(&edge.to)?;
    let mut projections = vec![
        id_expr.alias("id"),
        from_expr.alias("from_id"),
        to_expr.alias("to_id"),
        lit(edge.label.clone()).alias("label"),
        lit(source.id.clone()).alias("source_id"),
        col("__source_row").alias("source_row"),
    ];
    projections.extend(
        properties
            .iter()
            .map(|(source_column, prop_column)| ident(source_column).alias(prop_column))
            .collect::<Vec<_>>(),
    );
    projections.extend(
        edge
            .columns
            .iter()
            .map(|(column, mapping)| compile_expr(mapping.expr()).map(|expr| expr.alias(column)))
            .collect::<Result<Vec<_>>>()?,
    );

    let mut predicate = is_not_empty_mapping_expr(&edge.from)?.and(is_not_empty_mapping_expr(&edge.to)?);
    if let Some(extra) = &edge.predicate {
        predicate = predicate.and(compile_predicate(extra)?);
    }

    dataframe.filter(predicate)?.select(projections).map_err(Into::into)
}

pub(crate) async fn build_identity_view_dataframe(
    session: &SessionContext,
    label: &str,
    vertices: &[(&VertexMapping, &SourceDescriptor)],
    rule: Option<&IdentityRule>,
) -> Result<DataFrame> {
    let rule = rule.cloned().unwrap_or(IdentityRule {
        canonical_id: None,
        aliases: BTreeMap::new(),
        normalizer: ReferenceNormalizer::Raw,
    });
    let mut union_df: Option<DataFrame> = None;

    for (vertex, source) in vertices {
        let table = source.require_single_table()?;
        let base = session.table(table.table_name.as_str()).await?;
        let canonical_expr = normalize_expr(
            compile_expr(rule.canonical_id.as_ref().unwrap_or(&vertex.id))?,
            &rule.normalizer,
        );

        let mut base_predicate = is_not_empty_df_expr(canonical_expr.clone());
        if let Some(predicate) = &vertex.predicate {
            base_predicate = base_predicate.and(compile_predicate(predicate)?);
        }

        let canonical_df = base
            .clone()
            .filter(base_predicate.clone())?
            .select(vec![
                lit(label.to_string()).alias("label"),
                canonical_expr.clone().alias("canonical_id"),
                lit("canonical").alias("alias_name"),
                canonical_expr.clone().alias("alias_key"),
                lit(source.id.clone()).alias("source_id"),
                col("__source_row").alias("source_row"),
                lit("").alias("transform_id"),
            ])?;
        union_df = Some(match union_df {
            Some(existing) => existing.union(canonical_df)?,
            None => canonical_df,
        });

        for (alias_name, alias_expr) in &rule.aliases {
            let alias_key = normalize_expr(compile_expr(alias_expr)?, &rule.normalizer);
            let alias_df = base
                .clone()
                .filter(base_predicate.clone().and(is_not_empty_df_expr(alias_key.clone())))?
                .select(vec![
                    lit(label.to_string()).alias("label"),
                    canonical_expr.clone().alias("canonical_id"),
                    lit(alias_name.clone()).alias("alias_name"),
                    alias_key.alias("alias_key"),
                    lit(source.id.clone()).alias("source_id"),
                    col("__source_row").alias("source_row"),
                    lit("").alias("transform_id"),
                ])?;
            union_df = Some(match union_df {
                Some(existing) => existing.union(alias_df)?,
                None => alias_df,
            });
        }
    }

    union_df.ok_or_else(|| anyhow!("identity view for label `{label}` has no source rows"))
}

pub(crate) async fn build_reference_view_dataframe(
    session: &SessionContext,
    graph: &str,
    reference: &ReferenceRule,
    source: &SourceDescriptor,
) -> Result<DataFrame> {
    let table = source.require_single_table()?;
    let base = session.table(table.table_name.as_str()).await?;
    let from_key = normalize_expr(compile_expr(&reference.from_key)?, &reference.normalizer);
    let to_key = normalize_expr(compile_expr(&reference.to_key)?, &reference.normalizer);
    let mut predicate = is_not_empty_df_expr(from_key.clone()).and(is_not_empty_df_expr(to_key.clone()));
    if let Some(extra) = &reference.predicate {
        predicate = predicate.and(compile_predicate(extra)?);
    }
    let base = base.filter(predicate)?.select(vec![
        lit(reference.from_label.clone()).alias("from_label"),
        lit(reference.to_label.clone()).alias("to_label"),
        from_key.alias("from_key"),
        to_key.alias("to_key"),
        lit(source.id.clone()).alias("source_id"),
        col("__source_row").alias("source_row"),
    ])?;

    let from_identity = format!(
        "graph_{}_identity_{}",
        sanitize_output_column(graph),
        sanitize_output_column(&reference.from_label)
    );
    let to_identity = format!(
        "graph_{}_identity_{}",
        sanitize_output_column(graph),
        sanitize_output_column(&reference.to_label)
    );

    let from_resolved = session
        .table(from_identity.as_str())
        .await?
        .filter(is_not_empty_df_expr(col("alias_key")))?
        .aggregate(
            vec![col("alias_key")],
            vec![
                min(col("canonical_id")).alias("resolved_from_id"),
                count_distinct(col("canonical_id")).alias("match_count_from"),
            ],
        )?
        .with_column_renamed("alias_key", "from_alias_key")?;
    let to_resolved = session
        .table(to_identity.as_str())
        .await?
        .filter(is_not_empty_df_expr(col("alias_key")))?
        .aggregate(
            vec![col("alias_key")],
            vec![
                min(col("canonical_id")).alias("resolved_to_id"),
                count_distinct(col("canonical_id")).alias("match_count_to"),
            ],
        )?
        .with_column_renamed("alias_key", "to_alias_key")?;

    let resolution_status = when(
        col("resolved_from_id")
            .is_null()
            .or(col("resolved_to_id").is_null()),
        lit("unresolved"),
    )
    .when(
        col("match_count_from")
            .gt(lit(1_i64))
            .or(col("match_count_to").gt(lit(1_i64))),
        lit("ambiguous"),
    )
    .otherwise(lit("resolved"))?
    .alias("resolution_status");

    base.join(
        from_resolved,
        JoinType::Left,
        &["from_key"],
        &["from_alias_key"],
        None,
    )?
    .join(to_resolved, JoinType::Left, &["to_key"], &["to_alias_key"], None)?
    .select(vec![
        col("from_label"),
        col("to_label"),
        col("from_key"),
        col("to_key"),
        col("resolved_from_id"),
        col("resolved_to_id"),
        col("match_count_from"),
        col("match_count_to"),
        resolution_status,
        col("source_id"),
        col("source_row"),
    ])
    .map_err(Into::into)
}

fn node_projected_columns(vertex: &VertexMapping, schema: &SourceSchema) -> Result<Vec<String>> {
    let mut projected_columns = vec![
        "id".to_string(),
        "label".to_string(),
        "source_id".to_string(),
        "source_row".to_string(),
    ];
    projected_columns.extend(
        property_projection_pairs(&vertex.props, &vertex.columns, schema)?
            .into_iter()
            .map(|(_, prop_column)| prop_column),
    );
    projected_columns.extend(vertex.columns.keys().cloned());
    Ok(projected_columns)
}

fn edge_projected_columns(edge: &EdgeMapping, schema: &SourceSchema) -> Result<Vec<String>> {
    let mut projected_columns = vec![
        "id".to_string(),
        "from_id".to_string(),
        "to_id".to_string(),
        "label".to_string(),
        "source_id".to_string(),
        "source_row".to_string(),
    ];
    projected_columns.extend(
        property_projection_pairs(&edge.props, &edge.columns, schema)?
            .into_iter()
            .map(|(_, prop_column)| prop_column),
    );
    projected_columns.extend(edge.columns.keys().cloned());
    Ok(projected_columns)
}

fn property_projection_pairs(
    props: &PropsMapping,
    explicit_columns: &BTreeMap<String, ColumnMapping>,
    schema: &SourceSchema,
) -> Result<Vec<(String, String)>> {
    let selected = match props {
        PropsMapping::None => Vec::new(),
        PropsMapping::All => schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>(),
        PropsMapping::Except(exclude) => {
            let exclude = exclude.iter().collect::<HashSet<_>>();
            schema
                .columns
                .iter()
                .filter(|column| !exclude.contains(&column.name))
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        }
        PropsMapping::Only(only) => only.clone(),
    };
    let explicit = explicit_columns.keys().collect::<HashSet<_>>();
    let mut out = Vec::new();
    for source_column in selected {
        let prop_column = property_column_name(&source_column);
        if explicit.contains(&prop_column) {
            return Err(anyhow!(
                "explicit output column `{prop_column}` collides with generated property column"
            ));
        }
        out.push((source_column, prop_column));
    }
    Ok(out)
}

fn is_not_empty_mapping_expr(expr: &Expr) -> Result<DfExpr> {
    Ok(is_not_empty_df_expr(compile_expr(expr)?))
}

fn is_not_empty_df_expr(expr: DfExpr) -> DfExpr {
    expr.clone().is_not_null().and(expr.not_eq(lit("")))
}

fn normalize_expr(expr: DfExpr, normalizer: &ReferenceNormalizer) -> DfExpr {
    match normalizer {
        ReferenceNormalizer::Raw => expr,
        ReferenceNormalizer::TrimLower => lower(trim(vec![expr])),
        ReferenceNormalizer::FhirCanonical => {
            let trimmed = trim(vec![expr]);
            let no_path = regexp_replace(trimmed, lit("^.*/"), lit(""), None);
            lower(trim(vec![regexp_replace(
                no_path,
                lit("^urn:uuid:"),
                lit(""),
                None,
            )]))
        }
    }
}

fn as_string(expr: DfExpr) -> DfExpr {
    coalesce(vec![cast(expr, arrow::datatypes::DataType::Utf8), lit("")])
}

fn null_if_empty(expr: DfExpr) -> DfExpr {
    nullif(as_string(expr), lit(""))
}

fn compile_expr(expr: &Expr) -> Result<DfExpr> {
    Ok(match expr {
        Expr::Column { column } => ident(column),
        Expr::Literal { literal } => lit(literal.clone()),
        Expr::Text(text) => lit(text.clone()),
        Expr::Concat { concat: parts } => concat(
            parts.iter()
                .map(compile_expr)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(as_string)
                .collect::<Vec<_>>(),
        ),
        Expr::Coalesce { coalesce: parts } => coalesce(
            parts.iter()
                .map(compile_expr)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(null_if_empty)
                .collect::<Vec<_>>(),
        ),
        Expr::RowNumber { row_number } => {
            if *row_number {
                ident("__source_row")
            } else {
                lit("")
            }
        }
        Expr::Lower { lower: inner } => lower(as_string(compile_expr(inner)?)),
        Expr::Upper { upper: inner } => upper(as_string(compile_expr(inner)?)),
        Expr::Trim { trim: inner } => trim(vec![as_string(compile_expr(inner)?)]),
        Expr::Replace { replace: op } => replace(
            as_string(compile_expr(&op.value)?),
            lit(op.from.clone()),
            lit(op.to.clone()),
        ),
        Expr::NullIf { null_if: values } => {
            if values.len() != 2 {
                return Err(anyhow!("null_if requires exactly two expressions"));
            }
            nullif(
                as_string(compile_expr(&values[0])?),
                as_string(compile_expr(&values[1])?),
            )
        }
        Expr::Sha256 { sha256: inner } => sha256(as_string(compile_expr(inner)?)),
    })
}

fn compile_predicate(predicate: &super::model::Predicate) -> Result<DfExpr> {
    Ok(match predicate {
        super::model::Predicate::Eq { eq } => {
            as_string(compile_expr(&eq.left)?).eq(as_string(compile_expr(&eq.right)?))
        }
        super::model::Predicate::Neq { neq } => {
            as_string(compile_expr(&neq.left)?).not_eq(as_string(compile_expr(&neq.right)?))
        }
        super::model::Predicate::IsEmpty { is_empty } => {
            let expr = compile_expr(is_empty)?;
            expr.clone().is_null().or(as_string(expr).eq(lit("")))
        }
        super::model::Predicate::IsNotEmpty { is_not_empty } => {
            let expr = compile_expr(is_not_empty)?;
            expr.clone().is_not_null().and(as_string(expr).not_eq(lit("")))
        }
        super::model::Predicate::And { and } => and
            .iter()
            .map(compile_predicate)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|left, right| left.and(right))
            .unwrap_or_else(|| lit(true)),
        super::model::Predicate::Or { or } => or
            .iter()
            .map(compile_predicate)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|left, right| left.or(right))
            .unwrap_or_else(|| lit(false)),
        super::model::Predicate::Not { not } => compile_predicate(not)?.not(),
    })
}

fn default_edge_id_expr(edge: &EdgeMapping) -> Expr {
    Expr::Concat {
        concat: vec![
            Expr::Text(format!("edge/{}", edge.label)),
            Expr::Text("/".to_string()),
            edge.from.clone(),
            Expr::Text("/".to_string()),
            edge.to.clone(),
            Expr::Text("/".to_string()),
            Expr::RowNumber { row_number: true },
        ],
    }
}

fn property_column_name(source_column: &str) -> String {
    let normalized = source_column.to_ascii_lowercase();
    if normalized.starts_with("prop_") {
        normalized
    } else {
        format!("prop_{normalized}")
    }
}

fn sanitize_output_column(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

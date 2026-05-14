use std::collections::BTreeMap;

use crate::mapping::{ColumnMapping, EdgeMapping, Expr, PropsMapping, VertexMapping};

use super::rules::SourceFacts;

pub(super) fn has_any(source: &SourceFacts, columns: &[&str]) -> bool {
    columns.iter().any(|column| {
        source.columns.contains(*column)
            || source.columns.contains(&column.to_ascii_lowercase())
            || source.columns.contains(&column.to_ascii_uppercase())
    })
}

fn patient_col(source: &SourceFacts) -> &'static str {
    if source.columns.contains("PATIENT_ID") {
        "PATIENT_ID"
    } else {
        "person_id"
    }
}

fn sample_col(source: &SourceFacts) -> &'static str {
    if source.columns.contains("SAMPLE_ID") {
        "SAMPLE_ID"
    } else if source.columns.contains("Tumor_Sample_Barcode") {
        "Tumor_Sample_Barcode"
    } else {
        "sample_id"
    }
}

fn gene_col(source: &SourceFacts) -> &'static str {
    if source.columns.contains("Hugo_Symbol") {
        "Hugo_Symbol"
    } else if source.columns.contains("gene_symbol") {
        "gene_symbol"
    } else {
        "gene"
    }
}

fn col(name: &str) -> Expr {
    Expr::Column {
        column: name.to_string(),
    }
}

fn lit(value: &str) -> Expr {
    Expr::Text(value.to_string())
}

fn concat(parts: Vec<Expr>) -> Expr {
    Expr::Concat { concat: parts }
}

fn coalesce(cols: &[&str]) -> Expr {
    Expr::Coalesce {
        coalesce: cols.iter().map(|column| col(column)).collect(),
    }
}

fn row_number() -> Expr {
    Expr::RowNumber { row_number: true }
}

fn cm(expr: Expr) -> ColumnMapping {
    ColumnMapping::Expr(expr)
}

pub(super) fn patient_vertex(source: &str, facts: &SourceFacts) -> VertexMapping {
    let patient = patient_col(facts);
    VertexMapping {
        source: source.to_string(),
        label: "Patient".to_string(),
        id: concat(vec![lit("Patient/"), col(patient)]),
        predicate: None,
        columns: BTreeMap::from([("patient_id".to_string(), cm(col(patient)))]),
        props: PropsMapping::Except(vec![patient.to_string()]),
        prop_types: BTreeMap::new(),
    }
}

pub(super) fn specimen_vertex(source: &str, facts: &SourceFacts) -> VertexMapping {
    let sample = sample_col(facts);
    let patient = patient_col(facts);
    let mut columns = BTreeMap::from([("sample_id".to_string(), cm(col(sample)))]);
    let mut excluded = vec![sample.to_string()];
    if has_any(facts, &["PATIENT_ID", "person_id"]) {
        columns.insert("patient_id".to_string(), cm(col(patient)));
        excluded.push(patient.to_string());
    }
    VertexMapping {
        source: source.to_string(),
        label: "Specimen".to_string(),
        id: concat(vec![lit("Specimen/"), col(sample)]),
        predicate: None,
        columns,
        props: PropsMapping::Except(excluded),
        prop_types: BTreeMap::new(),
    }
}

pub(super) fn gene_vertex(source: &str, facts: &SourceFacts) -> VertexMapping {
    let gene = gene_col(facts);
    VertexMapping {
        source: source.to_string(),
        label: "Gene".to_string(),
        id: concat(vec![lit("Gene/"), col(gene)]),
        predicate: None,
        columns: BTreeMap::from([("gene_symbol".to_string(), cm(col(gene)))]),
        props: PropsMapping::None,
        prop_types: BTreeMap::new(),
    }
}

fn variant_id_expr() -> Expr {
    concat(vec![
        col("Chromosome"),
        lit(":"),
        col("Start_Position"),
        lit(":"),
        col("End_Position"),
        lit(":"),
        col("Reference_Allele"),
        lit(">"),
        coalesce(&["Tumor_Seq_Allele2", "Tumor_Seq_Allele1"]),
    ])
}

pub(super) fn variant_vertex(source: &str, _facts: &SourceFacts) -> VertexMapping {
    VertexMapping {
        source: source.to_string(),
        label: "Variant".to_string(),
        id: concat(vec![lit("Variant/"), variant_id_expr()]),
        predicate: None,
        columns: BTreeMap::from([
            ("sample_id".to_string(), cm(col("Tumor_Sample_Barcode"))),
            ("gene_symbol".to_string(), cm(col("Hugo_Symbol"))),
            ("variant_id".to_string(), cm(variant_id_expr())),
            ("chromosome".to_string(), cm(col("Chromosome"))),
            ("start_position".to_string(), cm(col("Start_Position"))),
            ("end_position".to_string(), cm(col("End_Position"))),
            ("reference_allele".to_string(), cm(col("Reference_Allele"))),
            (
                "tumor_seq_allele2".to_string(),
                cm(coalesce(&["Tumor_Seq_Allele2", "Tumor_Seq_Allele1"])),
            ),
        ]),
        props: PropsMapping::Except(vec![
            "Tumor_Sample_Barcode".to_string(),
            "Hugo_Symbol".to_string(),
            "Chromosome".to_string(),
            "Start_Position".to_string(),
            "End_Position".to_string(),
            "Reference_Allele".to_string(),
            "Tumor_Seq_Allele1".to_string(),
            "Tumor_Seq_Allele2".to_string(),
        ]),
        prop_types: BTreeMap::new(),
    }
}

fn finding_id_expr() -> Expr {
    concat(vec![
        lit("GenomicFinding/"),
        col("Tumor_Sample_Barcode"),
        lit("/"),
        variant_id_expr(),
        lit("/"),
        row_number(),
    ])
}

pub(super) fn genomic_finding_vertex(source: &str, _facts: &SourceFacts) -> VertexMapping {
    VertexMapping {
        source: source.to_string(),
        label: "GenomicFinding".to_string(),
        id: finding_id_expr(),
        predicate: None,
        columns: BTreeMap::from([
            ("sample_id".to_string(), cm(col("Tumor_Sample_Barcode"))),
            ("gene_symbol".to_string(), cm(col("Hugo_Symbol"))),
            ("variant_id".to_string(), cm(variant_id_expr())),
            ("chromosome".to_string(), cm(col("Chromosome"))),
            ("start_position".to_string(), cm(col("Start_Position"))),
            ("end_position".to_string(), cm(col("End_Position"))),
            ("reference_allele".to_string(), cm(col("Reference_Allele"))),
            (
                "tumor_seq_allele2".to_string(),
                cm(coalesce(&["Tumor_Seq_Allele2", "Tumor_Seq_Allele1"])),
            ),
        ]),
        props: PropsMapping::All,
        prop_types: BTreeMap::new(),
    }
}

pub(super) fn case_list_vertex(source: &str) -> VertexMapping {
    VertexMapping {
        source: source.to_string(),
        label: "CaseList".to_string(),
        id: concat(vec![lit("CaseList/"), col("stable_id")]),
        predicate: None,
        columns: BTreeMap::from([
            ("case_list_id".to_string(), cm(col("stable_id"))),
            ("case_list_name".to_string(), cm(col("case_list_name"))),
        ]),
        props: PropsMapping::Only(vec!["stable_id".to_string(), "case_list_name".to_string()]),
        prop_types: BTreeMap::new(),
    }
}

pub(super) fn patient_specimen_edge(source: &str, facts: &SourceFacts) -> EdgeMapping {
    let patient = patient_col(facts);
    EdgeMapping {
        source: source.to_string(),
        label: "HAS_SPECIMEN".to_string(),
        from_label: Some("Patient".to_string()),
        to_label: Some("Specimen".to_string()),
        from: concat(vec![lit("Patient/"), col(patient)]),
        to: concat(vec![lit("Specimen/"), col(sample_col(facts))]),
        id: Some(concat(vec![
            lit("edge/"),
            col(patient),
            lit("/HAS_SPECIMEN/"),
            col(sample_col(facts)),
        ])),
        predicate: None,
        columns: BTreeMap::new(),
        props: PropsMapping::None,
        prop_types: BTreeMap::new(),
    }
}

pub(super) fn specimen_finding_edge(source: &str, facts: &SourceFacts) -> EdgeMapping {
    EdgeMapping {
        source: source.to_string(),
        label: "HAS_OBSERVATION".to_string(),
        from_label: Some("Specimen".to_string()),
        to_label: Some("GenomicFinding".to_string()),
        from: concat(vec![lit("Specimen/"), col(sample_col(facts))]),
        to: finding_id_expr(),
        id: Some(concat(vec![
            lit("edge/"),
            col(sample_col(facts)),
            lit("/HAS_OBSERVATION/"),
            row_number(),
        ])),
        predicate: None,
        columns: BTreeMap::new(),
        props: PropsMapping::None,
        prop_types: BTreeMap::new(),
    }
}

pub(super) fn finding_variant_edge(source: &str, _facts: &SourceFacts) -> EdgeMapping {
    EdgeMapping {
        source: source.to_string(),
        label: "OBSERVES_VARIANT".to_string(),
        from_label: Some("GenomicFinding".to_string()),
        to_label: Some("Variant".to_string()),
        from: finding_id_expr(),
        to: concat(vec![lit("Variant/"), variant_id_expr()]),
        id: Some(concat(vec![
            lit("edge/OBSERVES_VARIANT/"),
            col("Tumor_Sample_Barcode"),
            lit("/"),
            row_number(),
        ])),
        predicate: None,
        columns: BTreeMap::new(),
        props: PropsMapping::None,
        prop_types: BTreeMap::new(),
    }
}

pub(super) fn variant_gene_edge(source: &str, facts: &SourceFacts) -> EdgeMapping {
    EdgeMapping {
        source: source.to_string(),
        label: "IN_GENE".to_string(),
        from_label: Some("Variant".to_string()),
        to_label: Some("Gene".to_string()),
        from: concat(vec![lit("Variant/"), variant_id_expr()]),
        to: concat(vec![lit("Gene/"), col(gene_col(facts))]),
        id: Some(concat(vec![
            lit("edge/IN_GENE/"),
            variant_id_expr(),
            lit("/"),
            col(gene_col(facts)),
        ])),
        predicate: None,
        columns: BTreeMap::new(),
        props: PropsMapping::None,
        prop_types: BTreeMap::new(),
    }
}

pub(super) fn case_specimen_edge(source: &str) -> EdgeMapping {
    EdgeMapping {
        source: source.to_string(),
        label: "HAS_CASE".to_string(),
        from_label: Some("CaseList".to_string()),
        to_label: Some("Specimen".to_string()),
        from: concat(vec![lit("CaseList/"), col("stable_id")]),
        to: concat(vec![lit("Specimen/"), col("sample_id")]),
        id: Some(concat(vec![
            lit("edge/"),
            col("stable_id"),
            lit("/HAS_CASE/"),
            col("sample_id"),
        ])),
        predicate: None,
        columns: BTreeMap::new(),
        props: PropsMapping::None,
        prop_types: BTreeMap::new(),
    }
}

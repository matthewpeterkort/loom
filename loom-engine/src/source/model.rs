use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceRegistration {
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub format: SourceFormat,
    pub location: SourceLocation,
    #[serde(default)]
    pub read_options: ReadOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceDescriptor {
    pub id: String,
    pub display_name: Option<String>,
    pub format: SourceFormat,
    pub location: SourceLocation,
    #[serde(default)]
    pub read_options: ReadOptions,
    #[serde(default)]
    pub tables: Vec<SourceTableDescriptor>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    pub stats: SourceStats,
    pub created_unix_seconds: u64,
    pub updated_unix_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SourceSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceTableDescriptor {
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub table_name: String,
    pub schema: SourceSchema,
    pub stats: SourceStats,
    pub created_unix_seconds: u64,
    pub updated_unix_seconds: u64,
    #[serde(default)]
    pub registered: bool,
    #[serde(default)]
    pub kind: SourceTableKind,
    #[serde(default)]
    pub header_row_index: Option<usize>,
    #[serde(default)]
    pub data_start_row_index: Option<usize>,
    #[serde(default)]
    pub original_column_names: Vec<String>,
    #[serde(default)]
    pub inferred_column_names: Vec<String>,
    #[serde(default)]
    pub quality: SourceTableQualitySummary,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceTableRef {
    pub source_id: String,
    pub table_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceTableKind {
    #[default]
    RawFile,
    RawSheet,
    Derived,
    View,
    Imported,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceTableQualitySummary {
    #[serde(default)]
    pub row_count_sampled: usize,
    #[serde(default)]
    pub column_count: usize,
    #[serde(default)]
    pub empty_column_count: usize,
    #[serde(default)]
    pub duplicate_header_count: usize,
    #[serde(default)]
    pub type_conflict_count: usize,
    #[serde(default)]
    pub possible_id_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceFormat {
    Csv,
    Tsv,
    CbioTsv,
    CbioCaseList,
    Parquet,
    Xlsx,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceLocation {
    Local { path: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadOptions {
    #[serde(default)]
    pub delimiter: Option<char>,
    #[serde(default)]
    pub has_header: Option<bool>,
    #[serde(default)]
    pub comment_prefix: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceSchema {
    pub columns: Vec<SourceColumn>,
    #[serde(default)]
    pub skipped_comment_rows: usize,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceColumn {
    pub name: String,
    pub ordinal: usize,
    pub inferred_type: ColumnType,
    pub nullable: bool,
    pub sample_values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Boolean,
    Integer,
    Float,
    String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceStats {
    pub row_count: Option<usize>,
    pub file_size_bytes: Option<u64>,
    pub modified_unix_seconds: Option<u64>,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SourceTable {
    pub source_id: String,
    pub table_id: String,
    pub headers: Vec<String>,
    pub skipped_comment_rows: usize,
    pub rows: Vec<SourceRow>,
}

#[derive(Debug, Clone)]
pub struct SourceRow {
    pub line_number: usize,
    pub values: HashMap<String, String>,
}

impl SourceDescriptor {
    pub fn default_table(&self) -> Option<&SourceTableDescriptor> {
        self.tables.first()
    }

    pub fn is_multi_table(&self) -> bool {
        self.tables.len() > 1
    }

    pub fn require_single_table(&self) -> anyhow::Result<&SourceTableDescriptor> {
        match self.tables.as_slice() {
            [table] => Ok(table),
            [] => Err(anyhow::anyhow!("source `{}` has no logical tables", self.id)),
            _ => Err(anyhow::anyhow!(
                "source `{}` has multiple logical tables; specify a table id",
                self.id
            )),
        }
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            delimiter: None,
            has_header: Some(true),
            comment_prefix: None,
        }
    }
}

impl Default for SourceFormat {
    fn default() -> Self {
        SourceFormat::Tsv
    }
}

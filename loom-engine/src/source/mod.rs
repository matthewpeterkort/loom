//! Source catalog types and raw table readers.

pub mod catalog;
pub mod model;
pub mod profile;
pub mod readers;

pub use catalog::{SourceCatalog, SourceCatalogSnapshot};
pub use model::{
    ColumnType, ReadOptions, SourceColumn, SourceDescriptor, SourceFormat, SourceLocation,
    SourceRegistration, SourceRow, SourceSchema, SourceStats, SourceTable,
    SourceTableDescriptor, SourceTableKind, SourceTableQualitySummary, SourceTableRef,
};
pub use profile::{
    profile_source_table, profile_source_table_with_schema, SourceColumnProfile, SourceProfile,
    SourceProfileSuggestion,
};
pub use readers::{
    infer_and_read_source, infer_table_descriptor_from_table, read_source_rows,
    read_source_table_rows, record_batches_to_source_table, source_rows_to_batch,
    source_rows_to_batch_with_provenance, source_table_name,
};

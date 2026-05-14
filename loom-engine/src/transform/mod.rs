//! Spreadsheet and table normalization models and compilers.

pub mod catalog;
pub mod compiler;
pub mod expr;
pub mod model;
pub mod validation;

pub use catalog::{TransformCatalog, TransformCatalogSnapshot};
pub use model::TransformPlanKind;
pub use model::{
    CoerceOnErrorPolicy, DeriveColumnExpr, DeriveColumnOp, ExplodeColumnOp, FilterRowsOp,
    PreviewTransformRequest, SplitColumnBehavior, SplitColumnOp, TransformDescriptor,
    TransformDescriptorStatus, TransformOperation, TransformPreviewResult, TransformRef,
    TransformSpec, TransformValidationReport,
};

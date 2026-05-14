use anyhow::Result;
use loom_engine::source::{SourceFormat, SourceLocation, SourceRegistration};
use loom_engine::Engine;
use rust_xlsxwriter::Workbook;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("cbioportal")
        .join("brca_tcga_small")
}

#[allow(dead_code)]
pub fn local_source(id: &str, file: &str, format: SourceFormat) -> SourceRegistration {
    SourceRegistration {
        id: id.to_string(),
        display_name: Some(id.to_string()),
        format,
        location: SourceLocation::Local {
            path: fixture_dir().join(file).to_string_lossy().to_string(),
        },
        read_options: Default::default(),
    }
}

#[allow(dead_code)]
pub async fn register_fixture_sources(engine: &Engine) -> Result<()> {
    engine
        .register_source(local_source(
            "patients",
            "data_clinical_patient.txt",
            SourceFormat::CbioTsv,
        ))
        .await?;
    engine
        .register_source(local_source(
            "samples",
            "data_clinical_sample.txt",
            SourceFormat::CbioTsv,
        ))
        .await?;
    engine
        .register_source(local_source(
            "mutations",
            "data_mutations.txt",
            SourceFormat::Tsv,
        ))
        .await?;
    engine
        .register_source(local_source(
            "sequenced_cases",
            "case_lists/cases_sequenced.txt",
            SourceFormat::CbioCaseList,
        ))
        .await?;
    Ok(())
}

#[allow(dead_code)]
pub fn create_workbook_fixture(path: &Path) -> Result<()> {
    let mut workbook = Workbook::new();

    let sheet1 = workbook.add_worksheet().set_name("Tumor Data")?;
    sheet1.write_string(0, 0, "patient id")?;
    sheet1.write_string(0, 1, "sample-id")?;
    sheet1.write_string(1, 0, "P001")?;
    sheet1.write_string(1, 1, "S001")?;
    sheet1.write_string(2, 0, "P002")?;
    sheet1.write_string(2, 1, "S002")?;

    let sheet2 = workbook.add_worksheet().set_name("Tumor-Data")?;
    sheet2.write_string(0, 0, "patient id")?;
    sheet2.write_string(0, 1, "assay")?;
    sheet2.write_string(1, 0, "P001")?;
    sheet2.write_string(1, 1, "WGS")?;

    let hidden = workbook.add_worksheet().set_name("Hidden Sheet")?;
    hidden.set_hidden(true);
    hidden.write_string(0, 0, "ignore")?;

    workbook.save(path)?;
    Ok(())
}

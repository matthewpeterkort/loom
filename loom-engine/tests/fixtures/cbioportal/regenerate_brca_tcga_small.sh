#!/usr/bin/env bash
set -euo pipefail

out_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/brca_tcga_small"
base_url="https://raw.githubusercontent.com/cBioPortal/datahub/master/public/brca_tcga"

mkdir -p "${out_dir}/case_lists"

curl -fsSL "${base_url}/data_clinical_patient.txt" | sed -n '1,7p' > "${out_dir}/data_clinical_patient.txt"
curl -fsSL "${base_url}/data_clinical_sample.txt" | sed -n '1,8p' > "${out_dir}/data_clinical_sample.txt"
curl -fsSL "${base_url}/data_mutations.txt" | sed -n '1,4p' > "${out_dir}/data_mutations.txt"
curl -fsSL "${base_url}/case_lists/cases_sequenced.txt" > "${out_dir}/case_lists/cases_sequenced.txt"

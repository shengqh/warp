version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC PheTK PheWAS Workflow
##
## This workflow performs a Phenome-Wide Association Study (PheWAS) using the PheTK tool
## and generates Manhattan and forest plot visualizations of the results.
## Developed by VUMC Biostatistics for genotype-phenotype association analysis.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given a cohort file and a phecode count matrix, this workflow runs a PheWAS analysis
## to identify phenotypes significantly associated with an independent variable of interest,
## then produces Manhattan and forest plots for visualization.
##
## ### Workflow Steps:
## 1. **PheTK**: Runs the PheWAS analysis using the PheTK tool, producing a TSV of association results.
## 2. **PheTKVis**: Generates Manhattan (with and without labels) and forest plot visualizations.
## 3. **CopyFile (Optional)**: If `target_gcp_folder` is provided, copies all four output files
##    to the specified GCP folder.
##
## ### Inputs:
## - cohort_file_path: Input cohort file containing sample metadata and covariates.
## - phecode_count_file_path: File containing phecode counts per sample.
## - phecode_version: Version of the phecode mapping to use (e.g., "1.2").
## - sex_at_birth_col: Column name for sex at birth in the cohort file.
## - covariate_cols: Comma-separated column names to use as covariates in the regression model.
## - independent_variable_of_interest: Column name for the independent variable to test.
## - output_file_prefix: Prefix for all output file names.
## - min_cases: Minimum number of cases required to include a phecode in the analysis. Default is 50.
## - min_phecode_count: Minimum phecode count per sample to be considered a case. Default is 1.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - output_phewas_file: TSV file with PheWAS association results for all tested phecodes.
## - output_phewas_manhattan_file: Manhattan plot of PheWAS results with labels.
## - output_phewas_manhattan_no_labels_file: Manhattan plot of PheWAS results without labels.
## - output_phewas_forest_file: Forest plot of PheWAS results.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPheTK {
  input {
    File cohort_file_path
    File phecode_count_file_path
    String phecode_version
    String sex_at_birth_col
    String covariate_cols
    String independent_variable_of_interest
    String output_file_prefix

    Int min_cases=50
    Int min_phecode_count=1

    String? target_gcp_folder
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    cohort_file_path: "Input cohort file containing sample metadata and covariates"
    phecode_count_file_path: "File containing phecode counts per sample"
    phecode_version: "Version of the phecode mapping to use (e.g., '1.2')"
    sex_at_birth_col: "Column name for sex at birth in the cohort file"
    covariate_cols: "Comma-separated column names to use as covariates in the regression model"
    independent_variable_of_interest: "Column name for the independent variable to test for phenotype associations"
    output_file_prefix: "Prefix for all output file names"
    min_cases: "Minimum number of cases required to include a phecode in the PheWAS analysis. Default is 50."
    min_phecode_count: "Minimum phecode count per sample to be considered a case. Default is 1."
    target_gcp_folder: "Optional GCP folder path to copy output files to after completion"
  }

  call PheTK {
    input:
      cohort_file_path = cohort_file_path,
      phecode_count_file_path = phecode_count_file_path,
      phecode_version = phecode_version,
      sex_at_birth_col = sex_at_birth_col,
      covariate_cols = covariate_cols,
      independent_variable_of_interest = independent_variable_of_interest,
      output_file_prefix = output_file_prefix,
      min_cases = min_cases,
      min_phecode_count = min_phecode_count
  }

  call PheTKVis {
    input:
      phewas_result_file = PheTK.output_phewas_file,
      output_file_prefix = output_file_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyFourFiles as CopyFile {
      input:
        source_file1 = PheTK.output_phewas_file,
        source_file2 = PheTKVis.output_phewas_manhattan_file,
        source_file3 = PheTKVis.output_phewas_manhattan_no_labels_file,
        source_file4 = PheTKVis.output_phewas_forest_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder]),
    }
  }

  output {
    File output_phewas_file = select_first([CopyFile.output_file1, PheTK.output_phewas_file])
    File output_phewas_manhattan_file = select_first([CopyFile.output_file2, PheTKVis.output_phewas_manhattan_file])
    File output_phewas_manhattan_no_labels_file = select_first([CopyFile.output_file3, PheTKVis.output_phewas_manhattan_no_labels_file])
    File output_phewas_forest_file = select_first([CopyFile.output_file4, PheTKVis.output_phewas_forest_file])
  }
}

task PheTK {
  input {
    File cohort_file_path
    File phecode_count_file_path
    String phecode_version
    String sex_at_birth_col
    String covariate_cols
    String independent_variable_of_interest
    String output_file_prefix

    Int min_cases
    Int min_phecode_count
    
    String docker = "phetk/phetk:0.2.2"

    Int preemptible = 3
    Int cpu = 1
    Int memory_gb = 10
  }

  Int disk_size = ceil(size([cohort_file_path, phecode_count_file_path],"GB")) + 10

  String output_file = output_file_prefix + ".phewas.tsv"

  command <<<

python3 -m phetk.phewas \
  --cohort_file_path ~{cohort_file_path} \
  --phecode_count_file_path ~{phecode_count_file_path} \
  --phecode_version ~{phecode_version} \
  --sex_at_birth_col ~{sex_at_birth_col} \
  --covariate_cols ~{covariate_cols} \
  --independent_variable_of_interest ~{independent_variable_of_interest} \
  --min_cases ~{min_cases} \
  --min_phecode_count ~{min_phecode_count} \
  --output_file_path  ~{output_file}

>>>

  runtime {
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    # The output has to be defined as File, otherwise the file would not be delocalized
    File output_phewas_file = "~{output_file}"
  }
}

task PheTKVis {
  input {
    File phewas_result_file
    String output_file_prefix
    
    String docker = "phetk/phetk:0.2.2"

    Int preemptible = 3
    Int cpu = 1
    Int memory_gb = 10
  }

  Int disk_size = ceil(size([phewas_result_file],"GB")) + 10

  command <<<
#!/bin/bash

set -e

# Create Python script
cat > phetk_vis.py << 'EOF'

from phetk.phewas import PheWAS
from phetk.plot import Plot

p = Plot("~{phewas_result_file}")

p.manhattan(label_values="p_value", label_count=16, save_plot=True, output_file_path="~{output_file_prefix}_manhattan.png")

p.manhattan(label_values="p_value", label_count=0, save_plot=True, output_file_path="~{output_file_prefix}_manhattan_no_labels.png")

p.forest(highlight_significance=True, show_p_value_asterisks=True, save_plot=True, output_file_path="~{output_file_prefix}_forest.png")

EOF

python3 phetk_vis.py

>>>

  runtime {
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_phewas_manhattan_file = "~{output_file_prefix}_manhattan.png"
    File output_phewas_manhattan_no_labels_file = "~{output_file_prefix}_manhattan_no_labels.png"
    File output_phewas_forest_file = "~{output_file_prefix}_forest.png"
  }
}


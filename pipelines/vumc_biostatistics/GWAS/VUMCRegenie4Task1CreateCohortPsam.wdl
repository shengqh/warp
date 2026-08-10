version 1.0

## VUMC Regenie GWAS Workflow - Task 1: Create Cohort PSAM
##
## This workflow handles the creation of cohort PSAM file for Regenie GWAS analysis.
## Developed by VUMC Biostatistics for population-specific GWAS studies.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline prepares input cohort PSAM file for Regenie GWAS, 
## with options for ancestry filtering and sample selection.
##
## ### Workflow Steps:
## 1. CreateCohortPsam: Create a cohort PSAM file with optional ancestry filtering
## 2. Optionally copy output files to a specified GCP folder
##
## ### Inputs:
## - input_psam: Input PSAM file
## - input_grid: Optional grid file for sample selection
## - input_ancestry: Optional ancestry specification
## - input_ancestry_file: Optional file containing ancestry information
## - output_prefix: Prefix for output files
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - ancestry: Selected ancestry (if specified)
## - output_psam: Final PSAM file
## - output_sample_count: Number of samples in the cohort
##
## ### Notes:
## - Uses AgdUtils for cohort creation functionality
## - Supports ancestry-specific analyses
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../agd/AgdUtils.wdl" as AgdUtils

workflow VUMCRegenie4Task1CreateCohortPsam {
  input {
    File input_psam

    File? remove_grid_file

    File? input_grid
    Int input_grid_column = 0

    String? input_ancestry
    String input_ancestry_column="ANCESTRY"    
    File? input_ancestry_file

    String output_prefix

    String? target_gcp_folder
  }

  if(!defined(input_grid) && (!defined(input_ancestry) || !defined(input_ancestry_file))){
    call WDLUtils.FailWithMessage {
      input:
        message = "Either input_grid must be defined, or both input_ancestry and input_ancestry_file must be defined."
    }
  }

  call AgdUtils.CreateCohortPsam as CreateCohortPsam {
    input:
      input_psam = input_psam,
      input_grid = input_grid,
      input_grid_column = input_grid_column,
      remove_grid_file = remove_grid_file,
      input_ancestry = input_ancestry,
      input_ancestry_column = input_ancestry_column,
      input_ancestry_file = input_ancestry_file,
      output_prefix = output_prefix
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = CreateCohortPsam.output_psam,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String? ancestry = input_ancestry
    File output_psam = select_first([CopyFile.output_file, CreateCohortPsam.output_psam])
    Int output_sample_count = CreateCohortPsam.output_sample_count
  }
}

version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

import "../agd/AgdUtils.wdl" as AgdUtils

/**
 * Workflow: VUMCRegenie4Task1CreateCohortPsam
 * 
 * Description:
 * This workflow creates a cohort PSAM file for use in the VUMC Regenie GWAS pipeline.
 * It takes an input PSAM file and optionally a grid file and ancestry information.
 * The output is a PSAM file with the specified output prefix.
 * Current in AGD163K cohort, the ancestry information is as below:
 * ANCESTRY	count
 * EUR	      ~120000
 * AFR	      ~30000
 * AMR	      ~4500
 * EAS	      ~2500
 * SAS	      ~500
 *
 * Author:
 * Quanhu Sheng, quanhu.sheng.1@vumc.org
 */
workflow VUMCRegenie4Task1CreateCohortPsam {
  input {
    File input_psam

    File? input_grid
    Int input_grid_column = 0

    String? input_ancestry
    String input_ancestry_column="ANCESTRY"    
    File? input_ancestry_file

    String output_prefix

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  call AgdUtils.CreateCohortPsam as CreateCohortPsam {
    input:
      input_psam = input_psam,
      input_grid = input_grid,
      input_grid_column = input_grid_column,
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
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String? ancestry = input_ancestry
    File output_psam = select_first([CopyFile.output_file, CreateCohortPsam.output_psam])
    Int output_sample_count = CreateCohortPsam.output_sample_count
  }
}

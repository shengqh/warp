version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

import "../agd/AgdUtils.wdl" as AgdUtils

workflow VUMCRegenie4Task1CreateCohortPsam {
  input {
    File input_psam

    File? input_grid
    Int input_grid_column = 0

    String? input_ancestry
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
    File output_psam = select_first([CopyFile.output_file, CreateCohortPsam.output_psam])
    Int output_sample_count = CreateCohortPsam.output_sample_count
  }
}

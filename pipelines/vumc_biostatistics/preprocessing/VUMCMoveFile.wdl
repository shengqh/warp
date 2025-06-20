version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCMoveFile {
  input {
    String source_file

    String? billing_gcp_project_id
    String target_gcp_folder
  }

  call GcpUtils.MoveOrCopyOneFile as MoveFile {
    input:
      source_file = source_file,
      is_move_file = true,
      project_id = billing_gcp_project_id,
      target_gcp_folder = select_first([target_gcp_folder])
  }

  output {
    String target_file = MoveFile.output_file
  }
}

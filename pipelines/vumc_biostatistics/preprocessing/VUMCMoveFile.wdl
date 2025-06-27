version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCMoveFile {
  input {
    String source_file

    String target_gcp_folder
  }

  call GcpUtils.MoveOrCopyOneFile as MoveFile {
    input:
      source_file = source_file,
      is_move_file = true,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String target_file = MoveFile.output_file
  }
}

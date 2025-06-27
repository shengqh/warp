version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCCopyFile {
  input {
    String source_file
    String target_gcp_folder
  }

  call GcpUtils.MoveOrCopyOneFile as CopyFile {
    input:
      source_file = source_file,
      is_move_file = false,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String target_file = CopyFile.output_file
    Array[String] target_file_array = [CopyFile.output_file]
  }
}

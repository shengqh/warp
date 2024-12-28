version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "Utils.wdl" as Utils

workflow VUMCMergePgenFiles {
  input {
    Array[File] input_pgen_files
    Array[File] input_pvar_files
    Array[File] input_psam_files

    String output_prefix

    String? project_id
    String? target_gcp_folder
  }

  call Plink2Utils.MergePgenFiles {
    input:
      input_pgen_files = input_pgen_files,
      input_pvar_files = input_pvar_files,
      input_psam_files = input_psam_files,

      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    String pgen = "~{MergePgenFiles.output_pgen}"
    String pvar = "~{MergePgenFiles.output_pvar}"
    String psam = "~{MergePgenFiles.output_psam}"

    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = pgen,
        source_file2 = pvar,
        source_file3 = psam,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, MergePgenFiles.output_pgen])
    File output_pvar = select_first([CopyFile.output_file2, MergePgenFiles.output_pvar])
    File output_psam = select_first([CopyFile.output_file3, MergePgenFiles.output_psam])

    Int num_samples = MergePgenFiles.num_samples
    Int num_variants = MergePgenFiles.num_variants
  }
}

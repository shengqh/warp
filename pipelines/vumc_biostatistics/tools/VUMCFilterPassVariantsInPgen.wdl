version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../plink/Plink2Utils.wdl" as Plink2Utils

workflow VUMCFilterPassVariantsInPgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String target_prefix

    String? project_id
    String? target_gcp_folder
  }
  
  call Plink2Utils.FilterPassVariantsInPgen {
    input: 
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,

      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    String filtered_pgen = "~{FilterPassVariantsInPgen.output_pgen}"
    String filtered_pvar = "~{FilterPassVariantsInPgen.output_pvar}"
    String filtered_psam = "~{FilterPassVariantsInPgen.output_psam}"

    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = filtered_pgen,
        source_file2 = filtered_pvar,
        source_file3 = filtered_psam,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, FilterPassVariantsInPgen.output_pgen])
    File output_pvar = select_first([CopyFile.output_file2, FilterPassVariantsInPgen.output_pvar])
    File output_psam = select_first([CopyFile.output_file3, FilterPassVariantsInPgen.output_psam])

    Int num_samples = FilterPassVariantsInPgen.num_samples
    Int num_variants = FilterPassVariantsInPgen.num_variants
  }
}

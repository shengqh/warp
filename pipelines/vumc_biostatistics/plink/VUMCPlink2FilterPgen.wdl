version 1.0

import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPlink2FilterPgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    File? keep_psam
    File? keep_bed
    File? keep_variant_names

    String output_prefix

    String plink2_filter_option

    String? project_id
    String? target_gcp_folder
  }

  File check_a_file = select_first([ keep_variant_names, keep_psam, keep_bed ])

  call Plink2Utils.Plink2FilterPgen as Plink2FilterPgen {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      keep_psam = keep_psam,
      keep_bed = keep_bed,
      keep_variant_names = keep_variant_names,
      output_prefix = output_prefix,
      plink2_filter_option = plink2_filter_option
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = Plink2FilterPgen.output_pgen,
        source_file2 = Plink2FilterPgen.output_pvar,
        source_file3 = Plink2FilterPgen.output_psam,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_pgen = select_first([CopyFile.output_file1, Plink2FilterPgen.output_pgen])
    String output_pvar = select_first([CopyFile.output_file2, Plink2FilterPgen.output_pvar])
    String output_psam = select_first([CopyFile.output_file3, Plink2FilterPgen.output_psam])
    Int output_num_samples = Plink2FilterPgen.num_samples
    Int output_num_variants = Plink2FilterPgen.num_variants
  }
}

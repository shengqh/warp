version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils

workflow VUMCFilterVariantsForModelling {
  input {
    File phenoFile
    String phenoColList
    File covarFile
    String covarColList
    String catCovarColList

    String step1_regenie_option

    File model_pgen_file
    File model_psam_file
    File model_pvar_file

    String output_prefix

    String? target_gcp_folder
  }

  call BioUtils.FilterVariantsForModelling as GetVariants {
    input:
      phenoFile = phenoFile,
      phenoColList = phenoColList,
      covarFile = covarFile,
      covarColList = covarColList,
      catCovarColList = catCovarColList,
      step1_regenie_option = step1_regenie_option,
      model_pgen_file = model_pgen_file,
      model_pvar_file = model_pvar_file,
      model_psam_file = model_psam_file,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    String filtered_snp_list = "~{GetVariants.output_snp_list}"

    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = filtered_snp_list,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_snp_list = select_first([CopyFile.output_file, GetVariants.output_snp_list])
  }
}
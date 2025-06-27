version 1.0

import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPgen2Vcf {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix
    String? plink2_option

    String docker = "shengqh/plink_1.9_2.0:20250304"

    String? target_bucket
  }

  call Plink2Utils.Pgen2Vcf {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      output_prefix = output_prefix,
      plink2_option = plink2_option,
      docker = docker
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = Pgen2Vcf.output_vcf,
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file, Pgen2Vcf.output_vcf])
  }
}

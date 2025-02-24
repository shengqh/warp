version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils

workflow VUMCQCFilterAndMergePgen {
  input {
    Array[String] chromosomes

    Array[File] pgen_files
    Array[File] pvar_files
    Array[File] psam_files

    String output_prefix

    String qc_option="--mac 100 --geno 0.1 --maf 0.1 --max-maf 0.9 --hwe 1e-15 --snps-only --not-chr 23-27"

    String? project_id
    String? target_gcp_folder
  }

  Array[Int] indices = range(length(chromosomes))

  scatter (i in indices) {
    String chromosome = chromosomes[i]
    File pgen_file = pgen_files[i]
    File pvar_file = pvar_files[i]
    File psam_file = psam_files[i]
    String cur_output_prefix = output_prefix + "." + chromosome

    call BioUtils.QCFilterPgen {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        output_prefix = cur_output_prefix,
        qc_filter_option = qc_option
    }
  }

  call Plink2Utils.MergePgenFiles {
    input:
      input_pgen_files = QCFilterPgen.output_pgen,
      input_pvar_files = QCFilterPgen.output_pvar,
      input_psam_files = QCFilterPgen.output_psam,
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
    File qc_filtered_pgen = select_first([CopyFile.output_file1, MergePgenFiles.output_pgen])
    File qc_filtered_pvar = select_first([CopyFile.output_file2, MergePgenFiles.output_pvar])
    File qc_filtered_psam = select_first([CopyFile.output_file3, MergePgenFiles.output_psam])
  }
}

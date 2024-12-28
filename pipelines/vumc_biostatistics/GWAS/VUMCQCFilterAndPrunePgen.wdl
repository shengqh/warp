version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils

workflow VUMCQCFilterAndPrunePgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix

    String qc_filter_option
    String indep_pairwise_option = "50 5 0.2"

    Int max_variants = 1000000

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  call BioUtils.QCFilterAndPrunePgen {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,

      qc_filter_option = qc_filter_option,
      indep_pairwise_option = indep_pairwise_option,

      max_variants = max_variants,

      output_prefix = output_prefix,
  }

  if(defined(target_gcp_folder)) {
    String gcs_output_dir = select_first([target_gcp_folder])

    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = QCFilterAndPrunePgen.output_pgen,
        source_file2 = QCFilterAndPrunePgen.output_pvar,
        source_file3 = QCFilterAndPrunePgen.output_psam,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, QCFilterAndPrunePgen.output_pgen])
    File output_pvar = select_first([CopyFile.output_file2, QCFilterAndPrunePgen.output_pvar])
    File output_psam = select_first([CopyFile.output_file3, QCFilterAndPrunePgen.output_psam])

    Int num_samples = QCFilterAndPrunePgen.num_samples
    Int num_variants = QCFilterAndPrunePgen.num_variants
  }
}

version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "./AgdUtils.wdl" as AgdUtils

workflow VUMCPrepareGeneGenotype {
  input {
    String gene_symbol
    File agd_primary_grid_file
    File annovar_file
    File vcf_file

    String? project_id
    String? target_gcp_folder
  }

  call AgdUtils.PrepareGeneGenotype {
    input:
      gene_symbol = gene_symbol,
      agd_primary_grid_file = agd_primary_grid_file,
      annovar_file = annovar_file,
      vcf_file = vcf_file
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = PrepareGeneGenotype.lof_genotype_file,
        source_file2 = PrepareGeneGenotype.vuc_genotype_file,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File lof_genotype_file = select_first([CopyFile.output_file1, PrepareGeneGenotype.lof_genotype_file])
    File vuc_genotype_file = select_first([CopyFile.output_file2, PrepareGeneGenotype.vuc_genotype_file])
  }
}

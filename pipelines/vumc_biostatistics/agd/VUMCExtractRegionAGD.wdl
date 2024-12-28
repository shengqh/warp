version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "./AgdUtils.wdl" as AgdUtils

workflow VUMCExtractRegionAGD {
  input {
    Array[File] input_pgen_files
    Array[File] input_pvar_files
    Array[File] input_psam_files

    Array[String] chromosomes

    String plink2_filter_option

    File region_bed
    String output_prefix

    File id_map_file

    String? project_id
    String? target_gcp_folder
  }

  scatter (idx in range(length(chromosomes))) {
    String chromosome = chromosomes[idx]
    File pgen_file = input_pgen_files[idx]
    File pvar_file = input_pvar_files[idx]
    File psam_file = input_psam_files[idx]
    String replaced_sample_name = "~{chromosome}.psam"

    call AgdUtils.ReplaceICAIdWithGrid as ReplaceICAIdWithGrid {
      input:
        input_psam = psam_file,
        id_map_file = id_map_file,
        target_psam = replaced_sample_name
    }

    call Plink2Utils.ExtractPgenRegions as ExtractPgenRegions {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = ReplaceICAIdWithGrid.output_psam,
        output_prefix = chromosome,
        plink2_filter_option = plink2_filter_option,
        region_bed = region_bed
    }
  }

  call Plink2Utils.MergePgenFiles as MergePgenFiles{
    input:
      input_pgen_files = ExtractPgenRegions.output_pgen,
      input_pvar_files = ExtractPgenRegions.output_pvar,
      input_psam_files = ExtractPgenRegions.output_psam,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = "~{MergePgenFiles.output_pgen}",
        source_file2 = "~{MergePgenFiles.output_pvar}",
        source_file3 = "~{MergePgenFiles.output_psam}",
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

version 1.0

# This workflow extracts a cohort-specific subset of variant genotypes from a set of
# per-chromosome PLINK2 PGEN/PVAR/PSAM files, keeping only the variants listed in
# keep_variants_file and the samples listed in keep_grid_file, then merges the
# filtered per-chromosome results into a single PGEN/PVAR/PSAM file set.
#
# Workflow steps:
# 1. Build a cohort PSAM file restricted to the samples listed in keep_grid_file
# 2. Check for overlapping variants between each input pvar file and the keep_variants_file
# 3. For chromosomes with overlapping variants, extract the matching variants and samples
#    from the PGEN files
# 4. Merge filtered PGEN files across valid chromosomes if more than one exists
# 5. Optionally copy the result PGEN/PVAR/PSAM files to a GCP storage location
#
# Inputs:
# - keep_variants_file: Text file with one variant ID per line (no bed/pvar format)
# - keep_grid_file: Text file with sample IIDs (matching the IID column of the psam files) to keep
# - output_prefix: Prefix for all output files
# - chromosomes: List of chromosomes to process
# - input_pgen_files: PGEN files (one per chromosome)
# - input_psam_files: PSAM files (one per chromosome)
# - input_pvar_files: PVAR files (one per chromosome)
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_pgen: Final filtered/merged PGEN file path
# - output_pvar: Final filtered/merged PVAR file path
# - output_psam: Final filtered/merged PSAM file path
# - output_num_variants: Number of variants in the final result
# - output_num_samples: Number of samples in the final result

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "./AgdUtils.wdl" as AgdUtils

workflow VUMCExtractCohortPgen {
  input {
    File keep_variants_file
    File keep_grid_file

    String output_prefix

    Array[String] chromosomes
    Array[File] input_pgen_files
    Array[File] input_psam_files
    Array[File] input_pvar_files

    String? target_gcp_folder    
  }

  call AgdUtils.CreateCohortPsam as CreateCohortPsam {
    input:
      input_psam = input_psam_files[0],
      input_grid = keep_grid_file,
      input_grid_column = 0,
      output_prefix = output_prefix
  }

  Int num_all_chromsome = length(chromosomes)

  scatter(all_chrom_ind in range(num_all_chromsome)){
    call BioUtils.CheckOverlapVariantsByID as CheckOverlapVariants {
      input:
        input_pgen_pvar = input_pvar_files[all_chrom_ind],
        input_id_file = keep_variants_file,
        input_id_col = 0
    }
  }

  call WdlUtils.get_index_of_true {
    input:
      values = CheckOverlapVariants.has_variant
  }

  Int num_valid_chromsome = length(get_index_of_true.indices)

  scatter(chrom_ind in range(num_valid_chromsome)){
    Int old_ind = get_index_of_true.indices[chrom_ind]

    call Plink2Utils.PgenFilter as PgenFilter_variants {
      input:
        input_pgen = input_pgen_files[old_ind],
        input_pvar = input_pvar_files[old_ind],
        input_psam = input_psam_files[old_ind],
        keep_variant_ids = keep_variants_file,
        keep_psam = CreateCohortPsam.output_psam,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chromosomes[old_ind]
    }
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = PgenFilter_variants.output_pgen,
        input_pvar_files = PgenFilter_variants.output_pvar,
        input_psam_files = PgenFilter_variants.output_psam,
        output_prefix = output_prefix
    }
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = select_first([MergePgenFiles.output_pgen, PgenFilter_variants.output_pgen[0]]),
        source_file2 = select_first([MergePgenFiles.output_pvar, PgenFilter_variants.output_pvar[0]]),
        source_file3 = select_first([MergePgenFiles.output_psam, PgenFilter_variants.output_psam[0]]),
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, select_first([MergePgenFiles.output_pgen, PgenFilter_variants.output_pgen[0]])])
    File output_pvar = select_first([CopyFile.output_file2, select_first([MergePgenFiles.output_pvar, PgenFilter_variants.output_pvar[0]])])
    File output_psam = select_first([CopyFile.output_file3, select_first([MergePgenFiles.output_psam, PgenFilter_variants.output_psam[0]])])
    Int output_num_variants = select_first([MergePgenFiles.num_variants, PgenFilter_variants.num_variants[0]])
    Int output_num_samples = select_first([MergePgenFiles.num_samples, PgenFilter_variants.num_samples[0]])
  }
}

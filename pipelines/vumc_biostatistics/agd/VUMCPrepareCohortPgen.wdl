version 1.0

# This workflow prepares cohort-specific PLINK2 genotype files by filtering and merging chromosome-specific files.
# Developed by VUMC Biostatistics for cohort genotype preparation.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Checks for overlapping variants between input genotypes and the cohort variant file
# 2. Filters input genotypes to keep only variants present in the cohort variant file
# 3. Restricts genotypes to the specified cohort samples
# 4. Merges filtered chromosome-specific files if multiple chromosomes are present
# 5. Optionally copies the prepared genotype files to a GCP storage location
#
# Inputs:
# - input_pvar/pgen/psam_files: PLINK2 format genotype files for each chromosome
# - cohort_variant: File containing variant IDs to retain in the prepared genotype files
# - cohort_psam: File containing cohort samples to retain in the prepared genotype files
# - output_prefix: Prefix for output files
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_pgen: Path to the prepared PLINK2 genotype file (.pgen format)
# - output_pvar: Path to the prepared PLINK2 variant information file (.pvar format)
# - output_psam: Path to the prepared PLINK2 sample information file (.psam format)
# - output_num_samples: Number of samples in the prepared genotype files
# - output_num_variants: Number of variants in the prepared genotype files

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils

workflow VUMCPrepareCohortPgen {
  input {
    Array[File] input_pvar_files
    Array[File] input_pgen_files
    Array[File] input_psam_files

    File cohort_variant
    File cohort_psam

    String output_prefix

    String? target_gcp_folder
  }

  Int num_all_chromsome = length(input_pvar_files)

  scatter(all_chrom_ind in range(num_all_chromsome)){
    call BioUtils.CheckOverlapVariantsByID as CheckOverlapVariants {
      input:
        input_pgen_pvar = input_pvar_files[all_chrom_ind],
        input_id_file = cohort_variant,
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
    File pgen_file = input_pgen_files[old_ind]
    File pvar_file = input_pvar_files[old_ind]
    File psam_file = input_psam_files[old_ind]

    call Plink2Utils.PgenFilter as PgenFilter_variants {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_psam = cohort_psam,
        keep_variant_ids = cohort_variant,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chrom_ind + ".snp"
    }
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = PgenFilter_variants.output_pgen,
        input_pvar_files = PgenFilter_variants.output_pvar,
        input_psam_files = PgenFilter_variants.output_psam,
        output_prefix = output_prefix + '.allchroms'
    }
  }

  File all_chroms_pgen = select_first([MergePgenFiles.output_pgen, PgenFilter_variants.output_pgen[0]])
  File all_chroms_pvar = select_first([MergePgenFiles.output_pvar, PgenFilter_variants.output_pvar[0]])
  File all_chroms_psam = select_first([MergePgenFiles.output_psam, PgenFilter_variants.output_psam[0]])
  
  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = all_chroms_pgen,
        source_file2 = all_chroms_pvar,
        source_file3 = all_chroms_psam,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, all_chroms_pgen])
    File output_pvar = select_first([CopyFile.output_file2, all_chroms_pvar])
    File output_psam = select_first([CopyFile.output_file3, all_chroms_psam])
    Int output_num_samples = select_first([MergePgenFiles.num_samples, PgenFilter_variants.num_samples[0]])
    Int output_num_variants = select_first([MergePgenFiles.num_variants, PgenFilter_variants.num_variants[0]])
  }
}

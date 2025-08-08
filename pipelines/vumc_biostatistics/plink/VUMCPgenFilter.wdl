version 1.0

## VUMC Plink2 PGEN Filter Workflow
##
## This workflow filters PGEN files using various filtering criteria including sample, variant, and genomic region filters.
## Developed by VUMC Biostatistics for genetic analysis projects.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline filters PGEN format files by applying sample, variant, and/or genomic region filters,
## and produces filtered PGEN, PVAR, and PSAM files as output.
##
## ### Workflow Steps:
## 1. PgenFilter: Filter PGEN files using provided sample, variant, and/or genomic region filters.
## 2. Optionally copy the output files to a specified GCP folder.
##
## ### Inputs:
## - input_pgen: Input PGEN file to be filtered
## - input_pvar: Input PVAR file containing variant information
## - input_psam: Input PSAM file containing sample information
## - keep_psam: Optional PSAM file specifying which samples to keep
## - keep_pvar: Optional PVAR file specifying which variants to keep, ID should match those in input_pvar
## - keep_bed: Optional BED file specifying genomic regions to keep
## - output_prefix: Prefix for output files
## - plink2_filter_option: Additional filtering options for Plink2
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_pgen: Path to the filtered PGEN file
## - output_pvar: Path to the filtered PVAR file
## - output_psam: Path to the filtered PSAM file
## - output_num_samples: Number of samples in the filtered dataset
## - output_num_variants: Number of variants in the filtered dataset
##
## ### Notes:
## - Uses GcpUtils for file copy operations.
## - File copy operation to GCP is optional and only executed if a target folder is provided.
## - All filter files (keep_psam, keep_pvar, keep_bed) are optional and can be used independently or in combination.
## 

import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPgenFilter {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    File? keep_psam
    File? keep_pvar
    File? keep_bed
    File? keep_variant_ids
        
    String output_prefix

    String plink2_filter_option = ""

    String? target_gcp_folder
  }

  call Plink2Utils.PgenFilter as PgenFilter {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      keep_psam = keep_psam,
      keep_pvar = keep_pvar,
      keep_bed = keep_bed,
      keep_variant_ids = keep_variant_ids,
      output_prefix = output_prefix,
      plink2_filter_option = plink2_filter_option
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = PgenFilter.output_pgen,
        source_file2 = PgenFilter.output_pvar,
        source_file3 = PgenFilter.output_psam,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_pgen = select_first([CopyFile.output_file1, PgenFilter.output_pgen])
    String output_pvar = select_first([CopyFile.output_file2, PgenFilter.output_pvar])
    String output_psam = select_first([CopyFile.output_file3, PgenFilter.output_psam])
    Int output_num_samples = PgenFilter.num_samples
    Int output_num_variants = PgenFilter.num_variants
  }
}

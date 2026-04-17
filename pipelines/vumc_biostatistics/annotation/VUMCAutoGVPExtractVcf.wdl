version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP Extract VCF by Region Sub-workflow
##
## This workflow extracts variants from a VCF file that overlap with a specified BED region.
## Developed by VUMC Biostatistics as a standalone test wrapper for the ExtractVcfByRegion task.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given a VCF file and a BED file defining genomic regions, this workflow extracts
## overlapping variants using bcftools and produces a filtered VCF.
##
## ### Workflow Steps:
## 1. **ExtractVcfByRegion**: Extract variants overlapping the BED region from the input VCF.
## 2. **CopyFile (Optional)**: Copy output to GCP if target_gcp_folder is provided.
##
## ### Inputs:
## - input_vcf: Input VCF file to filter.
## - input_vcf_index: Index file for the input VCF.
## - region_bed: BED file defining regions to extract.
## - output_prefix: Prefix for the output filtered VCF.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - filtered_vcf: VCF file containing only variants in the specified regions.
## - filtered_vcf_index: Index file for the filtered VCF.

import "VUMCAutoGVP.wdl" as AutoGVP
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAutoGVPExtractVcf {
  input {
    File input_vcf
    File input_vcf_index
    File region_bed
    String output_prefix

    String? target_gcp_folder
  }

  call AutoGVP.ExtractVcfByRegion {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      region_bed = region_bed,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = ExtractVcfByRegion.filtered_vcf,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File filtered_vcf = select_first([CopyFile.output_file, ExtractVcfByRegion.filtered_vcf])
    File filtered_vcf_index = ExtractVcfByRegion.filtered_vcf_index
  }
}

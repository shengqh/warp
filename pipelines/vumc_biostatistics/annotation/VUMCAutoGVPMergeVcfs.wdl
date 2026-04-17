version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP Merge VCFs Sub-workflow
##
## This workflow merges multiple VCF files into a single sorted VCF using bcftools.
## Developed by VUMC Biostatistics as a standalone test wrapper for the MergeVcfs task.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given an array of VCF files, this workflow concatenates them into a single merged,
## sorted, and indexed VCF file.
##
## ### Workflow Steps:
## 1. **MergeVcfs**: Merge input VCF files using bcftools concat and sort.
## 2. **CopyFile (Optional)**: Copy output to GCP if target_gcp_folder is provided.
##
## ### Inputs:
## - input_vcfs: Array of VCF files to merge.
## - target_prefix: Prefix for the merged output VCF.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - merged_vcf: Merged and sorted VCF file.
## - merged_vcf_index: Index file for the merged VCF.

import "VUMCAutoGVP.wdl" as AutoGVP
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAutoGVPMergeVcfs {
  input {
    Array[File] input_vcfs
    String target_prefix

    String? target_gcp_folder
  }

  call AutoGVP.MergeVcfs {
    input:
      input_vcfs = input_vcfs,
      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = MergeVcfs.merged_vcf,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File merged_vcf = select_first([CopyFile.output_file, MergeVcfs.merged_vcf])
    File merged_vcf_index = MergeVcfs.merged_vcf_index
  }
}

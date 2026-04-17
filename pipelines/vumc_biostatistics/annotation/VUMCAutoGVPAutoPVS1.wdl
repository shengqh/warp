version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP AutoPVS1 Sub-workflow
##
## This workflow runs AutoPVS1 to assess the PVS1 (null variant) pathogenicity criterion.
## Developed by VUMC Biostatistics as a standalone test wrapper for the RunAutoPVS1 task.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given a VEP-annotated VCF and AutoPVS1 data, this workflow runs AutoPVS1
## to evaluate the PVS1 criterion for variant pathogenicity classification.
## Supports both local AutoPVS1 data folder and cloud tar.gz archive.
##
## ### Workflow Steps:
## 1. **RunAutoPVS1**: Run AutoPVS1 for PVS1 criterion assessment with dynamically generated config.
## 2. **CopyFile (Optional)**: Copy output to GCP if target_gcp_folder is provided.
##
## ### Inputs:
## - input_vcf: VEP-annotated VCF file.
## - autopvs_data_folder: Optional local AutoPVS1 data directory.
## - autopvs_data_tar_gz: Optional AutoPVS1 data tar.gz archive (cloud).
## - autopvs_data_uncompressed_gb: Optional uncompressed data size in GB for disk estimation.
## - target_prefix: Prefix for the output AutoPVS1 results.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - autopvs1_file: AutoPVS1 PVS1 criterion results file.

import "VUMCAutoGVP.wdl" as AutoGVP
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAutoGVPAutoPVS1 {
  input {
    File input_vcf
    String? autopvs_data_folder
    File? autopvs_data_tar_gz
    Float? autopvs_data_uncompressed_gb

    String target_prefix

    String? target_gcp_folder
  }

  call AutoGVP.RunAutoPVS1 {
    input:
      input_vcf = input_vcf,
      autopvs_data_folder = autopvs_data_folder,
      autopvs_data_tar_gz = autopvs_data_tar_gz,
      autopvs_data_uncompressed_gb = autopvs_data_uncompressed_gb,
      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = RunAutoPVS1.autopvs1_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File autopvs1_file = select_first([CopyFile.output_file, RunAutoPVS1.autopvs1_file])
  }
}

version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP InterVar Sub-workflow
##
## This workflow runs InterVar for ACMG/AMP clinical variant interpretation.
## Developed by VUMC Biostatistics as a standalone test wrapper for the RunInterVar task.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given a VEP-annotated VCF and ANNOVAR database, this workflow runs InterVar
## to classify variants according to ACMG/AMP guidelines.
## Supports both local ANNOVAR database folder and cloud tar.gz archive.
##
## ### Workflow Steps:
## 1. **ValidateAnnovarDb**: Validate that at least one ANNOVAR database source is provided.
## 2. **RunInterVar**: Run InterVar for clinical variant interpretation.
## 3. **CopyFile (Optional)**: Copy output to GCP if target_gcp_folder is provided.
##
## ### Inputs:
## - input_vcf: VEP-annotated VCF file.
## - annovar_db_folder: Optional local ANNOVAR database directory.
## - annovar_db_tar_gz: Optional ANNOVAR database tar.gz archive (cloud).
## - annovar_db_uncompressed_gb: Optional uncompressed ANNOVAR DB size in GB for disk estimation.
## - target_prefix: Prefix for the output InterVar results.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - intervar_file: InterVar interpretation results file.

import "VUMCAutoGVP.wdl" as AutoGVP
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils

workflow VUMCAutoGVPInterVar {
  input {
    File input_vcf
    String? annovar_db_folder
    File? annovar_db_tar_gz
    String? annovar_db_tar_folder_name = "humandb"
    Float? annovar_db_uncompressed_gb

    String target_prefix

    String? target_gcp_folder
  }

  # Validate: at least one of annovar_db_folder or annovar_db_tar_gz must be provided
  if (!defined(annovar_db_folder) && !defined(annovar_db_tar_gz)) {
    call WDLUtils.FailWithMessage as ValidateAnnovarDb {
      input:
        message = "Either annovar_db_folder or annovar_db_tar_gz must be provided."
    }
  }

  call AutoGVP.RunInterVar {
    input:
      input_vcf = input_vcf,
      annovar_db_folder = annovar_db_folder,
      annovar_db_tar_gz = annovar_db_tar_gz,
      annovar_db_uncompressed_gb = annovar_db_uncompressed_gb,
      annovar_db_tar_folder_name = annovar_db_tar_folder_name,
      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = RunInterVar.intervar_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File intervar_file = select_first([CopyFile.output_file, RunInterVar.intervar_file])
  }
}

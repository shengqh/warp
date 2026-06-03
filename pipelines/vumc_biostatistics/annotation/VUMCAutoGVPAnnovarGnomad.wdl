version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP ANNOVAR gnomAD Sub-workflow
##
## This workflow runs ANNOVAR to annotate variants with gnomAD allele frequencies.
## Developed by VUMC Biostatistics as a standalone test wrapper for the RunAnnovarGnomad task.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given a VEP-annotated VCF and ANNOVAR database, this workflow runs ANNOVAR
## to add gnomAD population allele frequency annotations.
## Supports both local ANNOVAR database folder and cloud tar.gz archive.
##
## ### Workflow Steps:
## 1. **ValidateAnnovarDb**: Validate that at least one ANNOVAR database source is provided.
## 2. **RunAnnovarGnomad**: Run ANNOVAR for gnomAD allele frequency annotation.
## 3. **CopyFile (Optional)**: Copy output to GCP if target_gcp_folder is provided.
##
## ### Inputs:
## - input_vcf: VEP-annotated VCF file.
## - annovar_gnomAD_folder: gnomAD ANNOVAR database folder.
## - annovar_gnomAD_tar_gz: gnomAD ANNOVAR database tar.gz archive.
## - annovar_gnomAD_tar_folder_name: Optional folder name inside the gnomAD ANNOVAR tar.gz archive (default "humandb_gnomad41").
## - annovar_gnomAD_uncompressed_gb: Estimated uncompressed size of the gnomAD ANNOVAR database in GB (required if using tar.gz).
## - target_prefix: Prefix for the output ANNOVAR results.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - annovar_file: ANNOVAR gnomAD annotation results file.

import "VUMCAutoGVP.wdl" as AutoGVP
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils

workflow VUMCAutoGVPAnnovarGnomad {
  input {
    File input_vcf

    # Annovar database (tar.gz archive) for gnomAD annotation
    String? annovar_gnomAD_folder
    File? annovar_gnomAD_tar_gz
    String? annovar_gnomAD_tar_folder_name = "humandb_gnomad41"
    Float? annovar_gnomAD_uncompressed_gb

    String target_prefix

    String? target_gcp_folder
  }

  call AutoGVP.RunAnnovarGnomad {
    input:
      input_vcf = input_vcf,
      annovar_gnomAD_folder = annovar_gnomAD_folder,
      annovar_gnomAD_tar_gz = annovar_gnomAD_tar_gz,
      annovar_gnomAD_tar_folder_name = annovar_gnomAD_tar_folder_name,
      annovar_gnomAD_uncompressed_gb = annovar_gnomAD_uncompressed_gb,
      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = RunAnnovarGnomad.annovar_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File annovar_file = select_first([CopyFile.output_file, RunAnnovarGnomad.annovar_file])
  }
}

version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP VEP Annotation Sub-workflow
##
## This workflow annotates a VCF file using Ensembl VEP (Variant Effect Predictor).
## Developed by VUMC Biostatistics as a standalone test wrapper for the RunVEP task.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given a VCF file, reference genome, and VEP cache, this workflow runs VEP annotation
## to produce a VCF with variant effect predictions.
## Supports both local VEP cache folder and cloud tar.gz archive.
##
## ### Workflow Steps:
## 1. **RunVEP**: Annotate variants with VEP using the provided cache and reference genome.
## 2. **CopyFile (Optional)**: Copy output to GCP if target_gcp_folder is provided.
##
## ### Inputs:
## - input_vcf: Input VCF file to annotate.
## - genome_fasta: Reference genome FASTA file.
## - genome_fasta_fai: Reference genome FASTA index file.
## - vep_cache_folder: Optional local VEP cache directory.
## - vep_cache_tar_gz: Optional VEP cache tar.gz archive (cloud).
## - vep_cache_uncompressed_gb: Optional uncompressed VEP cache size in GB for disk estimation.
## - target_prefix: Prefix for the output VEP-annotated VCF.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - vep_vcf: VEP-annotated VCF file.

import "VUMCAutoGVP.wdl" as AutoGVP
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAutoGVPVEP {
  input {
    File input_vcf
    File genome_fasta
    File genome_fasta_fai
    String? vep_cache_folder
    File? vep_cache_tar_gz
    Float? vep_cache_uncompressed_gb

    String target_prefix

    String? target_gcp_folder
  }

  call AutoGVP.RunVEP {
    input:
      input_vcf = input_vcf,
      genome_fasta = genome_fasta,
      genome_fasta_fai = genome_fasta_fai,
      vep_cache_folder = vep_cache_folder,
      vep_cache_tar_gz = vep_cache_tar_gz,
      vep_cache_uncompressed_gb = vep_cache_uncompressed_gb,
      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = RunVEP.vep_vcf,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File vep_vcf = select_first([CopyFile.output_file, RunVEP.vep_vcf])
  }
}

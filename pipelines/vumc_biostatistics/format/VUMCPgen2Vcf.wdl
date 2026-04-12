version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC PGEN to VCF Conversion Workflow
##
## This workflow converts PLINK2 PGEN format genetic data files to VCF format.
## Developed by VUMC Biostatistics for population genetics format conversion.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## VCF is a widely used format for sharing genetic variant data. This workflow converts
## PGEN files to VCF format and optionally copies the result to GCS.
##
## ### Workflow Steps:
## 1. **Pgen2Vcf**: Uses plink2 to convert the PGEN files to VCF format.
## 2. **CopyFile (Optional)**: If `target_bucket` is provided, copies the VCF file
##    to the specified GCS bucket.
##
## ### Inputs:
## - input_pgen: PLINK2 PGEN file containing genotype data.
## - input_pvar: PLINK2 PVAR file containing variant information.
## - input_psam: PLINK2 PSAM file containing sample information.
## - output_prefix: Prefix for output filenames.
## - plink2_option: Optional additional plink2 command-line parameters.
## - docker: Docker image for plink2. Default is 'shengqh/plink_1.9_2.0:20250304'.
## - target_bucket: Optional GCS bucket path to copy output files to after completion.
##
## ### Outputs:
## - output_vcf: Generated VCF file.

import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix
    String? plink2_option

    String docker = "shengqh/plink_1.9_2.0:20250304"

    String? target_bucket
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    input_pgen: "PLINK2 PGEN file containing genotype data"
    input_pvar: "PLINK2 PVAR file containing variant information"
    input_psam: "PLINK2 PSAM file containing sample information"
    output_prefix: "Prefix for output filenames"
    plink2_option: "Optional additional plink2 command-line parameters"
    docker: "Docker image for plink2. Default is 'shengqh/plink_1.9_2.0:20250304'."
    target_bucket: "Optional GCS bucket path to copy output files to after completion"
  }

  call Plink2Utils.Pgen2Vcf {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      output_prefix = output_prefix,
      plink2_option = plink2_option,
      docker = docker
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = Pgen2Vcf.output_vcf,
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file, Pgen2Vcf.output_vcf])
  }
}

version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC PGEN to BGEN Conversion Workflow
##
## This workflow converts PLINK2 PGEN format genetic data files to BGEN format.
## Developed by VUMC Biostatistics for population genetics format conversion.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Some downstream tools (e.g., REGENIE, BOLT-LMM) require BGEN format. This workflow
## converts PGEN files to BGEN format with 8-bit encoding and optionally copies results to GCS.
##
## ### Workflow Steps:
## 1. **Pgen2Bgen**: Uses plink2 to convert the PGEN files to BGEN format.
## 2. **CopyFile (Optional)**: If `target_bucket` is provided, copies the BGEN and sample
##    files to the specified GCS bucket.
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
## - output_bgen: Generated BGEN file.
## - output_bgen_sample: Generated BGEN sample file.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPgen2Bgen {
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

  call Pgen2Bgen {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      output_prefix = output_prefix,
      plink2_option = plink2_option,
      docker = docker
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = Pgen2Bgen.output_bgen,
        source_file2 = Pgen2Bgen.output_sample,
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    File output_bgen = select_first([CopyFile.output_file1, Pgen2Bgen.output_bgen])
    File output_bgen_sample = select_first([CopyFile.output_file2, Pgen2Bgen.output_sample])
  }
}

task Pgen2Bgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam
    
    String? plink2_option

    String output_prefix
    
    String docker = "shengqh/plink_1.9_2.0:20250304"
    Int? memory_gb_override
    Int? disk_size_override
  }

  Int pgen_file_size = ceil(size([input_pgen, input_pvar, input_psam], "GB"))
  Int disk_size = select_first([disk_size_override, pgen_file_size * 3 + 20])
  Int memory_gb = select_first([memory_gb_override, pgen_file_size * 3])

  String target_bgen = output_prefix + ".bgen"
  String target_sample = output_prefix + ".sample"

  command <<<

plink2 ~{plink2_option} \
  --pgen ~{input_pgen} \
  --pvar ~{input_pvar} \
  --psam ~{input_psam} \
  --export bgen-1.2 bits=8 ref-first \
  --out ~{output_prefix} 

>>>

  runtime {
    docker: docker
    preemptible: 3
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_bgen = target_bgen
    File output_sample = target_sample
  }
}

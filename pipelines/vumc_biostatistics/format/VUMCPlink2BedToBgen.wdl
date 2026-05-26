version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC PLINK1 BED to BGEN Conversion Workflow
##
## This workflow converts PLINK1 BED format genetic data files to BGEN format.
## Developed by VUMC Biostatistics for population genetics format conversion.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Some downstream tools require BGEN format for association testing. This workflow converts
## PLINK1 BED/BIM/FAM files to BGEN format and optionally copies the results to GCS.
##
## ### Workflow Steps:
## 1. **Plink2BedToBgen**: Uses plink2 to convert the BED/BIM/FAM files to BGEN format.
## 2. **CopyFile (Optional)**: If `target_bucket` is provided, copies the BGEN and sample
##    files to the specified GCS bucket.
##
## ### Inputs:
## - input_bed: PLINK1 BED file containing genotype data.
## - input_bim: PLINK1 BIM file containing variant information.
## - input_fam: PLINK1 FAM file containing sample information.
## - output_prefix: Prefix for output filenames.
## - plink2_option: Optional additional plink2 command-line parameters.
## - docker: Docker image for plink2. Default is 'shengqh/plink_1.9_2.0:20260526'.
## - target_bucket: Optional GCS bucket path to copy output files to after completion.
##
## ### Outputs:
## - output_bgen: Generated BGEN file.
## - output_bgen_sample: Generated BGEN sample file.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPlink2BedToBgen {
  input {
    File input_bed
    File input_bim
    File input_fam

    String output_prefix
    String? plink2_option
    String docker = "shengqh/plink_1.9_2.0:20260526"

    String? target_bucket
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    input_bed: "PLINK1 BED file containing genotype data"
    input_bim: "PLINK1 BIM file containing variant information"
    input_fam: "PLINK1 FAM file containing sample information"
    output_prefix: "Prefix for output filenames"
    plink2_option: "Optional additional plink2 command-line parameters"
    docker: "Docker image for plink2. Default is 'shengqh/plink_1.9_2.0:20260526'."
    target_bucket: "Optional GCS bucket path to copy output files to after completion"
  }

  call Plink2BedToBgen {
    input:
      input_bed = input_bed,
      input_bim = input_bim,
      input_fam = input_fam,

      plink2_option = plink2_option,

      output_prefix = output_prefix,

      docker = docker
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = "~{Plink2BedToBgen.output_bgen}",
        source_file2 = "~{Plink2BedToBgen.output_bgen_sample}",
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    File output_bgen = select_first([CopyFile.output_file1, Plink2BedToBgen.output_bgen])
    File output_bgen_sample = select_first([CopyFile.output_file2, Plink2BedToBgen.output_bgen_sample])
  }
}

task Plink2BedToBgen {
  input {
    File input_bed
    File input_bim
    File input_fam

    String? plink2_option
    
    String output_prefix
    
    String docker = "shengqh/plink_1.9_2.0:20260526"
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([input_bed, input_bim, input_fam], "GB")  * 2) + 20

  String target_bgen = output_prefix + ".bgen"
  String target_bgen_sample = output_prefix + ".sample"

  command <<<

## convert plink to pgen
plink2 ~{plink2_option} \
  --bed ~{input_bed} \
  --bim ~{input_bim} \
  --fam ~{input_fam} \
  --export bgen-1.2 bits=8 ref-first \
  --out ~{output_prefix}

>>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_bgen = target_bgen
    File output_bgen_sample = target_bgen_sample
  }
}

version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC PGEN to PLINK1 BED Conversion Workflow
##
## This workflow converts PLINK2 PGEN format genetic data files to PLINK1 BED format.
## Developed by VUMC Biostatistics for population genetics format conversion.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Some downstream tools require PLINK1 BED format. This workflow converts PGEN files
## to BED/BIM/FAM format and optionally copies the results to GCS.
##
## ### Workflow Steps:
## 1. **Pgen2Bed**: Uses plink2 to convert the PGEN file to PLINK1 BED format.
## 2. **CopyFile (Optional)**: If `target_bucket` is provided, copies the BED/BIM/FAM
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
## - output_plink_bed: Generated PLINK1 BED file.
## - output_plink_bim: Generated PLINK1 BIM file.
## - output_plink_fam: Generated PLINK1 FAM file.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPgen2Bed {
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

  call Pgen2Bed {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      output_prefix = output_prefix,
      plink2_option = plink2_option,
      docker = docker
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = Pgen2Bed.output_plink_bed,
        source_file2 = Pgen2Bed.output_plink_bim,
        source_file3 = Pgen2Bed.output_plink_fam,
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    File output_plink_bed = select_first([CopyFile.output_file1, Pgen2Bed.output_plink_bed])
    File output_plink_bim = select_first([CopyFile.output_file2, Pgen2Bed.output_plink_bim])
    File output_plink_fam = select_first([CopyFile.output_file3, Pgen2Bed.output_plink_fam])
  }
}

task Pgen2Bed {
  input {
    File input_pgen
    File input_pvar
    File input_psam
    
    String? plink2_option

    String output_prefix
    
    String docker = "shengqh/plink_1.9_2.0:20250304"
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([input_pgen, input_pvar, input_psam], "GB")  * 15) + 20

  String target_plink_bed = output_prefix + ".bed"
  String target_plink_bim = output_prefix + ".bim"
  String target_plink_fam = output_prefix + ".fam"

  command <<<

plink2 ~{plink2_option} \
  --pgen ~{input_pgen} \
  --pvar ~{input_pvar} \
  --psam ~{input_psam} \
  --make-bed \
  --out ~{output_prefix} 

>>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_plink_bed = target_plink_bed
    File output_plink_bim = target_plink_bim
    File output_plink_fam = target_plink_fam
  }
}

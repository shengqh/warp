version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC PLINK1 BED to PGEN Conversion Workflow
##
## This workflow converts PLINK1 BED/BIM/FAM genetic data files to PLINK2 PGEN format.
## Developed by VUMC Biostatistics for population genetics format conversion.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Some downstream tools require PLINK2 PGEN format for association testing. This workflow
## converts PLINK1 BED/BIM/FAM files to PGEN format and optionally copies the results to GCS.
##
## ### Workflow Steps:
## 1. **Plink2BedToPgen**: Uses plink2 to convert BED/BIM/FAM files to PGEN, PSAM, and PVAR files.
## 2. **CopyFile (Optional)**: If `target_gcp_folder` is provided, copies the output files to the
##    specified GCS folder.
##
## ### Inputs:
## - input_bed: PLINK1 BED file containing genotype data.
## - input_bim: PLINK1 BIM file containing variant information.
## - input_fam: PLINK1 FAM file containing sample information.
## - output_prefix: Prefix for output filenames.
## - plink2_option: Optional additional plink2 command-line parameters.
## - docker: Docker image for plink2. Default is 'shengqh/plink_1.9_2.0:20260526'.
## - target_gcp_folder: Optional GCS folder path to copy output files to after completion.
##
## ### Outputs:
## - output_pgen: Generated PGEN genotype file.
## - output_psam: Generated PSAM sample file.
## - output_pvar: Generated PVAR variant file.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPlink2BedToPgen {
  input {
    File input_bed
    File input_bim
    File input_fam

    String output_prefix
    String? plink2_option
    String docker = "shengqh/plink_1.9_2.0:20260526"

    String? target_gcp_folder
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
    target_gcp_folder: "Optional GCS folder path to copy output files to after completion"
  }

  call Plink2BedToPgen {
    input:
      input_bed = input_bed,
      input_bim = input_bim,
      input_fam = input_fam,

      plink2_option = plink2_option,

      output_prefix = output_prefix,

      docker = docker
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = Plink2BedToPgen.output_pgen,
        source_file2 = Plink2BedToPgen.output_psam,
        source_file3 = Plink2BedToPgen.output_pvar,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, Plink2BedToPgen.output_pgen])
    File output_psam = select_first([CopyFile.output_file2, Plink2BedToPgen.output_psam])
    File output_pvar = select_first([CopyFile.output_file3, Plink2BedToPgen.output_pvar])
  }
}

task Plink2BedToPgen {
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

  command <<<

plink2 ~{plink2_option} \
  --bed ~{input_bed} \
  --bim ~{input_bim} \
  --fam ~{input_fam} \
  --make-pgen \
  --out ~{output_prefix}

>>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_pgen = "~{output_prefix}.pgen"
    File output_psam = "~{output_prefix}.psam"
    File output_pvar = "~{output_prefix}.pvar"
  }
}

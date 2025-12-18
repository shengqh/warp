version 1.0

## VUMC Untar Pair FASTQ Workflow
##
## This workflow extracts paired FASTQ files from tar.gz archives and optionally copies them to GCP storage.
## Developed by VUMC/VANGARD team for efficient paired-end sequencing data extraction and transfer.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## Paired-end sequencing data is often distributed as compressed tar.gz archives. This workflow
## handles the extraction of exactly two FASTQ files and provides optional cloud storage integration.
##
## ### Workflow Steps:
## 1. Extract paired FASTQ files from the input tar.gz archive
## 2. Identify the first and second reads from the extracted files
## 3. Optionally copy the extracted paired FASTQ files to a specified GCP folder
##
## ### Inputs:
## - input_tar_gz: Input tar.gz archive containing paired FASTQ files
## - output_extension: File extension to filter extracted files (default: ".fastq.gz")
## - target_gcp_folder: Optional target GCP folder for storing the extracted files
##
## ### Outputs:
## - fastq1: First FASTQ file (read 1)
## - fastq2: Second FASTQ file (read 2)
##
## ### Notes:
## - Expects exactly two FASTQ files in the archive
## - Uses tar command for extraction
## - File copy operation to GCP is optional and only executed if target_gcp_folder is provided
## - Disk space is automatically calculated based on archive size with a 3x factor and 5GB buffer

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCUntarPairFASTQ {
  input {
    File input_tar_gz

    String output_extension = ".fastq.gz"

    String? target_gcp_folder
  }

  call Untar {
    input:
      input_tar_gz = input_tar_gz,
      output_extension = output_extension,
  }

  File untar_fastq1 = Untar.output_files[0]
  File untar_fastq2 = Untar.output_files[1]

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyFiles as CopyFile {
      input:
        source_file1 = untar_fastq1,
        source_file2 = untar_fastq2,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File fastq1 = select_first([CopyFile.output_file1, untar_fastq1])
    File fastq2 = select_first([CopyFile.output_file2, untar_fastq2])
  }
}

task Untar {
  input {
    File input_tar_gz

    String output_extension

    Int memory_gb = 20
    Int cpu = 1
    Float disk_size_factor = 3.0
    Int additional_disk_gb = 5

    Int? disk_size_override

    String docker = "us.gcr.io/broad-dsde-methods/ubuntu:20.04"
  }

  Int disk_size = select_first([disk_size_override, ceil(disk_size_factor * size(input_tar_gz, "GB")) + additional_disk_gb])

  command <<<
set -euo pipefail

tar -xzvf "~{input_tar_gz}"

  >>>

  runtime {
    docker: docker
    memory: "~{memory_gb} GiB"
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    preemptible: 3
  }

  output {
    Array[File] output_files = glob("*~{output_extension}")
  }
}
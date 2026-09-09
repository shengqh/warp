version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC Untar Workflow
##
## This workflow extracts files from tar.gz archives and optionally copies them to GCP storage.
## Developed by VUMC/VANGARD team for efficient file extraction and transfer.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## Tar.gz archives are commonly used for compressing and bundling multiple files. This workflow
## handles the extraction process and provides optional cloud storage integration.
##
## ### Workflow Steps:
## 1. Extract files from the input tar.gz archive
## 2. Filter extracted files by specified extension
## 3. Optionally copy the extracted files to a specified GCP folder
##
## ### Inputs:
## - input_tar_gz: Input tar.gz archive file to be extracted
## - output_extension: File extension to filter extracted files (default: ".fastq.gz")
## - target_gcp_folder: Optional target GCP folder for the extracted files
##
## ### Outputs:
## - output_files: Array of extracted files matching the specified extension
##
## ### Notes:
## - Uses tar command for extraction
## - File copy operation to GCP is optional and only executed if a target folder is provided
## - Disk space is automatically calculated based on archive size with a configurable factor

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCUntar {
  input {
    File input_tar_gz

    String output_extension = ".fastq.gz"

    String? target_gcp_folder
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    input_tar_gz: "Input tar.gz archive file to be extracted"
    output_extension: "File extension to filter extracted files. Default is '.fastq.gz'."
    target_gcp_folder: "Optional GCP folder path to copy output files to after completion"
  }

  call Untar {
    input:
      input_tar_gz = input_tar_gz,
      output_extension = output_extension,
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyFileArray as CopyFile {
      input:
        source_files = Untar.output_files,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    Array[File] output_files = select_first([CopyFile.outputFiles, Untar.output_files])
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

status=$?
if [ $status -ne 0 ]; then
  echo "Error: Failed to extract ~{input_tar_gz}"
  exit $status
fi

mv */*~{output_extension} . 2>/dev/null || true 

echo "Successfully extracted ~{input_tar_gz}"
exit 0

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
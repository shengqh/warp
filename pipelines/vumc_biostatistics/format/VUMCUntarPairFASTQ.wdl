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
import "./VUMCUntar.wdl" as UntarModule

workflow VUMCUntarPairFASTQ {
  input {
    File input_tar_gz

    String output_extension = ".fastq.gz"

    String? target_gcp_folder
  }

  call UntarModule.Untar {
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

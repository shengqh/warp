version 1.0

## VUMC BGEN Indexing Workflow
##
## This workflow creates an index file for a BGEN format genetic data file.
## Developed by VUMC/VANGARD team for efficient processing of population genetic data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## BGEN is a file format for storing large genetic datasets used in population genetics.
## Creating an index (.bgi) file allows for efficient querying and access to variants in the BGEN file.
##
## ### Workflow Steps:
## 1. Run the bgenix tool to create an index (.bgi) file for the input BGEN file
## 2. Optionally copy the resulting index file to a specified GCP folder
##
## ### Inputs:
## - input_bgen: Input BGEN file to be indexed
## - reference_genome: Reference genome version (default: "GRCh38")
## - project_id: Optional GCP project ID for file copy operations
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - bgen_bgi_index: Generated BGEN index file (.bgi)
##
## ### Notes:
## - Uses bgenix which is optimized for efficient indexing of BGEN files
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCBgenIndex {
 
  input {
    File input_bgen

    String reference_genome = "GRCh38"

    String? project_id
    String? target_gcp_folder
  }

  call BgenIndex {
    input:
      input_bgen = input_bgen
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = BgenIndex.bgen_bgi_index,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File bgen_bgi_index = select_first([CopyFile.output_file, BgenIndex.bgen_bgi_index])
  }
}

task BgenIndex {
  input {
    File input_bgen

    String docker = "htgenomeanalysisunit/bgenix:2.2.0"
    Int memory_gb = 10
    Int preemptible = 0
    Int cpu = 1
    Int boot_disk_gb = 25
  }

  Int disk_size = ceil(size(input_bgen, "GB")) + boot_disk_gb + 20
  Int total_memory_gb = memory_gb + 2

  String basename_bgen = basename(input_bgen)
  String basename_bgen_bgi = basename_bgen + ".bgi"

  command <<<

ln -s ~{input_bgen} ~{basename_bgen}

bgenix -index -g ~{basename_bgen}

>>>


  runtime {
    cpu: cpu
    docker: docker
    preemptible: preemptible
    disks: "local-disk ~{disk_size} SSD"
    memory: "~{total_memory_gb} GiB"
    bootDiskSizeGb: boot_disk_gb
  }
  output {
    File bgen_bgi_index = basename_bgen_bgi
  }
}

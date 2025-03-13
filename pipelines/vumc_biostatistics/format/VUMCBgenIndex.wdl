version 1.0

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
        source_file = BgenIndex.bgen_index,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File bgen_index = select_first([CopyFile.output_file, BgenIndex.bgen_index])
  }
}

task BgenIndex {
  input {
    File input_bgen

    String docker = "htgenomeanalysisunit/bgenix:2.2.0"
    Int memory_gb = 10
    Int preemptible = 0
    Int cpu = 1
    Int? disk_size_override
    Int boot_disk_gb = 25
  }

  Int disk_size = ceil(size(input_bgen, "GB")) + 10
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
    File bgen_index = basename_bgen_bgi
  }
}

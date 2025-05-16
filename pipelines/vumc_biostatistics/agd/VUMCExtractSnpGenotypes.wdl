version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCExtractSnpGenotypes {
  input {
    File input_rsid_file
    String output_prefix

    String? billing_gcp_project_id
    String? target_gcp_folder    
  }

  call ConvertRsidToBed {
    input:
      input_rsid_file = input_rsid_file,
      output_prefix = output_prefix
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyOneFile {
      input:
        source_file = ConvertRsidToBed.output_bed,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_bed = select_first([MoveOrCopyOneFile.output_file, ConvertRsidToBed.output_bed])
  }
}

task ConvertRsidToBed {
  input {
    File input_rsid_file
    String dbSnp155_bb_file = "http://hgdownload.soe.ucsc.edu/gbdb/hg38/snp/dbSnp155.bb"
    String output_prefix
  }

  command {
    bigBedNamedItems -nameFile ~{dbSnp155_bb_file} ~{input_rsid_file} request.tmp.bed
    grep -v "_alt" request.tmp.bed > ~{output_prefix}.bed
    rm -f request.tmp.bed dbSnp155.bb
  }
  runtime {
    docker: "shengqh/ucsctools:latest"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "5 GiB"
  }
  output {
    File output_bed = "~{output_prefix}.bed"
  }
}

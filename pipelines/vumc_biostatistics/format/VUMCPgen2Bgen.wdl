version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPgen2Bgen {
  input {
    File source_pgen
    File source_pvar
    File source_psam

    String target_prefix
    String? plink2_option

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"

    String? project_id
    String? target_bucket
  }

  call Pgen2Bgen {
    input:
      source_pgen = source_pgen,
      source_pvar = source_pvar,
      source_psam = source_psam,
      target_prefix = target_prefix,
      plink2_option = plink2_option,
      docker = docker
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = Pgen2Bgen.output_bgen,
        source_file2 = Pgen2Bgen.output_sample,
        is_move_file = false,
        project_id = project_id,
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
    File source_pgen
    File source_pvar
    File source_psam
    
    String? plink2_option

    String target_prefix
    
    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([source_pgen, source_pvar, source_psam], "GB")  * 2) + 20

  String new_bgen = target_prefix + ".bgen"
  String new_sample = target_prefix + ".sample"

  command <<<

plink2 \
  --pgen ~{source_pgen} \
  --pvar ~{source_pvar} \
  --psam ~{source_psam} \
  --export bgen-1.2 bits=8 \
  --out ~{target_prefix} "~{plink2_option}"

>>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_bgen = new_bgen
    File output_sample = new_sample
  }
}

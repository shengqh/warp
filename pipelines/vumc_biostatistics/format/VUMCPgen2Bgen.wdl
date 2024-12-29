version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPgen2Bgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix
    String? plink2_option

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"

    String? project_id
    String? target_bucket
  }

  call Pgen2Bgen {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      output_prefix = output_prefix,
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
    File input_pgen
    File input_pvar
    File input_psam
    
    String? plink2_option

    String output_prefix
    
    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    Int memory_gb = 100
  }

  Int disk_size = ceil(size([input_pgen, input_pvar, input_psam], "GB")  * 3) + 20

  String target_bgen = output_prefix + ".bgen"
  String target_sample = output_prefix + ".sample"

  command <<<

plink2 ~{plink2_option} \
  --pgen ~{input_pgen} \
  --pvar ~{input_pvar} \
  --psam ~{input_psam} \
  --export bgen-1.2 bits=8 \
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
    File output_sample = target_sample
  }
}

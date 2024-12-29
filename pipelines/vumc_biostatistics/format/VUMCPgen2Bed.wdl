version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPgen2Bed {
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
        project_id = project_id,
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
    
    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
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

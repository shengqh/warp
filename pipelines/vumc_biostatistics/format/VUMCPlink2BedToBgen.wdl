version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPlink2BedToBgen {
  input {
    File input_bed
    File input_bim
    File input_fam

    String output_prefix
    String? plink2_option
    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"

    String? project_id
    String? target_bucket
  }

  call Plink2BedToBgen {
    input:
      input_bed = input_bed,
      input_bim = input_bim,
      input_fam = input_fam,

      plink2_option = plink2_option,

      output_prefix = output_prefix,

      docker = docker
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = "~{Plink2BedToBgen.output_bgen}",
        source_file2 = "~{Plink2BedToBgen.output_bgen_sample}",
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    File output_bgen = select_first([CopyFile.output_file1, Plink2BedToBgen.output_bgen])
    File output_bgen_sample = select_first([CopyFile.output_file2, Plink2BedToBgen.output_bgen_sample])
  }
}

task Plink2BedToBgen {
  input {
    File input_bed
    File input_bim
    File input_fam

    String? plink2_option
    
    String output_prefix
    
    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([input_bed, input_bim, input_fam], "GB")  * 2) + 20

  String target_bgen = output_prefix + ".bgen"
  String target_bgen_sample = output_prefix + ".sample"

  command <<<

## convert plink to pgen
plink2 ~{plink2_option} \
  --bed ~{input_bed} \
  --bim ~{input_bim} \
  --fam ~{input_fam} \
  --export bgen-1.2 bits=8 ref-first \
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
    File output_bgen_sample = target_bgen_sample
  }
}

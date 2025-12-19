version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPRScs {
  input {
    File input_bim

    Int n_gwas
    File input_sst

    # use files instead of .tar.gz to avoid unzipping the file, save time and disk space
    Array[File] ld_files
    # the folder name should contains either 1kg or ukbb, for example: "ldblk_ukbb_eur"
    String ld_folder_name

    String chromosomes = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
    
    String output_prefix

    String? target_gcp_folder
  }

  call WDLUtils.string_to_array {
    input:
      str = chromosomes,
      delimiter = ","
  }

  scatter (chromosome in string_to_array.arr) {
    String output_prefix_chromosome = output_prefix + ".chr" + chromosome

    call PRScs as PRScs {
      input:
        input_bim = input_bim,
        n_gwas = n_gwas,
        input_sst = input_sst,
        ld_files = ld_files,
        ld_folder_name = ld_folder_name,
        chromosome = chromosome,
        output_prefix = output_prefix_chromosome
    }
  }

  call WDLUtils.concat_files {
    input:
      input_files = PRScs.output_effort_file,
      output_file = output_prefix + ".effect.txt"
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = concat_files.concat_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_effect_file = select_first([CopyFile.output_file, concat_files.concat_file])
  }
}

task PRScs {
  input {
    File input_bim

    Int n_gwas
    File input_sst

    Int seed = 20250610
    String chromosome

    Array[File] ld_files
    String ld_folder_name

    String output_prefix

    String docker = "shengqh/prs:20250610"
    String PRSsc_script = "/opt/PRScs/PRScs.py"

    Int preemptible=3
    Int memory_gb = 10
    Int addtional_disk_space_gb = 10
  }

  Int disk_size = ceil(size([input_bim], "GB")) + ceil(size(ld_files, "GB")) + addtional_disk_space_gb

  command <<<
    ld_cur_folder=$(dirname ~{ld_files[1]})
    ln -s ${ld_cur_folder} ~{ld_folder_name}
    echo "ld_folder: ~{ld_folder_name} : $ld_cur_folder"

    bim_file="~{input_bim}"
    bim_prefix="${bim_file%.bim}"
    echo "bim_prefix: $bim_prefix"

    echo "Running PRScs ..."
    python3 ~{PRSsc_script} \
      --ref_dir=~{ld_folder_name} \
      --bim_prefix=${bim_prefix} \
      --sst_file=~{input_sst} \
      --chrom=~{chromosome} \
      --n_gwas=~{n_gwas} \
      --seed=~{seed} \
      --out=~{output_prefix}

    echo "Done ..."
  >>>

  runtime{
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
   }

  output {
    File output_effort_file = "~{output_prefix}_pst_eff_a1_b0.5_phiauto_chr~{chromosome}.txt"
  }
}

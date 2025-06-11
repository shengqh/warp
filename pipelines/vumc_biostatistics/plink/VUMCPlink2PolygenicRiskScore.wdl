version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPlink2PolygenicRiskScore {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    # if is_pgen is false, then input_pgen should be a .bed file
    Boolean is_pgen = true

    File input_score

    # for example, "2 4 6", 2:Variant IDs, 4:allele codes, 6:coefficients
    # for AGD dataset, the variant id should be: chr:pos:ref:alt
    String input_score_columns 

    String output_prefix

    String? project_id
    String? target_gcp_folder
  }

  call Plink2PolygenicRiskScore {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      is_pgen = is_pgen,
      input_score = input_score,
      input_score_columns = input_score_columns,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = Plink2PolygenicRiskScore.output_sscore,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_sscore = select_first([CopyFile.output_file, Plink2PolygenicRiskScore.output_sscore])
  }
}

task Plink2PolygenicRiskScore {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    Boolean is_pgen = true

    File input_score
    String input_score_columns

    String output_prefix

    String docker = "shengqh/plink_1.9_2.0:20250304"

    Int preemptible=3
    Int memory_gb = 40
    Int addtional_disk_space_gb = 10
  }

  Int disk_size = ceil(size([input_pgen, input_psam, input_pvar], "GB")) + addtional_disk_space_gb

  command <<<
    if [[ "~{is_pgen}" == "true" ]]; then
      plink2 \
        --pgen ~{input_pgen} \
        --pvar ~{input_pvar} \
        --psam ~{input_psam} \
        --score ~{input_score} ~{input_score_columns} \
        --out ~{output_prefix}
    else
      bed_prefix="${input_pgen%.bed}"
      plink2 \
        --bfile $bed_prefix \
        --score ~{input_score} ~{input_score_columns} \
        --out ~{output_prefix}
    fi
  >>>

  runtime{
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
   }

  output {
    File output_sscore = "~{output_prefix}.sscore"
  }
}

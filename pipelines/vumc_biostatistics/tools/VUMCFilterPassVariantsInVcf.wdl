version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCFilterPassVariantsInVcf {
  input {
    File input_vcf
    String target_prefix

    String? project_id
    String? target_gcp_folder
  }
  
  call FilterPassVariantsInVcf {
    input: 
      input_vcf = input_vcf,
      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    String filtered_vcf = "~{FilterPassVariantsInVcf.output_vcf}"
    String filtered_vcf_index = "~{FilterPassVariantsInVcf.output_vcf_index}"

    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = filtered_vcf,
        source_file2 = filtered_vcf_index,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file1, FilterPassVariantsInVcf.output_vcf])
    File output_vcf_index = select_first([CopyFile.output_file2, FilterPassVariantsInVcf.output_vcf_index])
  }
}

task FilterPassVariantsInVcf {
  input{
    # Command parameters
    File input_vcf

    String target_prefix

    String vcftools_docker = "us.gcr.io/broad-gotc-prod/imputation-bcf-vcf:1.0.7-1.10.2-0.1.16-1669908889"

    Int machine_mem_gb = 4
    Int addtional_disk_space_gb = 10
  }

  Int disk_size = ceil(size(input_vcf, "GB") * 2) + addtional_disk_space_gb

  String target_vcf = "~{target_prefix}.PASS.vcf.gz"

  command <<<
    # After test, awk is almost 2 times faster than bcftools filter for this specific task
    zcat ~{input_vcf} | awk '$7 == "PASS" || $1 ~ /^#/' | bgzip > ~{target_vcf}
    tabix -p vcf ~{target_vcf}
  >>>

  runtime{
    docker: vcftools_docker
    memory: machine_mem_gb + " GB"
    disks: "local-disk " + disk_size + " HDD"
  }

  output{
    File output_vcf = "~{target_vcf}"
    File output_vcf_index = "~{target_vcf}.tbi"
  }
}


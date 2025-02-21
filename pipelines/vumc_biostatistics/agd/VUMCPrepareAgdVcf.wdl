version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

#This workflow will do prepare the AGD VCF file for release
#1) Replace the GRID used in the AGD with the primary GRID based on ID map file
#2) Keep PASS variants only
workflow VUMCPrepareAgdVcf {
  input {
    File input_vcf
    File input_vcf_index

    File id_map_file

    String output_prefix

    String? project_id
    String? target_gcp_folder
  }
  
  call PrepareAgdVcf {
    input: 
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      id_map_file = id_map_file,
      output_prefix = output_prefix + ".primary_pass"
  }

  if(defined(target_gcp_folder)){
    String filtered_vcf = "~{PrepareAgdVcf.output_vcf}"
    String filtered_vcf_index = "~{PrepareAgdVcf.output_vcf_index}"

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
    File output_vcf = select_first([CopyFile.output_file1, PrepareAgdVcf.output_vcf])
    File output_vcf_index = select_first([CopyFile.output_file2, PrepareAgdVcf.output_vcf_index])
    Int num_samples = PrepareAgdVcf.num_samples
    Int num_variants = PrepareAgdVcf.num_variants
  }
}

task PrepareAgdVcf {
  input{
    File input_vcf
    File input_vcf_index

    File id_map_file

    String output_prefix

    String vcftools_docker = "us.gcr.io/broad-gotc-prod/imputation-bcf-vcf:1.0.7-1.10.2-0.1.16-1669908889"

    Int cpu = 4
    Int machine_mem_gb = 4
    Int addtional_disk_space_gb = 10
  }

  Int bgzip_thread = cpu -1
  Int disk_size = ceil(size(input_vcf, "GB") * 2) + addtional_disk_space_gb

  String target_vcf = "~{output_prefix}.vcf.gz"
  String output_sample_file = "~{output_prefix}.samples.txt"

  command <<<

total_variants=$(bcftools index -n ~{input_vcf})
echo total_variants=$total_variants

wget https://raw.githubusercontent.com/shengqh/agd_vcf/refs/heads/main/agd_vcf
chmod +x agd_vcf

echo agd_vcf ...
zcat ~{input_vcf} | ./agd_vcf --id_map_file=~{id_map_file} --total_variants=$total_variants | bgzip -@ ~{bgzip_thread} -c > ~{target_vcf}

echo tabix ...
tabix -p vcf ~{target_vcf}

bcftools query -l ~{target_vcf} > ~{output_sample_file}

cat ~{output_sample_file} | wc -l > num_samples.txt

bcftools index -n ~{target_vcf} > num_variants.txt

  >>>

  runtime{
    cpu: cpu
    docker: vcftools_docker
    preemptible: 0
    memory: machine_mem_gb + " GB"
    disks: "local-disk " + disk_size + " HDD"
  }

  output{
    File output_vcf = "~{target_vcf}"
    File output_vcf_index = "~{target_vcf}.tbi"
    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}


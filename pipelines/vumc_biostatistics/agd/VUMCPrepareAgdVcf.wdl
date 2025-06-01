version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow will do prepare the AGD VCF file for release
# 1) Replace the GRID used in the AGD with the primary GRID based on ID map file
# 2) Keep PASS variants only
# 
# Since prepare vcf cost a lot of space and not in preemptible node which cost a lot of money, 
# the task PrepareAgdVcf would generate vcf and the task VcfIndexAndInfo would generate index and get inforamtion.
# Since the VCF and index would be genreated by two tasks, target_gcp_folder would be highly recommended for put those 
# two files together.

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

  call VcfIndexAndInfo {
    input: 
      input_vcf = PrepareAgdVcf.output_vcf
  }

  if(defined(target_gcp_folder)){
    String filtered_vcf = "~{PrepareAgdVcf.output_vcf}"
    String filtered_vcf_index = "~{VcfIndexAndInfo.output_vcf_index}"

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
    File output_vcf_index = select_first([CopyFile.output_file2, VcfIndexAndInfo.output_vcf_index])
    Int num_samples = VcfIndexAndInfo.num_samples
    Int num_variants = VcfIndexAndInfo.num_variants
  }
}

task PrepareAgdVcf {
  input{
    File input_vcf
    File input_vcf_index

    File id_map_file

    String output_prefix

    String bcftools_docker = "shengqh/samtools_bcftools_tabix:v1.21"

    Int cpu = 3
    Int machine_mem_gb = 4
    Int addtional_disk_space_gb = 10
    Int preemptible = 0 # for shard vcf, we can use preemptible node, but for whole chromosome vcf, we should not use preemptible node
  }

  Int bgzip_thread = cpu - 2 #one cpu for zcat, one cpu for agd_vcf and other cpu for bgzip
  String bgzip_thread_str = if bgzip_thread > 1 then "-@ " + bgzip_thread else ""
  Int disk_size = ceil(size(input_vcf, "GB") * 2) + addtional_disk_space_gb

  String target_vcf = "~{output_prefix}.vcf.gz"

  command <<<

total_variants=$(bcftools index -n ~{input_vcf})
echo total_variants=$total_variants

echo `date`: agd_vcf ...
zcat ~{input_vcf} | agd_vcf --id_map_file=~{id_map_file} --total_variants=$total_variants | bgzip ~{bgzip_thread_str} -c > ~{target_vcf}

echo `date`: done.

  >>>

  runtime{
    cpu: cpu
    docker: bcftools_docker
    preemptible: preemptible
    memory: machine_mem_gb + " GB"
    disks: "local-disk " + disk_size + " HDD"
  }

  output{
    File output_vcf = "~{target_vcf}"
  }
}

task VcfIndexAndInfo {
  input{
    File input_vcf

    String bcftools_docker = "shengqh/samtools_bcftools_tabix:v1.21"

    Int cpu = 4
    Int machine_mem_gb = 4
    Int addtional_disk_space_gb = 5
  }

  Int disk_size = ceil(size(input_vcf, "GB")) + addtional_disk_space_gb

  String target_vcf = basename(input_vcf)
  String output_sample_file = "samples.txt"

  command <<<

ln -s ~{input_vcf} ~{target_vcf}

echo `date`: tabix ...
tabix -@ ~{cpu} -p vcf ~{target_vcf}

echo `date`: bcftools query number of samples ...
bcftools query -l ~{target_vcf} > ~{output_sample_file}

cat ~{output_sample_file} | wc -l > num_samples.txt

echo `date`: bcftools query number of variants ...
bcftools index -n ~{target_vcf} > num_variants.txt

echo `date`: done.

  >>>

  runtime{
    cpu: cpu
    docker: bcftools_docker
    preemptible: 1
    memory: machine_mem_gb + " GB"
    disks: "local-disk " + disk_size + " HDD"
  }

  output{
    File output_vcf_index = "~{target_vcf}.tbi"
    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}


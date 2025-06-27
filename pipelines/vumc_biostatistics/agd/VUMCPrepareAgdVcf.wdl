version 1.0

import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow prepares AGD VCF files by processing input VCF with ID mapping.
# The workflow performs the following steps:
# 1. Processes input VCF file using AGD tool with ID mapping file.
# 2. Indexes the processed VCF file and extracts sample/variant information.
# 3. Optionally copies the processed VCF file and index to a GCP folder.
#
# Input parameters:
# - input_vcf: The input VCF file to be processed.
# - id_map_file: File containing ID mappings for AGD processing.
# - output_prefix: Prefix for the output processed VCF file.
# - target_gcp_folder: Optional GCP folder to copy processed files to.
#
# Output parameters:
# - output_vcf: The merged VCF file.
# - output_vcf_index: The index file for the merged VCF.
#
# Note: The workflow uses bcftools concat with --naive option for merging files.

workflow VUMCPrepareAgdVcf {
  input {
    File input_vcf

    File id_map_file

    String output_prefix

    String? target_gcp_folder
  }
  
  call PrepareAgdVcf {
    input: 
      input_vcf = input_vcf,
      id_map_file = id_map_file,
      output_prefix = output_prefix + ".primary_pass"
  }

  call BioUtils.VcfIndexAndInfo {
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

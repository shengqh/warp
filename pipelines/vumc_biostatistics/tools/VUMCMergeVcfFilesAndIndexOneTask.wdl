version 1.0

# This workflow merges multiple VCF files into a single VCF file.
# The workflow performs the following steps:
# 1. Sorts input VCF files based on their start positions.
# 2. Concatenates the sorted VCF files using bcftools.
# 3. Indexes the merged VCF file.
# 4. Optionally copies the merged VCF file and index to a GCP folder.
#
# Input parameters:
# - input_vcfs: Array of VCF files to be merged.
# - input_vcf_indexes: Array of index files corresponding to the input VCFs.
# - input_vcf_startpos: Array of starting positions for each VCF file.
# - output_prefix: Prefix for the output merged VCF file.
# - billing_gcp_project_id: Optional GCP project ID for billing.
# - target_gcp_folder: Optional GCP folder to copy merged files to.
#
# Output parameters:
# - output_vcf: The merged VCF file.
# - output_vcf_index: The index file for the merged VCF.
#
# Note: The workflow uses bcftools concat with --naive option for merging files.

import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCMergeVcfFilesAndIndexOneTask {
  input {
    Array[File] input_vcfs
    Array[Int] input_vcf_startpos
    String output_prefix

    String? billing_gcp_project_id
    String? target_gcp_folder    
  }

  call MergeVcfFilesAndIndex as MergeVcfFiles {
    input:
      input_vcfs = input_vcfs,
      input_vcf_startpos = input_vcf_startpos,
      output_prefix = output_prefix
  }
  
  if(defined(target_gcp_folder)){
    String merged_vcf = "~{MergeVcfFiles.output_vcf}"
    String merged_vcf_index = "~{MergeVcfFiles.output_vcf_index}"

    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = merged_vcf,
        source_file2 = merged_vcf_index,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }
  output {
    File output_vcf = select_first([CopyFile.output_file1, MergeVcfFiles.output_vcf])
    File output_vcf_index = select_first([CopyFile.output_file2, MergeVcfFiles.output_vcf_index])
    Int output_vcf_num_variants = MergeVcfFiles.output_vcf_num_variants
  }
}

task MergeVcfFilesAndIndex {
  input {
    Array[File] input_vcfs
    Array[Int] input_vcf_startpos
    String output_prefix

    String bcftools_docker = "shengqh/samtools_bcftools_tabix:v1.21"

    Int preemptible = 0 
    Int machine_mem_gb = 10
    Float disk_size_multiplier = 3
    Int addtional_disk_space_gb = 10
    Int cpu = 4
  }

  Int disk_size = ceil(size(input_vcfs, "GB") * disk_size_multiplier) + addtional_disk_space_gb

  String target_vcf = "~{output_prefix}.vcf.gz"

  command <<<

# Sort vcf files based on start positions
paste ~{write_lines(input_vcfs)} ~{write_lines(input_vcf_startpos)} | sort -k2,2n | cut -f1 > sorted_vcfs.txt

# Concat VCF files using the list file
echo `date`: bcftools concat ...
bcftools concat -f sorted_vcfs.txt --naive -o ~{target_vcf} --threads ~{cpu}

echo `date`: bcftools index ...
bcftools index --tbi ~{target_vcf} --threads ~{cpu}

echo `date`: bcftools query number of variants ...
bcftools index -n ~{target_vcf} > num_variants.txt

echo `date`: done.

  >>>

  runtime {
    docker: bcftools_docker
    memory: "~{machine_mem_gb} GB"
    disks: "local-disk ~{disk_size} HDD"
    preemptible: preemptible
    cpu: cpu
  }

  output {
    File output_vcf = target_vcf
    File output_vcf_index = "~{target_vcf}.tbi"
    Int output_vcf_num_variants = read_int("num_variants.txt")
  }
}
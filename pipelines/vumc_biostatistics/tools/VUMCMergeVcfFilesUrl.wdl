version 1.0

# This workflow merges multiple VCF files into a single VCF file.
# The workflow performs the following steps:
# 1. Sorts input VCF files based on their start positions.
# 2. Concatenates the sorted VCF files using bcftools.
# 3. Outputs the merged VCF file to the specified GCP folder.
#
# Input parameters:
# - input_vcfs: Array of VCF files to be merged.
# - input_vcf_startpos: Array of starting positions for each VCF file.
# - output_prefix: Prefix for the output merged VCF file.
# - target_gcp_folder: GCP folder where the merged VCF file will be stored.
#
# Output parameters:
# - output_vcf: The merged VCF file URL in GCP.
#
# Note: The workflow uses bcftools concat with --naive option for merging files.


workflow VUMCMergeVcfFilesUrl {
  input {
    Array[String] input_vcfs
    Array[Int] input_vcf_startpos
    String output_prefix
    String target_gcp_folder    
  }

  call MergeVcfFiles {
    input:
      input_vcfs = input_vcfs,
      input_vcf_startpos = input_vcf_startpos,
      output_prefix = output_prefix,
      target_gcp_folder = target_gcp_folder
  }
  
  output {
    File output_vcf = MergeVcfFiles.output_vcf
  }
}

task MergeVcfFiles {
  input {
    Array[String] input_vcfs
    Array[Int] input_vcf_startpos
    String output_prefix
    String target_gcp_folder    

    String bcftools_docker = "shengqh/samtools_bcftools_tabix:v1.21"

    Int machine_mem_gb = 4
    Int disk_size_gb = 10
    Int cpu = 1
  }

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")
  String target_vcf = "~{gcs_output_dir}/~{output_prefix}.vcf.gz"

  command <<<

# Sort vcf files based on start positions
paste ~{write_lines(input_vcfs)} ~{write_lines(input_vcf_startpos)} | sort -k2,2n | cut -f1 > sorted_vcfs.txt

# Concat VCF files using the list file
echo `date`: bcftools concat ...
bcftools concat -f sorted_vcfs.txt --naive -o ~{target_vcf} --threads ~{cpu}

echo `date`: done.

  >>>

  runtime {
    docker: bcftools_docker
    memory: "~{machine_mem_gb} GB"
    disks: "local-disk ~{disk_size_gb} HDD"
    cpu: cpu
  }

  output {
    File output_vcf = target_vcf
  }
}
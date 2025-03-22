version 1.0

## VUMC VCF Information Extraction Workflow
##
## This workflow extracts sample information and basic statistics from VCF files.
## Developed by VUMC/VANGARD team for efficient analysis of genetic data files.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This workflow extracts metadata and basic statistics from VCF files, such as
## sample names, sample counts, and variant counts.
##
## ### Workflow Steps:
## 1. Extract sample IDs from the VCF header
## 2. Count the number of samples in the VCF file
## 3. Count the number of variants in the VCF file
## 4. Optionally copy the resulting sample list file to a specified GCP folder
##
## ### Inputs:
## - input_vcf: Input VCF file to analyze
## - input_vcf_index: Index file for the input VCF
## - output_prefix: Prefix for output filenames
## - project_id: Optional GCP project ID for file copy operations
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - samples_file: File containing the list of sample IDs
## - num_samples: Total number of samples found in the VCF
## - num_variants: Total number of variants found in the VCF
##
## ### Notes:
## - Uses bcftools for efficient extraction of VCF metadata
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCVcfInfo {
  input {
    File input_vcf
    File input_vcf_index
    String output_prefix

    String? project_id
    String? target_gcp_folder    
  }

  call VcfInfo {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = VcfInfo.samples_file,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File samples_file = select_first([CopyFile.output_file, VcfInfo.samples_file])
    Int num_samples = VcfInfo.num_samples
    Int num_variants = VcfInfo.num_variants
  }
}

task VcfInfo {
  input {
    File input_vcf
    File input_vcf_index
    String output_prefix
    String docker = "shengqh/samtools_bcftools_tabix:v1.21"
  }

  Int disk_size = ceil(size([input_vcf, input_vcf_index], "GB")) + 10

  command <<<
    # Extract samples from VCF header
    bcftools query -l ~{input_vcf} > ~{output_prefix}.samples.txt
    
    # Count samples
    cat ~{output_prefix}.samples.txt | wc -l > num_samples.txt

    # Count variants
    bcftools index -n ~{input_vcf} > num_variants.txt
  >>>

  runtime {
    docker: docker
    preemptible: 3
    disks: "local-disk " + disk_size + " HDD"
    memory: "2 GiB"
  }
  
  output {
    File samples_file = "~{output_prefix}.samples.txt"
    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}

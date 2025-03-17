version 1.0

## VUMC VCF to BGEN Conversion Workflow
##
## This workflow converts VCF format genetic data files to BGEN format.
## Developed by VUMC/VANGARD team for efficient processing of population genetic data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## VCF files are commonly used for storing genetic variants, but BGEN format offers 
## advantages for certain analyses. This workflow handles the conversion process.
##
## ### Workflow Steps:
## 1. Run plink2 to convert VCF file to BGEN format with 8-bit encoding
## 2. Optionally copy the resulting BGEN and sample files to a specified GCP folder
##
## ### Inputs:
## - input_vcf: Input VCF file to be converted
## - input_vcf_index: Index file for the input VCF
## - input_psam: Optional PSAM file for additional sample information
## - output_prefix: Prefix for output filenames
## - project_id: Optional GCP project ID for file copy operations
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_bgen: Generated BGEN file
## - output_sample: Generated sample file
##
## ### Notes:
## - Uses plink2 with settings optimized for BGEN conversion
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
    wc -l ~{output_prefix}.samples.txt | cut -d ' ' -f 1 > num_samples.txt

    # Count variants
    bcftools view -H ~{input_vcf} | wc -l | cut -d ' ' -f 1 > num_variants.txt
  >>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "2 GiB"
  }
  
  output {
    File samples_file = "~{output_prefix}.samples.txt"
    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}

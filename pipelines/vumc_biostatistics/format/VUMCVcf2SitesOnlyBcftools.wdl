version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC VCF Sites-Only Conversion Workflow
##
## This workflow generates a VCF file that contains only the sites information, removing genotype data.
## Developed by VUMC/VANGARD team for efficient processing of population genetic data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## When variant positions are needed without the full genotype information, a sites-only VCF
## is more efficient. This workflow creates a simplified VCF file containing only the sites.
##
## ### Workflow Steps:
## 1. Use bcftools to generate a sites-only version of the input VCF
## 2. Create an index for the sites-only VCF using tabix
## 3. Optionally copy the resulting files to a specified GCP folder
##
## ### Inputs:
## - input_vcf: Input VCF file to be processed
## - input_vcf_index: Index file for the input VCF
## - output_prefix: Prefix for output filenames
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_sites_vcf: Generated sites-only VCF file
## - output_sites_vcf_index: Index file for the sites-only VCF
##
## ### Notes:
## - Uses bcftools to remove genotype fields while preserving site information
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCVcf2SitesOnlyBcftools {
  input {
    File input_vcf
    File input_vcf_index

    String output_prefix

    String? target_gcp_folder
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    input_vcf: "Input VCF file to be processed"
    input_vcf_index: "Index file for the input VCF"
    output_prefix: "Prefix for output filenames"
    target_gcp_folder: "Optional GCP folder path to copy output files to after completion"
  }

  call Vcf2SitesOnlyBcftools {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      output_prefix = output_prefix,
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = Vcf2SitesOnlyBcftools.output_sites_vcf,
        source_file2 = Vcf2SitesOnlyBcftools.output_sites_vcf_index,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_sites_vcf = select_first([CopyFile.output_file1, Vcf2SitesOnlyBcftools.output_sites_vcf])
    File output_sites_vcf_index = select_first([CopyFile.output_file2, Vcf2SitesOnlyBcftools.output_sites_vcf_index])
  }
}

task Vcf2SitesOnlyBcftools {
  input {
    File input_vcf
    File input_vcf_index
    String output_prefix
    Int memory_gb = 20
    Int? disk_size_override
    String docker = "us.gcr.io/broad-gatk/gatk:4.5.0.0"
  }

  # The maximum file size of chr1 sites file is only 1G for AGD250K, we don't need a lot of additional disk
  Int disk_size = ceil(select_first([disk_size_override, ceil(size(input_vcf, "GB")) + 10]))

  command <<<
set -euo pipefail

echo target=~{output_prefix}.sites.vcf.gz

bcftools view -G ~{input_vcf} -O z -o ~{output_prefix}.sites.vcf.gz

tabix -f -p vcf ~{output_prefix}.sites.vcf.gz

  >>>

  runtime {
    docker: docker
    memory: "~{memory_gb} GiB"
    cpu: 1
    disks: "local-disk " + disk_size + " HDD"
    preemptible: 3
  }

  output {
    File output_sites_vcf = "~{output_prefix}.sites.vcf.gz"
    File output_sites_vcf_index = "~{output_prefix}.sites.vcf.gz.tbi"
  }
}

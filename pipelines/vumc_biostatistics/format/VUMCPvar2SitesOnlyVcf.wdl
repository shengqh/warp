version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC PVAR to Sites-Only VCF Conversion Workflow
##
## This workflow converts a PLINK2 PVAR file into a bgzip-compressed, indexed sites-only VCF.
## Developed by VUMC Biostatistics for population genetics format conversion.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Some downstream tools require a VCF that contains only site-level variant information
## without sample genotype data. This workflow reformats a PVAR file into a sites-only VCF
## and optionally copies the compressed VCF and index to GCS.
##
## ### Workflow Steps:
## 1. **Pvar2SitesOnlyVcf**: Builds a sites-only VCF from the input PVAR file and creates
##    a tabix index for the compressed output.
## 2. **CopyFile (Optional)**: If `target_bucket` is provided, copies the VCF and index
##    files to the specified GCS bucket.
##
## ### Inputs:
## - input_pvar: PLINK2 PVAR file containing variant information.
## - output_prefix: Prefix for output filenames.
## - target_bucket: Optional GCS bucket path to copy output files to after completion.
##
## ### Outputs:
## - output_sites_only_vcf: Generated bgzip-compressed sites-only VCF file.
## - output_sites_only_vcf_tbi: Tabix index for the sites-only VCF file.
##
## ### Notes:
## - The output VCF includes the standard VCF header plus CHROM, POS, ID, REF, ALT, QUAL,
##   FILTER, and INFO columns derived directly from the PVAR records.
## - The workflow expects the runtime image to provide standard Unix text utilities,
##   bgzip, and tabix.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
#import "https://raw.githubusercontent.com/shengqh/warp/refs/heads/develop/tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPvar2SitesOnlyVcf {
  input {
    File input_pvar

    String output_prefix

    String docker = "shengqh/samtools_bcftools_tabix:v1.23"

    String? target_bucket
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    input_pvar: "PLINK2 PVAR file containing variant information"
    output_prefix: "Prefix for output filenames"
    docker: "Docker image containing the tools required to generate and index the sites-only VCF. Default is 'shengqh/plink_1.9_2.0:20250304'."
    target_bucket: "Optional GCS bucket path to copy output files to after completion"
  }

  call Pvar2SitesOnlyVcf {
    input:
      input_pvar = input_pvar,
      output_prefix = output_prefix,
      docker = docker
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyFiles as CopyFile {
      input:
        source_file1 = Pvar2SitesOnlyVcf.output_sites_only_vcf,
        source_file2 = Pvar2SitesOnlyVcf.output_sites_only_vcf_tbi,
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    File output_sites_only_vcf = select_first([CopyFile.output_file1, Pvar2SitesOnlyVcf.output_sites_only_vcf])
    File output_sites_only_vcf_tbi = select_first([CopyFile.output_file2, Pvar2SitesOnlyVcf.output_sites_only_vcf_tbi])
  }
}

task Pvar2SitesOnlyVcf {
  input {
    File input_pvar

    String output_prefix
    
    String docker = "shengqh/plink_1.9_2.0:20250304"
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([input_pvar], "GB")) + 10

  String target_sites_only_vcf = output_prefix + ".sites.vcf.gz"

  command <<<


(echo "##fileformat=VCFv4.2"; \
 echo "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"; \
 grep -v '^#' ~{input_pvar} | \
 awk '{print $1"\t"$2"\t"$3"\t"$4"\t"$5"\t.\t.\t."}') > ~{output_prefix}.sites.vcf

bgzip ~{output_prefix}.sites.vcf
tabix -p vcf ~{output_prefix}.sites.vcf.gz

>>>

  runtime {
    docker: docker
    preemptible: 3
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_sites_only_vcf = target_sites_only_vcf
    File output_sites_only_vcf_tbi = target_sites_only_vcf + ".tbi"
  }
}

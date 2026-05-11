version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC Variant ID to Sites-Only VCF Conversion Workflow
##
## This workflow converts a file containing colon-delimited variant IDs (CHROM:POS:REF:ALT)
## into a bgzip-compressed, indexed sites-only VCF.
## Developed by VUMC Biostatistics for population genetics format conversion.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Some downstream tools require a VCF that contains only site-level variant information
## without sample genotype data. This workflow parses variant IDs of the form
## CHROM:POS:REF:ALT from a specified column of the input file and reformats them into
## a sites-only VCF, then optionally copies the compressed VCF and index to GCS.
##
## ### Workflow Steps:
## 1. **VariantID2SitesOnlyVcf**: Parses variant IDs from the input file using the specified
##    column, builds a sites-only VCF, compresses it with bgzip, and creates a tabix index.
## 2. **CopyFile (Optional)**: If `target_bucket` is provided, copies the VCF and index
##    files to the specified GCS bucket.
##
## ### Inputs:
## - input_file: File containing variant IDs in the format CHROM:POS:REF:ALT.
## - input_id_col: Column number (1-based) in the input file that contains the variant ID. 
##                 3 for PVAR file.
## - output_prefix: Prefix for output filenames.
## - target_bucket: Optional GCS bucket path to copy output files to after completion.
##
## ### Outputs:
## - output_sites_only_vcf: Generated bgzip-compressed sites-only VCF file.
## - output_sites_only_vcf_tbi: Tabix index for the sites-only VCF file.
##
## ### Notes:
## - The output VCF includes the standard VCF header plus CHROM, POS, ID, REF, ALT, QUAL,
##   FILTER, and INFO columns derived by splitting the variant ID on ':'.
## - Comment lines (starting with '#') in the input file are skipped.
## - The workflow expects the runtime image to provide standard Unix text utilities,
##   bgzip, and tabix.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCVariantID2SitesOnlyVcf {
  input {
    File input_file
    Int input_id_col

    String output_prefix

    String docker = "shengqh/samtools_bcftools_tabix:v1.23"

    String? target_bucket
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    input_file: "File containing variant information"
    input_id_col: "Column number for the variant ID"
    output_prefix: "Prefix for output filenames"
    docker: "Docker image containing the tools required to generate and index the sites-only VCF. Default is 'shengqh/samtools_bcftools_tabix:v1.23'."
    target_bucket: "Optional GCS bucket path to copy output files to after completion"
  }

  call VariantID2SitesOnlyVcf {
    input:
      input_file = input_file,
      input_id_col = input_id_col,
      output_prefix = output_prefix,
      docker = docker
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyFiles as CopyFile {
      input:
        source_file1 = VariantID2SitesOnlyVcf.output_sites_only_vcf,
        source_file2 = VariantID2SitesOnlyVcf.output_sites_only_vcf_tbi,
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    File output_sites_only_vcf = select_first([CopyFile.output_file1, VariantID2SitesOnlyVcf.output_sites_only_vcf])
    File output_sites_only_vcf_tbi = select_first([CopyFile.output_file2, VariantID2SitesOnlyVcf.output_sites_only_vcf_tbi])
  }
}

task VariantID2SitesOnlyVcf {
  input {
    File input_file
    Int input_id_col

    String output_prefix

    String docker = "shengqh/samtools_bcftools_tabix:v1.23"
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([input_file], "GB")) + 10

  String target_sites_only_vcf = output_prefix + ".sites.vcf.gz"

  command <<<


(echo "##fileformat=VCFv4.2"; \
 echo "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"; \
 grep -v '^#' ~{input_file} | \
 awk -v id_col=~{input_id_col} '{split($id_col, v, ":"); print v[1]"\t"v[2]"\t"$id_col"\t"v[3]"\t"v[4]"\t.\t.\t."}') > ~{output_prefix}.sites.vcf

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

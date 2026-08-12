version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC PVAR to Sites-Only VCF Conversion Workflow
##
## This workflow converts a PLINK2 PVAR file into a bgzip-compressed, tabix-indexed
## sites-only VCF with configurable chromosome-prefix handling and variant ID generation.
## Developed by VUMC Biostatistics for population genetics and downstream interoperability.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Some downstream tools require a VCF that contains only site-level variant information
## without sample genotype data. This workflow reformats a PVAR file into a sites-only VCF,
## optionally prefixes chromosome names with `chr`, optionally creates variant IDs, and can
## copy the compressed VCF and index to GCS.
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
## - add_chr_prefix: Whether to prepend `chr` to chromosome names.
## - create_variant_id: Whether to generate IDs as CHROM:POS:REF:ALT.
## - docker: Runtime image containing tools for VCF creation and indexing.
## - target_bucket: Optional GCS bucket path to copy output files to after completion.
##
## ### Outputs:
## - output_sites_only_vcf: Generated bgzip-compressed sites-only VCF file.
## - output_sites_only_vcf_tbi: Tabix index for the sites-only VCF file.
##
## ### Notes:
## - The output VCF includes the standard VCF header plus CHROM, POS, ID, REF, ALT, QUAL,
##   FILTER, and INFO columns derived directly from PVAR records.
## - When `create_variant_id=true`, ID is generated as CHROM:POS:REF:ALT; otherwise the
##   original PVAR ID field is used.
## - The workflow expects the runtime image to provide standard Unix text utilities,
##   bgzip, and tabix.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
#import "https://raw.githubusercontent.com/shengqh/warp/refs/heads/develop/tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPvar2SitesOnlyVcf {
  input {
    File input_pvar

    Boolean add_chr_prefix = true

    Boolean create_variant_id = true

    String output_prefix

    String docker = "shengqh/samtools_bcftools_tabix:v1.23"

    String? target_bucket
  }

  meta {
    allowNestedInputs: true
  }

  call Pvar2SitesOnlyVcf {
    input:
      input_pvar = input_pvar,
      add_chr_prefix = add_chr_prefix,
      create_variant_id = create_variant_id,
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
    Boolean add_chr_prefix
    Boolean create_variant_id
    String output_prefix
    
    String docker = "shengqh/plink_1.9_2.0:20260526"
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([input_pvar], "GB")) + 10

  String chr_prefix = if add_chr_prefix then '"chr"' else '""'

  command <<<

echo "##fileformat=VCFv4.2" > ~{output_prefix}.sites.vcf
grep "^#" ~{input_pvar} >> ~{output_prefix}.sites.vcf

if [[ "~{create_variant_id}" == "true" ]]; then
  grep -v '^#' ~{input_pvar} | \
  awk '{
    chr = ~{chr_prefix} $1
    if (chr == "MT") chr = "M"
    else if (chr == "chrMT") chr = "chrM"

    print chr "\t" $2 "\t" chr ":" $2 ":" $4 ":" $5 "\t" $4 "\t" $5 "\t" $6 "\t" $7 "\t" $8
  }' >> ~{output_prefix}.sites.vcf
else
  grep -v '^#' ~{input_pvar} | \
  awk '{
    chr = ~{chr_prefix} $1
    if (chr == "MT") chr = "M"
    else if (chr == "chrMT") chr = "chrM"
    
    print chr "\t" $2 "\t" $3 "\t" $4 "\t" $5 "\t" $6 "\t" $7 "\t" $8
  }' >> ~{output_prefix}.sites.vcf
fi

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
    File output_sites_only_vcf = output_prefix + ".sites.vcf.gz"
    File output_sites_only_vcf_tbi = output_prefix + ".sites.vcf.gz.tbi"
  }
}

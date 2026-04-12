version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC VCF Sample Extraction and Allele Frequency Filtering Workflow
##
## This workflow extracts specific samples from a VCF file and filters variants based on allele frequency.
## Developed by VUMC/VANGARD team for efficient processing of population genetic data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## When only a subset of samples and variants with non-zero allele frequency are needed, this workflow
## efficiently extracts the samples and filters the variants to produce a cleaner, more focused VCF.
##
## ### Workflow Steps:
## 1. Use bcftools to extract specified samples 
## 2. Empty INFO, QUAL, FILTER fields, and keep GT only
## 3. Normalize variants (multiallelic to biallelic) and calculate allele frequencies using reference genome
## 4. Filter variants to keep only those with non-zero allele frequency
## 5. Convert from biallelic to multiallelic 
## 6. Empty the INFO field and output the filtered VCF
## 7. Create an index for the filtered VCF
## 8. Optionally copy the resulting files to a specified GCP folder
##
## ### Inputs:
## - input_vcf: Input VCF file to be processed
## - input_vcf_index: Index file for the input VCF
## - ref_fasta: Reference genome FASTA file for variant normalization
## - include_samples: File containing sample IDs to include
## - bcftools_view_option: Optional parameters for bcftools view
## - target_prefix: Prefix for output filenames
## - target_suffix: Suffix for output filenames (default: .vcf.gz)
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_vcf: Filtered VCF file containing only specified samples and non-zero AF variants
## - output_vcf_index: Index file for the filtered VCF
## - output_vcf_sample: File containing sample IDs in the filtered VCF
## - output_vcf_num_samples: Number of samples in the filtered VCF
## - output_vcf_num_variants: Number of variants in the filtered VCF
##
## ### Notes:
## - The workflow removes all annotations except GT, normalizes variants, and recalculates AF
## - Only variants with AF > 0 are retained in the final output
## - Compatible with multi-allelic sites through proper normalization

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCVcfExtractSampleAndFilterAF {
  input {
    File input_vcf
    File input_vcf_index
    File ref_fasta
    File include_samples

    String bcftools_view_option = ""

    String target_prefix
    String target_suffix = ".vcf.gz"

    String? target_gcp_folder
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    input_vcf: "Input VCF file to be processed"
    input_vcf_index: "Index file for the input VCF"
    ref_fasta: "Reference genome FASTA file for variant normalization"
    include_samples: "File containing sample IDs to extract from the VCF"
    bcftools_view_option: "Optional additional parameters passed to bcftools view. Default is empty."
    target_prefix: "Prefix for output filenames"
    target_suffix: "Suffix for output filenames. Default is '.vcf.gz'."
    target_gcp_folder: "Optional GCP folder path to copy output files to after completion"
  }

  call BcftoolsExtractSamplesAndGTOnly {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      bcftools_view_option = bcftools_view_option,
      include_samples = include_samples,
      target_prefix = target_prefix,
      target_suffix = ".extract.GTonly.vcf.gz"
  }

  call BcftoolsNormAndFilterAF {
    input:
      input_vcf = BcftoolsExtractSamplesAndGTOnly.output_vcf,
      input_vcf_index = BcftoolsExtractSamplesAndGTOnly.output_vcf_index,
      ref_fasta = ref_fasta,
      target_prefix = target_prefix,
      target_suffix = ".norm.AF_filtered.vcf.gz"
  }

  call BcftoolsMultiallelicAndSlim {
    input:
      input_vcf = BcftoolsNormAndFilterAF.output_vcf,
      input_vcf_index = BcftoolsNormAndFilterAF.output_vcf_index,
      ref_fasta = ref_fasta,
      target_prefix = target_prefix,
      target_suffix = target_suffix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = BcftoolsMultiallelicAndSlim.output_vcf,
        source_file2 = BcftoolsMultiallelicAndSlim.output_vcf_index,
        source_file3 = BcftoolsMultiallelicAndSlim.output_vcf_sample,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file1, BcftoolsMultiallelicAndSlim.output_vcf])
    File output_vcf_index = select_first([CopyFile.output_file2, BcftoolsMultiallelicAndSlim.output_vcf_index])
    File output_vcf_sample = select_first([CopyFile.output_file3, BcftoolsMultiallelicAndSlim.output_vcf_sample])
    Int output_vcf_num_samples = BcftoolsMultiallelicAndSlim.output_vcf_num_samples
    Int output_vcf_num_variants = BcftoolsMultiallelicAndSlim.output_vcf_num_variants
  }
}

task BcftoolsExtractSamplesAndGTOnly {
  input {
    File input_vcf
    File input_vcf_index
    File include_samples

    String bcftools_view_option = ""

    String target_prefix
    String target_suffix
    
    String docker = "shengqh/hail_gcp:20240213"
    Float disk_factor = 2
    Int memory_gb = 20
    Int preemptible = 1
    Int cpu = 8
  }

  Int disk_size = ceil(size(input_vcf, "GB") * disk_factor) + 10

  String target_vcf = target_prefix + target_suffix
  String target_vcf_index = target_vcf + ".tbi"

  Int first_cpu = ceil((cpu - 2) / 2)
  Int second_cpu = cpu - 1 - first_cpu

  command <<<

echo "get all samples in original VCF"
bcftools query -l ~{input_vcf} > all.id.txt

echo "get included samples"
tr -d '\r' < ~{include_samples} > filter.id.txt
grep -Fxf all.id.txt filter.id.txt | sort | uniq > keep.id.txt

if [[ ! -s keep.id.txt ]]; then
  echo "ERROR: no samples to keep"
  exit 1
fi

echo "bcftools extract/annotate ..."
bcftools view ~{bcftools_view_option} --threads ~{first_cpu} -S keep.id.txt ~{input_vcf} | bcftools annotate --threads ~{second_cpu} -x QUAL,FILTER,INFO,^FORMAT/GT -o ~{target_vcf}

echo "build index"
bcftools index -t --threads ~{cpu} ~{target_vcf}

>>>

  runtime {
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    # The output has to be defined as File, otherwise the file would not be delocalized
    File output_vcf = "~{target_vcf}"
    File output_vcf_index = "~{target_vcf_index}"
  }
}

task BcftoolsNormAndFilterAF {
  input {
    File input_vcf
    File input_vcf_index
    File ref_fasta

    String bcftools_view_option = ""

    String target_prefix
    String target_suffix
    
    String docker = "shengqh/hail_gcp:20240213"
    Float disk_factor = 2
    Int memory_gb = 20
    Int preemptible = 1
    Int cpu = 8
  }

  Int disk_size = ceil(size(input_vcf, "GB") * disk_factor) + 10

  String target_vcf = target_prefix + target_suffix
  String target_vcf_index = target_vcf + ".tbi"
  String target_sample_file = target_vcf + ".samples.txt"

  Int first_cpu = ceil((cpu - 2) / 2)
  Int second_cpu = cpu - 1 - first_cpu

  command <<<

echo "bcftools biallelic/fill-tags/filter/multiallelic ..."
bcftools norm --threads ~{first_cpu} -f ~{ref_fasta} -m - ~{input_vcf} | bcftools +fill-tags - -- -t AF | bcftools view --threads ~{second_cpu} -i 'INFO/AF > 0' -o ~{target_vcf}

echo "build index"
bcftools index -t --threads ~{cpu} ~{target_vcf}

>>>

  runtime {
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    # The output has to be defined as File, otherwise the file would not be delocalized
    File output_vcf = "~{target_vcf}"
    File output_vcf_index = "~{target_vcf_index}"
  }
}

task BcftoolsMultiallelicAndSlim {
  input {
    File input_vcf
    File input_vcf_index
    File ref_fasta

    String target_prefix
    String target_suffix
    
    String docker = "shengqh/hail_gcp:20240213"
    Float disk_factor = 2
    Int memory_gb = 20
    Int preemptible = 1
    Int cpu = 8
  }

  Int disk_size = ceil(size(input_vcf, "GB") * disk_factor) + 10

  String target_vcf = target_prefix + target_suffix
  String target_vcf_index = target_vcf + ".tbi"
  String target_sample_file = target_vcf + ".samples.txt"

  Int first_cpu = ceil((cpu - 2) / 2)
  Int second_cpu = cpu - 1 - first_cpu

  command <<<

echo "bcftools multiallelic/slim ..."
bcftools norm --threads ~{first_cpu} -f ~{ref_fasta} -m + ~{input_vcf} | bcftools annotate --threads ~{second_cpu} -x INFO -o ~{target_vcf}

echo "build index"
bcftools index -t --threads ~{cpu} ~{target_vcf}

bcftools query -l ~{target_vcf} > ~{target_sample_file}

cat ~{target_sample_file} | wc -l > num_samples.txt

bcftools index -n ~{target_vcf} > num_variants.txt

>>>

  runtime {
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    # The output has to be defined as File, otherwise the file would not be delocalized
    File output_vcf = "~{target_vcf}"
    File output_vcf_index = "~{target_vcf_index}"
    File output_vcf_sample = "~{target_sample_file}"
    Int output_vcf_num_samples = read_int("num_samples.txt")
    Int output_vcf_num_variants = read_int("num_variants.txt")
  }
}

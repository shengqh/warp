version 1.0

## VUMC Bcftools Workflow
##
## This workflow provides a flexible interface to run bcftools commands on VCF files.
## Developed by VUMC/VANGARD team for efficient processing of genetic variant data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## Provides a generalized interface to run custom bcftools operations on VCF files,
## with options to copy results to Google Cloud Storage.
##
## ### Workflow Steps:
## 1. Run specified bcftools command on the input VCF
## 2. Create index for the output VCF
## 3. Extract sample information from the output VCF
## 4. Count number of samples and variants in the output VCF
## 5. Optionally copy the resulting files to a specified GCP folder
##
## ### Inputs:
## - input_vcf: Input VCF file to be processed
## - input_vcf_index: Index file for the input VCF
## - bcftools_option: Custom bcftools command options to be applied
## - target_prefix: Prefix for output filenames
## - target_suffix: Suffix for output filenames (default: .vcf.gz)
## - project_id: Optional GCP project ID for file copy operations
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_vcf: Processed VCF file
## - output_vcf_index: Index file for the processed VCF
## - output_vcf_sample: File containing sample IDs in the processed VCF
## - output_vcf_num_samples: Number of samples in the processed VCF
## - output_vcf_num_variants: Number of variants in the processed VCF
##
## ### Notes:
## - Uses bcftools to apply custom operations to VCF files
## - Automatically handles indexing and sample extraction
## - Can optionally move/copy files to Google Cloud Storage

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCBcftools {
  input {
    File input_vcf
    File input_vcf_index

    String bcftools_option

    String? input_file1_arg
    File? input_file1
    String? input_file2_arg
    File? input_file2
    String? input_file3_arg
    File? input_file3

    String target_prefix
    String target_suffix = ".vcf.gz"

    String? project_id
    String? target_gcp_folder
  }

  call Bcftools {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      bcftools_option = bcftools_option,
      input_file1_arg = input_file1_arg,
      input_file1 = input_file1,
      input_file2_arg = input_file2_arg,
      input_file2 = input_file2,
      input_file3_arg = input_file3_arg,
      input_file3 = input_file3,
      target_prefix = target_prefix,
      target_suffix = target_suffix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = Bcftools.output_vcf,
        source_file2 = Bcftools.output_vcf_index,
        source_file3 = Bcftools.output_vcf_sample,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file1, Bcftools.output_vcf])
    File output_vcf_index = select_first([CopyFile.output_file2, Bcftools.output_vcf_index])
    File output_vcf_sample = select_first([CopyFile.output_file3, Bcftools.output_vcf_sample])
    Int output_vcf_num_samples = Bcftools.output_vcf_num_samples
    Int output_vcf_num_variants = Bcftools.output_vcf_num_variants
  }
}

task Bcftools {
  input {
    File input_vcf
    File input_vcf_index

    String bcftools_option = ""

    String? input_file1_arg
    File? input_file1
    String? input_file2_arg
    File? input_file2
    String? input_file3_arg
    File? input_file3

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

  command <<<

echo bcftools ~{bcftools_option} ~{input_file1_arg} ~{input_file1} ~{input_file2_arg} ~{input_file2} ~{input_file3_arg} ~{input_file3}  --threads ~{cpu} -o ~{target_vcf} ~{input_vcf}
bcftools ~{bcftools_option} ~{input_file1_arg} ~{input_file1} ~{input_file2_arg} ~{input_file2} ~{input_file3_arg} ~{input_file3}  --threads ~{cpu} -o ~{target_vcf} ~{input_vcf}

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

version 1.0

## Copyright Vanderbilt Health, 2025
##
## VUMC Plink2 Pipeline
##
## A flexible wrapper workflow for running PLINK2 commands on PLINK binary fileset inputs.
## Developed by VUMC Biostatistics for genetic data analysis.
##
## ### Workflow Purpose:
## This workflow provides a generic interface to run arbitrary PLINK2 operations on a
## binary fileset (BED/BIM/FAM). Up to three additional parameter files can be passed
## to the PLINK2 command, making it reusable across many different PLINK2 use cases
## (e.g., filtering, frequency calculation, association testing).
##
## ### Workflow Steps:
## 1. **BuildExpectedFiles**: Constructs the list of expected output file paths from the target prefix and suffix list.
## 2. **Plink2**: Executes the PLINK2 command with the provided options and parameter files.
##
## ### Inputs:
## - source_bed: PLINK binary genotype file (.bed).
## - source_bim: PLINK variant information file (.bim).
## - source_fam: PLINK sample information file (.fam).
## - plink_option: PLINK2 command-line options string.
## - parameter_file1_arg: Optional argument flag for the first parameter file.
## - parameter_file1: Optional first parameter file.
## - parameter_file2_arg: Optional argument flag for the second parameter file.
## - parameter_file2: Optional second parameter file.
## - parameter_file3_arg: Optional argument flag for the third parameter file.
## - parameter_file3: Optional third parameter file.
## - suffix_list: List of expected output file suffixes.
## - target_prefix: Output file prefix for PLINK2 --out.
## - docker: Docker image for PLINK2 execution.
## - memory_size: Optional memory allocation in GiB.
##
## ### Outputs:
## - output_files: Array of output files produced by PLINK2.

workflow VUMCPlink2 {
  input {
    File source_bed
    File source_bim
    File source_fam

    String plink_option

    String? parameter_file1_arg
    File? parameter_file1

    String? parameter_file2_arg
    File? parameter_file2

    String? parameter_file3_arg
    File? parameter_file3

    Array[String] suffix_list
    String target_prefix

    String docker = "shengqh/plink_1.9_2.0:20250304"

    Int? memory_size=10
  }

  parameter_meta {
    source_bed: "PLINK binary genotype file (.bed)"
    source_bim: "PLINK variant information file (.bim)"
    source_fam: "PLINK sample information file (.fam)"
    plink_option: "PLINK2 command-line options string specifying the operation to perform"
    parameter_file1_arg: "Optional. Argument flag for the first parameter file (e.g., --extract, --keep)."
    parameter_file1: "Optional. First parameter file to pass to PLINK2."
    parameter_file2_arg: "Optional. Argument flag for the second parameter file."
    parameter_file2: "Optional. Second parameter file to pass to PLINK2."
    parameter_file3_arg: "Optional. Argument flag for the third parameter file."
    parameter_file3: "Optional. Third parameter file to pass to PLINK2."
    suffix_list: "List of expected output file suffixes appended to target_prefix"
    target_prefix: "Output file prefix passed to PLINK2 --out"
    docker: "Optional. Docker image containing PLINK2. Defaults to shengqh/plink_1.9_2.0:20250304."
    memory_size: "Optional. Memory allocation in GiB. Defaults to 10."
  }

  scatter(suffix in suffix_list){
    String expect_file = target_prefix + suffix
  }

  call Plink2 {
    input:
      source_bed = source_bed,
      source_bim = source_bim,
      source_fam = source_fam,

      plink_option = plink_option,

      parameter_file1_arg = parameter_file1_arg,
      parameter_file1 = parameter_file1,

      parameter_file2_arg = parameter_file2_arg,
      parameter_file2 = parameter_file2,

      parameter_file3_arg = parameter_file3_arg,
      parameter_file3 = parameter_file3,

      target_prefix = target_prefix,

      expected_files = expect_file,

      docker = docker,

      memory_size = memory_size
  }

  output {
    Array[File] output_files = Plink2.output_files
  }
}

task Plink2 {
  input {
    String source_bed_key = "--bed"
    File source_bed
    String source_bim_key = "--bim"
    File source_bim
    String source_fam_key = "--fam"
    File source_fam

    String plink_option

    String? parameter_file1_arg
    File? parameter_file1

    String? parameter_file2_arg
    File? parameter_file2

    String? parameter_file3_arg
    File? parameter_file3

    String target_prefix

    Array[String] expected_files

    String docker = "shengqh/plink_1.9_2.0:20250304"

    Int? memory_size=10
  }

  parameter_meta {
    source_bed_key: "Command-line flag for the BED file input"
    source_bed: {
      description: "PLINK binary genotype file (.bed)",
      localization_optional: true
    }
    source_bim_key: "Command-line flag for the BIM file input"
    source_bim: {
      description: "PLINK variant information file (.bim)",
      localization_optional: true
    }
    source_fam_key: "Command-line flag for the FAM file input"
    source_fam: {
      description: "PLINK sample information file (.fam)",
      localization_optional: true
    }
    plink_option: "PLINK2 command-line options string specifying the operation to perform"
    parameter_file1_arg: "Optional. Argument flag for the first parameter file."
    parameter_file1: {
      description: "Optional. First parameter file to pass to PLINK2.",
      localization_optional: true
    }
    parameter_file2_arg: "Optional. Argument flag for the second parameter file."
    parameter_file2: {
      description: "Optional. Second parameter file to pass to PLINK2.",
      localization_optional: true
    }
    parameter_file3_arg: "Optional. Argument flag for the third parameter file."
    parameter_file3: {
      description: "Optional. Third parameter file to pass to PLINK2.",
      localization_optional: true
    }
    target_prefix: "Output file prefix passed to PLINK2 --out"
    expected_files: "Array of expected output file paths"
    docker: "Optional. Docker image containing PLINK2. Defaults to shengqh/plink_1.9_2.0:20250304."
    memory_size: "Optional. Memory allocation in GiB. Defaults to 10."
  }

  Int disk_size = ceil(size(source_bed, "GB") * 2) + 2

  command <<<

plink2 \
  ~{source_bed_key} ~{source_bed} \
  ~{source_bim_key} ~{source_bim} \
  ~{source_fam_key} ~{source_fam} \
  ~{parameter_file1_arg + " " + parameter_file1} \
  ~{parameter_file2_arg + " " + parameter_file2} \
  ~{parameter_file3_arg + " " + parameter_file3} \
  ~{plink_option} \
  --out ~{target_prefix}

>>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_size + " GiB"
  }
  output {
    Array[File] output_files = expected_files
  }
}
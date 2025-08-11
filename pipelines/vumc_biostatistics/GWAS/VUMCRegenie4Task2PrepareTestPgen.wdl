version 1.0

## VUMC Regenie GWAS Workflow - Task 2: Prepare Test Pgen Files
##
## This workflow prepares test Pgen files for Regenie GWAS analysis.
## Developed by VUMC Biostatistics for population-specific GWAS studies.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline prepares test Pgen files for Regenie GWAS step 2,
## with options for QC filtering of genetic data.
##
## ### Workflow Steps:
## 1. QCFilterPgen: Filter Pgen files with specified QC parameters for each chromosome
## 2. Optionally copy output files to a specified GCP folder
##
## ### Inputs:
## - chromosomes: Array of chromosome identifiers
## - input_pgen_files: Array of input Pgen files
## - input_pvar_files: Array of input Pvar files
## - input_psam_files: Array of input Psam files
## - filter_psam_file: Optional Psam file for sample filtering
## - output_prefix: Prefix for output files
## - step2_plink2_option: Plink2 options for QC filtering
## - target_gcp_folder: Optional target GCP folder for output files
##
## ### Outputs:
## - test_pgen_files: Filtered Pgen files
## - test_psam_files: Filtered Psam files
## - test_pvar_files: Filtered Pvar files
## - test_plink2_option: Plink2 options used for filtering
## - test_num_variants: Total number of variants after filtering
##
## ### Notes:
## - Processes each chromosome separately in parallel
## - File copy operations to GCP are optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils

workflow VUMCRegenie4Task2PrepareTestPgen {
  input {
    Array[String] chromosomes

    Array[File] input_pgen_files
    Array[File] input_pvar_files
    Array[File] input_psam_files

    File? filter_psam_file

    String output_prefix
    
    String step2_plink2_option="--geno 0.05 --maf 0.01 --max-alleles 2"

    # for EUR cohort, 1.5 size_factor with addtional 10G is sufficient for default filter "--geno 0.05 --maf 0.01 --max-alleles 2"
    # for AFR cohort, 1.2 size_factor with addtional 10G is sufficient for default filter "--geno 0.05 --maf 0.01 --max-alleles 2"
    disk_size_factor = 1.5
    additional_disk_gb = 10

    # for both EUR and AFR cohort, 16G memory is sufficient for default filter "--geno 0.05 --maf 0.01 --max-alleles 2"
    memory_gb = 16

    String? target_gcp_folder
  }

  Int num_chromosomes = length(chromosomes)

  Array[Int] chrom_indecies = range(num_chromosomes)
  scatter(chrom_ind in chrom_indecies){
    File pgen_file = input_pgen_files[chrom_ind]
    File pvar_file = input_pvar_files[chrom_ind]
    File psam_file = input_psam_files[chrom_ind]
    String chromosome = chromosomes[chrom_ind]

    call BioUtils.QCFilterPgen as Step2Filter {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        filter_psam_file = filter_psam_file,
        qc_filter_option = step2_plink2_option,
        output_prefix = output_prefix + "." + chromosome + ".step2",
        disk_size_factor = disk_size_factor, 
        memory_gb = memory_gb,
        additional_disk_gb = additional_disk_gb,
    }
  }

  if(defined(target_gcp_folder)){
    String gcs_output_dir = select_first([target_gcp_folder])

    call GcpUtils.MoveOrCopyFileArray as CopyFile7 {
      input:
        source_files = Step2Filter.output_pgen,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
    scatter(pgen in CopyFile7.outputFiles) {
      String step2_pgen = pgen
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile8 {
      input:
        source_files = Step2Filter.output_psam,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
    scatter(psam in CopyFile8.outputFiles) {
      String step2_psam = psam
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile9 {
      input:
        source_files = Step2Filter.output_pvar,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
    scatter(pvar in CopyFile9.outputFiles) {
      String step2_pvar = pvar
    }
  }

  call WDLUtils.sum_integers {
    input:
      input_integers = Step2Filter.num_variants
  }

  output {
    Array[File] test_pgen_files = select_first([step2_pgen, Step2Filter.output_pgen])
    Array[File] test_psam_files = select_first([step2_psam, Step2Filter.output_psam])
    Array[File] test_pvar_files = select_first([step2_pvar, Step2Filter.output_pvar])

    String test_plink2_option = step2_plink2_option
    Int test_num_variants = sum_integers.sum
  }
}


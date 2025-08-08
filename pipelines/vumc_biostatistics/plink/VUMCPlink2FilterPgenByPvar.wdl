version 1.0
## VUMC Plink2 Filter PGEN by PVAR Workflow
##
## This workflow filters PGEN files using variant information from a PVAR file.
## Developed by VUMC Biostatistics for genetic analysis projects.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline filters PGEN format files by keeping only variants specified in a provided PVAR file,
## and produces filtered PGEN, PVAR, and PSAM files as output.
##
## ### Workflow Steps:
## 1. Plink2FilterPgenByPvar: Filter PGEN files using the provided PVAR file to keep specified variants.
## 2. Optionally copy the output files to a specified GCP folder.
##
## ### Inputs:
## - input_pgen: Input PGEN file to be filtered
## - input_pvar: Input PVAR file containing variant information
## - input_psam: Input PSAM file containing sample information
## - keep_pvar: PVAR file specifying which variants to keep
## - output_prefix: Prefix for output files
## - plink2_filter_option: Additional filtering options for Plink2
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_pgen: Path to the filtered PGEN file
## - output_pvar: Path to the filtered PVAR file
## - output_psam: Path to the filtered PSAM file
## - output_num_samples: Number of samples in the filtered dataset
## - output_num_variants: Number of variants in the filtered dataset
##
## ### Notes:
## - Uses GcpUtils for file copy operations.
## - File copy operation to GCP is optional and only executed if a target folder is provided.
## 

import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPlink2FilterPgenByPvar {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    File keep_pvar
    
    String output_prefix

    String plink2_filter_option = ""

    String? target_gcp_folder
  }

  call Plink2Utils.Plink2FilterPgenByPvar as Plink2FilterPgen {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      keep_pvar = keep_pvar,
      output_prefix = output_prefix,
      plink2_filter_option = plink2_filter_option
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = Plink2FilterPgen.output_pgen,
        source_file2 = Plink2FilterPgen.output_pvar,
        source_file3 = Plink2FilterPgen.output_psam,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_pgen = select_first([CopyFile.output_file1, Plink2FilterPgen.output_pgen])
    String output_pvar = select_first([CopyFile.output_file2, Plink2FilterPgen.output_pvar])
    String output_psam = select_first([CopyFile.output_file3, Plink2FilterPgen.output_psam])
    Int output_num_samples = Plink2FilterPgen.num_samples
    Int output_num_variants = Plink2FilterPgen.num_variants
  }
}

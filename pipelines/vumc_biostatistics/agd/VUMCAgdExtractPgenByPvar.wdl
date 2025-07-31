version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow extracts variant genotypes from PLINK2 files based on variant IDs defined in a plink2 pvar file.
#
# Workflow steps:
# 1. Check for overlapping variants between input pvar files and the keep_pvar file
# 2. Extract variants from PGEN files for chromosomes with overlapping variants
# 3. Merge filtered PGEN files across valid chromosomes if more than one exists
# 4. Optionally copy results to a GCP storage location
#
# Inputs:
# - keep_pvar: PVAR file containing variant IDs to extract
# - output_prefix: Prefix for all output files
# - chromosomes: List of chromosomes to process
# - input_pgen_files: PGEN files (one per chromosome)
# - input_psam_files: PSAM files (one per chromosome)
# - input_pvar_files: PVAR files (one per chromosome)
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_pgen: Final filtered PGEN file path
# - output_pvar: Final filtered PVAR file path
# - output_psam: Final filtered PSAM file path


workflow VUMCAgdExtractPgenByPvar {
  input {
    File keep_pvar

    String output_prefix

    Array[String] chromosomes
    Array[File] input_pgen_files
    Array[File] input_psam_files
    Array[File] input_pvar_files

    String? target_gcp_folder    
  }

  Int num_all_chromsome = length(chromosomes)

  scatter(all_chrom_ind in range(num_all_chromsome)){
    call BioUtils.CheckOverlapVariants as CheckOverlapVariants {
      input:
        chromosome = chromosomes[all_chrom_ind],
        input_pgen_pvar = input_pvar_files[all_chrom_ind],
        input_ucsc_bed = keep_pvar
    }
  }

  call WdlUtils.get_index_of_true {
    input:
      values = CheckOverlapVariants.has_variant
  }

  Int num_valid_chromsome = length(get_index_of_true.indices)

  scatter(chrom_ind in range(num_valid_chromsome)){
    Int old_ind = get_index_of_true.indices[chrom_ind]
    File pgen_file = input_pgen_files[old_ind]
    File pvar_file = input_pvar_files[old_ind]
    File psam_file = input_psam_files[old_ind]
    String chromosome = chromosomes[old_ind]

    call Plink2Utils.Plink2FilterPgenByPvar as Plink2FilterPgen_variants {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_pvar = keep_pvar,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chromosome + ".snp"
    }
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = Plink2FilterPgen_variants.output_pgen,
        input_pvar_files = Plink2FilterPgen_variants.output_pvar,
        input_psam_files = Plink2FilterPgen_variants.output_psam,
        output_prefix = output_prefix + '.allsamples'
    }
  }

  File all_samples_pgen = select_first([MergePgenFiles.output_pgen, Plink2FilterPgen_variants.output_pgen[0]])
  File all_samples_pvar = select_first([MergePgenFiles.output_pvar, Plink2FilterPgen_variants.output_pvar[0]])
  File all_samples_psam = select_first([MergePgenFiles.output_psam, Plink2FilterPgen_variants.output_psam[0]])

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = all_samples_pgen,
        source_file2 = all_samples_pvar,
        source_file3 = all_samples_psam,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_pgen = select_first([CopyFile.output_file1, all_samples_pgen])
    String output_pvar = select_first([CopyFile.output_file2, all_samples_pvar])
    String output_psam = select_first([CopyFile.output_file3, all_samples_psam])
  }
}

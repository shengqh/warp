version 1.0

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
# - keep_psam: Optional PSAM file containing sample GRIDs to extract
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

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAgdPgenExtractAllSamplesByPvar {
  input {
    File keep_pvar
    File? keep_psam

    String output_prefix

    Array[String] chromosomes
    Array[File] input_pgen_files
    Array[File] input_psam_files
    Array[File] input_pvar_files

    String? target_gcp_folder    
  }

  Int num_all_chromsome = length(chromosomes)

  scatter(all_chrom_ind in range(num_all_chromsome)){
    call BioUtils.CheckOverlapVariantsReturnIndex as CheckOverlapVariants {
      input:
        chromosome = chromosomes[all_chrom_ind],
        input_pgen_pvar = input_pvar_files[all_chrom_ind],
        input_ucsc_bed = keep_pvar,
        chrom_index = all_chrom_ind
    }
  }

  Array[Int] valid_indices = select_all(CheckOverlapVariants.res_chrom_index)
  Int num_valid_chromsome = length(valid_indices)

  scatter(old_ind in valid_indices){
    File pgen_file = input_pgen_files[old_ind]
    File pvar_file = input_pvar_files[old_ind]
    File psam_file = input_psam_files[old_ind]
    String chromosome = chromosomes[old_ind]

    call Plink2Utils.PgenFilter as PgenFilter_variants {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_pvar = keep_pvar,
        keep_psam = keep_psam,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chromosome + ".snp"
    }
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = PgenFilter_variants.output_pgen,
        input_pvar_files = PgenFilter_variants.output_pvar,
        input_psam_files = PgenFilter_variants.output_psam,
        output_prefix = output_prefix + '.allsamples'
    }
  }

  File all_samples_pgen = select_first([MergePgenFiles.output_pgen, PgenFilter_variants.output_pgen[0]])
  File all_samples_pvar = select_first([MergePgenFiles.output_pvar, PgenFilter_variants.output_pvar[0]])
  File all_samples_psam = select_first([MergePgenFiles.output_psam, PgenFilter_variants.output_psam[0]])

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
    File output_allsamples_pgen = select_first([CopyFile.output_file1, all_samples_pgen])
    File output_allsamples_pvar = select_first([CopyFile.output_file2, all_samples_pvar])
    File output_allsamples_psam = select_first([CopyFile.output_file3, all_samples_psam])
  }
}

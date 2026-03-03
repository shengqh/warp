version 1.0

import "./VUMCAgdPgenExtractAllSamplesByPvar.wdl" as VUMCUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow extracts samples with variant genotypes from PLINK2 files based on variant IDs defined in a plink2 pvar file.
#
# Workflow steps:
# 1. Extract variants from PGEN files for all samples
# 2. Keep samples with variant
# 3. Optionally copy results to a GCP storage location
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
# - output_sample_with_variant_pgen: Final filtered PGEN file path
# - output_sample_with_variant_pvar: Final filtered PVAR file path
# - output_sample_with_variant_psam: Final filtered PSAM file path

workflow VUMCAgdPgenExtractSampleWithVariantByPvar {
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

  call VUMCUtils.VUMCAgdPgenExtractAllSamplesByPvar as AllSamples {
    input:
      keep_pvar = keep_pvar,
      keep_psam = keep_psam,
      output_prefix = output_prefix,
      chromosomes = chromosomes,
      input_pgen_files = input_pgen_files,
      input_psam_files = input_psam_files,
      input_pvar_files = input_pvar_files
  }

  call Plink2Utils.KeepSampleWithVariant as KeepSampleWithVariant {
    input:
      input_pgen = AllSamples.output_allsamples_pgen,
      input_pvar = AllSamples.output_allsamples_pvar,
      input_psam = AllSamples.output_allsamples_psam,
      output_prefix = output_prefix + '.sample_with_variant',
      is_agd_data = true
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyFourFiles as CopyFile {
      input:
        source_file1 = KeepSampleWithVariant.output_pgen,
        source_file2 = KeepSampleWithVariant.output_pvar,
        source_file3 = KeepSampleWithVariant.output_psam,
        source_file4 = KeepSampleWithVariant.output_allele_freq,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_sample_with_variant_pgen = select_first([CopyFile.output_file1, KeepSampleWithVariant.output_pgen])
    File output_sample_with_variant_pvar = select_first([CopyFile.output_file2, KeepSampleWithVariant.output_pvar])
    File output_sample_with_variant_psam = select_first([CopyFile.output_file3, KeepSampleWithVariant.output_psam])
  }
}

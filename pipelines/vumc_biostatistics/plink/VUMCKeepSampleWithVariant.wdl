version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow keeps samples with variants from PLINK2 files.
#
# Workflow steps:
# 1. Keep samples with variants
# 2. Optionally copy results to a GCP storage location
#
# Inputs:
# - output_prefix: Prefix for all output files
# - input_pgen: Input PGEN file
# - input_psam: Input PSAM file
# - input_pvar: Input PVAR file
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_pgen: Final PGEN file path
# - output_pvar: Final PVAR file path
# - output_psam: Final PSAM file path
# - output_num_samples: Number of samples in the output
# - output_num_variants: Number of variants in the output

workflow VUMCKeepSampleWithVariant {
  input {
    String output_prefix

    File input_pgen
    File input_psam
    File input_pvar

    Boolean is_agd_data = true

    String? target_gcp_folder    
  }

  call Plink2Utils.KeepSampleWithVariant as KeepSampleWithVariant {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      output_prefix = output_prefix + '.sample_with_variant',
      is_agd_data = is_agd_data
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = KeepSampleWithVariant.output_pgen,
        source_file2 = KeepSampleWithVariant.output_pvar,
        source_file3 = KeepSampleWithVariant.output_psam,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_pgen = select_first([CopyFile.output_file1, KeepSampleWithVariant.output_pgen])
    String output_pvar = select_first([CopyFile.output_file2, KeepSampleWithVariant.output_pvar])
    String output_psam = select_first([CopyFile.output_file3, KeepSampleWithVariant.output_psam])
    Int output_num_samples = KeepSampleWithVariant.output_num_samples
    Int output_num_variants = KeepSampleWithVariant.output_num_variants
  }
}

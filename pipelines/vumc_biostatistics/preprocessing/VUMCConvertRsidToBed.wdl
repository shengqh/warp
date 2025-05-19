version 1.0

import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow converts rsIDs to chromosome positions in BED format.
# The workflow performs the following steps:
# 1. Convert the input rsid file or rsids to a BED file.
# 2. Optionally copy the output BED file to a specified GCP folder.
#
# Input parameters:
# - input_rsid_file: A file containing rsids, one per line.
# - input_rsids: A string containing rsids separated by spaces.
# - output_prefix: The prefix for the output files.
# - billing_gcp_project_id: The GCP project ID for billing.
# - target_gcp_folder: The GCP folder to move the output files to.
# Output files:
# - output_variant_bed: The output BED file.
# Note: The workflow uses the BioUtils task for converting rsids to BED format.
# The GcpUtils task is used for copying files to GCP.

workflow VUMCConvertRsidToBed {
  input {
    # Input rsid file, each line contains one rsid
    File? input_rsid_file

    # Input rsids, separated by space
    String? input_rsids

    String output_prefix

    String? billing_gcp_project_id
    String? target_gcp_folder    
  }

  call BioUtils.ConvertRsidToBed {
    input:
      input_rsid_file = input_rsid_file,
      input_rsids = input_rsids,
      output_prefix = output_prefix
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = ConvertRsidToBed.output_bed,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_variant_bed = select_first([CopyFile.output_file, ConvertRsidToBed.output_bed])
  }
}

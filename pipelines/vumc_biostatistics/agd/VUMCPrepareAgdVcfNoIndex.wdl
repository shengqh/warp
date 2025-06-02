version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "./VUMCPrepareAgdVcf.wdl" as VUMCPrepareAgdVcf

# This workflow prepares AGD VCF files by processing input VCF with ID mapping.
# The workflow performs the following steps:
# 1. Processes input VCF file using AGD tool with ID mapping file.
# 2. Optionally copies the processed VCF file to a GCP folder.
#
# Input parameters:
# - input_vcf: The input VCF file to be processed.
# - id_map_file: File containing ID mappings for AGD processing.
# - output_prefix: Prefix for the output processed VCF file.
# - project_id: Optional GCP project ID.
# - target_gcp_folder: Optional GCP folder to copy processed files to.
#
# Output parameters:
# - output_vcf: The processed VCF file.
#
# Note: This workflow does not generate index files, unlike the standard AGD VCF preparation workflow.


workflow VUMCPrepareAgdVcfNoIndex {
  input {
    File input_vcf

    File id_map_file

    String output_prefix

    String? project_id
    String? target_gcp_folder
  }
  
  call VUMCPrepareAgdVcf.PrepareAgdVcf {
    input: 
      input_vcf = input_vcf,
      id_map_file = id_map_file,
      output_prefix = output_prefix + ".primary_pass"
  }

  if(defined(target_gcp_folder)){
    String filtered_vcf = "~{PrepareAgdVcf.output_vcf}"

    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = filtered_vcf,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file, PrepareAgdVcf.output_vcf])
  }
}

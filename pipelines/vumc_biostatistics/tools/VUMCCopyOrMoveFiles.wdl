version 1.0

## VUMC File Update in Data Table Workflow
##
## This workflow is useful when you need to verify a file exists in a specified GCP location
## and want to get its target path in a standardized format. It checks if the target file 
## exists in the specified GCP folder and returns the target file path.
##
## Developed by VUMC/VANGARD team for efficient file management on GCP.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This workflow checks if a file exists in a specified GCP folder location
## and provides the target file path as output.
##
## ### Workflow Steps:
## 1. Verify the existence of the target file in the specified GCP folder
##
## ### Inputs:
## - source_file: Source file path (only the basename is used)
## - project_id: Google Cloud project ID
## - output_gcp_folder: Target Google Cloud Storage folder
##
## ### Outputs:
## - output_file: Resulting GCS URL of the target file
##
## ### Notes:
## - Uses Google Cloud SDK for file checks
## - Returns target file path in URL format
## - Verifies file existence but does not move or copy files

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCCopyOrMoveFiles {
  input {
    String? sub_folder

    String source_file01
    String? source_file02
    String? source_file03
    String? source_file04
    String? source_file05
    String? source_file06
    String? source_file07
    String? source_file08
    String? source_file09
    String? source_file10

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  call GcpUtils.MoveOrCopyFiles as MoveOrCopyFiles {
    input:
      sub_folder = sub_folder,
      source_file01 = source_file01,
      source_file02 = source_file02,
      source_file03 = source_file03,
      source_file04 = source_file04,
      source_file05 = source_file05,
      source_file06 = source_file06,
      source_file07 = source_file07,
      source_file08 = source_file08,
      source_file09 = source_file09,
      source_file10 = source_file10,
      is_move_file = is_move_file,
      project_id = project_id,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String output_file01 = MoveOrCopyFiles.output_file01
    String output_file02 = MoveOrCopyFiles.output_file02
    String output_file03 = MoveOrCopyFiles.output_file03
    String output_file04 = MoveOrCopyFiles.output_file04
    String output_file05 = MoveOrCopyFiles.output_file05
    String output_file06 = MoveOrCopyFiles.output_file06
    String output_file07 = MoveOrCopyFiles.output_file07
    String output_file08 = MoveOrCopyFiles.output_file08
    String output_file09 = MoveOrCopyFiles.output_file09
    String output_file10 = MoveOrCopyFiles.output_file10
  }
}

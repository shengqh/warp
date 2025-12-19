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
## - project_id: Google Cloud project ID (only work when you copy file out of requestor pay bucket, but not copy into requestor pay bucket)
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

    String source_file1
    String? source_file2
    String? source_file3
    String? source_file4
    String? source_file5
    String? source_file6
    String? source_file7
    String? source_file8
    String? source_file9

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  call GcpUtils.MoveOrCopyFiles as MoveOrCopyFiles {
    input:
      sub_folder = sub_folder,
      source_file1 = source_file1,
      source_file2 = source_file2,
      source_file3 = source_file3,
      source_file4 = source_file4,
      source_file5 = source_file5,
      source_file6 = source_file6,
      source_file7 = source_file7,
      source_file8 = source_file8,
      source_file9 = source_file9,
      is_move_file = is_move_file,
      project_id = project_id,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String output_file1 = MoveOrCopyFiles.output_file1
    String? output_file2 = MoveOrCopyFiles.output_file2
    String? output_file3 = MoveOrCopyFiles.output_file3
    String? output_file4 = MoveOrCopyFiles.output_file4
    String? output_file5 = MoveOrCopyFiles.output_file5
    String? output_file6 = MoveOrCopyFiles.output_file6
    String? output_file7 = MoveOrCopyFiles.output_file7
    String? output_file8 = MoveOrCopyFiles.output_file8
    String? output_file9 = MoveOrCopyFiles.output_file9
    }
}

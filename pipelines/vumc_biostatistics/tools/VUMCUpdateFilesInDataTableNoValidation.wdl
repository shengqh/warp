version 1.0

## VUMC File Update in Data Table Workflow (No Validation)
##
## This workflow is useful when you need to generate target file paths in a specified GCP location
## without validating file existence. It constructs target paths for multiple files in a 
## standardized format and returns the target file paths.
##
## Developed by VUMC/VANGARD team for efficient file management on GCP.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This workflow generates target file paths for multiple files in a specified GCP folder
## without performing validation checks.
##
## ### Workflow Steps:
## 1. Construct target file paths for up to 5 source files in the specified GCP folder
##
## ### Inputs:
## - source_file1: Primary source file path (required)
## - source_file2-5: Additional source file paths (optional)
## - target_gcp_folder: Target Google Cloud Storage folder
##
## ### Outputs:
## - target_file1-5: Resulting GCS URLs of the target files
##
## ### Notes:
## - Does not validate file existence or perform file operations
## - Returns target file paths in URL format based on source file basenames
## - Supports up to 5 files in a single workflow execution

workflow VUMCUpdateFilesInDataTableNoValidation {
  input {
    String source_file1
    String? source_file2
    String? source_file3
    String? source_file4
    String? source_file5

    String target_gcp_folder
  }

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String output_file1 = "${gcs_output_dir}/${basename(source_file1)}"
  String output_file2 = if defined(source_file2) then "${gcs_output_dir}/${basename(source_file2)}" else ""
  String output_file3 = if defined(source_file3) then "${gcs_output_dir}/${basename(source_file3)}" else ""
  String output_file4 = if defined(source_file4) then "${gcs_output_dir}/${basename(source_file4)}" else ""
  String output_file5 = if defined(source_file5) then "${gcs_output_dir}/${basename(source_file5)}" else ""

  output {
    String target_file1 = output_file1
    String target_file2 = output_file2
    String target_file3 = output_file3
    String target_file4 = output_file4
    String target_file5 = output_file5
  }
}

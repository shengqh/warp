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
## - target_gcp_folder: Target Google Cloud Storage folder
##
## ### Outputs:
## - target_file: Resulting GCS URL of the target file
##
## ### Notes:
## - Uses Google Cloud SDK for file checks
## - Returns target file path in URL format
## - Verifies file existence but does not move or copy files

workflow VUMCUpdateFileInDataTable {
  input {
    String source_file
    String project_id
    String target_gcp_folder
  }

  call UpdateFileInDataTable {
    input:
      source_file = source_file,
      project_id = project_id,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String target_file = UpdateFileInDataTable.target_file
  }
}

task UpdateFileInDataTable {
  input {
    String source_file
    String project_id
    String target_gcp_folder
  }

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String target_url = "${gcs_output_dir}/${basename(source_file)}"

  command <<<

check_file(){
  set +e

  TARGET_FILE=$1

  echo "Checking if target file exists: $TARGET_FILE"

  gsutil -u ~{project_id} -q stat $TARGET_FILE
  status=$?
  if [[ $status -eq 0 ]]; then
    echo "Target file exists, great"
  else
    echo "Target file does not exist, failed"
  fi

  set -e
  return $status
}

set -e

check_file ~{target_url}

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String target_file = "~{target_url}"
  }
}

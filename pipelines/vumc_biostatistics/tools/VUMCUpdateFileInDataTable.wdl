version 1.0

## VUMC File Update in Data Table Workflow
##
## This workflow is useful to update a file path to a specified Google Cloud Storage bucket.
## Sometimes, we move the files from one bucket to another bucket and then we want to update the URL in
## The Terra data table. This workflow is useful for that purpose by setting the source_file and 
## target_file to same column in the data table.
## Developed by VUMC/VANGARD team for efficient file management on GCP.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This workflow checks if a file exists in a specified GCP bucket location
## and provides the target file path as output.
##
## ### Workflow Steps:
## 1. Verify the existence of the target file in the specified bucket
##
## ### Inputs:
## - source_file: Source file path to be checked
## - target_bucket: Target Google Cloud Storage bucket
##
## ### Outputs:
## - target_file: Resulting GCS URL of the target file
##
## ### Notes:
## - Uses Google Cloud SDK for file checks
## - Returns target file path in URL format
## - Does not actually move or copy files, despite the name


workflow VUMCUpdateFileInDataTable {
  input {
    String source_file
    String target_bucket
  }

  call UpdateFileInDataTable {
    input:
      source_file = source_file,
      target_bucket = target_bucket
  }

  output {
    String target_file = UpdateFileInDataTable.target_file
  }
}

task UpdateFileInDataTable {
  input {
    String source_file
    String target_bucket
  }

  String gcs_output_dir = sub(target_bucket, "/+$", "")

  String target_url = "${gcs_output_dir}/${basename(source_file)}"

  command <<<

check_file(){
  set +e

  TARGET_FILE=$1

  echo "Checking if target file exists: $TARGET_FILE"

  gsutil -q stat $TARGET_FILE
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

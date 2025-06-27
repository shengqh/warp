version 1.0

# Since the WDL task will use service account to access GCP resources, 
# Put project_id in gsutil command could not work if you copy the files to requstor pay bucket.
# If you need to copy file into or out of a normal bucket, project_id is not required.
# If you need to copy file out of requstor pay bucket, project_id is required.
# Copy file into requestor pay bucket would be failed even if you provided project_id if your service id doesn't have write permission.
# User must have write permission to the target GCP folder.

task MoveOrCopyOneFile {
  input {
    String source_file

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String new_file = "~{gcs_output_dir}/~{basename(source_file)}"

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_file} ~{gcs_output_dir}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_file = new_file
  }
}

task MoveOrCopyTwoFiles {
  input {
    String source_file1
    String source_file2

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String new_file1 = "~{gcs_output_dir}/~{basename(source_file1)}"
  String new_file2 = "~{gcs_output_dir}/~{basename(source_file2)}"

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_file1} ~{source_file2} ~{gcs_output_dir}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_file1 = new_file1
    String output_file2 = new_file2
  }
}

task MoveOrCopyThreeFiles {
  input {
    String source_file1
    String source_file2
    String source_file3

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String new_file1 = "~{gcs_output_dir}/~{basename(source_file1)}"
  String new_file2 = "~{gcs_output_dir}/~{basename(source_file2)}"
  String new_file3 = "~{gcs_output_dir}/~{basename(source_file3)}"

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_file1} ~{source_file2} ~{source_file3} ~{gcs_output_dir}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_file1 = new_file1
    String output_file2 = new_file2
    String output_file3 = new_file3
  }
}

task MoveOrCopyFourFiles {
  input {
    String source_file1
    String source_file2
    String source_file3
    String source_file4

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String new_file1 = "~{gcs_output_dir}/~{basename(source_file1)}"
  String new_file2 = "~{gcs_output_dir}/~{basename(source_file2)}"
  String new_file3 = "~{gcs_output_dir}/~{basename(source_file3)}"
  String new_file4 = "~{gcs_output_dir}/~{basename(source_file4)}"

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_file1} ~{source_file2} ~{source_file3} ~{source_file4} ~{gcs_output_dir}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_file1 = new_file1
    String output_file2 = new_file2
    String output_file3 = new_file3
    String output_file4 = new_file4
  }
}

task MoveOrCopyFiveFiles {
  input {
    String source_file1
    String source_file2
    String source_file3
    String source_file4
    String source_file5

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String new_file1 = "~{gcs_output_dir}/~{basename(source_file1)}"
  String new_file2 = "~{gcs_output_dir}/~{basename(source_file2)}"
  String new_file3 = "~{gcs_output_dir}/~{basename(source_file3)}"
  String new_file4 = "~{gcs_output_dir}/~{basename(source_file4)}"
  String new_file5 = "~{gcs_output_dir}/~{basename(source_file5)}"

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_file1} ~{source_file2} ~{source_file3} ~{source_file4} ~{source_file5} ~{gcs_output_dir}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_file1 = new_file1
    String output_file2 = new_file2
    String output_file3 = new_file3
    String output_file4 = new_file4
    String output_file5 = new_file5
  }
}

task MoveOrCopySixFiles {
  input {
    String source_file1
    String source_file2
    String source_file3
    String source_file4
    String source_file5
    String source_file6

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String new_file1 = "~{gcs_output_dir}/~{basename(source_file1)}"
  String new_file2 = "~{gcs_output_dir}/~{basename(source_file2)}"
  String new_file3 = "~{gcs_output_dir}/~{basename(source_file3)}"
  String new_file4 = "~{gcs_output_dir}/~{basename(source_file4)}"
  String new_file5 = "~{gcs_output_dir}/~{basename(source_file5)}"
  String new_file6 = "~{gcs_output_dir}/~{basename(source_file6)}"

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_file1} ~{source_file2} ~{source_file3} ~{source_file4} ~{source_file5} ~{source_file6}  ~{gcs_output_dir}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_file1 = new_file1
    String output_file2 = new_file2
    String output_file3 = new_file3
    String output_file4 = new_file4
    String output_file5 = new_file5
    String output_file6 = new_file6
  }
}

task MoveOrCopySevenFiles {
  input {
    String source_file1
    String source_file2
    String source_file3
    String source_file4
    String source_file5
    String source_file6
    String source_file7

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String new_file1 = "~{gcs_output_dir}/~{basename(source_file1)}"
  String new_file2 = "~{gcs_output_dir}/~{basename(source_file2)}"
  String new_file3 = "~{gcs_output_dir}/~{basename(source_file3)}"
  String new_file4 = "~{gcs_output_dir}/~{basename(source_file4)}"
  String new_file5 = "~{gcs_output_dir}/~{basename(source_file5)}"
  String new_file6 = "~{gcs_output_dir}/~{basename(source_file6)}"
  String new_file7 = "~{gcs_output_dir}/~{basename(source_file7)}"

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_file1} ~{source_file2} ~{source_file3} ~{source_file4} ~{source_file5} ~{source_file6} ~{source_file7} ~{gcs_output_dir}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_file1 = new_file1
    String output_file2 = new_file2
    String output_file3 = new_file3
    String output_file4 = new_file4
    String output_file5 = new_file5
    String output_file6 = new_file6
    String output_file7 = new_file7
  }
}

task MoveOrCopyFileArray {
  input {
    Array[String] source_files

    Boolean is_move_file = false

    String? project_id
    String target_gcp_folder
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} '~{sep="' '" source_files}' ~{gcs_output_dir}/

# Generate a list of output files
for file in ~{sep=' ' source_files}; do
  base=$(basename $file)
  echo "~{gcs_output_dir}/$base" >> output_files.txt
done

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }

  output {
    Array[String] outputFiles = read_lines("output_files.txt")
  }
}

task MoveOrCopyFiles {
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

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String sub_folder_path = if defined(sub_folder) then gcs_output_dir + "/" + select_first([sub_folder]) else gcs_output_dir

  String new_file01 = "~{sub_folder_path}/~{basename(source_file01)}"
  String new_file02 = if (defined(source_file02)) then "~{sub_folder_path}/~{basename(select_first([source_file02]))}" else ""
  String new_file03 = if (defined(source_file03)) then "~{sub_folder_path}/~{basename(select_first([source_file03]))}" else ""
  String new_file04 = if (defined(source_file04)) then "~{sub_folder_path}/~{basename(select_first([source_file04]))}" else ""
  String new_file05 = if (defined(source_file05)) then "~{sub_folder_path}/~{basename(select_first([source_file05]))}" else ""
  String new_file06 = if (defined(source_file06)) then "~{sub_folder_path}/~{basename(select_first([source_file06]))}" else ""
  String new_file07 = if (defined(source_file07)) then "~{sub_folder_path}/~{basename(select_first([source_file07]))}" else ""
  String new_file08 = if (defined(source_file08)) then "~{sub_folder_path}/~{basename(select_first([source_file08]))}" else ""
  String new_file09 = if (defined(source_file09)) then "~{sub_folder_path}/~{basename(select_first([source_file09]))}" else ""
  String new_file10 = if (defined(source_file10)) then "~{sub_folder_path}/~{basename(select_first([source_file10]))}" else ""

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_file01} ~{source_file02} ~{source_file03} ~{source_file04} ~{source_file05} ~{source_file06} ~{source_file07} ~{source_file08} ~{source_file09} ~{source_file10} ~{sub_folder_path}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_file01 = new_file01
    String output_file02 = new_file02
    String output_file03 = new_file03
    String output_file04 = new_file04
    String output_file05 = new_file05
    String output_file06 = new_file06
    String output_file07 = new_file07
    String output_file08 = new_file08
    String output_file09 = new_file09
    String output_file10 = new_file10
  }
}

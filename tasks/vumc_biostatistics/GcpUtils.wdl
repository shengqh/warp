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

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

  String sub_folder_path = if defined(sub_folder) then gcs_output_dir + "/" + select_first([sub_folder]) else gcs_output_dir

  String new_file1 = "~{sub_folder_path}/~{basename(source_file1)}"
  String new_file2 = if (defined(source_file2)) then "~{sub_folder_path}/~{basename(select_first([source_file2]))}" else ""
  String new_file3 = if (defined(source_file3)) then "~{sub_folder_path}/~{basename(select_first([source_file3]))}" else ""
  String new_file4 = if (defined(source_file4)) then "~{sub_folder_path}/~{basename(select_first([source_file4]))}" else ""
  String new_file5 = if (defined(source_file5)) then "~{sub_folder_path}/~{basename(select_first([source_file5]))}" else ""
  String new_file6 = if (defined(source_file6)) then "~{sub_folder_path}/~{basename(select_first([source_file6]))}" else ""
  String new_file7 = if (defined(source_file7)) then "~{sub_folder_path}/~{basename(select_first([source_file7]))}" else ""
  String new_file8 = if (defined(source_file8)) then "~{sub_folder_path}/~{basename(select_first([source_file8]))}" else ""
  String new_file9 = if (defined(source_file9)) then "~{sub_folder_path}/~{basename(select_first([source_file9]))}" else ""

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_file1} ~{source_file2} ~{source_file3} ~{source_file4} ~{source_file5} ~{source_file6} ~{source_file7} ~{source_file8} ~{source_file9} ~{sub_folder_path}/

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
    String output_file8 = new_file8
    String output_file9 = new_file9
  }
}

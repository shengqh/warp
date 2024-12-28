version 1.0

task MoveOrCopyVcfFile {
  input {
    String input_vcf
    String input_vcf_index

    Boolean is_move_file = false

    String? project_id
    String target_bucket
    String genoset
    String? GRID
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_bucket, "/+$", "")

  String target_folder = if(defined(GRID)) then "~{gcs_output_dir}/~{genoset}/~{GRID}" else "~{gcs_output_dir}/~{genoset}"
  String new_vcf = "~{target_folder}/~{basename(input_vcf)}"
  String new_vcf_index = "~{target_folder}/~{basename(input_vcf_index)}"

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{input_vcf} \
  ~{input_vcf_index} \
  ~{target_folder}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_vcf = new_vcf
    String output_vcf_index = new_vcf_index
  }
}

task MoveOrCopyPlinkFile {
  input {
    String source_bed
    String source_bim
    String source_fam

    Boolean is_move_file = false

    String? project_id
    String target_bucket
  }

  String action = if (is_move_file) then "mv" else "cp"

  String gcs_output_dir = sub(target_bucket, "/+$", "")

  String new_bed = "~{gcs_output_dir}/~{basename(source_bed)}"
  String new_bim = "~{gcs_output_dir}/~{basename(source_bim)}"
  String new_fam = "~{gcs_output_dir}/~{basename(source_fam)}"

  command <<<

set -e

gsutil -m ~{"-u " + project_id} ~{action} ~{source_bed} \
  ~{source_bim} \
  ~{source_fam} \
  ~{gcs_output_dir}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    String output_bed = new_bed
    String output_bim = new_bim
    String output_fam = new_fam
  }
}

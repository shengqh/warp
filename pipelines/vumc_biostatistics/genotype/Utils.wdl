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

# Ancestry workflow in Doug's group used this function, so just keep it here. 
# It would be used for ancestry workflow only.
# There is another copy of this function in tasks/vumc_biostatistics/Plink2Utils.wdl with different parameters.
task MergePgenFiles {
  input {
    Array[File] pgen_files
    Array[File] pvar_files
    Array[File] psam_files

    String output_prefix

    Int memory_gb = 20
    Int cpu = 8

    String docker = "shengqh/plink_1.9_2.0:20250304"
  }

  Int disk_size = ceil((size(pgen_files, "GB") + size(pvar_files, "GB") + size(psam_files, "GB"))  * 3) + 20

  String target_pgen = output_prefix + ".pgen"
  String target_pvar = output_prefix + ".pvar"
  String target_psam = output_prefix + ".psam"

  String merged_pgen = output_prefix + "-merge.pgen"
  String merged_pvar = output_prefix + "-merge.pvar"
  String merged_psam = output_prefix + "-merge.psam"

  command <<<

cat ~{write_lines(pgen_files)} > pgen.list
cat ~{write_lines(pvar_files)} > pvar.list
cat ~{write_lines(psam_files)} > psam.list

paste pgen.list pvar.list psam.list > merge.list

plink2 --pmerge-list merge.list --make-pgen --out ~{output_prefix} --threads ~{cpu}

rm -f ~{target_pgen} ~{target_pvar} ~{target_psam}

mv ~{merged_pgen} ~{target_pgen}
mv ~{merged_pvar} ~{target_pvar}
mv ~{merged_psam} ~{target_psam}

grep -v "^#" ~{target_psam} | wc -l | cut -d ' ' -f 1 > num_samples.txt
grep -v "^#" ~{target_pvar} | wc -l | cut -d ' ' -f 1 > num_variants.txt

>>>

  runtime {
    cpu: cpu
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_pgen = target_pgen
    File output_pvar = target_pvar
    File output_psam = target_psam

    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}

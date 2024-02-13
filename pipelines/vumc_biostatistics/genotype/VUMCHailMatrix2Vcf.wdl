version 1.0

workflow VUMCHailMatrix2Vcf {
  input {
    String input_hail_mt_path
    Float expect_vcf_size #almost equals to the size of corresponding vcf.gz file

    String reference_genome = "GRCh38"

    String target_prefix

    String? project_id
    String target_gcp_folder
  }

  call HailMatrix2Vcf {
    input:
      input_hail_mt_path = input_hail_mt_path,
      expect_vcf_size = expect_vcf_size,
      reference_genome = reference_genome,
      target_prefix = target_prefix,
      project_id = project_id,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String output_vcf = HailMatrix2Vcf.output_vcf
    String output_vcf_tbi = HailMatrix2Vcf.output_vcf_tbi
  }
}

task HailMatrix2Vcf {
  input {
    String input_hail_mt_path
    Float expect_vcf_size
    String reference_genome
    String target_prefix
    String? project_id
    String target_gcp_folder

    String docker = "shengqh/hail_gcp:20240211"
    Int memory_gb = 64
    Int preemptible = 1
    Int cpu = 4
    Int boot_disk_gb = 10  
    Float disk_size_factor = 3
  }

  Int disk_size = ceil(expect_vcf_size / 1024 / 1024 / 1024 * disk_size_factor) + 20
  Int total_memory_gb = memory_gb + 2

  String target_vcf = target_prefix + ".vcf.bgz"
  String target_vcf_tbi = target_vcf + ".tbi"
  
  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")
  String gcs_output_vcf = gcs_output_dir + "/" + target_vcf
  String gcs_output_vcf_tbi = gcs_output_vcf + ".tbi"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899
export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' pyspark-shell"

mkdir -p ./tmp

python3 <<CODE

import hail as hl

hl.init(spark_conf={
    "spark.driver.memory": "~{memory_gb}g",
    "spark.local.dir": "./tmp"
  },
  tmp_dir="./tmp",
  local_tmpdir="./tmp",
  idempotent=True)
hl.default_reference("~{reference_genome}")

mt = hl.read_matrix_table("~{input_hail_mt_path}")
hl.export_vcf(mt, "~{target_vcf}", tabix = True)

CODE

gsutil ~{"-u " + project_id} -m cp ~{target_vcf} ~{gcs_output_vcf}
gsutil ~{"-u " + project_id} -m cp ~{target_vcf_tbi} ~{gcs_output_vcf_tbi}

>>>

  runtime {
    cpu: cpu
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk ~{disk_size} HDD"
    memory: "~{total_memory_gb} GiB"
    bootDiskSizeGb: boot_disk_gb
  }
  output {
    String output_vcf = "~{gcs_output_vcf}"
    String output_vcf_tbi = "~{gcs_output_vcf_tbi}"
  }
}

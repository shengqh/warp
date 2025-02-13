version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAnnovar {
  input {
    File input_vcf

    File? annovar_db_tar_gz
    Float? annovar_db_umcompressed_gb
    String? annovar_param

    String target_prefix

    String? billing_project_id
    String? target_gcp_folder
  }

  Float? true_annovar_db_umcompressed_gb = if(defined(annovar_db_tar_gz)) then if(defined(annovar_db_umcompressed_gb)) then annovar_db_umcompressed_gb else size(annovar_db_tar_gz, "GB") * 10 else 0

  call Annovar {
    input:
      input_vcf = input_vcf,
      annovar_db_tar_gz = annovar_db_tar_gz,
      annovar_db_umcompressed_gb = true_annovar_db_umcompressed_gb,
      annovar_param = annovar_param,
      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = Annovar.annovar_file,
        is_move_file = false,
        project_id = billing_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File annovar_file = select_first([CopyFile.output_file, Annovar.annovar_file])
  }
}


task Annovar {
  input {
    File input_vcf

    File? annovar_db_tar_gz
    Float? annovar_db_umcompressed_gb
    String? annovar_param

    String target_prefix

    String? buildver = "hg38"

    Int memory_gb = 20
    Int cpu = 1

    String docker = "shengqh/annovar:20241117"
    Float vcf_disk_size_factor = 5
  }

  Float true_annovar_db_umcompressed_gb = if(defined(annovar_db_umcompressed_gb)) then annovar_db_umcompressed_gb else 0
  Int disk_size = ceil(size([input_vcf], "GB") * vcf_disk_size_factor + size(annovar_db_tar_gz, "GB") + true_annovar_db_umcompressed_gb) + 20

  String real_annovar_db = if(defined(annovar_db_tar_gz)) then sub(basename(select_first([annovar_db_tar_gz])), ".tar.gz$", "") else "/opt/annovar/humandb"
  String delete_annovar_db = if(defined(annovar_db_tar_gz)) then sub(basename(select_first([annovar_db_tar_gz])), ".tar.gz$", "") else ""  
  String real_annovar_param= if(defined(annovar_db_tar_gz)) then annovar_param else "-protocol refGene -operation g --remove"

  command <<<

zcat ~{input_vcf} | cut -f1-9 > ~{target_prefix}.avinput.vcf

convert2annovar.pl -format vcf4old ~{target_prefix}.avinput.vcf | cut -f1-7 | awk '{gsub(",\\*", "", $0); print}'> ~{target_prefix}.avinput

rm ~{target_prefix}.avinput.vcf

if [[ "~{annovar_db_tar_gz}" == "" ]]; then
  echo "No annovar database provided, use default /opt/annovar/humandb"
else
  tar -zxvf ~{annovar_db_tar_gz}
fi

table_annovar.pl ~{target_prefix}.avinput ~{real_annovar_db} -buildver ~{buildver} ~{real_annovar_param} --outfile ~{target_prefix}.annovar

rm -rf ~{target_prefix}.avinput ~{delete_annovar_db}

gzip ~{target_prefix}.annovar.~{buildver}_multianno.txt

>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File annovar_file = "~{target_prefix}.annovar.~{buildver}_multianno.txt.gz"
  }
}

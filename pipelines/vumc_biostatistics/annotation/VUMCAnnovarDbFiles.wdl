version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAnnovarDbFiles {
  input {
    File input_vcf

    Array[File] annovar_db_files
    String? annovar_param

    String target_prefix

    String? billing_project_id
    String? target_gcp_folder
  }

  call AnnovarDbFiles as Annovar {
    input:
      input_vcf = input_vcf,
      annovar_db_files = annovar_db_files,
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
    String annovar_file = select_first([CopyFile.output_file, Annovar.annovar_file])
  }
}

task AnnovarDbFiles {
  input {
    File input_vcf

    Array[File] annovar_db_files
    String? annovar_param

    String target_prefix

    String buildver = "hg38"

    Int memory_gb = 20
    Int cpu = 1

    String docker = "shengqh/annovar:20241117"
    Float vcf_disk_size_factor = 5
  }

  Int disk_size = ceil(size([input_vcf], "GB") * vcf_disk_size_factor + size(annovar_db_files, "GB")) + 20

  String real_annovar_param= select_first([annovar_param, "-protocol refGene -operation g --remove"])

  File annovar_db_1 = annovar_db_files[0]

  command <<<

zcat ~{input_vcf} | cut -f1-9 > ~{target_prefix}.avinput.vcf

convert2annovar.pl -format vcf4old ~{target_prefix}.avinput.vcf | cut -f1-7 | awk '{gsub(",\\*", "", $0); print}'> ~{target_prefix}.avinput

rm ~{target_prefix}.avinput.vcf

# extract the directory of the annovar db files
real_annovar_db=$(dirname ~{annovar_db_1})

table_annovar.pl ~{target_prefix}.avinput $real_annovar_db -buildver ~{buildver} ~{real_annovar_param} --outfile ~{target_prefix}.annovar

rm -rf ~{target_prefix}.avinput

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

version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow annotates VCF files using ANNOVAR with special handling for AGD variant format
# The workflow performs the following steps:
# 1. Takes an input VCF file and processes it through ANNOVAR
# 2. Preserves the original variant format from the VCF during annotation
# 3. Optionally moves the output files to a specified GCP folder
#
# Input parameters:
# - input_vcf: The input VCF file to be annotated
# - annovar_db_tar_gz: Optional tar.gz file containing ANNOVAR databases
# - annovar_db_umcompressed_gb: Optional estimate of uncompressed ANNOVAR database size
# - annovar_param: Optional parameters for ANNOVAR
# - target_prefix: Prefix for output files
# - target_gcp_folder: Optional GCP folder to store output files
#
# Output files:
# - annovar_file: The annotated output file from ANNOVAR
#
# Note: The workflow handles special cases in AGD variant format to ensure
# that the original VCF variant representation is preserved in the annotation output

workflow VUMCAnnovarWithVcfColumns {
  input {
    File input_vcf

    File? annovar_db_tar_gz
    Float? annovar_db_umcompressed_gb
    String? annovar_param

    String target_prefix

    String? target_gcp_folder
  }

  Float? true_annovar_db_umcompressed_gb = if(defined(annovar_db_tar_gz)) then if(defined(annovar_db_umcompressed_gb)) then annovar_db_umcompressed_gb else size(annovar_db_tar_gz, "GB") * 10 else 0

  call AnnovarWithVcfColumns as Annovar {
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
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File annovar_file = select_first([CopyFile.output_file, Annovar.annovar_file])
  }
}

# When annovar convert vcf to annovar input format, it might change the deletion format.
# For example, in AGD250 data, the deletion is: 
# 1       17002326        chr1:17002326:TG:T      TG      T
# When it is converted to annovar input format, it will become:
# 1       17002327        17002327        G       - 
# So we need to keep the original vcf columns and replace the annovar output with the vcf columns.
task AnnovarWithVcfColumns {
  input {
    File input_vcf

    File? annovar_db_tar_gz
    Float? annovar_db_umcompressed_gb
    String? annovar_param

    String target_prefix

    String? buildver = "hg38"

    Int memory_gb = 20
    Int cpu = 1

    String docker = "shengqh/annovar:20250805"
    Float vcf_disk_size_factor = 10
  }

  Float true_annovar_db_umcompressed_gb = if(defined(annovar_db_umcompressed_gb)) then annovar_db_umcompressed_gb else 0
  Int disk_size = ceil(size([input_vcf], "GB") * vcf_disk_size_factor + size(annovar_db_tar_gz, "GB") + true_annovar_db_umcompressed_gb) + 20

  String real_annovar_db = if(defined(annovar_db_tar_gz)) then sub(basename(select_first([annovar_db_tar_gz])), ".tar.gz$", "") else "/opt/annovar/humandb"
  String delete_annovar_db = if(defined(annovar_db_tar_gz)) then sub(basename(select_first([annovar_db_tar_gz])), ".tar.gz$", "") else ""  
  String real_annovar_param= if(defined(annovar_db_tar_gz)) then annovar_param else "-protocol refGene -operation g --remove"

  command <<<
set -e

# decompress and keep only the first 9 columns
zcat ~{input_vcf} | cut -f1-9 > ~{target_prefix}.avinput.vcf

# for the final file, we don't need the last FORMAT column.
grep -v "^##" ~{target_prefix}.avinput.vcf | cut -f 1-8 > vcf_columns.txt

# convert vcf to annovar input format
convert2annovar.pl -format vcf4old ~{target_prefix}.avinput.vcf | cut -f1-7 | awk '{gsub(",\\*", "", $0); print}'> ~{target_prefix}.avinput

# remove the intermediate vcf file
rm ~{target_prefix}.avinput.vcf

# if annovar database is provided, extract it
# otherwise, use the default /opt/annovar/humandb
if [[ "~{annovar_db_tar_gz}" == "" ]]; then
  echo "No annovar database provided, use default /opt/annovar/humandb"
else
  tar -zxvf ~{annovar_db_tar_gz}
fi

# run annovar
table_annovar.pl ~{target_prefix}.avinput ~{real_annovar_db} -buildver ~{buildver} ~{real_annovar_param} --outfile ~{target_prefix}.annovar

status=\$?
if [[ \$status -ne 0 ]]; then
  echo "convert2annovar.pl failed with status \$status"
  exit 1
fi

if [[ ! -f ~{target_prefix}.annovar.hg38_multianno.txt ]]; then #it is possible that there is no enough space but the file is generated.
  echo "table_annovar.pl failed: output file ~{target_prefix}.annovar.hg38_multianno.txt not found"
  exit 1
fi

# extract the relevant columns from the annovar output. Since annovar will keep the order of the variants, we don't need to worry about it.
cut -f 6- ~{target_prefix}.annovar.hg38_multianno.txt > annovar.data.txt

# replace the annovar first 5 columns with the vcf columns
paste vcf_columns.txt annovar.data.txt > ~{target_prefix}.annovar.~{buildver}.txt

# remove the intermediate files
rm -rf ~{target_prefix}.avinput vcf_columns.txt annovar.data.txt ~{target_prefix}.annovar.hg38_multianno.txt

# compress the final output
gzip ~{target_prefix}.annovar.~{buildver}.txt

>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File annovar_file = "~{target_prefix}.annovar.~{buildver}.txt.gz"
  }
}

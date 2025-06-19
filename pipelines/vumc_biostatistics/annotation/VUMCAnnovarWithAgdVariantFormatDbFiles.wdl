version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow annotates VCF files using ANNOVAR with special handling for AGD variant format databases
# The workflow performs the following steps:
# 1. Takes an input VCF file and processes it through ANNOVAR using provided database files
# 2. Preserves the original variant format from the VCF during annotation
# 3. Optionally copies the output files to a specified GCP folder
#
# Input parameters:
# - input_vcf: The input VCF file to be annotated
# - annovar_db_files: Array of ANNOVAR database files
# - annovar_param: Optional parameters for ANNOVAR
# - target_prefix: Prefix for output files
# - billing_project_id: Optional GCP project ID for billing
# - target_gcp_folder: Optional GCP folder to store output files
#
# Output files:
# - annovar_file: The annotated output file from ANNOVAR
#
# Note: The workflow handles special cases in AGD variant format to ensure
# that the original VCF variant representation is preserved in the annotation output


workflow VUMCAnnovarWithAgdVariantFormatDbFiles {
  input {
    File input_vcf

    Array[File] annovar_db_files
    String? annovar_param

    String target_prefix

    String? billing_project_id
    String? target_gcp_folder
  }

  call AnnovarWithAgdVariantFormatDbFiles as Annovar {
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

# When annovar convert vcf to annovar input format, it might change the deletion format.
# For example, in AGD250 data, the deletion is: 
# 1       17002326        chr1:17002326:TG:T      TG      T
# When it is converted to annovar input format, it will become:
# 1       17002327        17002327        G       - 
# So we need to keep the original vcf columns and replace the annovar output with the vcf columns.
task AnnovarWithAgdVariantFormatDbFiles {
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

# decompress and keep only the first 9 columns
zcat ~{input_vcf} | cut -f1-9 > ~{target_prefix}.avinput.vcf

# for the final file, we don't need the last FORMAT column.
grep -v "^##" ~{target_prefix}.avinput.vcf | cut -f 1-8 > vcf_columns.txt

# convert vcf to annovar input format
convert2annovar.pl -format vcf4old ~{target_prefix}.avinput.vcf | cut -f1-7 | awk '{gsub(",\\*", "", $0); print}'> ~{target_prefix}.avinput

# remove the intermediate vcf file
rm ~{target_prefix}.avinput.vcf

# extract the directory of the annovar db files
real_annovar_db=$(dirname ~{annovar_db_1})

# run annovar
table_annovar.pl ~{target_prefix}.avinput $real_annovar_db -buildver ~{buildver} ~{real_annovar_param} --outfile ~{target_prefix}.annovar

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

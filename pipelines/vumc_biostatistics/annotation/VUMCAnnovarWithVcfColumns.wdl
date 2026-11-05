version 1.0

## VUMCAnnovarWithVcfColumns
##
## Annotates VCF files using ANNOVAR while preserving original variant format.
## Developed by VUMC Biostatistics for variant annotation.
##
## ### Workflow Purpose:
## This workflow processes VCF files through ANNOVAR while preserving the original variant
## representation from the VCF. It handles special cases in AGD variant format where ANNOVAR
## may alter variant format (e.g., deletions represented differently). Optionally filters
## results based on a specified column value and moves output files to GCP.
##
## ### Workflow Steps:
## 1. **Convert and Annotate**: Converts VCF input to ANNOVAR format and runs table_annovar
## 2. **Preserve VCF Format**: Replaces ANNOVAR's first 5 columns with original VCF columns
## 3. **Filter Results (Optional)**: If filter_column is specified, keeps only entries with non-empty values in that column
## 4. **Move to GCP (Optional)**: If target_gcp_folder is specified, copies the result to Google Cloud Storage
##
## ### Inputs:
## - input_vcf: The input VCF file to be annotated
## - annovar_db_tar_gz: Optional tar.gz file containing ANNOVAR databases
## - annovar_db_umcompressed_gb: Optional estimate of uncompressed ANNOVAR database size in GB
## - annovar_param: Optional parameters to pass to table_annovar
## - target_prefix: Prefix for output files
## - target_gcp_folder: Optional Google Cloud Storage folder path for copying results
##
## ### Outputs:
## - annovar_file: The annotated output file from ANNOVAR, optionally filtered and compressed
##
## ### Notes:
## - The workflow preserves the first 8 VCF columns (CHROM through INFO) in the output
## - ANNOVAR columns (6 onwards from multianno output) are appended after the VCF columns
## - If filter_column is specified, only rows with non-empty values in that column are retained
## - Output is compressed with gzip

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAnnovarWithVcfColumns {
  input {
    File input_vcf

    File? annovar_db_tar_gz
    Float? annovar_db_umcompressed_gb
    String? annovar_db_folder
    String? annovar_param
    String? filter_column

    String target_prefix

    String? target_gcp_folder
  }

  Float? true_annovar_db_umcompressed_gb = if(defined(annovar_db_tar_gz)) then if(defined(annovar_db_umcompressed_gb)) then annovar_db_umcompressed_gb else size(annovar_db_tar_gz, "GB") * 10 else 0

  call AnnovarWithVcfColumns as Annovar {
    input:
      input_vcf = input_vcf,
      annovar_db_tar_gz = annovar_db_tar_gz,
      annovar_db_umcompressed_gb = true_annovar_db_umcompressed_gb,
      annovar_db_folder = annovar_db_folder,
      annovar_param = annovar_param,
      filter_column = filter_column,
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
    String? annovar_db_folder
    String? annovar_param
    String? filter_column

    String target_prefix

    String? buildver = "hg38"

    Int memory_gb = 20
    Int cpu = 1

    String docker = "shengqh/annovar:20250805"
    Float vcf_disk_size_factor = 10
  }

  Float true_annovar_db_umcompressed_gb = if(defined(annovar_db_umcompressed_gb)) then annovar_db_umcompressed_gb else 0
  Int disk_size = ceil(size([input_vcf], "GB") * vcf_disk_size_factor + size(annovar_db_tar_gz, "GB") + true_annovar_db_umcompressed_gb) + 20

  String real_annovar_db = if(defined(annovar_db_tar_gz)) then if(defined(annovar_db_folder)) then annovar_db_folder else sub(basename(select_first([annovar_db_tar_gz])), ".tar.gz$", "") else "/opt/annovar/humandb"
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

# filter based on filter_column if provided
if [[ -n "~{filter_column}" ]]; then
  # Get the column number for filter_column
  filter_col_num=$(head -n 1 ~{target_prefix}.annovar.~{buildver}.txt | tr '\t' '\n' | grep -n "~{filter_column}" | cut -d: -f1)
  
  if [[ -n "$filter_col_num" ]]; then
    # Filter the file to keep only entries where the filter_column is not empty
    awk -v col="$filter_col_num" '(NR==1) || ($col != "")' ~{target_prefix}.annovar.~{buildver}.txt > ~{target_prefix}.annovar.~{buildver}.txt.tmp
    mv ~{target_prefix}.annovar.~{buildver}.txt.tmp ~{target_prefix}.annovar.~{buildver}.txt
  else
    echo "Warning: filter_column '~{filter_column}' not found in the header"
  fi
fi

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

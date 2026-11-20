version 1.0

## VUMCAgdAnnovarClinvar
##
## Annotates VCF files using ANNOVAR with ClinVar data while preserving original variant format.
## Developed by VUMC Biostatistics for variant annotation.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This workflow processes VCF files through ANNOVAR with ClinVar annotation while preserving the original variant
## representation from the VCF. It handles special cases in AGD variant format where ANNOVAR
## may alter variant format (e.g., deletions represented differently). Filters results based on ClinVar
## significance and optionally moves output files to GCP.
##
## ### Workflow Steps:
## 1. **Annotate with ClinVar**: Converts VCF input to ANNOVAR format and runs table_annovar with ClinVar database
## 2. **Preserve VCF Format**: Replaces ANNOVAR's first 5 columns with original VCF variant identifiers
## 3. **Filter by ClinVar Significance**: Keeps only entries with non-empty CLNSIG (ClinVar significance) values
## 4. **Move to GCP (Optional)**: If target_gcp_folder is specified, copies the result to Google Cloud Storage
##
## ### Inputs:
## - input_vcf: The input VCF file to be annotated
## - clinvar_txt: ClinVar annotation file (hg38 format)
## - clinvar_index: Index file for the ClinVar annotation file
## - target_prefix: Prefix for output files
## - target_gcp_folder: Optional Google Cloud Storage folder path for copying results
##
## ### Outputs:
## - annovar_file: The ClinVar-annotated output file, filtered and compressed
##
## ### Notes:
## - The workflow preserves the VCF variant identifiers (column 3) from the original input
## - ANNOVAR and ClinVar columns are appended after the VCF columns
## - Only variants with non-empty CLNSIG values are retained in the output
## - Output is compressed with gzip

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAgdAnnovarClinvar {
  input {
    File input_vcf

    File clinvar_txt
    File clinvar_index

    String target_prefix

    String? target_gcp_folder
  }

  call AgdAnnovarClinvar as Annovar {
    input:
      input_vcf = input_vcf,
      clinvar_txt = clinvar_txt,
      clinvar_index = clinvar_index,
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
task AgdAnnovarClinvar {
  input {
    File input_vcf

    File clinvar_txt
    File clinvar_index

    String filter_column = "CLNSIG"

    String target_prefix

    Int memory_gb = 20
    Int cpu = 1

    String docker = "shengqh/annovar:20250805"
    Float vcf_disk_size_factor = 10
  }

  Int disk_size = ceil(size([clinvar_txt, clinvar_index], "GB")  + size([input_vcf], "GB") * vcf_disk_size_factor ) + 20
  String clinvar_name = sub(basename(clinvar_txt, ".txt"), "hg38_", "")

  command <<<
set -e

mkdir -p humandb
cd humandb
ln -s ~{clinvar_txt} .
ln -s ~{clinvar_index} .
cd ..

# decompress and keep only the first 9 columns
zcat ~{input_vcf} | cut -f1-9 > ~{target_prefix}.avinput.vcf

# for the final file, we only need the 3rd column (the variant ID).
grep -v "^##" ~{target_prefix}.avinput.vcf | cut -f 3 > vcf_columns.txt

# convert vcf to annovar input format
convert2annovar.pl -format vcf4old ~{target_prefix}.avinput.vcf | cut -f1-7 | awk '{gsub(",\\*", "", $0); print}'> ~{target_prefix}.avinput

# remove the intermediate vcf file
rm ~{target_prefix}.avinput.vcf

# run annovar
table_annovar.pl ~{target_prefix}.avinput humandb -buildver hg38 -protocol ~{clinvar_name} -operation f --remove --outfile ~{target_prefix}.annovar

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
paste vcf_columns.txt annovar.data.txt > ~{target_prefix}.annovar.hg38.txt

# filter based on filter_column

# Get the column number for filter_column
filter_col_num=$(head -n 1 ~{target_prefix}.annovar.hg38.txt | tr '\t' '\n' | grep -n "~{filter_column}" | cut -d: -f1)

if [[ -n "$filter_col_num" ]]; then
  # Filter the file to keep only entries where the filter_column is not empty
  awk -v col="$filter_col_num" '(NR==1) || ($col != "")' ~{target_prefix}.annovar.hg38.txt > ~{target_prefix}.annovar.hg38.txt.tmp
  mv ~{target_prefix}.annovar.hg38.txt.tmp ~{target_prefix}.annovar.hg38.txt
else
  echo "Warning: filter_column '~{filter_column}' not found in the header"
fi

# remove the intermediate files
rm -rf ~{target_prefix}.avinput vcf_columns.txt annovar.data.txt ~{target_prefix}.annovar.hg38_multianno.txt

# compress the final output
gzip ~{target_prefix}.annovar.hg38.txt

>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File annovar_file = "~{target_prefix}.annovar.hg38.txt.gz"
  }
}

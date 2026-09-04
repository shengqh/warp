version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC ANNOVAR ClinVar Annotation Pipeline (hg38)
##
## This workflow annotates variants in a VCF file with ClinVar clinical significance
## information using ANNOVAR, producing a gzipped multianno text output.
## Developed by VUMC Biostatistics for clinical variant interpretation.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given an input VCF file and a preformatted ClinVar ANNOVAR database (text + index),
## this workflow converts the VCF to ANNOVAR input format and annotates variants
## with ClinVar information using ANNOVAR's table_annovar.pl.
##
## ### Workflow Steps:
## 1. **Annovar**: Convert VCF to ANNOVAR input, run table_annovar.pl with the ClinVar
##    database, and gzip the resulting multianno output file.
## 2. **CopyFile** (optional): Copy the annovar output file to a target GCP folder.
##
## ### Inputs:
## - input_vcf: Input VCF file to be annotated.
## - clinvar_txt: ANNOVAR-formatted ClinVar database text file.
## - clinvar_index: Index file for the ClinVar database text file.
## - annovar_param: Additional ANNOVAR parameters (currently unused in the command).
## - target_prefix: Prefix for all output files.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - annovar_file: Gzipped ANNOVAR multianno text file with ClinVar annotations.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAnnovarHg38Clinvar {
  input {
    File input_vcf

    File clinvar_txt
    File clinvar_index

    String target_prefix

    String? target_gcp_folder
  }

  call Annovar {
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


task Annovar {
  input {
    File input_vcf

    File clinvar_txt
    File clinvar_index

    String target_prefix

    String? buildver = "hg38"

    Int memory_gb = 20
    Int cpu = 1

    String docker = "shengqh/annovar:20241117"
    Float vcf_disk_size_factor = 5
  }

  Int disk_size = ceil(size([clinvar_txt, clinvar_index], "GB")  + size([input_vcf], "GB") * vcf_disk_size_factor ) + 20
  String clinvar_name = sub(basename(clinvar_txt, ".txt"), "hg38_", "")

  command <<<

mkdir -p humandb
cd humandb
ln -s ~{clinvar_txt} .
ln -s ~{clinvar_index} .
cd ..

zcat ~{input_vcf} | cut -f1-9 > ~{target_prefix}.avinput.vcf

convert2annovar.pl -format vcf4old ~{target_prefix}.avinput.vcf | cut -f1-7 | awk '{gsub(",\\*", "", $0); print}'> ~{target_prefix}.avinput

rm ~{target_prefix}.avinput.vcf

table_annovar.pl ~{target_prefix}.avinput humandb -buildver hg38 -protocol ~{clinvar_name} -operation f --remove --outfile ~{target_prefix}.annovar

rm -rf ~{target_prefix}.avinput humandb

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

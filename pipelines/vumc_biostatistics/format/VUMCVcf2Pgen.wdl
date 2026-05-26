version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC VCF to PGEN Conversion Workflow
##
## This workflow converts VCF format genetic data files to PGEN format.
## Developed by VUMC/VANGARD team for efficient processing of population genetic data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## VCF files are commonly used for storing genetic variants, but PGEN format offers 
## advantages for certain analyses. This workflow handles the conversion process.
##
## ### Workflow Steps:
## 1. Run plink2 to convert VCF file to PGEN format
## 2. Optionally copy the resulting PGEN, PSAM, and PVAR files to a specified GCP folder
##
## ### Inputs:
## - input_vcf: Input VCF file to be converted
## - input_vcf_index: Index file for the input VCF
## - output_prefix: Prefix for output filenames
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_pgen: Generated PGEN file
## - output_psam: Generated PSAM file
## - output_pvar: Generated PVAR file
##
## ### Notes:
## - Uses plink2 for the conversion
## - File copy operation to GCP is optional and only executed if a target folder is provided
## - If the original VCF file has chrX, it might be failed without sex information

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCVcf2Pgen {
  input {
    File input_vcf
    File input_vcf_index

    String output_prefix

    String? target_gcp_folder
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    input_vcf: "Input VCF file to be converted to PGEN format"
    input_vcf_index: "Index file for the input VCF"
    output_prefix: "Prefix for output filenames"
    target_gcp_folder: "Optional GCP folder path to copy output files to after completion"
  }

  call Vcf2Pgen {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      output_prefix = output_prefix,
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyFiles as CopyFile {
      input:
        source_file1 = Vcf2Pgen.output_pgen,
        source_file2 = Vcf2Pgen.output_psam,
        source_file3 = Vcf2Pgen.output_pvar,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, Vcf2Pgen.output_pgen])
    File output_psam = select_first([CopyFile.output_file2, Vcf2Pgen.output_psam])
    File output_pvar = select_first([CopyFile.output_file3, Vcf2Pgen.output_pvar])
  }
}

task Vcf2Pgen {
  input {
    File input_vcf
    File input_vcf_index

    String output_prefix

    Int memory_gb = 20
    Int cpu = 8
    Float disk_size_factor = 2.0
    Int additional_disk_gb = 5

    Int? disk_size_override

    String docker = "shengqh/plink_1.9_2.0:20260526"
  }

  Int disk_size = select_first([disk_size_override, ceil(disk_size_factor * size(input_vcf, "GB")) + additional_disk_gb])

  command <<<
set -euo pipefail

plink2 --vcf "~{input_vcf}" \
  --threads ~{cpu} \
  --make-pgen \
  --out "~{output_prefix}"

if [ -f "~{output_prefix}.pgen" ]; then
  echo "PGEN file created successfully"
else
  echo "Error: PGEN file was not created" >&2
  exit 1
fi
  >>>

  runtime {
    docker: docker
    memory: "~{memory_gb} GiB"
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    preemptible: 3
  }

  output {
    File output_pgen = "~{output_prefix}.pgen"
    File output_pvar = "~{output_prefix}.pvar"
    File output_psam = "~{output_prefix}.psam"
  }
}
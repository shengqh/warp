version 1.0

## VUMC VCF to BGEN Conversion Workflow
##
## This workflow converts VCF format genetic data files to BGEN format.
## Developed by VUMC/VANGARD team for efficient processing of population genetic data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## VCF files are commonly used for storing genetic variants, but BGEN format offers 
## advantages for certain analyses. This workflow handles the conversion process.
##
## ### Workflow Steps:
## 1. Run plink2 to convert VCF file to BGEN format with 8-bit encoding
## 2. Optionally copy the resulting BGEN and sample files to a specified GCP folder
##
## ### Inputs:
## - input_vcf: Input VCF file to be converted
## - input_vcf_index: Index file for the input VCF
## - input_psam: Optional PSAM file for additional sample information
## - output_prefix: Prefix for output filenames
## - project_id: Optional GCP project ID for file copy operations
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_bgen: Generated BGEN file
## - output_sample: Generated sample file
##
## ### Notes:
## - Uses plink2 with settings optimized for BGEN conversion
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCVcf2Bgen {
  input {
    File input_vcf
    File input_vcf_index
    File? input_psam
    String output_prefix

    String? project_id
    String? target_gcp_folder
  }

  call Vcf2Bgen {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      input_psam = input_psam,
      output_prefix = output_prefix,
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = Vcf2Bgen.output_bgen,
        source_file2 = Vcf2Bgen.output_sample,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_bgen = select_first([CopyFile.output_file1, Vcf2Bgen.output_bgen])
    File output_sample = select_first([CopyFile.output_file2, Vcf2Bgen.output_sample])
  }
}

task Vcf2Bgen {
  input {
    File input_vcf
    File input_vcf_index
    File? input_psam
    String output_prefix
    Int memory_gb = 64
    Int cpu = 2
    Int disk_size_factor = 3
    Int? disk_size_override
    String docker = "shengqh/plink_1.9_2.0:20250304"
  }

  Int disk_size = select_first([disk_size_override, disk_size_factor * ceil(size(input_vcf, "GB")) + 10])

  command <<<
set -euo pipefail

# Convert VCF to BGEN using plink2
plink2 --vcf "~{input_vcf}" ~{"--psam " + input_psam} \
  --out "~{output_prefix}" \
  --export bgen-1.2 bits=8 ref-first \
  --threads ~{cpu} 

if [ -f "~{output_prefix}.bgen" ]; then
  echo "BGEN file created successfully"
else
  echo "Error: BGEN file was not created" >&2
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
    File output_bgen = "~{output_prefix}.bgen"
    File output_sample = "~{output_prefix}.sample"
  }
}
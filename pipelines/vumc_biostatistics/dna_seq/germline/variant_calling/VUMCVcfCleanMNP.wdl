version 1.0

# This workflow cleans VCF files by removing multi-nucleotide polymorphisms (MNPs).
# Developed by VUMC Biostatistics for variant calling quality control.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Filters VCF to identify and remove MNP variants
# 2. Retains only single nucleotide variants (SNVs) and simple indels
# 3. Replace "MQ" to "RAW_MQ" for GVS joint call
# 4. Compresses cleaned VCF with bgzip
# 5. Indexes cleaned VCF with tabix
# 6. Optionally copies results to a GCP storage location
#
# Inputs:
# - sample_name: Name of the sample for output file naming
# - input_vcf: VCF file to clean
# - input_vcf_index: Index file for input VCF
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_vcf: Cleaned VCF file (bgzip compressed)
# - output_vcf_index: Index file for cleaned VCF (tabix format)


import "../../../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# WORKFLOW DEFINITION 
workflow VUMCVcfCleanMNP {
  input {
    String sample_name

    File input_vcf
    File input_vcf_index

    String? target_gcp_folder  
  }  

  call VcfCleanMNP {
    input:
      sample_name = sample_name,
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = VcfCleanMNP.output_vcf,
        source_file2 = VcfCleanMNP.output_vcf_index,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder]),
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file1, VcfCleanMNP.output_vcf])
    File output_vcf_index = select_first([CopyFile.output_file2, VcfCleanMNP.output_vcf_index])
  }
}

task VcfCleanMNP {
  input {
    String sample_name

    File input_vcf
    File input_vcf_index

    # Runtime parameters
    String docker = "shengqh/samtools_bcftools_tabix:v1.23"
    Int mem_gb = 10
    Int preemptible_attempts = 3
  }

  Int disk_size = ceil(size(input_vcf, "GB") * 2) + 10

  command <<<

    set -e

    bcftools view "~{input_vcf}" | awk -F"\t" 'BEGIN{OFS="\t"}
    /^#/ {print; next}
    {
        split($5, alt_arr, ",");

        is_mnp = 0;
        if (length($4) > 1 && length($4) == length(alt_arr[1])) {
            is_mnp = 1;
        }

        if (is_mnp == 0 && $2 != last_pos) {
            sub(";MQ", ";RAW_MQ", $8)
            print $0;
            last_pos = $2;
        }
    }' | bgzip -c > ~{sample_name}.clean.vcf.gz

    tabix -p vcf "~{sample_name}.clean.vcf.gz"

  >>>
  runtime {
    docker: docker
    memory: mem_gb + " GiB"
    disks: "local-disk " + disk_size + " HDD"
    preemptible: preemptible_attempts
  }
  output {
    File output_vcf = "~{sample_name}.clean.vcf.gz"
    File output_vcf_index = "~{sample_name}.clean.vcf.gz.tbi"
  }
}

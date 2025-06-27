version 1.0

## VUMC SRA to FASTQ Conversion Workflow
##
## This workflow converts SRA format files to FASTQ format for sequencing data analysis.
## Developed by VUMC/VANGARD team for efficient processing of sequencing data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## SRA (Sequence Read Archive) is a common format for storing sequencing data in public repositories.
## Converting SRA files to FASTQ format enables their use in standard bioinformatics pipelines.
##
## ### Workflow Steps:
## 1. Prefetch: Download SRA file from NCBI using the provided SRR accession
## 2. FasterqDump: Convert the downloaded SRA file to paired FASTQ files
## 3. Optionally copy the resulting FASTQ files to a specified GCP folder
##
## ### Inputs:
## - SRR: SRA Run accession number
## - ngc_file: Optional NGC file for accessing protected data
## - sra_gb: Disk space allocated for SRA file (default: 10GB)
## - umcompressed_fastq_gb: Disk space allocated for uncompressed FASTQ (default: 50GB)
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_fastq1: Forward reads FASTQ file
## - output_fastq2: Reverse reads FASTQ file
##
## ### Notes:
## - Uses NCBI SRA-tools for efficient conversion
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCSra2Fastq {
  input {
    String SRR
    File? ngc_file
    File user_settings_file

    String? target_gcp_folder
  }

  call Prefetch {
    input:
      SRR = SRR,
      ngc_file = ngc_file,
      user_settings_file = user_settings_file
  }

  call FasterqDump {
    input:
      input_sra = Prefetch.output_sra,
      user_settings_file = user_settings_file
  }

  if (defined(target_gcp_folder)) {
    String fastq1 = "~{FasterqDump.output_fastq1}"
    String fastq2 = "~{FasterqDump.output_fastq2}"

    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = fastq1,
        source_file2 = fastq2,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_fastq1 = select_first([CopyFile.output_file1, FasterqDump.output_fastq1])
    File output_fastq2 = select_first([CopyFile.output_file2, FasterqDump.output_fastq2])
  }
}

task Prefetch {
  input {
    String SRR
    File? ngc_file
    Int sra_gb = 20
    Int machine_mem_gb = 10
    File user_settings_file

    String docker="uwgac/fetch-dbgap-files:0.3.0"
    String prefetch = "/opt/sratoolkit.3.2.1-ubuntu64/bin/prefetch"
  }

  command <<<

if [[ ! -s ${HOME}/.ncbi ]]; then
  echo mkdir ${HOME}/.ncbi
  mkdir ${HOME}/.ncbi
fi

if [[ ! -s ${HOME}/.ncbi/user-settings.mkfg ]]; then
  echo cp user-settings.mkfg
  cp ~{user_settings_file} ${HOME}/.ncbi/user-settings.mkfg
fi

~{prefetch} ~{SRR} --max-size u ~{"--ngc " + ngc_file} -o ~{SRR}.sra

>>>

  runtime {
    #ncbi/sra-tools:3.2.1 doesn't support bash
    docker: docker
    preemptible: 3
    memory: machine_mem_gb + " GB"
    cpu: 1
    disks: "local-disk " + sra_gb + " HDD"
  }

  output {
    File output_sra = "~{SRR}.sra"
  }
}

task FasterqDump {
  input {
    File input_sra
    Int umcompressed_fastq_gb = 100
    Int machine_mem_gb = 10
    Int threads = 1
    File user_settings_file

    String docker="uwgac/fetch-dbgap-files:0.3.0"
    String fasterq = "/opt/sratoolkit.3.2.1-ubuntu64/bin/fasterq-dump"
  }

  Int disk_size_gb = ceil(size(input_sra) + umcompressed_fastq_gb * 1.5)
  String sra_name = basename(input_sra)

  command <<<

if [[ ! -s ${HOME}/.ncbi ]]; then
  echo mkdir ${HOME}/.ncbi
  mkdir ${HOME}/.ncbi
fi

if [[ ! -s ${HOME}/.ncbi/user-settings.mkfg ]]; then
  echo cp user-settings.mkfg
  cp ~{user_settings_file} ${HOME}/.ncbi/user-settings.mkfg
fi

~{fasterq} -e ~{threads} -p ~{input_sra}

status=$?
if [ $status -ne 0 ]; then
  echo "fasterq-dump failed with status $status"
  exit $status
fi

gzip ~{sra_name}_1.fastq ~{sra_name}_2.fastq

>>>

  runtime {
    #ncbi/sra-tools:3.2.1 doesn't support bash
    docker: docker
    preemptible: 3
    memory: machine_mem_gb + " GB"
    cpu: threads
    disks: "local-disk " + disk_size_gb + " HDD"
  }

  output {
    File output_fastq1 = "~{sra_name}_1.fastq.gz"
    File output_fastq2 = "~{sra_name}_2.fastq.gz"
  }
}

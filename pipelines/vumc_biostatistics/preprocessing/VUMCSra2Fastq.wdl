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
## - billing_gcp_project_id: Optional GCP project ID for file copy operations
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

    Int sra_gb = 10
    Int umcompressed_fastq_gb = 50

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  call Prefetch {
    input:
      SRR = SRR,
      ngc_file = ngc_file,
      sra_gb = sra_gb,
  }

  call FasterqDump {
    input:
      input_sra = Prefetch.output_sra,
      umcompressed_fastq_gb = umcompressed_fastq_gb,
      threads = 6
  }

  if (defined(target_gcp_folder)) {
    String fastq1 = "~{FasterqDump.output_fastq1}"
    String fastq2 = "~{FasterqDump.output_fastq2}"

    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = fastq1,
        source_file2 = fastq2,
        is_move_file = false,
        project_id = billing_gcp_project_id,
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
    Int sra_gb = 10
    Int machine_mem_gb = 4
  }

  command <<<
  
prefetch ~{SRR} --max-size u ~{"--ngc " + ngc_file} -o ~{SRR}.sra

>>>

  runtime {
    docker: "ncbi/sra-tools:3.2.1"
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
    Int umcompressed_fastq_gb = 50
    Int machine_mem_gb = 4
    Int threads = 1
  }

  Int disk_size_gb = ceil(size(input_sra) + umcompressed_fastq_gb * 1.5)
  String sra_name = basename(input_sra)

  command <<<
  
fasterq-dump -e ~{threads} -p ~{input_sra}

status=$?
if [ $status -ne 0 ]; then
  echo "fasterq-dump failed with status $status"
  exit $status
fi

gzip ~{sra_name}_1.fastq ~{sra_name}_2.fastq

>>>

  runtime {
    docker: "ncbi/sra-tools:3.2.1"
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

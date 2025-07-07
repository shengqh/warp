version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow converts Regenie output to PRScs-SST format for polygenic risk score calculation.
# Developed by VUMC Biostatistics for population-specific GWAS studies.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Transform Regenie output file into PRScs-SST compatible summary statistics format
# 2. Format columns to match required PRScs-SST input format (SNP, A1, A2, BETA, P)
# 3. Optionally copy results to a GCP storage location
#
# Inputs:
# - input_regenie: Regenie output file containing summary statistics
# - output_prefix: Prefix for the output SST file
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_sst_file: Path to the formatted PRScs-SST summary statistics file

workflow VUMCPrsStep1Regenie2PRScsSST {
  input {
    File input_regenie

    String output_prefix

    String? target_gcp_folder
  }

  call Regenie2PRScsSST {
    input:
      input_regenie = input_regenie,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = Regenie2PRScsSST.output_sst_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_sst_file = select_first([CopyFile.output_file, Regenie2PRScsSST.output_sst_file])
  }
}

task Regenie2PRScsSST {
  input {
    File input_regenie

    String output_prefix

    Int preemptible=3
    Int memory_gb = 5
    Int additional_disk_size_gb = 2
  }

  Int disk_size = ceil(size([input_regenie], "GB") * 3) + additional_disk_size_gb

  command <<<

  zcat ~{input_regenie} | awk 'NR==1 {print "SNP\tA1\tA2\tBETA\tP"}; NR>1 {print $3"\t"$5"\t"$4"\t"$9"\t"10^-$12}' > ~{output_prefix}.sst

  >>>

  runtime{
    docker: "ubuntu:20.04"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
   }

  output {
    File output_sst_file = "~{output_prefix}.sst"
  }
}

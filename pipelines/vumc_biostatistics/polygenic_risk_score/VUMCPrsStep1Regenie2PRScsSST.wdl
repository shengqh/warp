version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow converts Regenie output to PRScs-SST format for polygenic risk score calculation.
# Developed by VUMC Biostatistics for population-specific GWAS studies.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Transform Regenie output file into PRScs-SST compatible summary statistics format
# 2. Format columns to match required PRScs-SST input format (SNP, A1, A2, BETA, P)
#    SNP: rsID
#    A1: Effect allele
#    A2: Non‑effect allele
#    BETA: Effect size estimate (or log odds ratio for case/control traits)
#    P: P‑value
# 3. Optionally map variant IDs to rsIDs using provided mapping file (ID,RSID columns)
# 4. Optionally copy results to a GCP storage location
#
# Inputs:
# - input_regenie: Regenie output file containing summary statistics
# - rsid_variantid_map_file: Optional CSV file mapping variant IDs to rsIDs (ID,RSID columns)
# - output_prefix: Prefix for the output SST file
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_sst_file: Path to the formatted PRScs-SST summary statistics file

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "./PRSUtils.wdl" as PRSUtils

workflow VUMCPrsStep1Regenie2PRScsSST {
  input {
    File input_regenie

    File? rsid_variantid_map_file # ID,RSID map file

    String output_prefix

    String? target_gcp_folder
  }

  call Regenie2PRScsSST {
    input:
      input_regenie = input_regenie,
      output_prefix = output_prefix
  }

  if(defined(rsid_variantid_map_file)){
    call PRSUtils.variantID2rsID {
      input:
        input_sst = Regenie2PRScsSST.output_sst_file,
        input_bim = Regenie2PRScsSST.output_bim_file_for_PRScs,
        rsid_variantid_map_file = select_first([rsid_variantid_map_file]),
        output_prefix = output_prefix
    }
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = select_first([variantID2rsID.output_sst_file, Regenie2PRScsSST.output_sst_file]),
        source_file2 = select_first([variantID2rsID.output_bim_file_for_PRScs, Regenie2PRScsSST.output_bim_file_for_PRScs]),
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_sst_file = select_first([CopyFile.output_file1, variantID2rsID.output_sst_file, Regenie2PRScsSST.output_sst_file])
    File output_bim_file_for_PRScs = select_first([CopyFile.output_file2, variantID2rsID.output_bim_file_for_PRScs, Regenie2PRScsSST.output_bim_file_for_PRScs])
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

  zcat ~{input_regenie} | awk 'BEGIN {OFS="\t"}; NR==1 {next}; {chr=$1; if (chr=="X") chr=23; else if (chr=="Y") chr=24; else if (chr=="MT" || chr=="M") chr=25; print chr, $3, 0, $2, $5, $4}' > ~{output_prefix}.bim

  >>>

  runtime {
    docker: "ubuntu:20.04"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    File output_sst_file = "~{output_prefix}.sst"
    File output_bim_file_for_PRScs = "~{output_prefix}.bim"
  }
}

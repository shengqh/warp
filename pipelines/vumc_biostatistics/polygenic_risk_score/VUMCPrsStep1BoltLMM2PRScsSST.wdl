version 1.0

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
# 3. Optionally map rsIDs to variant IDs using provided mapping file (ID,RSID columns)
# 4. Optionally copy results to a GCP storage location
#
# Inputs:
# - input_boltLMM: BoltLMM output file containing summary statistics
# - rsid_variantid_map_file: Optional CSV file mapping variant IDs to rsIDs (ID,RSID columns)
# - output_prefix: Prefix for the output SST file
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_sst_file: Path to the formatted PRScs-SST summary statistics file

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "./PRSUtils.wdl" as PRSUtils

workflow VUMCPrsStep1BoltLMM2PRScsSST {
  input {
    File input_boltLMM
    Boolean perform_rsid_to_variantid = true
    File? rsid_variantid_map_file # ID,RSID map file

    String output_prefix

    String? target_gcp_folder
  }

  # Validate: at least one of vep_cache_folder or vep_cache_tar_gz must be provided
  if (perform_rsid_to_variantid && !defined(rsid_variantid_map_file)) {
    call WDLUtils.FailWithMessage as Validate_rsid_variantid_map_file {
      input:
        message = "rsid_variantid_map_file must be provided when perform_rsid_to_variantid=true."
    }
  }

  call BoltLMM2PRScsSST {
    input:
      input_boltLMM = input_boltLMM,
      output_prefix = output_prefix
  }

  if (perform_rsid_to_variantid) {
    call PRSUtils.rsID2variantID {
      input:
        input_sst = BoltLMM2PRScsSST.output_sst_file,
        input_bim = BoltLMM2PRScsSST.output_bim_file_for_PRScs,
        rsid_variantid_map_file = select_first([rsid_variantid_map_file]),
        output_prefix = output_prefix
    }
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = select_first([rsID2variantID.output_sst_file, BoltLMM2PRScsSST.output_sst_file]),
        source_file2 = select_first([rsID2variantID.output_bim_file_for_PRScs, BoltLMM2PRScsSST.output_bim_file_for_PRScs]),
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_sst_file = select_first([CopyFile.output_file1, rsID2variantID.output_sst_file, BoltLMM2PRScsSST.output_sst_file])
    File output_bim_file_for_PRScs = select_first([CopyFile.output_file2, rsID2variantID.output_bim_file_for_PRScs, BoltLMM2PRScsSST.output_bim_file_for_PRScs])
  }
}

task BoltLMM2PRScsSST {
  input {
    File input_boltLMM

    String output_prefix

    Int preemptible=3
    Int memory_gb = 5
    Int additional_disk_size_gb = 2
  }

  Int disk_size = ceil(size([input_boltLMM], "GB") * 3) + additional_disk_size_gb

  command <<<

  # 2:SNP:rsid, 8:A1, 9:AX, 17:OR, 22:P

  awk 'BEGIN {OFS="\t"}; NR==1 {print "SNP", "A1", "A2", "BETA", "P"; next}; {print $2, $8, $9, log($17), $22}' ~{input_boltLMM} > ~{output_prefix}.sst

  awk 'BEGIN {OFS="\t"}; NR==1 {next}; {chr=$3; if (chr=="X") chr=23; else if (chr=="Y") chr=24; else if (chr=="MT" || chr=="M") chr=25; print chr, $2, 0, $4, $8, $9}' ~{input_boltLMM} > ~{output_prefix}.bim

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

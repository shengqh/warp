version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow converts Regenie output to PRScs-SST format for polygenic risk score calculation.
# Developed by VUMC Biostatistics for population-specific GWAS studies.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Transform Regenie output file into PRScs-SST compatible summary statistics format
# 2. Format columns to match required PRScs-SST input format (SNP, A1, A2, BETA, P)
#    SNP: rsID from ID column
#    A1: Effect allele from ALLELE1 column
#    A2: Non-effect allele from ALLELE0 column
#    BETA: Effect size estimate from the BETA column
#    P: P-value converted from the LOG10P column
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

cat <<CODE> convert.py
import csv
import gzip

input_file = "~{input_regenie}"
output_sst = "~{output_prefix}.sst"
output_bim = "~{output_prefix}.bim"

def open_maybe_gzip(path):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path, "rt")

chr_map = {"X": "23", "Y": "24", "MT": "25", "M": "25"}

with open_maybe_gzip(input_file) as fin, \
     open(output_sst, "w") as sst_out, \
     open(output_bim, "w") as bim_out:

    reader = csv.DictReader(fin, delimiter=' ')
    required = ["ID", "ALLELE1", "ALLELE0", "BETA", "LOG10P", "CHROM", "GENPOS"]
    missing = [c for c in required if c not in (reader.fieldnames or [])]
    if missing:
        raise ValueError("Missing required columns: " + ",".join(missing))

    sst_out.write("SNP\tA1\tA2\tBETA\tP\n")

    for row in reader:
        snpid = row["ID"]
        a1 = row["ALLELE1"]
        a2 = row["ALLELE0"]
        beta = row["BETA"]
        pval = 10**(-float(row["LOG10P"]))
        chr_val = chr_map.get(row["CHROM"], row["CHROM"])
        pos = row["GENPOS"]

        sst_out.write(f"{snpid}\t{a1}\t{a2}\t{beta}\t{pval}\n")
        bim_out.write(f"{chr_val}\t{snpid}\t0\t{pos}\t{a1}\t{a2}\n")

CODE

python3 convert.py

  >>>

  runtime {
    docker: "python:3.9-slim"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    File output_sst_file = "~{output_prefix}.sst"
    File output_bim_file_for_PRScs = "~{output_prefix}.bim"
  }
}
